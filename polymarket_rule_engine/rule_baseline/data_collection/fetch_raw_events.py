import json
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import (
    infer_latest_closed_time,
    rebuild_canonical_merged,
    reset_raw_batches,
    write_batch,
)

BASE_URL = "https://gamma-api.polymarket.com/events"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch raw resolved markets into append-only batches.")
    parser.add_argument("--full-refresh", action="store_true", help="Delete existing raw batches and rebuild from history start.")
    parser.add_argument("--date-start", type=str, default=None, help="Optional inclusive history start in ISO-8601 format.")
    parser.add_argument("--date-end", type=str, default=None, help="Optional inclusive fetch end in ISO-8601 format.")
    return parser.parse_args()


def get_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _parse_sequence(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        parsed = float(value)
        if pd.isna(parsed):
            return default
        return parsed
    except Exception:
        return default


def resolve_category(tags_list):
    if not tags_list:
        return "UNKNOWN"

    labels = [tag.get("label", "") for tag in tags_list]
    labels_upper = [label.upper() for label in labels]
    labels_lower = [label.lower() for label in labels]

    for category in config.BROAD_CATEGORIES:
        if category in labels_upper:
            return category

    for label in labels_lower:
        if label in config.TAG_MAPPING:
            return config.TAG_MAPPING[label]

    return "UNKNOWN"


def is_short_term_crypto_market(market, category):
    if category != "CRYPTO":
        return False

    slug = str(market.get("slug", "") or "").lower()
    question = str(market.get("question", market.get("title", "")) or "").lower()
    description = str(market.get("description", "") or "").lower()
    combined = f"{slug} {question} {description}"

    # Gamma samples for these markets consistently use slugs like btc-updown-15m-<timestamp> or btc-updown-5m-<timestamp>.
    if "updown-15m" in slug or "updown-5m" in slug:
        return True

    # Fallback if Polymarket changes the slug pattern but keeps the market phrasing.
    short_term_tokens = ("15m", "15 minute", "15-minute", "5m", "5 minute", "5-minute")
    return "up or down" in question and any(token in combined for token in short_term_tokens)


def process_market(market, category, window_start, window_end):
    closed_time_raw = market.get("closedTime")
    end_date_raw = market.get("endDate")

    if not closed_time_raw or len(str(closed_time_raw)) < 10:
        return None, "missing_closed_time"

    try:
        closed_time = pd.to_datetime(closed_time_raw, utc=True)
    except Exception as exc:
        return None, f"date_error_{exc}"

    try:
        scheduled_time = pd.to_datetime(end_date_raw, utc=True) if end_date_raw else pd.NaT
    except Exception:
        scheduled_time = pd.NaT

    if closed_time < window_start:
        return None, "too_old"
    if closed_time > window_end:
        return None, "too_new"

    delta_hours = np.nan
    if pd.notna(scheduled_time):
        delta_hours = abs((closed_time - scheduled_time).total_seconds()) / 3600.0
    # if delta_hours > config.DELTA_FIXED_HOURS:
    #     return None, f"delta_limit_{delta_hours:.1f}"

    if market.get("umaResolutionStatus") != "resolved":
        return None, "status_not_resolved"
    # if market.get("active") is False:
    #     return None, "inactive"
    if market.get("negRisk") is True:
        return None, "negrisk"
    if is_short_term_crypto_market(market, category):
        return None, "filtered_crypto_short_term"

    resolution_source = str(market.get("resolutionSource", "") or "").lower()
    if resolution_source and resolution_source in config.DOMAIN_BLACKLIST:
        return None, "blacklisted_resolution_source"

    tokens = _parse_sequence(market.get("clobTokenIds"))
    outcomes = _parse_sequence(market.get("outcomes"))
    if len(tokens) != 2 or len(outcomes) != 2:
        return None, "invalid_tokens"
    if any(not str(token).strip() for token in tokens):
        return None, "empty_token_id"
    if any(not str(outcome).strip() for outcome in outcomes):
        return None, "empty_outcome_label"
    if str(outcomes[0]).strip().lower() == str(outcomes[1]).strip().lower():
        return None, "duplicate_outcome_labels"

    volume = _to_float(market.get("volume"), default=0.0)
    liquidity = _to_float(market.get("liquidity") or market.get("liquidityNum"), default=0.0)
    spread = _to_float(market.get("spread"))
    rewards_spread = _to_float(market.get("rewardsMaxSpread"))
    best_bid = _to_float(market.get("bestBid"))
    best_ask = _to_float(market.get("bestAsk"))

    # if volume is None:
    #     return None, "volume_parse_error"
    if volume < config.MIN_MARKET_VOLUME:
        return None, "low_volume"
    # if liquidity is not None and liquidity < config.MIN_MARKET_LIQUIDITY:
    #     return None, "low_liquidity"
    # if spread is not None and spread > config.MAX_MARKET_SPREAD:
    #     return None, "spread_too_wide"
    # if rewards_spread is not None and rewards_spread > config.MAX_REWARD_SPREAD:
    #     return None, "reward_spread_too_wide"
    # if best_bid is not None and best_ask is not None and best_bid > best_ask:
    #     return None, "crossed_order_book"

    prices_raw = _parse_sequence(market.get("outcomePrices"))
    if len(prices_raw) != 2:
        return None, "invalid_prices"
    try:
        final_prices = [float(prices_raw[0]), float(prices_raw[1])]
    except Exception:
        return None, "price_parse_error"

    winner_candidates = [index for index, value in enumerate(final_prices) if value > 0.9]
    if len(winner_candidates) != 1:
        return None, "ambiguous_outcome"

    losing_index = 1 - winner_candidates[0]
    if final_prices[losing_index] >= 0.1:
        return None, "ambiguous_outcome_tail"

    cleaned = dict(market)
    cleaned["category"] = category
    cleaned["market_id"] = str(cleaned.get("id", ""))
    cleaned["clobTokenIds"] = json.dumps(tokens)
    cleaned["outcomes"] = json.dumps(outcomes)
    cleaned["outcomePrices"] = json.dumps(final_prices)
    cleaned["primary_token_id"] = str(tokens[0])
    cleaned["secondary_token_id"] = str(tokens[1])
    cleaned["primary_outcome"] = str(outcomes[0])
    cleaned["secondary_outcome"] = str(outcomes[1])
    cleaned["winning_outcome_index"] = int(winner_candidates[0])
    cleaned["winning_outcome_label"] = str(outcomes[winner_candidates[0]])
    cleaned["batch_window_start"] = window_start.isoformat()
    cleaned["batch_window_end"] = window_end.isoformat()
    cleaned["delta_hours"] = delta_hours

    cleaned.pop("image", None)
    cleaned.pop("icon", None)
    for column in config.COLS_TO_DROP:
        cleaned.pop(column, None)

    return cleaned, "ok"


def fetch_events_and_flatten(window_start, window_end, limit=100, max_workers=16):
    params_base = config.DEFAULT_API_PARAMS.copy()
    params_base.update({"limit": limit})

    print(
        f"[INFO] Starting append-only fetch. "
        f"Window=({window_start.isoformat()} -> {window_end.isoformat()}], workers={max_workers}"
    )

    all_markets = []
    quarantine_records = []
    seen_market_ids = set()
    session = get_session()

    def fetch_page(offset):
        params = params_base.copy()
        params["offset"] = offset
        try:
            response = session.get(BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            return data if isinstance(data, list) else []
        except Exception as exc:
            print(f"[WARN] Offset {offset} failed: {exc}")
            return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        offset = 0
        is_exhausted = False

        while not is_exhausted:
            futures = {}
            for _ in range(max_workers):
                futures[executor.submit(fetch_page, offset)] = offset
                offset += limit

            batch_empty = True
            batch_contains_window_data = False

            for future in as_completed(futures):
                events = future.result()
                if not events:
                    continue

                batch_empty = False
                for event in events:
                    category = resolve_category(event.get("tags", []))
                    for market in event.get("markets", []):
                        market_id = str(market.get("id", ""))
                        if market_id in seen_market_ids:
                            continue

                        processed_market, reason = process_market(market, category, window_start, window_end)
                        if reason not in ("too_old", "missing_closed_time"):
                            batch_contains_window_data = True
                        if processed_market:
                            seen_market_ids.add(market_id)
                            all_markets.append(processed_market)
                        else:
                            quarantine_records.append(
                                {
                                    "market_id": market_id,
                                    "category": category,
                                    "question": market.get("question", market.get("title", "")),
                                    "resolutionSource": market.get("resolutionSource"),
                                    "closedTime": market.get("closedTime"),
                                    "endDate": market.get("endDate"),
                                    "reject_reason": reason,
                                    "batch_window_start": window_start.isoformat(),
                                    "batch_window_end": window_end.isoformat(),
                                }
                            )

            if batch_empty:
                is_exhausted = True
            elif not batch_contains_window_data:
                print(f"\n[INFO] Reached data older than the current append-only window at offset {offset}.")
                is_exhausted = True

            print(
                f"\r[INFO] Collected {len(all_markets)} valid markets for current batch... "
                f"(Offset: {offset})",
                end="",
                flush=True,
            )

    print(f"\n[INFO] Fetch complete. Batch markets: {len(all_markets)}")
    if quarantine_records:
        quarantine_df = pd.DataFrame(quarantine_records)
        quarantine_df.to_csv(config.RAW_MARKET_QUARANTINE_PATH, index=False)
        print(f"[INFO] Saved raw-market quarantine to {config.RAW_MARKET_QUARANTINE_PATH}")
    return pd.DataFrame(all_markets)


def main():
    args = parse_args()
    if args.full_refresh:
        reset_raw_batches()
        latest_closed_time = None
        print("[INFO] Running raw-market full refresh from history start.")
    else:
        latest_closed_time = infer_latest_closed_time()
    window_start, window_end = config.get_fetch_window(
        latest_closed_time,
        now=config.parse_utc_datetime(args.date_end) if args.date_end else None,
        history_start_override=args.date_start,
    )

    df = fetch_events_and_flatten(window_start, window_end)
    if "closedTime" in df.columns:
        df["closedTime"] = pd.to_datetime(df["closedTime"], utc=True, errors="coerce")

    batch_path = write_batch(df, window_start=window_start, window_end=window_end, fetched_at=window_end)
    print(f"[INFO] Saved append-only raw batch to {batch_path}")

    merged = rebuild_canonical_merged()
    print(f"[INFO] Rebuilt canonical merged raw markets at {config.RAW_MERGED_PATH} ({len(merged)} rows)")


if __name__ == "__main__":
    main()
