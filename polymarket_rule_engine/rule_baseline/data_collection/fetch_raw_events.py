import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.raw_batches import (
    infer_latest_closed_time,
    rebuild_canonical_merged,
    write_batch,
)

BASE_URL = "https://gamma-api.polymarket.com/events"


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


def resolve_category(tags_list):
    if not tags_list:
        return "MISC"

    labels = [tag.get("label", "") for tag in tags_list]
    labels_upper = [label.upper() for label in labels]
    labels_lower = [label.lower() for label in labels]

    for category in config.BROAD_CATEGORIES:
        if category in labels_upper:
            return category

    for label in labels_lower:
        if label in config.TAG_MAPPING:
            return config.TAG_MAPPING[label]

    return "MISC"


def is_fifteen_minute_crypto_market(market, category):
    if category != "CRYPTO":
        return False

    slug = str(market.get("slug", "") or "").lower()
    question = str(market.get("question", market.get("title", "")) or "").lower()
    description = str(market.get("description", "") or "").lower()
    combined = f"{slug} {question} {description}"

    # Gamma samples for these markets consistently use slugs like btc-updown-15m-<timestamp>.
    if "updown-15m" in slug:
        return True

    # Fallback if Polymarket changes the slug pattern but keeps the market phrasing.
    return "up or down" in question and any(token in combined for token in ("15m", "15 minute", "15-minute"))


def process_market(market, category, window_start, window_end):
    closed_time_raw = market.get("closedTime")
    end_date_raw = market.get("endDate")

    if not closed_time_raw or len(str(closed_time_raw)) < 10:
        return None, "missing_closed_time"
    if not end_date_raw:
        return None, "missing_end_date"

    try:
        closed_time = pd.to_datetime(closed_time_raw, utc=True)
        scheduled_time = pd.to_datetime(end_date_raw, utc=True)
    except Exception as exc:
        return None, f"date_error_{exc}"

    if closed_time < window_start:
        return None, "too_old"
    if closed_time > window_end:
        return None, "too_new"

    if scheduled_time < config.history_start():
        return None, "too_old"

    delta_hours = abs((closed_time - scheduled_time).total_seconds()) / 3600.0
    if delta_hours > config.DELTA_FIXED_HOURS:
        return None, f"delta_limit_{delta_hours:.1f}"

    if market.get("umaResolutionStatus") != "resolved":
        return None, "status_not_resolved"
    if market.get("active") is False:
        return None, "inactive"
    if market.get("negRisk") is True:
        return None, "negrisk"
    if is_fifteen_minute_crypto_market(market, category):
        return None, "filtered_crypto_15m"

    tokens = market.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except Exception:
            tokens = []
    if not isinstance(tokens, list) or len(tokens) != 2:
        return None, "invalid_tokens"

    try:
        volume = float(market.get("volume", 0))
        if volume <= 50:
            return None, "low_volume"
    except Exception:
        return None, "volume_parse_error"

    prices_raw = market.get("outcomePrices", [])
    if isinstance(prices_raw, str):
        try:
            prices_raw = json.loads(prices_raw)
        except Exception:
            pass

    if isinstance(prices_raw, list) and len(prices_raw) == 2:
        try:
            p0 = float(prices_raw[0])
            p1 = float(prices_raw[1])
            if not (p0 > 0.9 or p1 > 0.9):
                return None, "ambiguous_outcome"
        except Exception:
            return None, "price_parse_error"
    else:
        return None, "invalid_prices"

    cleaned = dict(market)
    cleaned["category"] = category
    cleaned["market_id"] = str(cleaned.get("id", ""))
    cleaned["clobTokenIds"] = json.dumps(tokens)
    cleaned["outcomes"] = json.dumps(cleaned.get("outcomes", []))
    cleaned["outcomePrices"] = json.dumps(prices_raw)
    cleaned["batch_window_start"] = window_start.isoformat()
    cleaned["batch_window_end"] = window_end.isoformat()

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
                        if reason not in ("too_old", "missing_closed_time", "missing_end_date"):
                            batch_contains_window_data = True
                        if processed_market:
                            seen_market_ids.add(market_id)
                            all_markets.append(processed_market)

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
    return pd.DataFrame(all_markets)


def main():
    latest_closed_time = infer_latest_closed_time()
    window_start, window_end = config.get_fetch_window(latest_closed_time)

    df = fetch_events_and_flatten(window_start, window_end)
    if "closedTime" in df.columns:
        df["closedTime"] = pd.to_datetime(df["closedTime"], utc=True, errors="coerce")

    batch_path = write_batch(df, window_start=window_start, window_end=window_end, fetched_at=window_end)
    print(f"[INFO] Saved append-only raw batch to {batch_path}")

    merged = rebuild_canonical_merged()
    print(f"[INFO] Rebuilt canonical merged raw markets at {config.RAW_MERGED_PATH} ({len(merged)} rows)")


if __name__ == "__main__":
    main()
