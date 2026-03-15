import json
import math
import os
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.domain_extractor.market_annotations import build_and_save_market_annotations
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged
from rule_baseline.datasets.artifacts import write_json
from rule_baseline.datasets.snapshot_batches import (
    load_processed_market_ids,
    rebuild_canonical_snapshot_audit,
    rebuild_canonical_snapshot_quarantine,
    rebuild_canonical_snapshots,
    reset_snapshot_batches,
    write_snapshot_batch,
)

PRICES_URL = "https://clob.polymarket.com/prices-history"
PARTIAL_SNAPSHOTS_PATH = config.PROCESSED_DIR / "snapshots.partial.csv"
PROGRESS_PATH = config.PROCESSED_DIR / "snapshots.progress.csv"
QUARANTINE_PARTIAL_PATH = config.PROCESSED_DIR / "snapshots_quarantine.partial.csv"
FLUSH_MARKET_INTERVAL = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build append-only snapshot batches from merged raw markets.")
    parser.add_argument("--full-refresh", action="store_true", help="Delete existing snapshot batches and rebuild from all merged raw markets.")
    return parser.parse_args()


def get_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    return session


def _parse_json_list(value: Any) -> list[Any]:
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


def _extract_source_host(row: dict[str, Any]) -> str:
    raw_source = str(row.get("resolutionSource") or row.get("source_url") or "").strip()
    if not raw_source:
        return "UNKNOWN"
    parsed = urlparse(raw_source)
    return (parsed.netloc or raw_source).lower()


def fetch_history_batch(session, token_id, end_ts):
    start_ts = int(end_ts - (24.1 * 3600))
    end_ts = int(end_ts)
    try:
        params = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 1}
        response = session.get(PRICES_URL, params=params, timeout=10)
        if response.status_code == 200:
            return response.json().get("history", [])
    except Exception:
        pass
    return []


def find_prices_batch(timestamps, prices, target_ts_list, window_sec):
    import bisect

    if not timestamps:
        return [
            {
                "price": None,
                "selected_ts": None,
                "point_side": None,
                "offset_sec": None,
                "points_in_window": 0,
                "left_gap_sec": None,
                "right_gap_sec": None,
                "local_gap_sec": None,
                "stale_quote_flag": True,
            }
            for _ in target_ts_list
        ]

    results = []
    length = len(timestamps)
    for target_ts in target_ts_list:
        idx = bisect.bisect_left(timestamps, target_ts)
        left_window = bisect.bisect_left(timestamps, target_ts - window_sec)
        right_window = bisect.bisect_right(timestamps, target_ts + window_sec)
        points_in_window = max(right_window - left_window, 0)
        best_price = None
        best_ts = None
        point_side = None
        offset_sec = None
        local_gap_sec = None
        min_diff = float("inf")
        if idx > 0:
            ts_left = timestamps[idx - 1]
            diff = abs(ts_left - target_ts)
            if diff <= window_sec:
                min_diff = diff
                best_price = prices[idx - 1]
                best_ts = ts_left
                point_side = "left"
                offset_sec = diff
        if idx < length:
            ts_right = timestamps[idx]
            diff = abs(ts_right - target_ts)
            if diff <= window_sec and diff < min_diff:
                best_price = prices[idx]
                best_ts = ts_right
                point_side = "right"
                offset_sec = diff

        if best_ts is not None:
            best_idx = bisect.bisect_left(timestamps, best_ts)
            candidate_gaps = []
            if best_idx > 0:
                candidate_gaps.append(best_ts - timestamps[best_idx - 1])
            if best_idx + 1 < length:
                candidate_gaps.append(timestamps[best_idx + 1] - best_ts)
            local_gap_sec = min(candidate_gaps) if candidate_gaps else None

        left_gap_sec = target_ts - timestamps[idx - 1] if idx > 0 else None
        right_gap_sec = timestamps[idx] - target_ts if idx < length else None
        stale_quote_flag = (
            best_price is None
            or (offset_sec is not None and offset_sec > config.STALE_QUOTE_MAX_OFFSET_SEC)
            or (local_gap_sec is not None and local_gap_sec > config.STALE_QUOTE_MAX_GAP_SEC)
        )
        results.append(
            {
                "price": best_price,
                "selected_ts": best_ts,
                "point_side": point_side,
                "offset_sec": offset_sec,
                "points_in_window": points_in_window,
                "left_gap_sec": left_gap_sec,
                "right_gap_sec": right_gap_sec,
                "local_gap_sec": local_gap_sec,
                "stale_quote_flag": stale_quote_flag,
            }
        )
    return results


def parse_market_json(row):
    try:
        clob_ids = _parse_json_list(row.get("clobTokenIds"))
        outcomes = _parse_json_list(row.get("outcomes"))
        final_prices = [float(value) for value in _parse_json_list(row.get("outcomePrices"))]
        if len(clob_ids) != 2 or len(outcomes) != 2 or len(final_prices) != 2:
            return None, None, None, "invalid_market_structure"
        token_meta = {
            "primary_token_id": str(row.get("primary_token_id") or clob_ids[0]),
            "secondary_token_id": str(row.get("secondary_token_id") or clob_ids[1]),
            "primary_outcome": str(row.get("primary_outcome") or outcomes[0]),
            "secondary_outcome": str(row.get("secondary_outcome") or outcomes[1]),
        }
        return clob_ids, final_prices, token_meta, None
    except Exception:
        return None, None, None, "json_parse_error"


def validate_market_dates(row):
    try:
        t_start = row.get("startDate")
        t_sched = row.get("endDate")
        t_res = row.get("closedTime")

        if pd.isna(t_res):
            return None, None, None, "no_closed_time"

        if pd.notna(t_start) and hasattr(t_start, "to_pydatetime"):
            t_start = t_start.to_pydatetime()
        elif pd.isna(t_start):
            t_start = None
        if pd.notna(t_sched) and hasattr(t_sched, "to_pydatetime"):
            t_sched = t_sched.to_pydatetime()
        if hasattr(t_res, "to_pydatetime"):
            t_res = t_res.to_pydatetime()

        delta_hours = abs((t_res - t_sched).total_seconds()) / 3600.0 if pd.notna(t_sched) else math.nan
        return t_start, t_sched, t_res, delta_hours
    except Exception as exc:
        return None, None, None, f"date_validation_error_{exc}"


def determine_outcome(final_prices):
    try:
        winner_candidates = [index for index, value in enumerate(final_prices) if float(value) > 0.9]
        if len(winner_candidates) != 1:
            return None, None
        winner_index = winner_candidates[0]
        return (1 if winner_index == 0 else 0), winner_index
    except Exception:
        return None, None


def generate_snapshots(row, token_meta, winner_index, t_start, t_sched, t_res, delta_hours, y_ref, history):
    snapshots = []
    audit_rows = []
    ts_closed_unix = int(t_res.timestamp())
    ts_start_unix = int(t_start.timestamp()) if t_start is not None else None

    timestamps = [item["t"] for item in history]
    prices = [float(item["p"]) for item in history]
    target_times = [ts_closed_unix - (hours * 3600) for hours in config.HORIZONS]
    found_quotes = find_prices_batch(timestamps, prices, target_times, config.SNAP_WINDOW_SEC)

    market_id = str(row.get("market_id") or row.get("id"))
    category = row.get("category", "UNKNOWN")
    if pd.isna(category):
        category = "UNKNOWN"
    source_host = _extract_source_host(row)
    winning_outcome_label = token_meta["primary_outcome"] if winner_index == 0 else token_meta["secondary_outcome"]

    for horizon, quote_meta, target_time in zip(config.HORIZONS, found_quotes, target_times):
        horizon_eligible = True if ts_start_unix is None else target_time >= ts_start_unix
        price_raw = quote_meta["price"]
        audit_row = {
            "market_id": market_id,
            "source_host": source_host,
            "category": category,
            "horizon_hours": horizon,
            "horizon_eligible": bool(horizon_eligible),
            "snapshot_found": bool(horizon_eligible and price_raw is not None),
            "stale_quote_flag": bool(quote_meta["stale_quote_flag"]) if horizon_eligible else False,
            "offset_sec": quote_meta["offset_sec"],
            "points_in_window": quote_meta["points_in_window"],
        }
        audit_rows.append(audit_row)
        if not horizon_eligible or price_raw is None:
            continue

        price_prob = float(price_raw)
        denom = math.sqrt(max(price_prob * (1 - price_prob), config.EPSILON))
        r_std = (y_ref - price_prob) / denom
        snapshots.append(
            {
                "market_id": market_id,
                "category": category,
                "horizon_hours": horizon,
                "price": price_prob,
                "y": y_ref,
                "r_std": r_std,
                "scheduled_end": t_sched.isoformat() if pd.notna(t_sched) else None,
                "closedTime": t_res.isoformat(),
                "delta_hours": delta_hours,
                "source_host": source_host,
                "primary_token_id": token_meta["primary_token_id"],
                "secondary_token_id": token_meta["secondary_token_id"],
                "primary_outcome": token_meta["primary_outcome"],
                "secondary_outcome": token_meta["secondary_outcome"],
                "winning_outcome_index": winner_index,
                "winning_outcome_label": winning_outcome_label,
                "snapshot_target_ts": target_time,
                "selected_quote_ts": quote_meta["selected_ts"],
                "selected_quote_side": quote_meta["point_side"],
                "selected_quote_offset_sec": quote_meta["offset_sec"],
                "selected_quote_points_in_window": quote_meta["points_in_window"],
                "selected_quote_left_gap_sec": quote_meta["left_gap_sec"],
                "selected_quote_right_gap_sec": quote_meta["right_gap_sec"],
                "selected_quote_local_gap_sec": quote_meta["local_gap_sec"],
                "stale_quote_flag": bool(quote_meta["stale_quote_flag"]),
            }
        )
    return snapshots, audit_rows


def process_market(row, session):
    market_id = str(row.get("market_id") or row.get("id"))
    category = row.get("category", "UNKNOWN")
    source_host = _extract_source_host(row)
    clob_ids, final_prices, token_meta, parse_reason = parse_market_json(row)
    if clob_ids is None:
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": parse_reason or "json_parse_error",
            "snapshots": [],
            "audit_rows": [],
        }

    t_start, t_sched, t_res, delta_hours = validate_market_dates(row)
    if t_res is None:
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": delta_hours,
            "snapshots": [],
            "audit_rows": [],
        }

    y_ref, winner_index = determine_outcome(final_prices)
    if y_ref is None:
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": "outcome_parse_error",
            "snapshots": [],
            "audit_rows": [],
        }

    if t_start is not None:
        duration_hours = (t_res - t_start).total_seconds() / 3600.0
    else:
        duration_hours = None
    if duration_hours is not None and duration_hours < min(config.HORIZONS):
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": "market_too_short",
            "snapshots": [],
            "audit_rows": [],
        }

    token_id = token_meta["primary_token_id"]
    history = fetch_history_batch(session, token_id, int(t_res.timestamp()))
    if not history:
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": "no_history",
            "snapshots": [],
            "audit_rows": [],
        }

    snapshots, audit_rows = generate_snapshots(
        row=row,
        token_meta=token_meta,
        winner_index=winner_index,
        t_start=t_start,
        t_sched=t_sched,
        t_res=t_res,
        delta_hours=delta_hours,
        y_ref=y_ref,
        history=history,
    )
    if not snapshots:
        return {
            "market_id": market_id,
            "category": category,
            "source_host": source_host,
            "status": "no_snapshots_in_window",
            "snapshots": [],
            "audit_rows": audit_rows,
        }

    return {
        "market_id": market_id,
        "category": category,
        "source_host": source_host,
        "status": "success",
        "snapshots": snapshots,
        "audit_rows": audit_rows,
    }


def flush_buffers(
    snapshot_buffer,
    progress_buffer,
    quarantine_buffer,
    partial_path: Path,
    progress_path: Path,
    quarantine_path: Path,
) -> tuple[int, int, int]:
    snapshots_written = 0
    progress_written = 0
    quarantine_written = 0

    if snapshot_buffer:
        pd.DataFrame(snapshot_buffer).to_csv(
            partial_path,
            mode="a",
            header=not partial_path.exists(),
            index=False,
        )
        snapshots_written = len(snapshot_buffer)
        snapshot_buffer.clear()

    if progress_buffer:
        pd.DataFrame(progress_buffer).to_csv(
            progress_path,
            mode="a",
            header=not progress_path.exists(),
            index=False,
        )
        progress_written = len(progress_buffer)
        progress_buffer.clear()

    if quarantine_buffer:
        pd.DataFrame(quarantine_buffer).to_csv(
            quarantine_path,
            mode="a",
            header=not quarantine_path.exists(),
            index=False,
        )
        quarantine_written = len(quarantine_buffer)
        quarantine_buffer.clear()

    return snapshots_written, progress_written, quarantine_written


def build_market_audit_row(result: dict[str, Any]) -> dict[str, Any]:
    row = {
        "market_id": result["market_id"],
        "category": result["category"],
        "source_host": result["source_host"],
        "status": result["status"],
        "snapshots_generated": int(len(result["snapshots"])),
        "stale_snapshots": int(sum(1 for item in result["audit_rows"] if item.get("snapshot_found") and item.get("stale_quote_flag"))),
    }
    for horizon in config.HORIZONS:
        matching = [item for item in result["audit_rows"] if int(item["horizon_hours"]) == int(horizon)]
        eligible = bool(matching and matching[0].get("horizon_eligible"))
        found = bool(matching and matching[0].get("snapshot_found"))
        stale = bool(matching and matching[0].get("stale_quote_flag"))
        row[f"eligible_{horizon}h"] = int(eligible)
        row[f"hit_{horizon}h"] = int(found)
        row[f"stale_{horizon}h"] = int(stale)
        row[f"offset_{horizon}h_sec"] = matching[0].get("offset_sec") if matching else None
        row[f"points_{horizon}h_window"] = matching[0].get("points_in_window") if matching else 0
    return row


def write_snapshot_reports(audit_path: Path, quarantine_path: Path, stats: dict[str, int], total_snapshots: int) -> None:
    if audit_path.exists():
        audit_df = pd.read_csv(audit_path)
    else:
        audit_df = pd.DataFrame(columns=["market_id", "status"])

    if quarantine_path.exists():
        quarantine_df = pd.read_csv(quarantine_path)
    else:
        quarantine_df = pd.DataFrame(columns=["market_id", "reject_reason"])

    hit_rows = []
    missing_rows = []
    if not audit_df.empty:
        for horizon in config.HORIZONS:
            eligible_col = f"eligible_{horizon}h"
            hit_col = f"hit_{horizon}h"
            stale_col = f"stale_{horizon}h"
            if eligible_col not in audit_df.columns:
                continue

            eligible_total = int(audit_df[eligible_col].sum())
            hit_total = int(audit_df[hit_col].sum()) if hit_col in audit_df.columns else 0
            stale_total = int(audit_df[stale_col].sum()) if stale_col in audit_df.columns else 0
            hit_rows.append(
                {
                    "horizon_hours": horizon,
                    "eligible_markets": eligible_total,
                    "hit_markets": hit_total,
                    "missing_markets": max(eligible_total - hit_total, 0),
                    "hit_rate": float(hit_total / eligible_total) if eligible_total else 0.0,
                    "stale_quote_rate": float(stale_total / hit_total) if hit_total else 0.0,
                }
            )

            if "source_host" in audit_df.columns:
                grouped = (
                    audit_df.groupby("source_host", observed=False)
                    .agg(
                        eligible_markets=(eligible_col, "sum"),
                        hit_markets=(hit_col, "sum"),
                        stale_markets=(stale_col, "sum"),
                    )
                    .reset_index()
                )
                grouped["horizon_hours"] = horizon
                grouped["missing_markets"] = grouped["eligible_markets"] - grouped["hit_markets"]
                grouped["hit_rate"] = grouped["hit_markets"] / grouped["eligible_markets"].replace(0, pd.NA)
                grouped["stale_quote_rate"] = grouped["stale_markets"] / grouped["hit_markets"].replace(0, pd.NA)
                missing_rows.append(grouped)

    hit_df = pd.DataFrame(hit_rows)
    missing_df = pd.concat(missing_rows, ignore_index=True) if missing_rows else pd.DataFrame(
        columns=["source_host", "eligible_markets", "hit_markets", "stale_markets", "horizon_hours", "missing_markets", "hit_rate", "stale_quote_rate"]
    )
    hit_df.to_csv(config.SNAPSHOT_HIT_RATE_PATH, index=False)
    missing_df.to_csv(config.SNAPSHOT_MISSINGNESS_PATH, index=False)

    summary = {
        "total_markets": int(len(audit_df)),
        "status_counts": audit_df["status"].value_counts().to_dict() if "status" in audit_df.columns and not audit_df.empty else {},
        "total_snapshots": int(total_snapshots),
        "quarantine_markets": int(quarantine_df["market_id"].nunique()) if not quarantine_df.empty and "market_id" in quarantine_df.columns else 0,
        "rejection_stats": {key: int(value) for key, value in sorted(stats.items(), key=lambda item: (-item[1], item[0]))},
        "horizon_hit_rates": hit_df.to_dict("records"),
    }
    write_json(config.SNAPSHOT_BUILD_SUMMARY_PATH, summary)


def main():
    args = parse_args()
    if not config.RAW_MERGED_PATH.exists():
        rebuild_canonical_merged()

    if not config.RAW_MERGED_PATH.exists():
        print("[ERROR] Canonical merged raw markets not found. Run fetch_raw_events.py first.")
        return

    if args.full_refresh:
        reset_snapshot_batches()
        print("[INFO] Running snapshot full refresh from merged raw markets.")

    for temp_path in [PARTIAL_SNAPSHOTS_PATH, PROGRESS_PATH, QUARANTINE_PARTIAL_PATH]:
        if temp_path.exists():
            temp_path.unlink()

    print("[INFO] Refreshing market annotations before snapshot build...")
    build_and_save_market_annotations()

    print(f"[INFO] Processing markets from {config.RAW_MERGED_PATH}...")

    processed_market_ids = set() if args.full_refresh else load_processed_market_ids()
    if processed_market_ids:
        print(f"[INFO] Loaded {len(processed_market_ids)} processed market_ids from existing snapshot batches.")
    stats = {}
    session = get_session()
    snapshot_buffer = []
    progress_buffer = []
    quarantine_buffer = []

    date_cols = ["startDate", "endDate", "closedTime"]
    chunk_iter = pd.read_csv(config.RAW_MERGED_PATH, parse_dates=date_cols, chunksize=2000)

    min_horizon = min(config.HORIZONS)
    total_processed = 0
    total_snapshots = 0

    for index, chunk in enumerate(chunk_iter, start=1):
        for column in date_cols:
            if column in chunk.columns:
                chunk[column] = pd.to_datetime(chunk[column], utc=True, errors="coerce")

        required_date_cols = [column for column in ["closedTime"] if column in chunk.columns]
        chunk = chunk.dropna(subset=required_date_cols)
        if chunk.empty:
            continue

        duration_mask = chunk["startDate"].isna() | (
            (chunk["closedTime"] - chunk["startDate"]).dt.total_seconds() >= (min_horizon * 3600)
        )
        valid_chunk = chunk[duration_mask].copy()

        ignored_count = len(chunk) - len(valid_chunk)
        if ignored_count > 0:
            stats["pre_filtered_short"] = stats.get("pre_filtered_short", 0) + ignored_count
        if valid_chunk.empty:
            continue

        valid_chunk["market_id"] = valid_chunk["market_id"].astype(str)
        if processed_market_ids:
            valid_chunk = valid_chunk[~valid_chunk["market_id"].isin(processed_market_ids)].copy()
        if valid_chunk.empty:
            print(f"[INFO] Skipping chunk {index}; all markets already processed.")
            continue

        num_items = len(valid_chunk)
        max_workers = min(32, max(4, num_items // 10))
        print(f"[INFO] Processing chunk {index} ({num_items} markets). Workers: {max_workers}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            market_rows = valid_chunk.to_dict("records")
            futures = {executor.submit(process_market, row, session): row for row in market_rows}
            for future in as_completed(futures):
                row = futures[future]
                market_id = str(row.get("market_id") or row.get("id"))
                result = future.result()
                status = result["status"]
                if result["snapshots"]:
                    snapshot_buffer.extend(result["snapshots"])
                if status != "success":
                    stats[status] = stats.get(status, 0) + 1
                    quarantine_buffer.append(
                        {
                            "market_id": result["market_id"],
                            "category": result["category"],
                            "source_host": result["source_host"],
                            "reject_reason": status,
                        }
                    )
                progress_buffer.append(build_market_audit_row(result))
                processed_market_ids.add(market_id)
                total_processed += 1
                if len(progress_buffer) >= FLUSH_MARKET_INTERVAL:
                    written_snapshots, written_progress, _ = flush_buffers(
                        snapshot_buffer,
                        progress_buffer,
                        quarantine_buffer,
                        PARTIAL_SNAPSHOTS_PATH,
                        PROGRESS_PATH,
                        QUARANTINE_PARTIAL_PATH,
                    )
                    total_snapshots += written_snapshots
                    if written_progress:
                        print(
                            f"\r[INFO] Processed {total_processed} markets. Persisted snapshots: {total_snapshots}",
                            end="",
                        )
                elif total_processed % 100 == 0:
                    print(
                        f"\r[INFO] Processed {total_processed} markets. Buffered snapshots: {total_snapshots + len(snapshot_buffer)}",
                        end="",
                    )

    written_snapshots, _, _ = flush_buffers(
        snapshot_buffer,
        progress_buffer,
        quarantine_buffer,
        PARTIAL_SNAPSHOTS_PATH,
        PROGRESS_PATH,
        QUARANTINE_PARTIAL_PATH,
    )
    total_snapshots += written_snapshots

    print(f"\n[INFO] Done. Persisted {total_snapshots} snapshots.")
    print("[INFO] Rejection stats:")
    for reason, count in sorted(stats.items(), key=lambda item: -item[1]):
        print(f"  {reason}: {count}")

    if PARTIAL_SNAPSHOTS_PATH.exists() or PROGRESS_PATH.exists() or QUARANTINE_PARTIAL_PATH.exists():
        snapshot_batch_df = pd.read_csv(PARTIAL_SNAPSHOTS_PATH, low_memory=False) if PARTIAL_SNAPSHOTS_PATH.exists() else pd.DataFrame()
        audit_batch_df = pd.read_csv(PROGRESS_PATH, low_memory=False) if PROGRESS_PATH.exists() else pd.DataFrame()
        quarantine_batch_df = pd.read_csv(QUARANTINE_PARTIAL_PATH, low_memory=False) if QUARANTINE_PARTIAL_PATH.exists() else pd.DataFrame()

        if not snapshot_batch_df.empty and "closedTime" in snapshot_batch_df.columns:
            closed_times = pd.to_datetime(snapshot_batch_df["closedTime"], utc=True, errors="coerce")
            window_start = closed_times.min().to_pydatetime() if closed_times.notna().any() else config.current_utc()
            window_end = closed_times.max().to_pydatetime() if closed_times.notna().any() else config.current_utc()
        else:
            window_start = config.current_utc()
            window_end = window_start

        batch_id = write_snapshot_batch(
            snapshots=snapshot_batch_df,
            audit=audit_batch_df,
            quarantine=quarantine_batch_df,
            window_start=window_start,
            window_end=window_end,
        )
        print(f"[INFO] Saved append-only snapshot batch to {config.SNAPSHOT_BATCHES_DIR / f'{batch_id}.csv'}")

        for temp_path in [PARTIAL_SNAPSHOTS_PATH, PROGRESS_PATH, QUARANTINE_PARTIAL_PATH]:
            if temp_path.exists():
                temp_path.unlink()

    merged_snapshots = rebuild_canonical_snapshots()
    merged_audit = rebuild_canonical_snapshot_audit()
    rebuild_canonical_snapshot_quarantine()
    write_snapshot_reports(config.SNAPSHOT_MARKET_AUDIT_PATH, config.SNAPSHOT_QUARANTINE_PATH, stats, len(merged_snapshots))
    print(f"[INFO] Rebuilt canonical snapshots at {config.SNAPSHOTS_PATH} ({len(merged_snapshots)} rows)")
    print(f"[INFO] Rebuilt canonical snapshot audit at {config.SNAPSHOT_MARKET_AUDIT_PATH} ({len(merged_audit)} rows)")
    print(f"[INFO] Rebuilt canonical snapshot quarantine at {config.SNAPSHOT_QUARANTINE_PATH}")
    print(f"[INFO] Saved snapshot build summary to {config.SNAPSHOT_BUILD_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
