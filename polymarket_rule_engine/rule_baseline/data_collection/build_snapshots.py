import json
import math
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.raw_batches import rebuild_canonical_merged

PRICES_URL = "https://clob.polymarket.com/prices-history"
PARTIAL_SNAPSHOTS_PATH = config.PROCESSED_DIR / "snapshots.partial.csv"
PROGRESS_PATH = config.PROCESSED_DIR / "snapshots.progress.csv"
FLUSH_MARKET_INTERVAL = 500


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
        return [None] * len(target_ts_list)

    results = []
    length = len(timestamps)
    for target_ts in target_ts_list:
        idx = bisect.bisect_left(timestamps, target_ts)
        best_price = None
        min_diff = float("inf")
        if idx > 0:
            ts_left = timestamps[idx - 1]
            diff = abs(ts_left - target_ts)
            if diff <= window_sec:
                min_diff = diff
                best_price = prices[idx - 1]
        if idx < length:
            ts_right = timestamps[idx]
            diff = abs(ts_right - target_ts)
            if diff <= window_sec and diff < min_diff:
                best_price = prices[idx]
        results.append(best_price)
    return results


def parse_market_json(row):
    try:
        clob_ids = json.loads(row["clobTokenIds"])
        final_prices = json.loads(row["outcomePrices"])
        return clob_ids, final_prices
    except Exception:
        return None, None


def validate_market_dates(row):
    try:
        t_start = row.get("startDate")
        t_sched = row.get("endDate")
        t_res = row.get("closedTime") or row.get("resolveTime")

        if pd.isna(t_start):
            return None, None, None, "no_start_date"
        if pd.isna(t_sched):
            return None, None, None, "no_end_date"
        if pd.isna(t_res):
            return None, None, None, "no_resolve_time"

        if hasattr(t_start, "to_pydatetime"):
            t_start = t_start.to_pydatetime()
        if hasattr(t_sched, "to_pydatetime"):
            t_sched = t_sched.to_pydatetime()
        if hasattr(t_res, "to_pydatetime"):
            t_res = t_res.to_pydatetime()

        delta_hours = abs((t_res - t_sched).total_seconds()) / 3600.0
        if delta_hours > config.DELTA_FIXED_HOURS:
            return None, None, None, f"delta_hours_exceeded_{delta_hours:.1f}"
        return t_start, t_sched, t_res, delta_hours
    except Exception as exc:
        return None, None, None, f"date_validation_error_{exc}"


def determine_outcome(final_prices):
    try:
        return 1 if float(final_prices[0]) > 0.9 else 0
    except Exception:
        return None


def generate_snapshots(row, t_start, t_sched, t_res, delta_hours, y_ref, history):
    snapshots = []
    ts_sched_unix = int(t_sched.timestamp())
    ts_start_unix = int(t_start.timestamp())

    timestamps = [item["t"] for item in history]
    prices = [float(item["p"]) for item in history]
    target_times = [ts_sched_unix - (hours * 3600) for hours in config.HORIZONS]
    found_prices = find_prices_batch(timestamps, prices, target_times, config.SNAP_WINDOW_SEC)

    market_id = str(row.get("market_id") or row.get("id"))
    category = row.get("category", "UNKNOWN")
    if pd.isna(category):
        category = "UNKNOWN"

    for horizon, price_raw, target_time in zip(config.HORIZONS, found_prices, target_times):
        if target_time < ts_start_unix:
            continue
        if price_raw is None:
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
                "scheduled_end": t_sched.isoformat(),
                "resolve_time": t_res.isoformat(),
                "delta_hours": delta_hours,
            }
        )
    return snapshots


def process_market(row, session):
    clob_ids, final_prices = parse_market_json(row)
    if clob_ids is None:
        return [], "json_parse_error"

    t_start, t_sched, t_res, delta_hours = validate_market_dates(row)
    if t_start is None:
        return [], delta_hours

    y_ref = determine_outcome(final_prices)
    if y_ref is None:
        return [], "outcome_parse_error"

    duration_hours = (t_sched - t_start).total_seconds() / 3600.0
    if duration_hours < min(config.HORIZONS):
        return [], "market_too_short"

    token_id = clob_ids[0]
    history = fetch_history_batch(session, token_id, int(t_sched.timestamp()))
    if not history:
        return [], "no_history"

    snapshots = generate_snapshots(row, t_start, t_sched, t_res, delta_hours, y_ref, history)
    if not snapshots:
        return [], "no_snapshots_in_window"

    return snapshots, "success"


def load_processed_market_ids(progress_path: Path) -> set[str]:
    if not progress_path.exists():
        return set()
    progress_df = pd.read_csv(progress_path, usecols=["market_id"], dtype={"market_id": str})
    processed_ids = set(progress_df["market_id"].dropna().astype(str))
    print(f"[INFO] Resuming snapshot build. Loaded {len(processed_ids)} processed market_ids.")
    return processed_ids


def flush_buffers(
    snapshot_buffer,
    progress_buffer,
    partial_path: Path,
    progress_path: Path,
) -> tuple[int, int]:
    snapshots_written = 0
    progress_written = 0

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

    return snapshots_written, progress_written


def main():
    if not config.RAW_MERGED_PATH.exists():
        rebuild_canonical_merged()

    if not config.RAW_MERGED_PATH.exists():
        print("[ERROR] Canonical merged raw markets not found. Run fetch_raw_events.py first.")
        return

    print(f"[INFO] Processing markets from {config.RAW_MERGED_PATH}...")

    processed_market_ids = load_processed_market_ids(PROGRESS_PATH)
    stats = {}
    session = get_session()
    snapshot_buffer = []
    progress_buffer = []

    date_cols = ["startDate", "endDate", "closedTime"]
    chunk_iter = pd.read_csv(config.RAW_MERGED_PATH, parse_dates=date_cols, chunksize=2000)

    min_horizon = min(config.HORIZONS)
    total_processed = 0
    total_snapshots = 0

    for index, chunk in enumerate(chunk_iter, start=1):
        for column in date_cols:
            if column in chunk.columns:
                chunk[column] = pd.to_datetime(chunk[column], utc=True, errors="coerce")

        chunk = chunk.dropna(subset=[column for column in date_cols if column in chunk.columns])
        if chunk.empty:
            continue

        duration_mask = (chunk["endDate"] - chunk["startDate"]).dt.total_seconds() >= (min_horizon * 3600)
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
                result, reason = future.result()
                if result:
                    snapshot_buffer.extend(result)
                if reason != "success":
                    stats[reason] = stats.get(reason, 0) + 1
                progress_buffer.append({"market_id": market_id, "status": reason})
                processed_market_ids.add(market_id)
                total_processed += 1
                if len(progress_buffer) >= FLUSH_MARKET_INTERVAL:
                    written_snapshots, written_progress = flush_buffers(
                        snapshot_buffer,
                        progress_buffer,
                        PARTIAL_SNAPSHOTS_PATH,
                        PROGRESS_PATH,
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

    written_snapshots, _ = flush_buffers(
        snapshot_buffer,
        progress_buffer,
        PARTIAL_SNAPSHOTS_PATH,
        PROGRESS_PATH,
    )
    total_snapshots += written_snapshots

    print(f"\n[INFO] Done. Persisted {total_snapshots} snapshots.")
    print("[INFO] Rejection stats:")
    for reason, count in sorted(stats.items(), key=lambda item: -item[1]):
        print(f"  {reason}: {count}")

    if PARTIAL_SNAPSHOTS_PATH.exists():
        if config.SNAPSHOTS_PATH.exists():
            config.SNAPSHOTS_PATH.unlink()
        PARTIAL_SNAPSHOTS_PATH.replace(config.SNAPSHOTS_PATH)
        if PROGRESS_PATH.exists():
            PROGRESS_PATH.unlink()
        print(f"[INFO] Saved to {config.SNAPSHOTS_PATH}")
    else:
        print("[WARN] No snapshots generated.")


if __name__ == "__main__":
    main()
