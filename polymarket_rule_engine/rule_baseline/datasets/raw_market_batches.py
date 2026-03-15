from __future__ import annotations

import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from rule_baseline.utils import config

MANIFEST_COLUMNS = [
    "batch_id",
    "batch_path",
    "fetched_at",
    "window_start",
    "window_end",
    "row_count",
    "closedTime_min",
    "closedTime_max",
]

BATCH_ID_PATTERN = re.compile(r"^fetch_(\d{8}T\d{6}Z)$")


def load_manifest() -> pd.DataFrame:
    if config.RAW_BATCH_MANIFEST_PATH.exists():
        manifest = pd.read_csv(config.RAW_BATCH_MANIFEST_PATH)
        missing = [column for column in MANIFEST_COLUMNS if column not in manifest.columns]
        for column in missing:
            manifest[column] = pd.NA
        return manifest[MANIFEST_COLUMNS].copy()
    return pd.DataFrame(columns=MANIFEST_COLUMNS)


def save_manifest(manifest: pd.DataFrame) -> None:
    config.ensure_data_dirs()
    manifest[MANIFEST_COLUMNS].to_csv(config.RAW_BATCH_MANIFEST_PATH, index=False)


def reset_raw_batches() -> None:
    if config.RAW_BATCHES_DIR.exists():
        shutil.rmtree(config.RAW_BATCHES_DIR)
    for path in [config.RAW_BATCH_MANIFEST_PATH, config.RAW_MERGED_PATH, config.RAW_MARKET_QUARANTINE_PATH]:
        if path.exists():
            path.unlink()
    config.ensure_data_dirs()


def list_batch_files() -> list[Path]:
    config.ensure_data_dirs()
    return sorted(config.RAW_BATCHES_DIR.glob("fetch_*.csv"))


def batch_id_from_timestamp(fetched_at: datetime) -> str:
    return f"fetch_{fetched_at.strftime('%Y%m%dT%H%M%SZ')}"


def parse_batch_timestamp(batch_id: str) -> datetime | None:
    match = BATCH_ID_PATTERN.match(batch_id)
    if not match:
        return None
    return config.parse_utc_datetime(datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ"))


def infer_latest_closed_time() -> datetime | None:
    manifest = load_manifest()
    if not manifest.empty and manifest["closedTime_max"].notna().any():
        latest = pd.to_datetime(manifest["closedTime_max"], utc=True, errors="coerce").max()
        if pd.notna(latest):
            return latest.to_pydatetime()

    batch_files = list_batch_files()
    latest_closed_time: datetime | None = None
    for batch_path in batch_files:
        try:
            frame = pd.read_csv(batch_path, usecols=["closedTime"])
        except Exception:
            continue
        closed = pd.to_datetime(frame["closedTime"], utc=True, errors="coerce")
        if closed.notna().any():
            candidate = closed.max().to_pydatetime()
            if latest_closed_time is None or candidate > latest_closed_time:
                latest_closed_time = candidate

    if latest_closed_time is not None:
        return latest_closed_time

    if config.LEGACY_RAW_MARKETS_PATH.exists():
        legacy = pd.read_csv(config.LEGACY_RAW_MARKETS_PATH, usecols=["closedTime"])
        closed = pd.to_datetime(legacy["closedTime"], utc=True, errors="coerce")
        if closed.notna().any():
            return closed.max().to_pydatetime()

    return None


def write_batch(
    frame: pd.DataFrame,
    window_start: datetime,
    window_end: datetime,
    fetched_at: datetime | None = None,
) -> Path:
    fetched = fetched_at or config.current_utc()
    batch_id = batch_id_from_timestamp(fetched)
    batch_path = config.RAW_BATCHES_DIR / f"{batch_id}.csv"

    config.ensure_data_dirs()
    if frame.empty and len(frame.columns) == 0:
        frame = pd.DataFrame(columns=["id", "market_id", "closedTime"])
    frame.to_csv(batch_path, index=False)

    manifest = load_manifest()
    closed_time = pd.to_datetime(frame.get("closedTime"), utc=True, errors="coerce")
    record = {
        "batch_id": batch_id,
        "batch_path": str(batch_path),
        "fetched_at": fetched.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "row_count": int(len(frame)),
        "closedTime_min": closed_time.min().isoformat() if closed_time.notna().any() else "",
        "closedTime_max": closed_time.max().isoformat() if closed_time.notna().any() else "",
    }
    manifest = manifest[manifest["batch_id"] != batch_id]
    manifest = pd.concat([manifest, pd.DataFrame([record])], ignore_index=True)
    manifest = manifest.sort_values("fetched_at").reset_index(drop=True)
    save_manifest(manifest)
    return batch_path


def _load_batch_frame(batch_path: Path) -> pd.DataFrame:
    try:
        frame = pd.read_csv(batch_path, low_memory=False)
    except pd.errors.EmptyDataError:
        frame = pd.DataFrame(columns=["id", "market_id", "closedTime"])
    batch_id = batch_path.stem
    batch_ts = parse_batch_timestamp(batch_id)
    frame["batch_id"] = batch_id
    frame["batch_fetched_at"] = batch_ts.isoformat() if batch_ts else pd.NA
    return frame


def rebuild_canonical_merged() -> pd.DataFrame:
    config.ensure_data_dirs()

    batch_files = list_batch_files()
    frames: list[pd.DataFrame] = []
    for batch_path in batch_files:
        try:
            frames.append(_load_batch_frame(batch_path))
        except Exception as exc:
            print(f"[WARN] Failed to load batch {batch_path}: {exc}")

    if not frames and config.LEGACY_RAW_MARKETS_PATH.exists():
        legacy = pd.read_csv(config.LEGACY_RAW_MARKETS_PATH)
        legacy["batch_id"] = "legacy_bootstrap"
        legacy["batch_fetched_at"] = pd.NA
        frames.append(legacy)

    if not frames:
        empty = pd.DataFrame()
        empty.to_csv(config.RAW_MERGED_PATH, index=False)
        return empty

    merged = pd.concat(frames, ignore_index=True, sort=False)
    if "id" not in merged.columns:
        raise ValueError("Merged raw markets dataset is missing the 'id' column.")

    merged["id"] = merged["id"].astype(str)
    merged["closedTime_dt"] = pd.to_datetime(merged.get("closedTime"), utc=True, errors="coerce")
    merged["batch_fetched_at_dt"] = pd.to_datetime(merged.get("batch_fetched_at"), utc=True, errors="coerce")
    merged = merged.sort_values(
        by=["batch_fetched_at_dt", "closedTime_dt", "id"],
        ascending=[True, True, True],
        na_position="first",
    )
    merged = merged.drop_duplicates(subset=["id"], keep="last").reset_index(drop=True)
    merged = merged.drop(columns=["closedTime_dt", "batch_fetched_at_dt"])

    merged.to_csv(config.RAW_MERGED_PATH, index=False)
    return merged


def ensure_canonical_merged() -> pd.DataFrame:
    if config.RAW_MERGED_PATH.exists():
        return pd.read_csv(config.RAW_MERGED_PATH)
    return rebuild_canonical_merged()
