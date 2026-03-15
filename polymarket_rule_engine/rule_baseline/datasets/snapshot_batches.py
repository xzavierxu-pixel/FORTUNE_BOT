from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from rule_baseline.datasets.raw_market_batches import batch_id_from_timestamp, parse_batch_timestamp
from rule_baseline.utils import config

MANIFEST_COLUMNS = [
    "batch_id",
    "snapshot_batch_path",
    "audit_batch_path",
    "quarantine_batch_path",
    "fetched_at",
    "window_start",
    "window_end",
    "market_count",
    "snapshot_count",
    "closedTime_min",
    "closedTime_max",
]


def load_manifest() -> pd.DataFrame:
    if config.SNAPSHOT_BATCH_MANIFEST_PATH.exists():
        manifest = pd.read_csv(config.SNAPSHOT_BATCH_MANIFEST_PATH)
        missing = [column for column in MANIFEST_COLUMNS if column not in manifest.columns]
        for column in missing:
            manifest[column] = pd.NA
        return manifest[MANIFEST_COLUMNS].copy()
    return pd.DataFrame(columns=MANIFEST_COLUMNS)


def save_manifest(manifest: pd.DataFrame) -> None:
    config.ensure_data_dirs()
    manifest[MANIFEST_COLUMNS].to_csv(config.SNAPSHOT_BATCH_MANIFEST_PATH, index=False)


def list_snapshot_batch_files() -> list[Path]:
    config.ensure_data_dirs()
    return sorted(config.SNAPSHOT_BATCHES_DIR.glob("fetch_*.csv"))


def list_audit_batch_files() -> list[Path]:
    config.ensure_data_dirs()
    return sorted(config.SNAPSHOT_AUDIT_BATCHES_DIR.glob("fetch_*.csv"))


def list_quarantine_batch_files() -> list[Path]:
    config.ensure_data_dirs()
    return sorted(config.SNAPSHOT_QUARANTINE_BATCHES_DIR.glob("fetch_*.csv"))


def reset_snapshot_batches() -> None:
    for path in [
        config.SNAPSHOT_BATCHES_DIR,
        config.SNAPSHOT_AUDIT_BATCHES_DIR,
        config.SNAPSHOT_QUARANTINE_BATCHES_DIR,
    ]:
        if path.exists():
            shutil.rmtree(path)

    for path in [
        config.SNAPSHOT_BATCH_MANIFEST_PATH,
        config.SNAPSHOTS_PATH,
        config.SNAPSHOT_MARKET_AUDIT_PATH,
        config.SNAPSHOT_QUARANTINE_PATH,
        config.SNAPSHOT_BUILD_SUMMARY_PATH,
        config.SNAPSHOT_HIT_RATE_PATH,
        config.SNAPSHOT_MISSINGNESS_PATH,
    ]:
        if path.exists():
            path.unlink()

    config.ensure_data_dirs()


def load_processed_market_ids() -> set[str]:
    manifest = load_manifest()
    if manifest.empty:
        return set()

    processed_ids: set[str] = set()
    for batch_path_str in manifest["audit_batch_path"].dropna():
        batch_path = Path(batch_path_str)
        if not batch_path.exists():
            continue
        try:
            batch_df = pd.read_csv(batch_path, usecols=["market_id"], dtype={"market_id": str})
        except Exception:
            continue
        processed_ids.update(batch_df["market_id"].dropna().astype(str))
    return processed_ids


def write_snapshot_batch(
    snapshots: pd.DataFrame,
    audit: pd.DataFrame,
    quarantine: pd.DataFrame,
    window_start: datetime,
    window_end: datetime,
    fetched_at: datetime | None = None,
) -> str:
    fetched = fetched_at or config.current_utc()
    batch_id = batch_id_from_timestamp(fetched)
    snapshot_batch_path = config.SNAPSHOT_BATCHES_DIR / f"{batch_id}.csv"
    audit_batch_path = config.SNAPSHOT_AUDIT_BATCHES_DIR / f"{batch_id}.csv"
    quarantine_batch_path = config.SNAPSHOT_QUARANTINE_BATCHES_DIR / f"{batch_id}.csv"

    config.ensure_data_dirs()
    snapshots.to_csv(snapshot_batch_path, index=False)
    audit.to_csv(audit_batch_path, index=False)
    quarantine.to_csv(quarantine_batch_path, index=False)

    manifest = load_manifest()
    closed_time = pd.to_datetime(snapshots.get("closedTime"), utc=True, errors="coerce")
    record = {
        "batch_id": batch_id,
        "snapshot_batch_path": str(snapshot_batch_path),
        "audit_batch_path": str(audit_batch_path),
        "quarantine_batch_path": str(quarantine_batch_path),
        "fetched_at": fetched.isoformat(),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "market_count": int(audit["market_id"].nunique()) if "market_id" in audit.columns else 0,
        "snapshot_count": int(len(snapshots)),
        "closedTime_min": closed_time.min().isoformat() if closed_time.notna().any() else "",
        "closedTime_max": closed_time.max().isoformat() if closed_time.notna().any() else "",
    }
    manifest = manifest[manifest["batch_id"] != batch_id]
    manifest = pd.concat([manifest, pd.DataFrame([record])], ignore_index=True)
    manifest = manifest.sort_values("fetched_at").reset_index(drop=True)
    save_manifest(manifest)
    return batch_id


def _load_batch_frame(batch_path: Path) -> pd.DataFrame:
    try:
        frame = pd.read_csv(batch_path, low_memory=False)
    except pd.errors.EmptyDataError:
        frame = pd.DataFrame()
    batch_id = batch_path.stem
    batch_ts = parse_batch_timestamp(batch_id)
    frame["batch_id"] = batch_id
    frame["batch_fetched_at"] = batch_ts.isoformat() if batch_ts else pd.NA
    return frame


def _rebuild_merged(batch_files: list[Path], output_path: Path, dedupe_keys: list[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for batch_path in batch_files:
        try:
            frames.append(_load_batch_frame(batch_path))
        except Exception as exc:
            print(f"[WARN] Failed to load snapshot batch {batch_path}: {exc}")

    if not frames:
        empty = pd.DataFrame()
        empty.to_csv(output_path, index=False)
        return empty

    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged["batch_fetched_at_dt"] = pd.to_datetime(merged.get("batch_fetched_at"), utc=True, errors="coerce")
    sort_columns = ["batch_fetched_at_dt"]
    ascending = [True]
    if "closedTime" in merged.columns:
        merged["closedTime_dt"] = pd.to_datetime(merged["closedTime"], utc=True, errors="coerce")
        sort_columns.append("closedTime_dt")
        ascending.append(True)
    for column in ["market_id", "horizon_hours"]:
        if column in merged.columns:
            sort_columns.append(column)
            ascending.append(True)
    merged = merged.sort_values(by=sort_columns, ascending=ascending, na_position="first")
    keys = [key for key in dedupe_keys if key in merged.columns]
    if keys:
        merged = merged.drop_duplicates(subset=keys, keep="last").reset_index(drop=True)
    merged = merged.drop(columns=[column for column in ["batch_fetched_at_dt", "closedTime_dt"] if column in merged.columns])
    merged.to_csv(output_path, index=False)
    return merged


def rebuild_canonical_snapshots() -> pd.DataFrame:
    return _rebuild_merged(list_snapshot_batch_files(), config.SNAPSHOTS_PATH, ["market_id", "horizon_hours"])


def rebuild_canonical_snapshot_audit() -> pd.DataFrame:
    return _rebuild_merged(list_audit_batch_files(), config.SNAPSHOT_MARKET_AUDIT_PATH, ["market_id"])


def rebuild_canonical_snapshot_quarantine() -> pd.DataFrame:
    return _rebuild_merged(
        list_quarantine_batch_files(),
        config.SNAPSHOT_QUARANTINE_PATH,
        ["market_id", "reject_reason"],
    )
