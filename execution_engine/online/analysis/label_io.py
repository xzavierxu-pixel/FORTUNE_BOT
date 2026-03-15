"""Shared IO and coercion helpers for label analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable
import json

import pandas as pd


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_bool(value: Any) -> bool:
    if value is None or value is pd.NA:
        return False
    try:
        if pd.isna(value):
            return False
    except TypeError:
        pass
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def str_series(frame: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in frame.columns:
        return frame[column].fillna(default).astype(str)
    return pd.Series(default, index=frame.index, dtype="object")


def num_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series(float("nan"), index=frame.index, dtype=float)


def horizon_bucket(value: Any) -> str:
    hours = to_float(value, default=-1.0)
    if hours < 0:
        return "UNKNOWN"
    buckets = [
        (0.0, 1.0, "0-1h"),
        (1.0, 2.0, "1-2h"),
        (2.0, 4.0, "2-4h"),
        (4.0, 6.0, "4-6h"),
        (6.0, 12.0, "6-12h"),
        (12.0, 24.0, "12-24h"),
    ]
    for lower, upper, label in buckets:
        if lower <= hours < upper:
            return label
    return "24h+"


def derive_run_meta(runs_root_dir: Path, path: Path) -> Dict[str, str]:
    try:
        relative = path.relative_to(runs_root_dir)
        parts = relative.parts
        if len(parts) >= 2:
            return {"run_date": str(parts[0]), "run_id": str(parts[1])}
    except ValueError:
        pass
    return {"run_date": "", "run_id": ""}


def latest_by_order_attempt(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        order_attempt_id = str(row.get("order_attempt_id", "") or "")
        if not order_attempt_id:
            continue
        prior = latest.get(order_attempt_id)
        current_ts = str(row.get("updated_at_utc") or row.get("created_at_utc") or "")
        prior_ts = str((prior or {}).get("updated_at_utc") or (prior or {}).get("created_at_utc") or "")
        if prior is None or current_ts >= prior_ts:
            latest[order_attempt_id] = row
    return latest
