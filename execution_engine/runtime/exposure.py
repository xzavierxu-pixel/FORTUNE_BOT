"""Shared exposure helpers for live allocation logic."""

from __future__ import annotations

from typing import Any, Dict, List

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.io import list_run_artifact_paths, read_jsonl_many


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _latest_orders_by_id(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        order_id = str(row.get("order_attempt_id", ""))
        if not order_id:
            continue
        prior = latest.get(order_id)
        prior_time = str((prior or {}).get("updated_at_utc") or (prior or {}).get("created_at_utc") or "")
        current_time = str(row.get("updated_at_utc") or row.get("created_at_utc") or "")
        if prior is None or current_time >= prior_time:
            latest[order_id] = row
    return list(latest.values())


def active_exposures(cfg: PegConfig) -> dict[str, dict[str, float]]:
    active = {
        "domain": {},
        "category": {},
        "cluster": {},
        "settlement": {},
        "side": {},
    }
    terminal_states = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "ERROR"}
    for row in _latest_orders_by_id(read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))):
        status = str(row.get("status", "")).upper()
        if status in terminal_states:
            continue
        amount = _to_float(row.get("amount_usdc"))
        if amount <= 0:
            continue
        for key, field in [
            ("domain", "domain"),
            ("category", "category"),
            ("cluster", "cluster_key"),
            ("settlement", "settlement_key"),
            ("side", "position_side"),
        ]:
            value = str(row.get(field, "") or "")
            if value:
                active[key][value] = active[key].get(value, 0.0) + amount
    return active

