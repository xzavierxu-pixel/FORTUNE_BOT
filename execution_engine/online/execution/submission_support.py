"""Shared helpers for hourly order submission."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List
import json
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.alerts import record_alert
from execution_engine.shared.logger import log_structured
from execution_engine.shared.metrics import increment_metric
from execution_engine.shared.time import to_iso, utc_now

ALERT_REASONS = {
    "CIRCUIT_OPEN",
    "BALANCE_INSUFFICIENT",
    "DAILY_LOSS_LIMIT",
    "OPEN_ORDERS_LIMIT",
}

SUBMITTED_ORDER_STATUSES = {"NEW", "ACKED", "FILLED"}

_RULE_IMPORTS_READY = False


def ensure_rule_engine_import_path(cfg: PegConfig) -> None:
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)


def load_fee_rate(cfg: PegConfig) -> float:
    global _RULE_IMPORTS_READY
    if not _RULE_IMPORTS_READY:
        ensure_rule_engine_import_path(cfg)
        _RULE_IMPORTS_READY = True
    try:
        from rule_baseline.utils import config as rule_config  # type: ignore

        return float(getattr(rule_config, "FEE_RATE", 0.001))
    except Exception:
        return 0.001


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def load_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def log_rejection(cfg: PegConfig, record: Dict[str, object]) -> None:
    state_record = dict(record)
    state_record.setdefault("created_at_utc", to_iso(utc_now()))
    log_structured(cfg.logs_path, {"type": "rejection", **state_record})
    increment_metric(cfg.metrics_path, "rejections_count", 1)
    if state_record.get("reason_code") in ALERT_REASONS:
        record_alert(cfg.alerts_path, {"type": "alert", **state_record})


def record_rejection(
    cfg: PegConfig,
    state: Any,
    rejection: Dict[str, Any],
    *,
    decision_id: str = "",
    order_attempt_id: str = "",
) -> None:
    state.record_rejection(rejection)
    state.record_event(
        {
            "event_time_utc": to_iso(utc_now()),
            "event_type": "REJECTION",
            "decision_id": decision_id,
            "order_attempt_id": order_attempt_id,
            "payload": rejection,
        }
    )
    log_rejection(cfg, rejection)


def record_decision_created(cfg: PegConfig, state: Any, decision: Dict[str, Any], order_attempt_id: str) -> None:
    state.record_decision(decision)
    state.record_event(
        {
            "event_time_utc": to_iso(utc_now()),
            "event_type": "DECISION_CREATED",
            "decision_id": decision.get("decision_id"),
            "order_attempt_id": order_attempt_id,
            "payload": decision,
        }
    )
    log_structured(cfg.logs_path, {"type": "decision", **decision})
    increment_metric(cfg.metrics_path, "decisions_count", 1)


def record_order_submitted(cfg: PegConfig, state: Any, decision: Dict[str, Any], order: Dict[str, Any]) -> None:
    state.record_order(order)
    state.record_event(
        {
            "event_time_utc": to_iso(utc_now()),
            "event_type": "ORDER_SUBMITTED",
            "decision_id": decision.get("decision_id"),
            "order_attempt_id": order.get("order_attempt_id"),
            "payload": order,
        }
    )
    log_structured(cfg.logs_path, {"type": "order", **order})
    increment_metric(cfg.metrics_path, "orders_sent", 1)


def append_attempt(attempts: List[Dict[str, Any]], status_counts: Dict[str, int], row: Dict[str, Any]) -> None:
    status = str(row.get("status") or "UNKNOWN")
    attempts.append(row)
    status_counts[status] = status_counts.get(status, 0) + 1


def submitted_count(attempts: List[Dict[str, Any]]) -> int:
    return sum(1 for row in attempts if str(row.get("status", "")).upper() in SUBMITTED_ORDER_STATUSES)


def rejection_count(attempts: List[Dict[str, Any]]) -> int:
    return sum(1 for row in attempts if str(row.get("status", "")).upper() not in SUBMITTED_ORDER_STATUSES)

