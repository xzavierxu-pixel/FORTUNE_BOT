"""Run-scoped execution audit helpers for submit/monitor observability."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable

from execution_engine.online.reporting.summary_io import load_json
from execution_engine.shared.io import read_jsonl


def _optional_path(cfg: Any, attr: str | None, relative: str) -> Path | None:
    value = getattr(cfg, attr, None) if attr else None
    if value:
        return Path(value)
    data_dir = getattr(cfg, "data_dir", None)
    if data_dir:
        return Path(data_dir) / relative
    return None


def _read_jsonl(path: Path | None) -> list[Dict[str, Any]]:
    if path is None:
        return []
    return read_jsonl(path)


def _read_json(path: Path | None) -> Dict[str, Any]:
    if path is None:
        return {}
    payload = load_json(path)
    return payload if isinstance(payload, dict) else {}


def _read_csv_rows(path: Path | None) -> list[Dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _count_by(rows: Iterable[Dict[str, Any]], field: str, *, default: str = "UNKNOWN") -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = str(row.get(field, "") or default).strip() or default
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[0]))


def _latest_orders_by_attempt(rows: Iterable[Dict[str, Any]]) -> list[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        attempt_id = str(row.get("order_attempt_id", "") or "")
        if not attempt_id:
            continue
        prior = latest.get(attempt_id)
        current_ts = str(row.get("updated_at_utc") or row.get("created_at_utc") or "")
        prior_ts = str((prior or {}).get("updated_at_utc") or (prior or {}).get("created_at_utc") or "")
        if prior is None or current_ts >= prior_ts:
            latest[attempt_id] = row
    return list(latest.values())


def build_run_execution_audit(cfg: Any) -> Dict[str, Any]:
    selection_path = _optional_path(cfg, "run_snapshot_selection_path", "snapshot_score/selection_decisions.csv")
    attempts_path = _optional_path(cfg, "run_submit_attempts_path", "submit_hourly/submission_attempts.csv")
    submitted_orders_path = _optional_path(cfg, "run_submit_orders_submitted_path", "submit_hourly/orders_submitted.jsonl")
    submit_cancels_path = _optional_path(cfg, None, "submit_hourly/cancels.jsonl")
    submit_fills_path = _optional_path(cfg, None, "submit_hourly/fills.jsonl")
    opened_positions_path = _optional_path(cfg, None, "submit_hourly/opened_positions.jsonl")
    opened_position_events_path = _optional_path(cfg, None, "submit_hourly/opened_position_events.jsonl")
    exits_manifest_path = _optional_path(cfg, None, "exits/manifest.json")
    exit_orders_path = _optional_path(cfg, None, "exits/orders_submitted.jsonl")
    rejections_path = _optional_path(cfg, "rejections_path", "rejections.jsonl")
    orders_path = _optional_path(cfg, "orders_path", "orders.jsonl")

    selection_rows = _read_csv_rows(selection_path)
    attempt_rows = _read_csv_rows(attempts_path)
    submitted_rows = _read_jsonl(submitted_orders_path)
    submit_cancel_rows = _read_jsonl(submit_cancels_path)
    submit_fill_rows = _read_jsonl(submit_fills_path)
    opened_position_rows = _read_jsonl(opened_positions_path)
    opened_position_event_rows = _read_jsonl(opened_position_events_path)
    exit_manifest = _read_json(exits_manifest_path)
    exit_rows = _read_jsonl(exit_orders_path)
    rejection_rows = _read_jsonl(rejections_path)
    order_rows = _read_jsonl(orders_path)
    latest_order_rows = _latest_orders_by_attempt(order_rows)

    return {
        "selection_count": len(selection_rows),
        "submit_only": {
            "attempted_count": len(attempt_rows),
            "attempt_status_counts": _count_by(attempt_rows, "status"),
            "submitted_count": len(submitted_rows),
            "submitted_order_status_counts": _count_by(submitted_rows, "order_status"),
        },
        "rejections": {
            "count": len(rejection_rows),
            "reason_counts": _count_by(rejection_rows, "reason_code"),
        },
        "monitor_only": {
            "cancel_count": len(submit_cancel_rows),
            "fill_count": len(submit_fill_rows),
            "opened_position_count": len(opened_position_rows),
            "opened_position_event_count": len(opened_position_event_rows),
            "latest_run_order_count": len(latest_order_rows),
            "latest_run_order_status_counts": _count_by(latest_order_rows, "status"),
        },
        "exit_lifecycle": {
            "candidate_count": int(exit_manifest.get("candidate_count", 0) or 0),
            "submitted_count": len(exit_rows) if exit_rows else int(exit_manifest.get("submitted_count", 0) or 0),
            "status_counts": _count_by(exit_rows, "status"),
            "settlement_close_count": int(exit_manifest.get("settlement_close_count", 0) or 0),
            "canceled_exit_order_count": int(exit_manifest.get("canceled_exit_order_count", 0) or 0),
        },
        "paths": {
            "selection_decisions_path": str(selection_path) if selection_path else "",
            "submission_attempts_path": str(attempts_path) if attempts_path else "",
            "orders_submitted_path": str(submitted_orders_path) if submitted_orders_path else "",
            "submit_cancels_path": str(submit_cancels_path) if submit_cancels_path else "",
            "submit_fills_path": str(submit_fills_path) if submit_fills_path else "",
            "opened_positions_path": str(opened_positions_path) if opened_positions_path else "",
            "opened_position_events_path": str(opened_position_events_path) if opened_position_events_path else "",
            "exit_manifest_path": str(exits_manifest_path) if exits_manifest_path else "",
            "exit_orders_submitted_path": str(exit_orders_path) if exit_orders_path else "",
            "rejections_path": str(rejections_path) if rejections_path else "",
            "orders_path": str(orders_path) if orders_path else "",
        },
    }
