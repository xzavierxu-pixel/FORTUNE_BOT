"""Order lifecycle monitoring and reconciliation for the online pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import json
import time

from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.state import refresh_state_snapshot
from execution_engine.integrations.trading.clob_client import build_clob_client
from execution_engine.integrations.trading.order_manager import reconcile, sweep_expired_orders
from execution_engine.online.execution.positions import load_open_position_rows, refresh_market_state_cache
from execution_engine.online.reporting.run_summary import publish_run_summary
from execution_engine.shared.io import (
    append_jsonl,
    list_artifact_paths_recursive,
    list_run_artifact_paths,
    read_jsonl,
    read_jsonl_many,
    write_jsonl,
)
from execution_engine.shared.time import to_iso, utc_now


def _latest_orders_by_attempt(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
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


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _build_batch_lifecycle_exports(
    cfg: PegConfig,
    latest_orders: Dict[str, Dict[str, Any]],
    fills: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
    opened_position_events: List[Dict[str, Any]],
) -> Dict[str, int]:
    fills_by_attempt: Dict[str, List[Dict[str, Any]]] = {}
    for fill in fills:
        order_attempt_id = str(fill.get("order_attempt_id", "") or "")
        if not order_attempt_id:
            continue
        fills_by_attempt.setdefault(order_attempt_id, []).append(fill)

    open_positions_by_attempt: Dict[str, Dict[str, Any]] = {}
    for row in open_positions:
        order_attempt_id = str(row.get("entry_order_attempt_id", "") or "")
        if order_attempt_id:
            open_positions_by_attempt[order_attempt_id] = row
    opened_position_events_by_attempt: Dict[str, List[Dict[str, Any]]] = {}
    for row in opened_position_events:
        order_attempt_id = str(row.get("order_attempt_id", "") or "")
        if order_attempt_id:
            opened_position_events_by_attempt.setdefault(order_attempt_id, []).append(row)

    exported_submit_dirs = 0
    exported_fill_rows = 0
    exported_cancel_rows = 0
    exported_open_position_rows = 0
    exported_open_position_event_rows = 0

    for submitted_path in list_artifact_paths_recursive(cfg.runs_root_dir, "orders_submitted.jsonl"):
        submitted_rows = read_jsonl(submitted_path)
        submit_dir = submitted_path.parent
        if not submitted_rows:
            write_jsonl(submit_dir / "fills.jsonl", [])
            write_jsonl(submit_dir / "cancels.jsonl", [])
            write_jsonl(submit_dir / "opened_positions.jsonl", [])
            continue

        submitted_by_attempt = {
            str(row.get("order_attempt_id", "") or ""): row
            for row in submitted_rows
            if str(row.get("order_attempt_id", "") or "")
        }
        batch_fills: List[Dict[str, Any]] = []
        batch_cancels: List[Dict[str, Any]] = []
        batch_open_positions: List[Dict[str, Any]] = []
        batch_open_position_events: List[Dict[str, Any]] = []

        for order_attempt_id, submitted in submitted_by_attempt.items():
            for fill in fills_by_attempt.get(order_attempt_id, []):
                batch_fills.append(
                    {
                        "run_id": str(submitted.get("run_id") or ""),
                        "batch_id": str(submitted.get("batch_id") or ""),
                        "market_id": str(submitted.get("market_id") or fill.get("market_id") or ""),
                        "token_id": str(submitted.get("token_id") or fill.get("token_id") or ""),
                        "outcome_label": str(submitted.get("outcome_label") or fill.get("outcome_label") or ""),
                        "order_attempt_id": order_attempt_id,
                        "fill_id": str(fill.get("fill_id") or ""),
                        "fill_price": _to_float(fill.get("price")),
                        "fill_amount_usdc": _to_float(fill.get("amount_usdc")),
                        "fill_shares": _to_float(fill.get("shares")),
                        "filled_at_utc": str(fill.get("filled_at_utc") or ""),
                    }
                )

            latest_order = latest_orders.get(order_attempt_id, {})
            latest_status = str(latest_order.get("status", "") or "").upper()
            if latest_status in {"CANCELED", "EXPIRED", "REJECTED", "ERROR"}:
                batch_cancels.append(
                    {
                        "run_id": str(submitted.get("run_id") or ""),
                        "batch_id": str(submitted.get("batch_id") or ""),
                        "market_id": str(submitted.get("market_id") or latest_order.get("market_id") or ""),
                        "token_id": str(submitted.get("token_id") or latest_order.get("token_id") or ""),
                        "outcome_label": str(submitted.get("outcome_label") or latest_order.get("outcome_label") or ""),
                        "order_attempt_id": order_attempt_id,
                        "terminal_status": latest_status,
                        "terminal_reason": str(latest_order.get("status_reason") or ""),
                        "submitted_at_utc": str(submitted.get("submitted_at_utc") or latest_order.get("created_at_utc") or ""),
                        "terminal_at_utc": str(latest_order.get("updated_at_utc") or latest_order.get("created_at_utc") or ""),
                    }
                )

            open_position = open_positions_by_attempt.get(order_attempt_id)
            if open_position is not None:
                batch_open_positions.append(
                    {
                        "run_id": str(submitted.get("run_id") or ""),
                        "batch_id": str(submitted.get("batch_id") or ""),
                        "market_id": str(open_position.get("market_id") or ""),
                        "token_id": str(open_position.get("token_id") or ""),
                        "outcome_label": str(open_position.get("outcome_label") or ""),
                        "entry_run_id": str(open_position.get("entry_run_id") or ""),
                        "entry_order_attempt_id": str(open_position.get("entry_order_attempt_id") or ""),
                        "entry_price": _to_float(open_position.get("entry_price")),
                        "filled_amount_usdc": _to_float(open_position.get("filled_amount_usdc")),
                        "filled_shares": _to_float(open_position.get("filled_shares")),
                        "opened_at_utc": str(open_position.get("opened_at_utc") or ""),
                        "position_status": str(open_position.get("status") or "OPEN"),
                    }
                )
            for event in opened_position_events_by_attempt.get(order_attempt_id, []):
                batch_open_position_events.append(dict(event))

        write_jsonl(submit_dir / "fills.jsonl", batch_fills)
        write_jsonl(submit_dir / "cancels.jsonl", batch_cancels)
        write_jsonl(submit_dir / "opened_positions.jsonl", batch_open_positions)
        write_jsonl(submit_dir / "opened_position_events.jsonl", batch_open_position_events)
        exported_submit_dirs += 1
        exported_fill_rows += len(batch_fills)
        exported_cancel_rows += len(batch_cancels)
        exported_open_position_rows += len(batch_open_positions)
        exported_open_position_event_rows += len(batch_open_position_events)

    return {
        "exported_submit_dirs": exported_submit_dirs,
        "exported_fill_rows": exported_fill_rows,
        "exported_cancel_rows": exported_cancel_rows,
        "exported_open_position_rows": exported_open_position_rows,
        "exported_open_position_event_rows": exported_open_position_event_rows,
    }


def _export_shared_orders_live(
    cfg: PegConfig,
    latest_orders: Dict[str, Dict[str, Any]],
    fills: List[Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
    opened_position_events: List[Dict[str, Any]],
) -> Dict[str, int]:
    latest_order_rows = sorted(
        latest_orders.values(),
        key=lambda row: str(row.get("updated_at_utc") or row.get("created_at_utc") or ""),
        reverse=True,
    )
    cancel_rows = [
        {
            "order_attempt_id": str(row.get("order_attempt_id") or ""),
            "market_id": str(row.get("market_id") or ""),
            "token_id": str(row.get("token_id") or ""),
            "outcome_label": str(row.get("outcome_label") or ""),
            "terminal_status": str(row.get("status") or ""),
            "terminal_reason": str(row.get("status_reason") or ""),
            "terminal_at_utc": str(row.get("updated_at_utc") or row.get("created_at_utc") or ""),
            "run_id": str(row.get("run_id") or ""),
        }
        for row in latest_order_rows
        if str(row.get("status", "") or "").upper() in {"CANCELED", "EXPIRED", "REJECTED", "ERROR"}
    ]
    fill_rows = sorted(
        fills,
        key=lambda row: str(row.get("filled_at_utc") or ""),
        reverse=True,
    )
    open_position_rows = sorted(
        open_positions,
        key=lambda row: str(row.get("opened_at_utc") or ""),
        reverse=True,
    )

    write_jsonl(cfg.orders_live_latest_orders_path, latest_order_rows)
    write_jsonl(cfg.orders_live_fills_path, fill_rows)
    write_jsonl(cfg.orders_live_cancels_path, cancel_rows)
    write_jsonl(cfg.orders_live_opened_positions_path, open_position_rows)
    write_jsonl(cfg.orders_live_opened_position_events_path, opened_position_events)

    return {
        "shared_latest_order_count": len(latest_order_rows),
        "shared_fill_count": len(fill_rows),
        "shared_cancel_count": len(cancel_rows),
        "shared_open_position_count": len(open_position_rows),
        "shared_opened_position_event_count": len(opened_position_events),
    }


def _build_opened_position_events(
    cfg: PegConfig,
    latest_orders: Dict[str, Dict[str, Any]],
    open_positions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    existing_events = read_jsonl(cfg.orders_live_opened_position_events_path)
    seen_market_ids = {
        str(row.get("market_id") or "")
        for row in existing_events
        if str(row.get("market_id") or "")
    }
    opened_events: List[Dict[str, Any]] = []
    for position in open_positions:
        market_id = str(position.get("market_id") or "")
        if not market_id or market_id in seen_market_ids:
            continue
        order_attempt_id = str(position.get("entry_order_attempt_id") or "")
        order = latest_orders.get(order_attempt_id, {})
        event = {
            "event_time_utc": str(position.get("opened_at_utc") or to_iso(utc_now())),
            "event_type": "OPENED_POSITION",
            "decision_id": str(order.get("decision_id") or ""),
            "order_attempt_id": order_attempt_id,
            "run_id": str(position.get("entry_run_id") or order.get("run_id") or ""),
            "market_id": market_id,
            "token_id": str(position.get("token_id") or order.get("token_id") or ""),
            "outcome_label": str(position.get("outcome_label") or order.get("outcome_label") or ""),
            "filled_amount_usdc": _to_float(position.get("filled_amount_usdc")),
            "filled_shares": _to_float(position.get("filled_shares")),
            "entry_price": _to_float(position.get("entry_price")),
            "opened_at_utc": str(position.get("opened_at_utc") or ""),
            "position_status": str(position.get("status") or "OPEN"),
            "position_side": str(order.get("position_side") or ""),
            "category": str(order.get("category") or ""),
            "domain": str(order.get("domain") or ""),
            "market_type": str(order.get("market_type") or ""),
            "rule_group_key": str(order.get("rule_group_key") or ""),
            "rule_leaf_id": str(order.get("rule_leaf_id") or ""),
        }
        append_jsonl(cfg.events_path, {"event_time_utc": event["event_time_utc"], "event_type": "OPENED_POSITION", "decision_id": event["decision_id"], "order_attempt_id": event["order_attempt_id"], "payload": event})
        opened_events.append(event)
        seen_market_ids.add(market_id)
    return existing_events + opened_events


@dataclass(frozen=True)
class OrderMonitorResult:
    run_manifest_path: Path
    sleep_sec: int
    latest_order_count: int
    open_order_count: int
    fill_count: int
    open_position_count: int
    order_status_counts: Dict[str, int]
    exported_submit_dirs: int
    exported_fill_rows: int
    exported_cancel_rows: int
    exported_open_position_rows: int
    exported_open_position_event_rows: int
    shared_latest_order_count: int
    shared_fill_count: int
    shared_cancel_count: int
    shared_open_position_count: int
    shared_opened_position_event_count: int


def monitor_order_lifecycle(
    cfg: PegConfig,
    *,
    sleep_sec: int = 0,
    publish_summary_enabled: bool = True,
) -> OrderMonitorResult:
    if sleep_sec > 0:
        time.sleep(sleep_sec)

    client = build_clob_client(cfg)
    sweep_expired_orders(cfg, client)
    reconcile(cfg, client)
    state_snapshot = refresh_state_snapshot(cfg)
    market_state = refresh_market_state_cache(cfg)
    positions = load_open_position_rows(cfg)

    latest_orders = _latest_orders_by_attempt(
        read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
    )
    fills = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "fills.jsonl"))
    opened_position_events = _build_opened_position_events(cfg, latest_orders, positions)
    export_counts = _build_batch_lifecycle_exports(cfg, latest_orders, fills, positions, opened_position_events)
    shared_export_counts = _export_shared_orders_live(cfg, latest_orders, fills, positions, opened_position_events)

    order_status_counts: Dict[str, int] = {}
    open_order_count = 0
    for row in latest_orders.values():
        status = str(row.get("status", "") or "UNKNOWN").upper()
        order_status_counts[status] = order_status_counts.get(status, 0) + 1
        if status in {"NEW", "SENT", "ACKED", "CANCEL_REQUESTED", "DRY_RUN_SUBMITTED", "PARTIALLY_FILLED"}:
            open_order_count += 1

    manifest = {
        "generated_at_utc": to_iso(utc_now()),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "sleep_sec": int(max(sleep_sec, 0)),
        "dry_run": cfg.dry_run,
        "latest_order_count": int(len(latest_orders)),
        "open_order_count": int(open_order_count),
        "fill_count": int(len(fills)),
        "open_position_count": int(len(positions)),
        "order_status_counts": dict(sorted(order_status_counts.items())),
        "open_positions_path": str(cfg.open_positions_path),
        "market_state_cache_path": str(cfg.market_state_cache_path),
        "state_snapshot_path": str(cfg.state_snapshot_path),
        "pending_market_count": int(market_state.get("pending_market_count", 0)),
        "open_market_count": int(market_state.get("open_market_count", 0)),
        "state_open_orders_count": int(state_snapshot.get("open_orders_count", 0)),
        "state_net_exposure_usdc": float(state_snapshot.get("net_exposure_usdc", 0.0)),
        **export_counts,
        **shared_export_counts,
        "orders_live_latest_orders_path": str(cfg.orders_live_latest_orders_path),
        "orders_live_fills_path": str(cfg.orders_live_fills_path),
        "orders_live_cancels_path": str(cfg.orders_live_cancels_path),
        "orders_live_opened_positions_path": str(cfg.orders_live_opened_positions_path),
        "orders_live_opened_position_events_path": str(cfg.orders_live_opened_position_events_path),
        "orders_root": str(cfg.runs_root_dir),
    }
    _write_manifest(cfg.run_monitor_manifest_path, manifest)

    if publish_summary_enabled:
        publish_run_summary(
            cfg,
            status="order_monitor_completed",
            notes={"order_monitor": manifest},
        )

    return OrderMonitorResult(
        run_manifest_path=cfg.run_monitor_manifest_path,
        sleep_sec=int(max(sleep_sec, 0)),
        latest_order_count=len(latest_orders),
        open_order_count=open_order_count,
        fill_count=len(fills),
        open_position_count=len(positions),
        order_status_counts=dict(sorted(order_status_counts.items())),
        exported_submit_dirs=int(export_counts["exported_submit_dirs"]),
        exported_fill_rows=int(export_counts["exported_fill_rows"]),
        exported_cancel_rows=int(export_counts["exported_cancel_rows"]),
        exported_open_position_rows=int(export_counts["exported_open_position_rows"]),
        exported_open_position_event_rows=int(export_counts["exported_open_position_event_rows"]),
        shared_latest_order_count=int(shared_export_counts["shared_latest_order_count"]),
        shared_fill_count=int(shared_export_counts["shared_fill_count"]),
        shared_cancel_count=int(shared_export_counts["shared_cancel_count"]),
        shared_open_position_count=int(shared_export_counts["shared_open_position_count"]),
        shared_opened_position_event_count=int(shared_export_counts["shared_opened_position_event_count"]),
    )



