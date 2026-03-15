"""Close open positions at market settlement if exit orders never fill."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import json

import pandas as pd

from execution_engine.integrations.trading.order_manager import transition_order
from execution_engine.integrations.trading.state_machine import TERMINAL_STATES
from execution_engine.online.execution.positions import load_open_position_rows
from execution_engine.online.exits.pnl import realized_pnl_usdc
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.state import StateStore
from execution_engine.shared.io import list_run_artifact_paths, read_jsonl_many
from execution_engine.shared.logger import log_structured
from execution_engine.shared.time import to_iso, utc_now


@dataclass(frozen=True)
class SettlementCloseResult:
    settlements_path: Path
    closed_count: int
    canceled_exit_order_count: int


def _settlement_dir(cfg: PegConfig) -> Path:
    return cfg.data_dir / "exits"


def _settlements_path(cfg: PegConfig) -> Path:
    return _settlement_dir(cfg) / "settlements.jsonl"


def _write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


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


def _resolved_label_lookup(cfg: PegConfig) -> Dict[str, Dict[str, str]]:
    if not cfg.resolved_labels_path.exists():
        return {}
    try:
        frame = pd.read_csv(cfg.resolved_labels_path, dtype=str)
    except pd.errors.EmptyDataError:
        return {}
    lookup: Dict[str, Dict[str, str]] = {}
    for row in frame.to_dict(orient="records"):
        market_id = str(row.get("market_id") or "").strip()
        if market_id:
            lookup[market_id] = {key: str(value or "") for key, value in row.items()}
    return lookup


def _settlement_price(position: Dict[str, Any], resolved_label: Dict[str, str]) -> float:
    position_label = str(position.get("outcome_label") or "").strip()
    position_index = str(position.get("outcome_index") or "").strip()
    resolved_outcome_label = str(resolved_label.get("resolved_outcome_label") or "").strip()
    resolved_outcome_index = str(resolved_label.get("resolved_outcome_index") or "").strip()
    if position_label and resolved_outcome_label and position_label == resolved_outcome_label:
        return 1.0
    if position_index and resolved_outcome_index and position_index == resolved_outcome_index:
        return 1.0
    return 0.0


def settle_resolved_positions(cfg: PegConfig) -> SettlementCloseResult:
    positions = load_open_position_rows(cfg)
    resolved_by_market = _resolved_label_lookup(cfg)
    latest_orders = _latest_orders_by_attempt(read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl")))
    state = StateStore(cfg)
    settlement_rows: List[Dict[str, Any]] = []
    canceled_exit_order_count = 0

    for position in positions:
        market_id = str(position.get("market_id") or "")
        resolved_label = resolved_by_market.get(market_id)
        if resolved_label is None:
            continue

        shares = float(position.get("filled_shares", 0.0) or 0.0)
        if shares <= 0:
            continue
        settlement_price = _settlement_price(position, resolved_label)
        close_amount_usdc = round(shares * settlement_price, 6)
        open_cost_usdc = float(position.get("filled_amount_usdc", 0.0) or 0.0)
        filled_at_utc = str(resolved_label.get("resolved_closed_time_utc") or to_iso(utc_now()))
        entry_order_attempt_id = str(position.get("entry_order_attempt_id") or "")

        fill_record = {
            "fill_id": f"settlement:{market_id}:{entry_order_attempt_id}",
            "order_attempt_id": f"settlement_close:{entry_order_attempt_id}",
            "decision_id": f"settlement_close:{entry_order_attempt_id}",
            "run_id": cfg.run_id,
            "market_id": market_id,
            "outcome_index": int(position.get("outcome_index", 0) or 0),
            "action": "SELL",
            "amount_usdc": close_amount_usdc,
            "price": settlement_price,
            "shares": shares,
            "pnl_usdc": realized_pnl_usdc(open_cost_usdc, close_amount_usdc),
            "filled_at_utc": filled_at_utc,
            "category": str(position.get("category") or ""),
            "domain": str(position.get("domain") or ""),
            "position_side": "EXIT",
            "token_id": str(position.get("token_id") or ""),
            "outcome_label": str(position.get("outcome_label") or ""),
            "execution_phase": "SETTLEMENT",
            "parent_order_attempt_id": entry_order_attempt_id,
            "close_reason": "MARKET_RESOLVED",
        }
        state.record_fill(fill_record)
        state.record_event(
            {
                "event_time_utc": filled_at_utc,
                "event_type": "SETTLEMENT_CLOSE",
                "decision_id": fill_record["decision_id"],
                "order_attempt_id": fill_record["order_attempt_id"],
                "payload": fill_record,
            }
        )
        log_structured(cfg.logs_path, {"type": "settlement_close", **fill_record})
        settlement_rows.append(fill_record)

        for order in latest_orders.values():
            if str(order.get("execution_phase") or "").upper() != "EXIT":
                continue
            if str(order.get("parent_order_attempt_id") or "") != entry_order_attempt_id:
                continue
            current_status = str(order.get("status") or "").upper()
            if current_status in TERMINAL_STATES:
                continue
            cancel_requested = transition_order(order, "CANCEL_REQUESTED", "MARKET_RESOLVED")
            state.record_order(cancel_requested)
            canceled = transition_order(cancel_requested, "CANCELED", "MARKET_RESOLVED")
            state.record_order(canceled)
            canceled_exit_order_count += 1

    _write_jsonl(_settlements_path(cfg), settlement_rows)
    return SettlementCloseResult(
        settlements_path=_settlements_path(cfg),
        closed_count=len(settlement_rows),
        canceled_exit_order_count=canceled_exit_order_count,
    )
