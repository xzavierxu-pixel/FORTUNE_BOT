"""Submit fixed-price exit orders after entry TTL has elapsed."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from execution_engine.integrations.trading.clob_client import ClobClient, build_clob_client
from execution_engine.integrations.trading.nonce import NonceManager
from execution_engine.integrations.trading.order_manager import submit_order
from execution_engine.online.execution.positions import load_open_position_rows
from execution_engine.online.execution.submission_support import (
    record_decision_created,
    record_order_submitted,
    write_jsonl,
    write_manifest,
)
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.decision import build_decision_from_signal
from execution_engine.runtime.models import SignalPayload
from execution_engine.runtime.state import StateStore
from execution_engine.shared.io import list_run_artifact_paths, read_jsonl_many
from execution_engine.shared.time import parse_utc, to_iso, utc_now

EXIT_LIMIT_PRICE = 0.99
EXIT_TTL_SECONDS = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class ExitSubmissionResult:
    run_manifest_path: Path
    submitted_orders_path: Path
    candidate_count: int
    submitted_count: int
    status_counts: Dict[str, int]


def _exit_dir(cfg: PegConfig) -> Path:
    return cfg.data_dir / "exits"


def _submitted_orders_path(cfg: PegConfig) -> Path:
    return _exit_dir(cfg) / "orders_submitted.jsonl"


def _manifest_path(cfg: PegConfig) -> Path:
    return _exit_dir(cfg) / "manifest.json"


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


def _ttl_elapsed(order: Dict[str, Any]) -> bool:
    created_at = str(order.get("created_at_utc") or "")
    if not created_at:
        return False
    expiration_seconds = int(order.get("expiration_seconds", 0) or 0)
    if expiration_seconds <= 0:
        return True
    try:
        created_dt = parse_utc(created_at)
    except ValueError:
        return False
    return (utc_now() - created_dt).total_seconds() >= expiration_seconds


def _build_exit_signal(entry_order: Dict[str, Any], position: Dict[str, Any]) -> SignalPayload:
    filled_shares = float(position.get("filled_shares", 0.0) or 0.0)
    entry_attempt_id = str(entry_order.get("order_attempt_id") or "")
    return {
        "source": "exit_manager",
        "source_run_id": str(entry_order.get("run_id") or ""),
        "strategy_ref_id": f"exit:{entry_attempt_id}",
        "market_id": str(entry_order.get("market_id") or ""),
        "outcome_index": int(entry_order.get("outcome_index", 0) or 0),
        "action": "SELL",
        "order_type": "LIMIT",
        "price_limit": EXIT_LIMIT_PRICE,
        "amount_usdc": round(filled_shares * EXIT_LIMIT_PRICE, 6),
        "expiration_seconds": EXIT_TTL_SECONDS,
        "decision_window_start_utc": str(entry_order.get("created_at_utc") or to_iso(utc_now())),
        "category": str(entry_order.get("category") or ""),
        "domain": str(entry_order.get("domain") or ""),
        "market_type": str(entry_order.get("market_type") or ""),
        "source_host": "exit_manager",
        "position_side": str(entry_order.get("position_side") or ""),
        "rule_group_key": str(entry_order.get("rule_group_key") or ""),
        "rule_leaf_id": entry_order.get("rule_leaf_id"),
        "q_pred": entry_order.get("q_pred"),
        "growth_score": entry_order.get("growth_score"),
        "f_exec": entry_order.get("f_exec"),
        "edge_prob": entry_order.get("edge_prob"),
        "settlement_key": str(entry_order.get("settlement_key") or ""),
        "cluster_key": str(entry_order.get("cluster_key") or ""),
        "token_id": str(entry_order.get("token_id") or ""),
        "outcome_label": str(entry_order.get("outcome_label") or ""),
        "execution_phase": "EXIT",
        "parent_order_attempt_id": entry_attempt_id,
    }


def submit_pending_exit_orders(
    cfg: PegConfig,
    *,
    clob_client: ClobClient | None = None,
) -> ExitSubmissionResult:
    all_orders = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
    latest_orders = _latest_orders_by_attempt(all_orders)
    open_positions = load_open_position_rows(cfg)
    open_positions_by_entry = {
        str(row.get("entry_order_attempt_id") or ""): row
        for row in open_positions
        if str(row.get("entry_order_attempt_id") or "")
    }
    existing_exit_by_parent = {
        str(row.get("parent_order_attempt_id") or ""): row
        for row in latest_orders.values()
        if str(row.get("execution_phase") or "ENTRY").upper() == "EXIT"
        and str(row.get("parent_order_attempt_id") or "")
    }

    state = StateStore(cfg)
    nonce_manager = NonceManager(cfg.nonce_path)
    client = clob_client or build_clob_client(cfg)
    submitted_rows: List[Dict[str, Any]] = []
    status_counts: Dict[str, int] = {}
    candidate_count = 0

    entry_orders = sorted(
        [
            row
            for row in latest_orders.values()
            if str(row.get("execution_phase") or "ENTRY").upper() != "EXIT"
            and str(row.get("action") or "").upper() == "BUY"
        ],
        key=lambda row: str(row.get("created_at_utc") or ""),
    )
    for entry_order in entry_orders:
        entry_attempt_id = str(entry_order.get("order_attempt_id") or "")
        if not entry_attempt_id:
            continue
        position = open_positions_by_entry.get(entry_attempt_id)
        if position is None:
            continue
        if entry_attempt_id in existing_exit_by_parent:
            continue
        if not _ttl_elapsed(entry_order):
            continue

        candidate_count += 1
        signal = _build_exit_signal(entry_order, position)
        decision, reason = build_decision_from_signal(signal, cfg)
        if decision is None:
            status_counts[reason] = status_counts.get(reason, 0) + 1
            continue

        record_decision_created(cfg, state, decision, str(signal.get("order_attempt_id") or ""))
        order = submit_order(
            cfg,
            decision,
            signal,
            nonce_manager=nonce_manager,
            clob_client=client,
            token_id=str(entry_order.get("token_id") or ""),
        )
        record_order_submitted(cfg, state, decision, order)
        status = str(order.get("status") or "UNKNOWN").upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        submitted_rows.append(
            {
                "run_id": cfg.run_id,
                "market_id": str(entry_order.get("market_id") or ""),
                "token_id": str(entry_order.get("token_id") or ""),
                "outcome_label": str(entry_order.get("outcome_label") or ""),
                "parent_order_attempt_id": entry_attempt_id,
                "exit_order_attempt_id": str(order.get("order_attempt_id") or ""),
                "filled_shares": float(position.get("filled_shares", 0.0) or 0.0),
                "limit_price": EXIT_LIMIT_PRICE,
                "submitted_amount_usdc": float(order.get("amount_usdc", 0.0) or 0.0),
                "submitted_at_utc": str(order.get("created_at_utc") or ""),
                "status": status,
            }
        )

    write_jsonl(_submitted_orders_path(cfg), submitted_rows)
    write_manifest(
        _manifest_path(cfg),
        {
            "generated_at_utc": to_iso(utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "candidate_count": candidate_count,
            "submitted_count": len(submitted_rows),
            "status_counts": status_counts,
            "orders_submitted_path": str(_submitted_orders_path(cfg)),
        },
    )
    return ExitSubmissionResult(
        run_manifest_path=_manifest_path(cfg),
        submitted_orders_path=_submitted_orders_path(cfg),
        candidate_count=candidate_count,
        submitted_count=len(submitted_rows),
        status_counts=status_counts,
    )
