"""Order submission job for hourly online selection decisions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from execution_engine.integrations.providers.balance_provider import FileBalanceProvider
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.decision import build_decision_from_signal
from execution_engine.runtime.state import StateStore, refresh_state_snapshot
from execution_engine.runtime.validation import check_basic_risk, check_price_and_liquidity
from execution_engine.integrations.trading.clob_client import build_clob_client
from execution_engine.integrations.trading.nonce import NonceManager
from execution_engine.integrations.trading.order_manager import reconcile, submit_order, sweep_expired_orders
from execution_engine.online.execution.live_quote import get_live_quote
from execution_engine.online.execution.positions import refresh_market_state_cache
from execution_engine.online.execution.pricing import build_submission_signal, price_cap, to_float
from execution_engine.online.execution.submission_support import (
    append_attempt,
    load_fee_rate,
    load_frame,
    record_decision_created,
    record_order_submitted,
    record_rejection,
    rejection_count,
    submitted_count,
    to_bool,
    write_frame,
    write_jsonl,
    write_manifest,
)
from execution_engine.shared.logger import log_structured
from execution_engine.shared.time import to_iso, utc_now


@dataclass(frozen=True)
class SubmitHourlyResult:
    run_manifest_path: Path
    attempts_path: Path
    total_selected_rows: int
    attempted_count: int
    submitted_count: int
    rejection_count: int
    status_counts: Dict[str, int]


def _empty_result(cfg: PegConfig, selection_target: Path) -> SubmitHourlyResult:
    write_frame(cfg.run_submit_attempts_path, pd.DataFrame())
    write_jsonl(cfg.run_submit_orders_submitted_path, [])
    write_manifest(
        cfg.run_submit_manifest_path,
        {
            "generated_at_utc": to_iso(utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "status": "empty_selection",
            "selection_path": str(selection_target),
        },
    )
    return SubmitHourlyResult(
        run_manifest_path=cfg.run_submit_manifest_path,
        attempts_path=cfg.run_submit_attempts_path,
        total_selected_rows=0,
        attempted_count=0,
        submitted_count=0,
        rejection_count=0,
        status_counts={"empty_selection": 1},
    )


def _submitted_order_record(row: Dict[str, Any], signal: Dict[str, Any], order: Dict[str, Any], quote: Dict[str, Any], cfg: PegConfig) -> Dict[str, Any]:
    return {
        "run_id": cfg.run_id,
        "batch_id": str(row.get("batch_id") or ""),
        "market_id": str(row.get("market_id") or ""),
        "token_id": str(row.get("selected_token_id") or ""),
        "outcome_label": str(row.get("selected_outcome_label") or ""),
        "order_attempt_id": str(order.get("order_attempt_id") or ""),
        "limit_price": to_float(order.get("price_limit")),
        "best_bid_at_submit": to_float(signal.get("best_bid_at_submit")),
        "best_ask_at_submit": to_float(signal.get("best_ask_at_submit")),
        "tick_size": to_float(signal.get("tick_size"), default=to_float(quote.get("tick_size"), default=0.001)),
        "submitted_amount_usdc": to_float(order.get("amount_usdc")),
        "ttl_seconds": int(float(order.get("expiration_seconds") or cfg.order_ttl_sec)),
        "submitted_at_utc": str(order.get("created_at_utc") or ""),
        "order_status": str(order.get("status") or ""),
    }


def _record_dry_run_attempt(cfg: PegConfig, decision: Dict[str, Any], order: Dict[str, Any]) -> None:
    log_structured(
        cfg.logs_path,
        {
            "type": "dry_run_order",
            "decision_id": decision.get("decision_id"),
            "order_attempt_id": order.get("order_attempt_id"),
            **order,
        },
    )


def _handle_rejection(
    cfg: PegConfig,
    state: Any,
    attempts: List[Dict[str, Any]],
    status_counts: Dict[str, int],
    rejection: Dict[str, Any],
    attempt_row: Dict[str, Any],
    *,
    decision_id: str = "",
    order_attempt_id: str = "",
) -> None:
    record_rejection(
        cfg,
        state,
        rejection,
        decision_id=decision_id,
        order_attempt_id=order_attempt_id,
    )
    append_attempt(attempts, status_counts, attempt_row)


def submit_hourly_selection(
    cfg: PegConfig,
    *,
    selection_path: Path | None = None,
    token_state_path: Path | None = None,
    max_orders: int | None = None,
) -> SubmitHourlyResult:
    selection_target = selection_path or cfg.run_snapshot_selection_path
    token_state_target = token_state_path or cfg.token_state_current_path
    selection = load_frame(selection_target)
    if selection.empty:
        return _empty_result(cfg, selection_target)

    token_state = load_frame(token_state_target)
    token_state_by_token = {
        str(row.get("token_id") or ""): row
        for row in token_state.to_dict(orient="records")
        if str(row.get("token_id") or "")
    }

    attempts: List[Dict[str, Any]] = []
    status_counts: Dict[str, int] = {}
    submitted_orders: List[Dict[str, Any]] = []

    clob_client = build_clob_client(cfg)
    sweep_expired_orders(cfg, clob_client)
    reconcile(cfg, clob_client)
    state = StateStore(cfg)
    nonce_manager = NonceManager(cfg.nonce_path)
    balance_provider = FileBalanceProvider(cfg.balances_path)
    fee_rate = load_fee_rate(cfg)

    eligible_rows = selection[selection["selected_for_submission"].map(to_bool)].copy()
    if max_orders is not None and max_orders > 0:
        eligible_rows = eligible_rows.head(max_orders)

    for row in eligible_rows.to_dict(orient="records"):
        token_id = str(row.get("selected_token_id") or "")
        market_id = str(row.get("market_id") or "")
        quote = get_live_quote(clob_client, token_state_by_token, token_id)
        if quote is None:
            status = "MISSING_LIVE_QUOTE"
            _handle_rejection(
                cfg,
                state,
                attempts,
                status_counts,
                {"market_id": market_id, "reason_code": status, "created_at_utc": to_iso(utc_now())},
                {"market_id": market_id, "token_id": token_id, "status": status},
            )
            continue

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate)
        if signal is None:
            _handle_rejection(
                cfg,
                state,
                attempts,
                status_counts,
                {"market_id": market_id, "reason_code": reason, "created_at_utc": to_iso(utc_now())},
                {
                    "market_id": market_id,
                    "token_id": token_id,
                    "status": reason,
                    "best_bid": quote.get("best_bid"),
                    "best_ask": quote.get("best_ask"),
                    "tick_size": quote.get("tick_size"),
                    "price_cap": price_cap(row, cfg, fee_rate),
                },
            )
            continue

        current_mid = to_float(quote.get("mid"), default=to_float(signal.get("reference_mid_price")))
        spread = quote.get("spread")
        ok, price_reason = check_price_and_liquidity(
            float(signal["reference_mid_price"]),
            current_mid,
            float(spread) if spread is not None else None,
            None,
            cfg,
        )
        if not ok:
            _handle_rejection(
                cfg,
                state,
                attempts,
                status_counts,
                {
                    "decision_id": signal.get("decision_id"),
                    "market_id": signal.get("market_id"),
                    "outcome_index": signal.get("outcome_index"),
                    "action": signal.get("action"),
                    "reason_code": price_reason,
                    "created_at_utc": to_iso(utc_now()),
                },
                {
                    "market_id": market_id,
                    "token_id": token_id,
                    "status": price_reason,
                    "best_bid": quote.get("best_bid"),
                    "best_ask": quote.get("best_ask"),
                    "tick_size": quote.get("tick_size"),
                    "limit_price": signal.get("price_limit"),
                },
                decision_id=str(signal.get("decision_id") or ""),
                order_attempt_id=str(signal.get("order_attempt_id") or ""),
            )
            continue

        decision, merge_reason = build_decision_from_signal(signal, cfg)
        if decision is None:
            _handle_rejection(
                cfg,
                state,
                attempts,
                status_counts,
                {
                    "decision_id": signal.get("decision_id"),
                    "market_id": signal.get("market_id"),
                    "outcome_index": signal.get("outcome_index"),
                    "action": signal.get("action"),
                    "reason_code": merge_reason,
                    "created_at_utc": to_iso(utc_now()),
                },
                {"market_id": market_id, "token_id": token_id, "status": merge_reason},
                decision_id=str(signal.get("decision_id") or ""),
                order_attempt_id=str(signal.get("order_attempt_id") or ""),
            )
            continue

        record_decision_created(cfg, state, decision, str(signal.get("order_attempt_id") or ""))
        ok, risk_reason = check_basic_risk(signal, state, cfg, balance_provider)
        if not ok:
            _handle_rejection(
                cfg,
                state,
                attempts,
                status_counts,
                {
                    "decision_id": decision.get("decision_id"),
                    "market_id": decision.get("market_id"),
                    "outcome_index": decision.get("outcome_index"),
                    "action": decision.get("action"),
                    "reason_code": risk_reason,
                    "created_at_utc": to_iso(utc_now()),
                },
                {"market_id": market_id, "token_id": token_id, "status": risk_reason},
                decision_id=str(decision.get("decision_id") or ""),
                order_attempt_id=str(signal.get("order_attempt_id") or ""),
            )
            continue

        order = submit_order(
            cfg,
            decision,
            signal,
            nonce_manager,
            clob_client,
            token_id=token_id,
        )
        status = str(order.get("status") or "UNKNOWN")
        if cfg.dry_run and status.upper() == "DRY_RUN_SUBMITTED":
            _record_dry_run_attempt(cfg, decision, order)
        else:
            record_order_submitted(cfg, state, decision, order)
        append_attempt(
            attempts,
            status_counts,
            {
                "market_id": market_id,
                "token_id": token_id,
                "decision_id": signal.get("decision_id"),
                "order_attempt_id": order.get("order_attempt_id"),
                "status": status,
                "best_bid": quote.get("best_bid"),
                "best_ask": quote.get("best_ask"),
                "tick_size": quote.get("tick_size"),
                "reference_price": signal.get("reference_mid_price"),
                "limit_price": signal.get("price_limit"),
                "price_cap": price_cap(row, cfg, fee_rate),
                "stake_usdc": signal.get("amount_usdc"),
                "quote_source": quote.get("quote_source"),
            },
        )
        if status.upper() in {"DRY_RUN_SUBMITTED", "NEW", "ACKED", "FILLED"}:
            submitted_orders.append(_submitted_order_record(row, signal, order, quote, cfg))

    state_snapshot = refresh_state_snapshot(cfg)
    market_state = refresh_market_state_cache(cfg)
    write_frame(cfg.run_submit_attempts_path, pd.DataFrame(attempts))
    write_jsonl(cfg.run_submit_orders_submitted_path, submitted_orders)

    total_submitted = submitted_count(attempts)
    total_rejections = rejection_count(attempts)
    write_manifest(
        cfg.run_submit_manifest_path,
        {
            "generated_at_utc": to_iso(utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "selection_path": str(selection_target),
            "token_state_path": str(token_state_target),
            "total_selected_rows": int(len(eligible_rows)),
            "attempted_count": int(len(attempts)),
            "submitted_count": int(total_submitted),
            "rejection_count": int(total_rejections),
            "status_counts": status_counts,
            "orders_submitted_path": str(cfg.run_submit_orders_submitted_path),
            "market_state_cache_path": str(cfg.market_state_cache_path),
            "state_snapshot_path": str(cfg.state_snapshot_path),
            "pending_market_count": int(market_state.get("pending_market_count", 0)),
            "open_market_count": int(market_state.get("open_market_count", 0)),
            "state_open_orders_count": int(state_snapshot.get("open_orders_count", 0)),
            "state_net_exposure_usdc": float(state_snapshot.get("net_exposure_usdc", 0.0)),
        },
    )
    return SubmitHourlyResult(
        run_manifest_path=cfg.run_submit_manifest_path,
        attempts_path=cfg.run_submit_attempts_path,
        total_selected_rows=len(eligible_rows),
        attempted_count=len(attempts),
        submitted_count=total_submitted,
        rejection_count=total_rejections,
        status_counts=status_counts,
    )


