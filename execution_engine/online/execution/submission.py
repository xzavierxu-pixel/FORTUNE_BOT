"""Sequential per-order submission for the direct online pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List
import time

import pandas as pd

from execution_engine.integrations.providers.balance_provider import FileBalanceProvider
from execution_engine.integrations.trading.clob_client import build_clob_client
from execution_engine.integrations.trading.nonce import NonceManager
from execution_engine.integrations.trading.order_manager import reconcile, submit_order, sweep_expired_orders
from execution_engine.online.execution.live_quote import get_live_quote
from execution_engine.online.execution.positions import refresh_market_state_cache
from execution_engine.online.execution.pricing import build_submission_signal, price_cap, to_float
from execution_engine.online.execution.submission_support import (
    append_attempt,
    load_fee_rate,
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
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.decision import build_decision_from_signal
from execution_engine.runtime.state import StateStore, refresh_state_snapshot
from execution_engine.runtime.validation import check_basic_risk, check_price_and_liquidity
from execution_engine.shared.io import read_jsonl
from execution_engine.shared.time import to_iso, utc_now

CAPACITY_WAIT_REASONS = {
    "OPEN_ORDERS_LIMIT",
    "MARKET_EXPOSURE_LIMIT",
    "CATEGORY_EXPOSURE_LIMIT",
    "NET_EXPOSURE_LIMIT",
    "BALANCE_INSUFFICIENT",
}

SUBMITTED_ORDER_STATUSES = {"DRY_RUN_SUBMITTED", "NEW", "ACKED", "FILLED"}


def _submit_error_status(exc: Exception) -> str:
    message = str(exc).upper()
    if "TRADING RESTRICTED IN YOUR REGION" in message or "GEOBLOCK" in message:
        return "REGION_RESTRICTED"
    if "403" in message:
        return "SUBMIT_FORBIDDEN"
    return "SUBMIT_ERROR"


@dataclass(frozen=True)
class SubmitSelectionResult:
    run_manifest_path: Path
    attempts_path: Path
    total_selected_rows: int
    attempted_count: int
    submitted_count: int
    rejection_count: int
    capacity_wait_count: int
    quote_lookup_count: int
    quote_lookup_latency_ms: float
    spread_gate_reject_count: int
    gamma_to_submit_latency_ms: float
    selection_to_submit_latency_ms: float
    status_counts: Dict[str, int]


def _empty_result(cfg: PegConfig) -> SubmitSelectionResult:
    write_manifest(
        cfg.run_submit_manifest_path,
        {
            "generated_at_utc": to_iso(utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "status": "empty_selection",
        },
    )
    return SubmitSelectionResult(
        run_manifest_path=cfg.run_submit_manifest_path,
        attempts_path=cfg.run_submit_attempts_path,
        total_selected_rows=0,
        attempted_count=0,
        submitted_count=0,
        rejection_count=0,
        capacity_wait_count=0,
        quote_lookup_count=0,
        quote_lookup_latency_ms=0.0,
        spread_gate_reject_count=0,
        gamma_to_submit_latency_ms=0.0,
        selection_to_submit_latency_ms=0.0,
        status_counts={"empty_selection": 1},
    )


def _empty_result_noop(cfg: PegConfig, *, status: str = "empty_selection") -> SubmitSelectionResult:
    return SubmitSelectionResult(
        run_manifest_path=cfg.run_submit_manifest_path,
        attempts_path=cfg.run_submit_attempts_path,
        total_selected_rows=0,
        attempted_count=0,
        submitted_count=0,
        rejection_count=0,
        capacity_wait_count=0,
        quote_lookup_count=0,
        quote_lookup_latency_ms=0.0,
        spread_gate_reject_count=0,
        gamma_to_submit_latency_ms=0.0,
        selection_to_submit_latency_ms=0.0,
        status_counts={status: 1},
    )


def _selected_outcome_index(row: Dict[str, Any]) -> int:
    return 0 if int(float(row.get("direction_model") or 0)) > 0 else 1


def _submitted_order_record(
    row: Dict[str, Any],
    signal: Dict[str, Any],
    order: Dict[str, Any],
    quote: Dict[str, Any],
    cfg: PegConfig,
) -> Dict[str, Any]:
    submitted_at_utc = str(order.get("created_at_utc") or "")
    first_seen_at_utc = str(row.get("first_seen_at_utc") or "")
    snapshot_time_utc = str(row.get("snapshot_time_utc") or "")
    first_seen_dt = pd.to_datetime(first_seen_at_utc, utc=True, errors="coerce")
    snapshot_dt = pd.to_datetime(snapshot_time_utc, utc=True, errors="coerce")
    submitted_dt = pd.to_datetime(submitted_at_utc, utc=True, errors="coerce")
    gamma_to_submit_latency_ms = None
    selection_to_submit_latency_ms = None
    if pd.notna(first_seen_dt) and pd.notna(submitted_dt):
        gamma_to_submit_latency_ms = max((submitted_dt - first_seen_dt).total_seconds() * 1000.0, 0.0)
    if pd.notna(snapshot_dt) and pd.notna(submitted_dt):
        selection_to_submit_latency_ms = max((submitted_dt - snapshot_dt).total_seconds() * 1000.0, 0.0)
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
        "tick_size": to_float(signal.get("tick_size"), default=to_float(quote.get("tick_size"), default=0.01)),
        "submitted_amount_usdc": to_float(order.get("amount_usdc")),
        "ttl_seconds": int(float(order.get("expiration_seconds") or cfg.order_ttl_sec)),
        "submitted_at_utc": submitted_at_utc,
        "first_seen_at_utc": first_seen_at_utc,
        "snapshot_time_utc": snapshot_time_utc,
        "gamma_to_submit_latency_ms": gamma_to_submit_latency_ms,
        "selection_to_submit_latency_ms": selection_to_submit_latency_ms,
        "order_status": str(order.get("status") or ""),
    }


def _capacity_reason(
    row: Dict[str, Any],
    state: StateStore,
    cfg: PegConfig,
    balance_provider: FileBalanceProvider,
) -> str | None:
    amount_usdc = to_float(row.get("stake_usdc"))
    if amount_usdc <= 0:
        return None
    if state.open_orders_count >= cfg.max_open_orders:
        return "OPEN_ORDERS_LIMIT"
    if cfg.max_position_per_market_usdc > 0:
        exposure = state.get_market_exposure(str(row.get("market_id") or ""), _selected_outcome_index(row), "BUY")
        if exposure + amount_usdc > cfg.max_position_per_market_usdc:
            return "MARKET_EXPOSURE_LIMIT"
    if cfg.max_exposure_per_category_usdc > 0:
        category = str(row.get("category") or "").strip()
        if category:
            exposure = state.get_category_exposure(category)
            if exposure + amount_usdc > cfg.max_exposure_per_category_usdc:
                return "CATEGORY_EXPOSURE_LIMIT"
    if state.net_exposure_usdc + amount_usdc > cfg.max_net_exposure_usdc:
        return "NET_EXPOSURE_LIMIT"
    available = balance_provider.get_available_usdc()
    if available is not None and amount_usdc > available:
        return "BALANCE_INSUFFICIENT"
    return None


def _wait_for_capacity(
    cfg: PegConfig,
    row: Dict[str, Any],
    clob_client: Any,
    balance_provider: FileBalanceProvider,
) -> tuple[StateStore, int]:
    wait_count = 0
    while True:
        state = StateStore(cfg)
        reason = _capacity_reason(row, state, cfg, balance_provider)
        if reason is None:
            return state, wait_count
        wait_count += 1
        sweep_expired_orders(cfg, clob_client)
        reconcile(cfg, clob_client)
        refresh_market_state_cache(cfg)
        refresh_state_snapshot(cfg)
        time.sleep(max(int(cfg.online_capacity_wait_poll_sec), 1))


def submit_selected_orders(
    cfg: PegConfig,
    selection: pd.DataFrame,
    token_state: pd.DataFrame,
    *,
    max_orders: int | None = None,
) -> SubmitSelectionResult:
    if selection.empty:
        return _empty_result(cfg)

    token_state_by_token = {
        str(row.get("token_id") or ""): row
        for row in token_state.to_dict(orient="records")
        if str(row.get("token_id") or "")
    }
    eligible_rows = selection[selection["selected_for_submission"].map(to_bool)].copy()
    if max_orders is not None and max_orders > 0:
        eligible_rows = eligible_rows.head(max_orders)
    if eligible_rows.empty:
        return _empty_result(cfg)

    attempts: List[Dict[str, Any]] = []
    status_counts: Dict[str, int] = {}
    submitted_orders: List[Dict[str, Any]] = []
    capacity_wait_count = 0
    quote_lookup_latencies_ms: List[float] = []
    gamma_to_submit_latencies_ms: List[float] = []
    selection_to_submit_latencies_ms: List[float] = []
    spread_gate_reject_count = 0

    clob_client = build_clob_client(cfg)
    sweep_expired_orders(cfg, clob_client)
    reconcile(cfg, clob_client)
    nonce_manager = NonceManager(cfg.nonce_path)
    balance_provider = FileBalanceProvider(cfg.balances_path)
    fee_rate = load_fee_rate(cfg)

    for row in eligible_rows.to_dict(orient="records"):
        state, wait_count = _wait_for_capacity(cfg, row, clob_client, balance_provider)
        capacity_wait_count += wait_count
        token_id = str(row.get("selected_token_id") or "")
        market_id = str(row.get("market_id") or "")

        while True:
            quote_lookup_started = time.perf_counter()
            quote = get_live_quote(clob_client, token_state_by_token, token_id)
            quote_lookup_latencies_ms.append((time.perf_counter() - quote_lookup_started) * 1000.0)
            if quote is None:
                rejection = {"market_id": market_id, "reason_code": "MISSING_LIVE_QUOTE", "created_at_utc": to_iso(utc_now())}
                record_rejection(cfg, state, rejection)
                append_attempt(attempts, status_counts, {"market_id": market_id, "token_id": token_id, "status": "MISSING_LIVE_QUOTE"})
                break

            signal, reason = build_submission_signal(row, quote, cfg, fee_rate)
            if signal is None:
                rejection = {"market_id": market_id, "reason_code": reason, "created_at_utc": to_iso(utc_now())}
                record_rejection(cfg, state, rejection)
                append_attempt(
                    attempts,
                    status_counts,
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
                break

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
                if price_reason == "SPREAD_TOO_WIDE":
                    spread_gate_reject_count += 1
                rejection = {
                    "decision_id": signal.get("decision_id"),
                    "market_id": market_id,
                    "reason_code": price_reason,
                    "created_at_utc": to_iso(utc_now()),
                }
                record_rejection(
                    cfg,
                    state,
                    rejection,
                    decision_id=str(signal.get("decision_id") or ""),
                    order_attempt_id=str(signal.get("order_attempt_id") or ""),
                )
                append_attempt(
                    attempts,
                    status_counts,
                    {
                        "market_id": market_id,
                        "token_id": token_id,
                        "status": price_reason,
                        "best_bid": quote.get("best_bid"),
                        "best_ask": quote.get("best_ask"),
                        "tick_size": quote.get("tick_size"),
                        "limit_price": signal.get("price_limit"),
                    },
                )
                break

            ok, risk_reason = check_basic_risk(signal, state, cfg, balance_provider)
            if not ok and risk_reason in CAPACITY_WAIT_REASONS:
                state, extra_wait = _wait_for_capacity(cfg, row, clob_client, balance_provider)
                capacity_wait_count += extra_wait + 1
                continue
            if not ok:
                rejection = {
                    "decision_id": signal.get("decision_id"),
                    "market_id": market_id,
                    "reason_code": risk_reason,
                    "created_at_utc": to_iso(utc_now()),
                }
                record_rejection(
                    cfg,
                    state,
                    rejection,
                    decision_id=str(signal.get("decision_id") or ""),
                    order_attempt_id=str(signal.get("order_attempt_id") or ""),
                )
                append_attempt(attempts, status_counts, {"market_id": market_id, "token_id": token_id, "status": risk_reason})
                break

            decision, merge_reason = build_decision_from_signal(signal, cfg)
            if decision is None:
                rejection = {
                    "decision_id": signal.get("decision_id"),
                    "market_id": market_id,
                    "reason_code": merge_reason,
                    "created_at_utc": to_iso(utc_now()),
                }
                record_rejection(
                    cfg,
                    state,
                    rejection,
                    decision_id=str(signal.get("decision_id") or ""),
                    order_attempt_id=str(signal.get("order_attempt_id") or ""),
                )
                append_attempt(attempts, status_counts, {"market_id": market_id, "token_id": token_id, "status": merge_reason})
                break

            record_decision_created(cfg, state, decision, str(signal.get("order_attempt_id") or ""))
            try:
                order = submit_order(
                    cfg,
                    decision,
                    signal,
                    nonce_manager,
                    clob_client,
                    token_id=token_id,
                )
            except Exception as exc:
                status = _submit_error_status(exc)
                rejection = {
                    "decision_id": signal.get("decision_id"),
                    "market_id": market_id,
                    "reason_code": status,
                    "reason_detail": str(exc),
                    "created_at_utc": to_iso(utc_now()),
                }
                record_rejection(
                    cfg,
                    state,
                    rejection,
                    decision_id=str(signal.get("decision_id") or ""),
                    order_attempt_id=str(signal.get("order_attempt_id") or ""),
                )
                append_attempt(
                    attempts,
                    status_counts,
                    {
                        "market_id": market_id,
                        "token_id": token_id,
                        "decision_id": signal.get("decision_id"),
                        "order_attempt_id": signal.get("order_attempt_id"),
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
                break
            status = str(order.get("status") or "UNKNOWN").upper()
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
            if status in SUBMITTED_ORDER_STATUSES:
                order_record = _submitted_order_record(row, signal, order, quote, cfg)
                if order_record.get("gamma_to_submit_latency_ms") is not None:
                    gamma_to_submit_latencies_ms.append(float(order_record["gamma_to_submit_latency_ms"]))
                if order_record.get("selection_to_submit_latency_ms") is not None:
                    selection_to_submit_latencies_ms.append(float(order_record["selection_to_submit_latency_ms"]))
                submitted_orders.append(order_record)
            break

    state_snapshot = refresh_state_snapshot(cfg)
    market_state = refresh_market_state_cache(cfg)
    existing_attempts = pd.DataFrame()
    if cfg.run_submit_attempts_path.exists():
        try:
            existing_attempts = pd.read_csv(cfg.run_submit_attempts_path, dtype=str)
        except pd.errors.EmptyDataError:
            existing_attempts = pd.DataFrame()
    combined_attempts = pd.concat([existing_attempts, pd.DataFrame(attempts)], ignore_index=True)
    existing_orders = read_jsonl(cfg.run_submit_orders_submitted_path)
    combined_orders = existing_orders + submitted_orders
    write_frame(cfg.run_submit_attempts_path, combined_attempts)
    write_jsonl(cfg.run_submit_orders_submitted_path, combined_orders)

    total_submitted = submitted_count(attempts)
    total_rejections = rejection_count(attempts)
    cumulative_submitted = submitted_count(combined_attempts.to_dict(orient="records"))
    cumulative_rejections = rejection_count(combined_attempts.to_dict(orient="records"))
    quote_lookup_latency_ms = (
        sum(quote_lookup_latencies_ms) / len(quote_lookup_latencies_ms) if quote_lookup_latencies_ms else 0.0
    )
    gamma_to_submit_latency_ms = (
        sum(gamma_to_submit_latencies_ms) / len(gamma_to_submit_latencies_ms) if gamma_to_submit_latencies_ms else 0.0
    )
    selection_to_submit_latency_ms = (
        sum(selection_to_submit_latencies_ms) / len(selection_to_submit_latencies_ms)
        if selection_to_submit_latencies_ms
        else 0.0
    )
    write_manifest(
        cfg.run_submit_manifest_path,
        {
            "generated_at_utc": to_iso(utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "total_selected_rows": int(len(eligible_rows)),
            "attempted_count": int(len(combined_attempts)),
            "submitted_count": int(cumulative_submitted),
            "rejection_count": int(cumulative_rejections),
            "batch_attempted_count": int(len(attempts)),
            "batch_submitted_count": int(total_submitted),
            "batch_rejection_count": int(total_rejections),
            "capacity_wait_count": int(capacity_wait_count),
            "quote_lookup_count": int(len(quote_lookup_latencies_ms)),
            "quote_lookup_latency_ms": float(quote_lookup_latency_ms),
            "spread_gate_reject_count": int(spread_gate_reject_count),
            "gamma_to_submit_latency_ms": float(gamma_to_submit_latency_ms),
            "selection_to_submit_latency_ms": float(selection_to_submit_latency_ms),
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
    return SubmitSelectionResult(
        run_manifest_path=cfg.run_submit_manifest_path,
        attempts_path=cfg.run_submit_attempts_path,
        total_selected_rows=len(eligible_rows),
        attempted_count=len(attempts),
        submitted_count=total_submitted,
        rejection_count=total_rejections,
        capacity_wait_count=capacity_wait_count,
        quote_lookup_count=len(quote_lookup_latencies_ms),
        quote_lookup_latency_ms=quote_lookup_latency_ms,
        spread_gate_reject_count=spread_gate_reject_count,
        gamma_to_submit_latency_ms=gamma_to_submit_latency_ms,
        selection_to_submit_latency_ms=selection_to_submit_latency_ms,
        status_counts=status_counts,
    )
