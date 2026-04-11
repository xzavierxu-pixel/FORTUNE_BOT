"""Decision construction for the online execution pipeline."""

from __future__ import annotations

from typing import Tuple

from .config import PegConfig
from .models import DecisionRecord, SignalPayload, ensure_ids
from execution_engine.shared.time import parse_utc, to_iso, utc_now


def _is_valid_time(signal: SignalPayload, cfg: PegConfig, now_utc=None) -> Tuple[bool, str]:
    now = now_utc or utc_now()
    valid_until = signal.get("valid_until_utc")
    if valid_until and now > parse_utc(valid_until):
        return False, "SIGNAL_EXPIRED"

    decision_window_end = signal.get("decision_window_end_utc")
    if decision_window_end and now > parse_utc(decision_window_end):
        return False, "SIGNAL_EXPIRED"

    market_close_time = signal.get("market_close_time_utc")
    if market_close_time and cfg.min_time_to_close_sec > 0:
        close_dt = parse_utc(market_close_time)
        if now.timestamp() > close_dt.timestamp() - cfg.min_time_to_close_sec:
            return False, "MARKET_CLOSE_GUARD"

    return True, "OK"


def build_decision_from_signal(
    signal: SignalPayload,
    cfg: PegConfig,
) -> Tuple[DecisionRecord | None, str]:
    now = utc_now()
    signal = ensure_ids(signal)

    ok, reason = _is_valid_time(signal, cfg, now)
    if not ok:
        return None, reason

    decision: DecisionRecord = {
        "decision_id": signal["decision_id"],
        "market_id": signal.get("market_id"),
        "outcome_index": signal.get("outcome_index"),
        "action": signal.get("action"),
        "order_type": signal.get("order_type"),
        "price_limit": signal.get("price_limit"),
        "amount_usdc": float(signal.get("amount_usdc", cfg.order_usdc)),
        "valid_until_utc": signal.get("valid_until_utc"),
        "decision_window_end_utc": signal.get("decision_window_end_utc"),
        "market_close_time_utc": signal.get("market_close_time_utc"),
        "category": signal.get("category"),
        "source_signal_id": signal.get("signal_id"),
        "status": "READY",
        "created_at_utc": to_iso(now),
        "domain": signal.get("domain"),
        "market_type": signal.get("market_type"),
        "source_host": signal.get("source_host"),
        "event_id": signal.get("event_id"),
        "position_side": signal.get("position_side"),
        "rule_group_key": signal.get("rule_group_key"),
        "rule_leaf_id": signal.get("rule_leaf_id"),
        "q_pred": signal.get("q_pred"),
        "growth_score": signal.get("growth_score"),
        "f_exec": signal.get("f_exec"),
        "edge_prob": signal.get("edge_prob"),
        "settlement_key": signal.get("settlement_key"),
        "cluster_key": signal.get("cluster_key"),
        "token_id": signal.get("token_id"),
        "outcome_label": signal.get("outcome_label"),
        "best_bid_at_submit": signal.get("best_bid_at_submit"),
        "best_ask_at_submit": signal.get("best_ask_at_submit"),
        "tick_size": signal.get("tick_size"),
        "execution_phase": signal.get("execution_phase", "ENTRY"),
        "parent_order_attempt_id": signal.get("parent_order_attempt_id"),
    }
    return decision, "OK"
