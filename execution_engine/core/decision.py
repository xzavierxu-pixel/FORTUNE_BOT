"""Signal fusion logic."""

from __future__ import annotations

from typing import Optional, Tuple

from .config import PegConfig
from .models import DecisionRecord, SignalPayload, ensure_ids
from ..utils.time import parse_utc, utc_now, to_iso


def _key(signal: SignalPayload) -> str:
    return f"{signal.get('market_id')}|{signal.get('outcome_index')}|{signal.get('action')}"


def _is_valid_time(signal: SignalPayload, cfg: PegConfig, now_utc=None) -> Tuple[bool, str]:
    now = now_utc or utc_now()
    valid_until = signal.get("valid_until_utc")
    if valid_until:
        if now > parse_utc(valid_until):
            return False, "SIGNAL_EXPIRED"

    decision_window_end = signal.get("decision_window_end_utc")
    if decision_window_end:
        if now > parse_utc(decision_window_end):
            return False, "SIGNAL_EXPIRED"

    market_close_time = signal.get("market_close_time_utc")
    if market_close_time and cfg.min_time_to_close_sec > 0:
        close_dt = parse_utc(market_close_time)
        if now.timestamp() > close_dt.timestamp() - cfg.min_time_to_close_sec:
            return False, "MARKET_CLOSE_GUARD"

    return True, "OK"


def merge_rule_and_llm(
    rule_signal: SignalPayload,
    llm_signal: Optional[SignalPayload],
    cfg: PegConfig,
) -> Tuple[Optional[DecisionRecord], str]:
    now = utc_now()

    rule_signal = ensure_ids(rule_signal)
    if llm_signal is None:
        return None, "LLM_MISSING"

    llm_signal = ensure_ids(llm_signal)
    if _key(rule_signal) != _key(llm_signal):
        return None, "ENGINE_DISAGREE"

    ok, reason = _is_valid_time(rule_signal, cfg, now)
    if not ok:
        return None, reason
    ok, reason = _is_valid_time(llm_signal, cfg, now)
    if not ok:
        return None, reason

    price_limit = rule_signal.get("price_limit")
    llm_price = llm_signal.get("price_limit")
    action = str(rule_signal.get("action", "")).upper()
    if price_limit is None:
        price_limit = llm_price
    elif llm_price is not None:
        if action == "BUY":
            price_limit = min(float(price_limit), float(llm_price))
        elif action == "SELL":
            price_limit = max(float(price_limit), float(llm_price))

    decision: DecisionRecord = {
        "decision_id": rule_signal["decision_id"],
        "market_id": rule_signal.get("market_id"),
        "outcome_index": rule_signal.get("outcome_index"),
        "action": rule_signal.get("action"),
        "order_type": rule_signal.get("order_type"),
        "price_limit": price_limit,
        "amount_usdc": cfg.order_usdc,
        "valid_until_utc": rule_signal.get("valid_until_utc"),
        "decision_window_end_utc": rule_signal.get("decision_window_end_utc"),
        "market_close_time_utc": rule_signal.get("market_close_time_utc"),
        "category": rule_signal.get("category"),
        "source_rule_signal_id": rule_signal.get("signal_id"),
        "source_llm_signal_id": llm_signal.get("signal_id"),
        "fusion_mode": "HARD_AGREE",
        "status": "READY",
        "created_at_utc": to_iso(now),
    }
    return decision, "OK"
