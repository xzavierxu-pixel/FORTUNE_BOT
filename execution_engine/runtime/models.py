"""Data models and id helpers for PEG."""

from __future__ import annotations

from typing import Any, Dict, TypedDict
import hashlib


class SignalPayload(TypedDict, total=False):
    decision_id: str
    order_attempt_id: str
    attempt_no: int
    signal_id: str

    source: str
    source_run_id: str

    market_id: str
    outcome_index: int
    action: str
    order_type: str

    price_limit: float
    reference_mid_price: float
    reference_price_time_utc: str

    amount_usdc: float
    expiration_seconds: int

    strategy_ref_id: str
    created_at_utc: str
    valid_until_utc: str
    decision_window_start_utc: str
    decision_window_end_utc: str

    market_close_time_utc: str
    confidence: str
    reasoning_ref: str
    category: str
    domain: str
    market_type: str
    source_host: str
    position_side: str
    rule_group_key: str
    rule_leaf_id: int
    q_pred: float
    growth_score: float
    f_exec: float
    edge_prob: float
    settlement_key: str
    cluster_key: str
    token_id: str
    outcome_label: str
    best_bid_at_submit: float
    best_ask_at_submit: float
    tick_size: float


class DecisionRecord(TypedDict, total=False):
    decision_id: str
    market_id: str
    outcome_index: int
    action: str
    order_type: str
    price_limit: float
    amount_usdc: float
    valid_until_utc: str
    decision_window_end_utc: str
    market_close_time_utc: str
    source_signal_id: str
    status: str
    created_at_utc: str
    category: str
    domain: str
    market_type: str
    source_host: str
    position_side: str
    rule_group_key: str
    rule_leaf_id: int
    q_pred: float
    growth_score: float
    f_exec: float
    edge_prob: float
    settlement_key: str
    cluster_key: str
    token_id: str
    outcome_label: str
    best_bid_at_submit: float
    best_ask_at_submit: float
    tick_size: float


class OrderRecord(TypedDict, total=False):
    order_attempt_id: str
    clob_order_id: str
    nonce: int
    decision_id: str
    market_id: str
    outcome_index: int
    action: str
    order_type: str
    price_limit: float
    amount_usdc: float
    expiration_seconds: int
    status: str
    created_at_utc: str
    run_id: str
    updated_at_utc: str
    status_reason: str
    category: str
    domain: str
    market_type: str
    source_host: str
    position_side: str
    rule_group_key: str
    rule_leaf_id: int
    q_pred: float
    growth_score: float
    f_exec: float
    edge_prob: float
    settlement_key: str
    cluster_key: str
    token_id: str
    outcome_label: str
    best_bid_at_submit: float
    best_ask_at_submit: float
    tick_size: float


class FillRecord(TypedDict, total=False):
    fill_id: str
    order_attempt_id: str
    clob_order_id: str
    decision_id: str
    market_id: str
    outcome_index: int
    action: str
    amount_usdc: float
    price: float
    shares: float
    pnl_usdc: float
    filled_at_utc: str
    category: str
    domain: str
    position_side: str
    token_id: str
    outcome_label: str


class EventRecord(TypedDict, total=False):
    event_time_utc: str
    event_type: str
    decision_id: str
    order_attempt_id: str
    payload: Dict[str, Any]


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def compute_decision_id(signal: SignalPayload) -> str:
    parts = [
        signal.get("strategy_ref_id", ""),
        signal.get("market_id", ""),
        str(signal.get("outcome_index", "")),
        signal.get("action", ""),
        signal.get("decision_window_start_utc", ""),
    ]
    return _sha256("|".join(parts))


def compute_order_attempt_id(decision_id: str, attempt_no: int) -> str:
    return _sha256(f"{decision_id}|{attempt_no}")


def compute_signal_id(signal: SignalPayload) -> str:
    parts = [
        signal.get("source", ""),
        signal.get("source_run_id", ""),
        signal.get("decision_id", ""),
    ]
    return _sha256("|".join(parts))


def ensure_ids(signal: SignalPayload) -> SignalPayload:
    if not signal.get("decision_id"):
        signal["decision_id"] = compute_decision_id(signal)
    if not signal.get("attempt_no"):
        signal["attempt_no"] = 1
    if not signal.get("order_attempt_id"):
        signal["order_attempt_id"] = compute_order_attempt_id(
            signal["decision_id"], int(signal["attempt_no"])
        )
    if not signal.get("signal_id"):
        signal["signal_id"] = compute_signal_id(signal)
    return signal
