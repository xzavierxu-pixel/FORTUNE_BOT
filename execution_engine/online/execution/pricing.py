"""Pricing helpers for online limit-order submission."""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict

from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.models import SignalPayload, ensure_ids
from execution_engine.shared.time import parse_utc, to_iso, utc_now

MIN_EXECUTION_TICK_SIZE = 0.01
MIN_EXECUTION_ORDER_SHARES = 5.0
ABNORMAL_BOOK_MIN_BID = 0.01
ABNORMAL_BOOK_MAX_ASK = 0.99
ABNORMAL_BOOK_MAX_SPREAD = 0.50


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def round_down_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return round(price, 6)
    ticks = int(price / tick_size)
    return round(ticks * tick_size, 6)


def normalize_tick_size(value: Any) -> float:
    return max(to_float(value, default=MIN_EXECUTION_TICK_SIZE), MIN_EXECUTION_TICK_SIZE)


def normalize_min_order_shares(value: Any) -> float:
    return max(to_float(value, default=MIN_EXECUTION_ORDER_SHARES), MIN_EXECUTION_ORDER_SHARES)


def price_cap(row: Dict[str, Any], cfg: PegConfig, fee_rate: float) -> float:
    q_pred = to_float(row.get("q_pred"), default=0.0)
    return max(q_pred - fee_rate - cfg.online_price_cap_safety_buffer, 0.0)


def extend_iso(now_iso: str, seconds: int) -> str:
    return to_iso(parse_utc(now_iso) + timedelta(seconds=seconds))


def build_submission_signal(
    row: Dict[str, Any],
    quote: Dict[str, Any],
    cfg: PegConfig,
    fee_rate: float,
) -> tuple[SignalPayload | None, str]:
    token_id = str(row.get("selected_token_id") or "")
    market_id = str(row.get("market_id") or "")
    if not token_id or not market_id:
        return None, "MISSING_TOKEN_OR_MARKET_ID"

    best_bid = to_float(quote.get("best_bid"))
    best_ask = to_float(quote.get("best_ask"))
    tick_size = normalize_tick_size(quote.get("tick_size"))
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 and best_ask >= best_bid else None
    if best_bid <= 0:
        return None, "BEST_BID_MISSING"
    if best_ask <= 0:
        return None, "BEST_ASK_MISSING"
    if (
        best_bid <= ABNORMAL_BOOK_MIN_BID
        and best_ask >= ABNORMAL_BOOK_MAX_ASK
    ) or (spread is not None and spread > ABNORMAL_BOOK_MAX_SPREAD):
        return None, "ABNORMAL_TOP_OF_BOOK"

    limit_price = round_down_to_tick(
        best_bid - cfg.online_limit_ticks_below_best_bid * tick_size,
        tick_size,
    )
    if limit_price <= 0:
        return None, "INVALID_LIMIT_PRICE"
    if limit_price < cfg.rule_engine_min_price or limit_price > cfg.rule_engine_max_price:
        return None, "LIMIT_PRICE_OUTSIDE_RULE_RANGE"

    cap = price_cap(row, cfg, fee_rate)
    if cap <= 0:
        return None, "PRICE_CAP_NONPOSITIVE"
    if limit_price > cap:
        return None, "LIMIT_PRICE_ABOVE_CAP"

    planned_amount_usdc = to_float(row.get("stake_usdc"))
    min_order_size = normalize_min_order_shares(quote.get("min_order_size"))
    required_amount_usdc = min_order_size * limit_price if limit_price > 0 else 0.0
    amount_usdc = max(planned_amount_usdc, required_amount_usdc)
    if amount_usdc <= 0:
        return None, "INVALID_ORDER_SIZE"
    if cfg.max_trade_amount_usdc > 0 and amount_usdc > cfg.max_trade_amount_usdc:
        return None, "MIN_ORDER_SIZE_ABOVE_MAX_TRADE"

    now_iso = to_iso(utc_now())
    close_time = str(row.get("market_close_time_utc") or row.get("valid_until_utc") or "")
    signal: SignalPayload = {
        "source": "online_submit_hourly",
        "source_run_id": cfg.run_id,
        "market_id": market_id,
        "outcome_index": 0 if int(float(row.get("direction_model") or 0)) > 0 else 1,
        "action": "BUY",
        "order_type": "LIMIT",
        "price_limit": limit_price,
        "reference_mid_price": to_float(row.get("price"), default=to_float(quote.get("mid"))),
        "reference_price_time_utc": str(quote.get("quote_time_utc") or now_iso),
        "amount_usdc": amount_usdc,
        "expiration_seconds": cfg.order_ttl_sec,
        "strategy_ref_id": "online_hourly_selection",
        "created_at_utc": now_iso,
        "valid_until_utc": extend_iso(now_iso, cfg.order_ttl_sec),
        "decision_window_start_utc": now_iso,
        "decision_window_end_utc": extend_iso(now_iso, cfg.order_ttl_sec),
        "market_close_time_utc": close_time,
        "confidence": "high",
        "reasoning_ref": "execution_engine/online/pricing.py",
        "category": str(row.get("category") or ""),
        "domain": str(row.get("domain") or ""),
        "market_type": str(row.get("market_type") or ""),
        "source_host": str(row.get("domain") or ""),
        "position_side": str(row.get("position_side") or ""),
        "rule_group_key": str(row.get("rule_group_key") or ""),
        "rule_leaf_id": int(float(row.get("rule_leaf_id") or 0)),
        "q_pred": to_float(row.get("q_pred"), default=0.5),
        "growth_score": to_float(row.get("growth_score")),
        "f_exec": to_float(row.get("f_exec")),
        "edge_prob": to_float(row.get("q_pred")) - to_float(row.get("price")),
        "settlement_key": str(row.get("settlement_key") or ""),
        "cluster_key": str(row.get("cluster_key") or ""),
        "token_id": token_id,
        "outcome_label": str(row.get("selected_outcome_label") or ""),
        "best_bid_at_submit": best_bid,
        "best_ask_at_submit": best_ask,
        "min_order_size": min_order_size,
        "order_size_shares": min_order_size if amount_usdc <= required_amount_usdc + 1e-9 else amount_usdc / limit_price,
        "required_amount_usdc": required_amount_usdc,
        "planned_amount_usdc": planned_amount_usdc,
        "tick_size": tick_size,
    }
    return ensure_ids(signal), "OK"

