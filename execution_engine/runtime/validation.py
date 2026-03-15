"""Validation checks for PEG (Price, Liquidity, Risk)."""

from __future__ import annotations

from typing import Optional, Tuple

from .config import PegConfig
from .models import SignalPayload
from .state import StateStore
# Note: Providers are now in connectors. We use a forward reference or 'Any' 
# if circular imports become an issue, but structurally validation logic 
# should depend on interfaces.
from typing import Any

# Re-implementing imports based on the new structure
# BalanceProvider will be imported from execution_engine.integrations.providers.balance_provider


def check_price_and_liquidity(
    reference_mid: float,
    mid_now: float,
    spread_now: Optional[float],
    depth_usdc: Optional[float],
    cfg: PegConfig,
) -> Tuple[bool, str]:
    diff = abs(mid_now - reference_mid)
    if diff > cfg.price_dev_abs:
        return False, "PRICE_DEVIATION"

    if cfg.price_dev_rel > 0:
        if diff > cfg.price_dev_rel * reference_mid:
            return False, "PRICE_DEVIATION_REL"

    if cfg.price_dev_spread_k > 0 and spread_now is not None:
        if diff > cfg.price_dev_spread_k * spread_now:
            return False, "PRICE_DEVIATION_SPREAD"

    if cfg.max_spread > 0 and spread_now is not None:
        if spread_now > cfg.max_spread:
            return False, "SPREAD_TOO_WIDE"

    if cfg.min_depth_usdc > 0 and depth_usdc is not None:
        if depth_usdc < cfg.min_depth_usdc:
            return False, "DEPTH_TOO_THIN"

    return True, "OK"


def _fat_finger_check(price_limit: float, cfg: PegConfig) -> bool:
    return price_limit >= cfg.fat_finger_high or price_limit <= cfg.fat_finger_low


def check_basic_risk(
    signal: SignalPayload,
    state: StateStore,
    cfg: PegConfig,
    balance_provider: Any = None,  # Typed as Any to avoid circular import issues for now
) -> Tuple[bool, str]:
    order_type = str(signal.get("order_type", "")).upper()
    if order_type != "LIMIT":
        return False, "ORDER_TYPE_NOT_ALLOWED"

    amount_usdc = float(signal.get("amount_usdc", 0.0))
    if amount_usdc <= 0:
        return False, "INVALID_ORDER_SIZE"

    if cfg.max_trade_amount_usdc > 0 and amount_usdc > cfg.max_trade_amount_usdc:
        return False, "MAX_TRADE_AMOUNT_BREACH"

    if amount_usdc > cfg.max_notional:
        return False, "MAX_NOTIONAL_BREACH"

    price_limit = float(signal.get("price_limit", 0.0))
    if _fat_finger_check(price_limit, cfg):
        return False, "FAT_FINGER"

    if state.current_daily_pnl() < cfg.daily_loss_limit:
        return False, "DAILY_LOSS_LIMIT"

    if cfg.max_daily_orders > 0 and state.daily_order_count >= cfg.max_daily_orders:
        return False, "DAILY_ORDER_LIMIT"

    if cfg.enforce_one_order_per_market:
        market_id = str(signal.get("market_id", ""))
        outcome_index = int(signal.get("outcome_index", 0))
        action = str(signal.get("action", ""))
        if state.seen_market_action(market_id, outcome_index, action):
            return False, "DUPLICATE_MARKET_ACTION"

    decision_id = str(signal.get("decision_id", ""))
    if decision_id and state.seen_recent_decision(decision_id, cfg.dup_window_sec):
        return False, "DUPLICATE_DECISION"

    if state.open_orders_count >= cfg.max_open_orders:
        return False, "OPEN_ORDERS_LIMIT"

    if cfg.max_position_per_market_usdc > 0:
        market_id = str(signal.get("market_id", ""))
        outcome_index = int(signal.get("outcome_index", 0))
        action = str(signal.get("action", ""))
        exposure = state.get_market_exposure(market_id, outcome_index, action)
        if exposure + amount_usdc > cfg.max_position_per_market_usdc:
            return False, "MARKET_EXPOSURE_LIMIT"

    if cfg.max_exposure_per_category_usdc > 0:
        category = str(signal.get("category", "")).strip()
        if category:
            exposure = state.get_category_exposure(category)
            if exposure + amount_usdc > cfg.max_exposure_per_category_usdc:
                return False, "CATEGORY_EXPOSURE_LIMIT"

    if state.net_exposure_usdc + amount_usdc > cfg.max_net_exposure_usdc:
        return False, "NET_EXPOSURE_LIMIT"

    if balance_provider is not None:
        # Assuming duck typing for balance provider
        if hasattr(balance_provider, "get_available_usdc"):
            available = balance_provider.get_available_usdc()
            if available is None:
                if cfg.balance_strict:
                    return False, "BALANCE_UNKNOWN"
            else:
                if amount_usdc > available:
                    return False, "BALANCE_INSUFFICIENT"

    return True, "OK"

