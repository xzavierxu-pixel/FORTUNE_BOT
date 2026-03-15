"""PnL helpers for exit fills and settlement closes."""

from __future__ import annotations


def realized_pnl_usdc(open_cost_usdc: float, close_value_usdc: float) -> float:
    return round(float(close_value_usdc) - float(open_cost_usdc), 6)
