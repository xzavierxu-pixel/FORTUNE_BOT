from __future__ import annotations

import numpy as np
import pandas as pd

from rule_baseline.utils import config


def resolve_direction_column(frame: pd.DataFrame, direction_column: str | None = None) -> str:
    if direction_column is not None:
        if direction_column not in frame.columns:
            raise KeyError(f"Direction column '{direction_column}' not found.")
        return direction_column
    for candidate in ("rule_direction", "direction"):
        if candidate in frame.columns:
            return candidate
    raise KeyError("Could not resolve a direction column from frame.")


def compute_trade_value_from_q(
    frame: pd.DataFrame,
    q_pred: pd.Series | np.ndarray,
    *,
    direction_column: str | None = None,
    fee_rate: float = config.FEE_RATE,
) -> np.ndarray:
    resolved_direction = resolve_direction_column(frame, direction_column)
    price = frame["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    direction = frame[resolved_direction].astype(int).values
    q_value = np.asarray(q_pred, dtype=float).clip(1e-6, 1 - 1e-6)
    return np.where(
        direction > 0,
        q_value / price - 1.0 - fee_rate,
        (price - q_value) / np.maximum(1.0 - price, 1e-6) - fee_rate,
    )


def infer_q_from_trade_value(
    frame: pd.DataFrame,
    trade_value_pred: pd.Series | np.ndarray,
    *,
    direction_column: str | None = None,
    fee_rate: float = config.FEE_RATE,
) -> np.ndarray:
    resolved_direction = resolve_direction_column(frame, direction_column)
    price = frame["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    direction = frame[resolved_direction].astype(int).values
    trade_values = np.asarray(trade_value_pred, dtype=float)
    q_pred = np.where(
        direction > 0,
        price * (trade_values + 1.0 + fee_rate),
        price - (1.0 - price) * (trade_values + fee_rate),
    )
    return np.clip(q_pred, 0.0, 1.0)
