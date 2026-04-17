from __future__ import annotations

from collections.abc import Mapping
from typing import Any

FEATURE_SEMANTICS_VERSION = "decision_time_v1"
ONLINE_UNAVAILABLE_FEATURES = frozenset({"delta_hours_bucket"})

_DEFAULT_CRITICAL_COLUMNS = (
    "price",
    "horizon_hours",
    "selected_quote_offset_sec",
    "selected_quote_points_in_window",
    "snapshot_quality_score",
    "domain",
    "category",
    "market_type",
    "q_full",
    "direction",
    "group_key",
)


def online_feature_columns(feature_columns: list[str] | tuple[str, ...]) -> list[str]:
    return [column for column in feature_columns if column not in ONLINE_UNAVAILABLE_FEATURES]


def split_feature_contract_columns(
    feature_columns: list[str] | tuple[str, ...],
) -> tuple[list[str], list[str]]:
    critical = [column for column in feature_columns if column in _DEFAULT_CRITICAL_COLUMNS]
    noncritical = [column for column in feature_columns if column not in critical]
    return critical, noncritical


def build_decision_time_quote_features(
    *,
    quote_window: Mapping[str, Any],
    selected_quote_ts_fallback: int | None = None,
) -> dict[str, Any]:
    return {
        "selected_quote_offset_sec": quote_window.get("selected_quote_offset_sec"),
        "selected_quote_points_in_window": quote_window.get("selected_quote_points_in_window"),
        "selected_quote_left_gap_sec": quote_window.get("selected_quote_left_gap_sec"),
        "selected_quote_right_gap_sec": quote_window.get("selected_quote_right_gap_sec"),
        "selected_quote_local_gap_sec": quote_window.get("selected_quote_local_gap_sec"),
        "selected_quote_ts": quote_window.get("selected_quote_ts") or selected_quote_ts_fallback,
        "snapshot_target_ts": quote_window.get("snapshot_target_ts"),
        "selected_quote_side": quote_window.get("selected_quote_side"),
        "stale_quote_flag": quote_window.get("stale_quote_flag"),
        "snapshot_quality_score": quote_window.get("snapshot_quality_score"),
    }


def build_decision_time_history_features(
    *,
    history_features: Mapping[str, Any],
) -> dict[str, Any]:
    return dict(history_features)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def build_market_context_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "market_id": str(row.get("market_id") or row.get("id") or ""),
        "question": str(row.get("question") or ""),
        "description": str(row.get("description") or ""),
        "volume": _to_float(row.get("volume")),
        "liquidity": _to_float(row.get("liquidity")),
        "volume24hr": _to_float(row.get("volume24hr")),
        "volume1wk": _to_float(row.get("volume1wk")),
        "volume24hrClob": _to_float(row.get("volume24hr_clob") or row.get("volume24hrClob")),
        "volume1wkClob": _to_float(row.get("volume1wk_clob") or row.get("volume1wkClob")),
        "orderPriceMinTickSize": _to_float(row.get("order_price_min_tick_size") or row.get("orderPriceMinTickSize"), default=0.001),
        "negRisk": row.get("neg_risk") if row.get("neg_risk") is not None else row.get("negRisk"),
        "rewardsMinSize": _to_float(row.get("rewards_min_size") or row.get("rewardsMinSize")),
        "rewardsMaxSpread": _to_float(row.get("rewards_max_spread") or row.get("rewardsMaxSpread")),
        "bestBid": _to_float(row.get("best_bid") or row.get("bestBid")),
        "bestAsk": _to_float(row.get("best_ask") or row.get("bestAsk")),
        "spread": _to_float(row.get("spread")),
        "lastTradePrice": _to_float(row.get("last_trade_price") or row.get("lastTradePrice")),
        "line": _to_float(row.get("line")),
        "oneHourPriceChange": _to_float(row.get("one_hour_price_change") or row.get("oneHourPriceChange")),
        "oneDayPriceChange": _to_float(row.get("one_day_price_change") or row.get("oneDayPriceChange")),
        "oneWeekPriceChange": _to_float(row.get("one_week_price_change") or row.get("oneWeekPriceChange")),
        "liquidityAmm": _to_float(row.get("liquidity_amm") or row.get("liquidityAmm")),
        "liquidityClob": _to_float(row.get("liquidity_clob") or row.get("liquidityClob")),
        "groupItemTitle": str(row.get("group_item_title") or row.get("groupItemTitle") or "UNKNOWN"),
        "gameId": str(row.get("game_id") or row.get("gameId") or "UNKNOWN"),
        "marketMakerAddress": str(row.get("market_maker_address") or row.get("marketMakerAddress") or "UNKNOWN"),
        "startDate": str(row.get("start_time_utc") or row.get("startDate") or row.get("created_at_utc") or ""),
        "endDate": str(row.get("end_time_utc") or row.get("endDate") or ""),
        "closedTime": str(row.get("end_time_utc") or row.get("closedTime") or ""),
    }


def compute_contract_safe_defaults(
    frame,
    *,
    feature_columns: list[str] | tuple[str, ...],
    categorical_columns: list[str] | tuple[str, ...],
    required_critical_columns: list[str] | tuple[str, ...] = (),
    required_noncritical_columns: list[str] | tuple[str, ...] = (),
):
    out = frame.copy()
    categorical = set(categorical_columns)
    observed_feature_columns = {column for column in feature_columns if column in out.columns}
    missing_critical = [column for column in required_critical_columns if column not in out.columns]
    defaulted_noncritical: list[str] = []
    resolved_noncritical = tuple(required_noncritical_columns or feature_columns)
    for column in resolved_noncritical:
        if column in out.columns or column in missing_critical:
            continue
        out[column] = "UNKNOWN" if column in categorical else 0.0
        defaulted_noncritical.append(column)
    summary = {
        "expected_feature_column_count": len(feature_columns),
        "available_feature_column_count": len(observed_feature_columns),
        "missing_critical_columns": sorted(missing_critical),
        "defaulted_noncritical_columns": sorted(defaulted_noncritical),
        "defaulted_noncritical_count": len(defaulted_noncritical),
    }
    keep_columns = [column for column in feature_columns if column in out.columns]
    aligned = out[keep_columns].copy()
    aligned.attrs["feature_contract_summary"] = summary
    return aligned


def build_decision_time_snapshot_row(
    row: Mapping[str, Any],
    *,
    snapshot_time: Any,
    price: float,
    horizon_hours: float,
    scheduled_end: str,
    quote_window: Mapping[str, Any],
    history_features: Mapping[str, Any],
    source_host: str,
    market_duration_hours: float | None,
    in_price_range: bool | None = None,
    selected_quote_ts_fallback: int | None = None,
    extra_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_date = getattr(snapshot_time, "date", lambda: snapshot_time)()
    out = {
        "market_id": str(row.get("market_id") or ""),
        "price": float(price),
        "horizon_hours": float(horizon_hours),
        "snapshot_time": snapshot_time,
        "snapshot_date": snapshot_date,
        "scheduled_end": scheduled_end,
        "closedTime": scheduled_end,
        "domain": str(row.get("domain") or "UNKNOWN"),
        "category": str(row.get("category") or "UNKNOWN"),
        "market_type": str(row.get("market_type") or "UNKNOWN"),
        "source_host": source_host,
        "primary_outcome": str(row.get("outcome_0_label") or ""),
        "secondary_outcome": str(row.get("outcome_1_label") or ""),
        "market_duration_hours": market_duration_hours,
        "duration_is_negative_flag": bool((market_duration_hours or 0.0) < 0.0) if market_duration_hours is not None else False,
        "duration_below_min_horizon_flag": bool((market_duration_hours or 0.0) < 1.0) if market_duration_hours is not None else False,
        "outcome_0_label": str(row.get("outcome_0_label") or ""),
        "outcome_1_label": str(row.get("outcome_1_label") or ""),
        "token_0_id": str(row.get("token_0_id") or ""),
        "token_1_id": str(row.get("token_1_id") or ""),
        "selected_reference_token_id": str(row.get("selected_reference_token_id") or ""),
        "selected_reference_outcome_label": str(row.get("selected_reference_outcome_label") or ""),
        "selected_reference_side_index": row.get("selected_reference_side_index"),
    }
    out.update(
        build_decision_time_quote_features(
            quote_window=quote_window,
            selected_quote_ts_fallback=selected_quote_ts_fallback,
        )
    )
    if in_price_range is not None:
        out["price_in_range_flag"] = bool(in_price_range)
        out["quality_pass"] = bool(in_price_range)
    out.update(build_decision_time_history_features(history_features=history_features))
    if extra_fields:
        out.update(dict(extra_fields))
    return out
