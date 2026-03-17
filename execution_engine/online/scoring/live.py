"""Live batch inference for the direct online submit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

from execution_engine.online.pipeline.eligibility import LiveFilterResult, apply_live_price_filter
from execution_engine.online.pipeline.prewarm import OnlineRuntimeContainer
from execution_engine.online.scoring.price_history import (
    ClobPriceHistoryClient,
    PricePoint,
    build_historical_price_features,
)
from execution_engine.online.scoring.rule_runtime import (
    FeatureContract,
    RuleModelResult,
    add_rule_match_reasons,
    collapse_rule_hits,
    prepare_feature_inputs,
)
from execution_engine.runtime.config import PegConfig


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def _market_duration_hours(start_time_utc: Any, end_time_utc: Any) -> float:
    start_dt = _parse_utc(start_time_utc)
    end_dt = _parse_utc(end_time_utc)
    if start_dt is None or end_dt is None:
        return 0.0
    return round(max((end_dt - start_dt).total_seconds(), 0.0) / 3600.0, 6)


def _build_market_feature_context(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in frame.to_dict(orient="records"):
        rows.append(
            {
                "market_id": str(row.get("market_id") or ""),
                "question": str(row.get("question") or ""),
                "description": str(row.get("description") or ""),
                "volume": _to_float(row.get("volume")),
                "liquidity": _to_float(row.get("liquidity")),
                "volume24hr": _to_float(row.get("volume24hr")),
                "volume1wk": _to_float(row.get("volume1wk")),
                "volume24hrClob": _to_float(row.get("volume24hr_clob")),
                "volume1wkClob": _to_float(row.get("volume1wk_clob")),
                "orderPriceMinTickSize": _to_float(row.get("order_price_min_tick_size"), default=0.001),
                "negRisk": str(row.get("neg_risk") or ""),
                "rewardsMinSize": _to_float(row.get("rewards_min_size")),
                "rewardsMaxSpread": _to_float(row.get("rewards_max_spread")),
                "bestBid": _to_float(row.get("best_bid")),
                "bestAsk": _to_float(row.get("best_ask")),
                "spread": _to_float(row.get("spread")),
                "lastTradePrice": _to_float(row.get("last_trade_price")),
                "line": _to_float(row.get("line")),
                "oneHourPriceChange": _to_float(row.get("one_hour_price_change")),
                "oneDayPriceChange": _to_float(row.get("one_day_price_change")),
                "oneWeekPriceChange": _to_float(row.get("one_week_price_change")),
                "liquidityAmm": _to_float(row.get("liquidity_amm")),
                "liquidityClob": _to_float(row.get("liquidity_clob")),
                "groupItemTitle": str(row.get("group_item_title") or "UNKNOWN"),
                "gameId": str(row.get("game_id") or "UNKNOWN"),
                "marketMakerAddress": str(row.get("market_maker_address") or "UNKNOWN"),
                "startDate": str(row.get("start_time_utc") or row.get("created_at_utc") or ""),
                "endDate": str(row.get("end_time_utc") or ""),
                "closedTime": str(row.get("end_time_utc") or ""),
            }
        )
    return pd.DataFrame(rows)


def _build_market_annotations(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["market_id"])
    return frame[
        [
            "market_id",
            "domain",
            "domain_parsed",
            "sub_domain",
            "source_url",
            "category",
            "category_raw",
            "category_parsed",
            "category_override_flag",
            "market_type",
            "outcome_pattern",
        ]
    ].drop_duplicates(subset=["market_id"]).reset_index(drop=True)


def _build_live_snapshot_rows(cfg: PegConfig, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    now = _utc_now()
    now_ts = int(pd.Timestamp(now).timestamp())
    history_client = ClobPriceHistoryClient(cfg)
    rows = []
    for row in frame.to_dict(orient="records"):
        price = _to_float(row.get("live_mid_price"))
        token_state_age_sec = _to_float(row.get("token_state_age_sec"))
        raw_event_count = max(_to_float(row.get("raw_event_count"), default=1.0), 1.0)
        end_dt = _parse_utc(row.get("end_time_utc"))
        end_ts = int(end_dt.timestamp()) if end_dt is not None else now_ts
        try:
            history_points = history_client.fetch_history(
                str(row.get("selected_reference_token_id") or ""),
                start_ts=now_ts - 24 * 3600,
                end_ts=now_ts,
                fidelity_minutes=1,
            )
        except Exception:
            history_points = []
        merged_points = list(history_points)
        if price > 0:
            merged_points.append(PricePoint(ts=now_ts, price=price, source="live_token_state"))
        merged_points.sort(key=lambda point: point.ts)
        history_features = build_historical_price_features(
            current_price=price,
            now_ts=now_ts,
            end_ts=end_ts,
            merged_points=merged_points,
        )
        quality = max(0.0, 1.0 - token_state_age_sec / max(float(cfg.online_token_state_max_age_sec), 1.0))
        quality *= max(1.0, min(raw_event_count, 10.0))
        rows.append(
            {
                "market_id": str(row.get("market_id") or ""),
                "batch_id": str(row.get("batch_id") or ""),
                "first_seen_at_utc": str(row.get("first_seen_at_utc") or ""),
                "price": price,
                "horizon_hours": _to_float(row.get("remaining_hours")),
                "snapshot_time": pd.Timestamp(now),
                "snapshot_date": pd.Timestamp(now).date(),
                "scheduled_end": str(row.get("end_time_utc") or ""),
                "closedTime": str(row.get("end_time_utc") or ""),
                "delta_hours_bucket": 0.0,
                "selected_quote_offset_sec": token_state_age_sec,
                "selected_quote_points_in_window": raw_event_count,
                "selected_quote_left_gap_sec": 0.0,
                "selected_quote_right_gap_sec": 0.0,
                "selected_quote_local_gap_sec": 0.0,
                "selected_quote_ts": now_ts,
                "snapshot_target_ts": now_ts,
                "selected_quote_side": "reference_token",
                "stale_quote_flag": False,
                "snapshot_quality_score": round(quality, 6),
                "domain": str(row.get("domain") or "UNKNOWN"),
                "category": str(row.get("category") or "UNKNOWN"),
                "market_type": str(row.get("market_type") or "UNKNOWN"),
                "source_host": str(row.get("domain") or "UNKNOWN"),
                "primary_outcome": str(row.get("outcome_0_label") or ""),
                "secondary_outcome": str(row.get("outcome_1_label") or ""),
                "market_duration_hours": _market_duration_hours(row.get("start_time_utc"), row.get("end_time_utc")),
                "outcome_0_label": str(row.get("outcome_0_label") or ""),
                "outcome_1_label": str(row.get("outcome_1_label") or ""),
                "token_0_id": str(row.get("token_0_id") or ""),
                "token_1_id": str(row.get("token_1_id") or ""),
                "selected_reference_token_id": str(row.get("selected_reference_token_id") or ""),
                "selected_reference_outcome_label": str(row.get("selected_reference_outcome_label") or ""),
                "selected_reference_side_index": _to_int(row.get("selected_reference_side_index"), default=0),
                "best_bid": _to_float(row.get("best_bid")),
                "best_ask": _to_float(row.get("best_ask")),
                "mid_price": price,
                "last_trade_price": _to_float(row.get("last_trade_price")),
                "tick_size": _to_float(row.get("tick_size"), default=_to_float(row.get("order_price_min_tick_size"), default=0.001)),
                "liquidity": _to_float(row.get("liquidity")),
                "volume24hr": _to_float(row.get("volume24hr")),
                "token_state_age_sec": token_state_age_sec,
                "remaining_hours": _to_float(row.get("remaining_hours")),
                **history_features,
            }
        )
    return pd.DataFrame(rows)


def _ensure_feature_contract(frame: pd.DataFrame, feature_contract: FeatureContract) -> pd.DataFrame:
    out = frame.copy()
    categorical = set(feature_contract.categorical_columns)
    for column in feature_contract.feature_columns:
        if column in out.columns:
            continue
        out[column] = "UNKNOWN" if column in categorical else 0.0
    return out


def _predict_from_feature_inputs(
    runtime: OnlineRuntimeContainer,
    rule_hits: pd.DataFrame,
    feature_inputs: pd.DataFrame,
) -> pd.DataFrame:
    from rule_baseline.backtesting.backtest_portfolio_qmodel import (  # type: ignore
        compute_trade_value_from_q,
        infer_q_from_trade_value,
    )
    from rule_baseline.models import predict_probabilities, predict_regression  # type: ignore

    contract_inputs = _ensure_feature_contract(feature_inputs, runtime.feature_contract)
    predicted = rule_hits.copy()
    target_mode = runtime.model_payload.get("target_mode", "q")
    if target_mode == "q":
        predicted["q_pred"] = predict_probabilities(runtime.model_payload, contract_inputs)
        predicted["trade_value_pred"] = compute_trade_value_from_q(predicted, predicted["q_pred"].values)
    elif target_mode == "residual_q":
        residual_pred = predict_regression(runtime.model_payload, contract_inputs)
        predicted["q_pred"] = (predicted["price"].astype(float).values + residual_pred).clip(0.0, 1.0)
        predicted["trade_value_pred"] = compute_trade_value_from_q(predicted, predicted["q_pred"].values)
    else:
        predicted["trade_value_pred"] = predict_regression(runtime.model_payload, contract_inputs)
        predicted["q_pred"] = infer_q_from_trade_value(predicted, predicted["trade_value_pred"].values)
    return predicted


@dataclass(frozen=True)
class LiveInferenceResult:
    live_filter: LiveFilterResult
    snapshots: pd.DataFrame
    rule_model: RuleModelResult


def run_live_inference(
    runtime: OnlineRuntimeContainer,
    candidates: pd.DataFrame,
    token_state: pd.DataFrame,
) -> LiveInferenceResult:
    live_filter = apply_live_price_filter(runtime.cfg, candidates, runtime.rules_frame, token_state)
    if live_filter.eligible.empty:
        empty = pd.DataFrame()
        return LiveInferenceResult(
            live_filter=live_filter,
            snapshots=empty,
            rule_model=RuleModelResult(
                rule_hits=empty,
                feature_inputs=empty,
                model_outputs=empty,
                viable_candidates=empty,
            ),
        )

    snapshots = _build_live_snapshot_rows(runtime.cfg, live_filter.eligible)
    market_context = _build_market_feature_context(live_filter.eligible)
    market_feature_cache = runtime.rule_runtime.build_market_feature_cache(
        market_context,
        _build_market_annotations(live_filter.eligible),
    )
    matched = runtime.rule_runtime.match_rules(snapshots, runtime.rules_frame)
    if matched.empty:
        empty = pd.DataFrame()
        return LiveInferenceResult(
            live_filter=live_filter,
            snapshots=snapshots,
            rule_model=RuleModelResult(
                rule_hits=empty,
                feature_inputs=empty,
                model_outputs=empty,
                viable_candidates=empty,
            ),
        )

    rule_hits = add_rule_match_reasons(collapse_rule_hits(matched))
    feature_inputs = prepare_feature_inputs(
        rule_hits,
        market_feature_cache,
        runtime.rule_runtime.preprocess_features,
    )
    predicted = _predict_from_feature_inputs(runtime, rule_hits, feature_inputs)
    viable = runtime.rule_runtime.compute_growth_and_direction(predicted, runtime.rule_runtime.backtest_config)
    if not viable.empty:
        viable = (
            viable.sort_values(
                by=["market_id", "snapshot_time", "growth_score"],
                ascending=[True, True, False],
            )
            .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
            .reset_index(drop=True)
        )
        viable = runtime.rule_runtime.apply_earliest_market_dedup(viable, score_column="growth_score")
        viable = viable.sort_values(["batch_id", "growth_score"], ascending=[True, False]).reset_index(drop=True)

    return LiveInferenceResult(
        live_filter=live_filter,
        snapshots=snapshots,
        rule_model=RuleModelResult(
            rule_hits=rule_hits,
            feature_inputs=feature_inputs,
            model_outputs=predicted,
            viable_candidates=viable,
        ),
    )
