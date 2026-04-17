"""Live batch inference for the direct online submit pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import sys
from typing import Any, Dict

import pandas as pd

from execution_engine.online.pipeline.eligibility import LiveFilterResult, apply_live_price_filter
from execution_engine.online.pipeline.prewarm import OnlineRuntimeContainer
from execution_engine.online.scoring.price_history import (
    ClobPriceHistoryClient,
    PricePoint,
    build_offline_aligned_history_window,
    build_quote_window_features,
    build_source_host,
    build_historical_price_features,
)
from execution_engine.online.scoring.rule_runtime import (
    FeatureContract,
    RuleModelResult,
    add_rule_match_reasons,
    collapse_rule_hits,
    prepare_feature_inputs,
)
from execution_engine.online.scoring.rules import build_group_default_rule_hits
from execution_engine.runtime.config import PegConfig

LOGGER = logging.getLogger(__name__)
_MODEL_INPUT_BRIDGE_COLUMNS = (
    "market_id",
    "snapshot_time",
    "rule_group_key",
    "rule_leaf_id",
)


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


def _load_snapshot_semantics(cfg: PegConfig):
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)
    from rule_baseline.features.snapshot_semantics import (  # type: ignore
        build_decision_time_snapshot_row,
        build_market_context_projection,
        compute_contract_safe_defaults,
    )

    return build_decision_time_snapshot_row, build_market_context_projection, compute_contract_safe_defaults


def _build_market_feature_context(cfg: PegConfig, frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    if hasattr(cfg, "rule_engine_dir"):
        _, build_market_context_projection, _ = _load_snapshot_semantics(cfg)
        return pd.DataFrame([build_market_context_projection(row) for row in frame.to_dict(orient="records")])

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
    build_decision_time_snapshot_row, _, _ = _load_snapshot_semantics(cfg)
    now = _utc_now()
    now_ts = int(pd.Timestamp(now).timestamp())
    history_client = ClobPriceHistoryClient(cfg)
    rows = []
    for row in frame.to_dict(orient="records"):
        price = _to_float(row.get("live_mid_price"))
        token_state_age_sec = _to_float(row.get("token_state_age_sec"))
        end_dt = _parse_utc(row.get("end_time_utc"))
        end_ts = int(end_dt.timestamp()) if end_dt is not None else now_ts
        history_start_ts, history_end_ts = build_offline_aligned_history_window(end_ts=end_ts)
        try:
            history_points = history_client.fetch_history(
                str(row.get("selected_reference_token_id") or ""),
                start_ts=history_start_ts,
                end_ts=history_end_ts,
                fidelity_minutes=1,
            )
        except Exception:
            history_points = []
        merged_points = list(history_points)
        if price > 0:
            merged_points.append(PricePoint(ts=now_ts, price=price, source="live_token_state"))
        merged_points.sort(key=lambda point: point.ts)
        quote_window = build_quote_window_features(cfg, merged_points=merged_points, target_ts=now_ts)
        history_features = build_historical_price_features(
            current_price=price,
            now_ts=now_ts,
            end_ts=end_ts,
            merged_points=history_points,
        )
        rows.append(
            build_decision_time_snapshot_row(
                row,
                snapshot_time=pd.Timestamp(now),
                price=price,
                horizon_hours=_to_float(row.get("remaining_hours")),
                scheduled_end=str(row.get("end_time_utc") or ""),
                quote_window=quote_window,
                history_features=history_features,
                source_host=build_source_host(
                    source_url=row.get("source_url"),
                    resolution_source=row.get("resolution_source"),
                    domain=row.get("domain"),
                ),
                market_duration_hours=_market_duration_hours(row.get("start_time_utc"), row.get("end_time_utc")),
                extra_fields={
                    "batch_id": str(row.get("batch_id") or ""),
                    "first_seen_at_utc": str(row.get("first_seen_at_utc") or ""),
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
                },
            )
        )
    return pd.DataFrame(rows)


def _ensure_feature_contract(frame: pd.DataFrame, feature_contract: FeatureContract) -> pd.DataFrame:
    from rule_baseline.features.snapshot_semantics import compute_contract_safe_defaults  # type: ignore

    aligned = compute_contract_safe_defaults(
        frame,
        feature_columns=feature_contract.feature_columns,
        categorical_columns=feature_contract.categorical_columns,
        required_critical_columns=getattr(feature_contract, "required_critical_columns", ()),
        required_noncritical_columns=getattr(feature_contract, "required_noncritical_columns", ()) or feature_contract.feature_columns,
    )
    summary = dict(aligned.attrs.get("feature_contract_summary", {}))
    missing_critical = list(summary.get("missing_critical_columns", []))
    defaulted_noncritical = list(summary.get("defaulted_noncritical_columns", []))
    if missing_critical:
        LOGGER.error(
            "Missing critical feature_contract columns in live inference: %s",
            ", ".join(sorted(missing_critical)),
        )
        raise CriticalFeatureContractError(summary)
    if defaulted_noncritical:
        LOGGER.warning(
            "Missing non-critical feature_contract columns defaulted in live inference: %s",
            ", ".join(sorted(defaulted_noncritical)),
        )
    out = frame.copy()
    for column in aligned.columns:
        if column not in out.columns:
            out[column] = aligned[column]
    keep_columns: list[str] = []
    for column in [*_MODEL_INPUT_BRIDGE_COLUMNS, *aligned.columns]:
        if column in out.columns and column not in keep_columns:
            keep_columns.append(column)
    aligned = out[keep_columns].copy()
    aligned.attrs["feature_contract_summary"] = summary
    return aligned


def _predict_from_feature_inputs(
    runtime: OnlineRuntimeContainer,
    rule_hits: pd.DataFrame,
    feature_inputs: pd.DataFrame,
) -> pd.DataFrame:
    predicted = rule_hits.copy()
    predicted["q_pred"] = runtime.model_payload.predict_q(feature_inputs)
    return predicted


def _merge_growth_columns(predicted: pd.DataFrame, viable: pd.DataFrame) -> pd.DataFrame:
    if predicted.empty:
        return predicted.copy()

    model_outputs = predicted.copy()
    growth_columns = [
        "market_id",
        "snapshot_time",
        "rule_group_key",
        "rule_leaf_id",
        "edge_prob",
        "direction_model",
        "edge_final",
        "f_star",
    ]
    if viable.empty:
        for column in growth_columns[4:]:
            if column not in model_outputs.columns:
                model_outputs[column] = None
        return model_outputs

    available_growth_columns = [column for column in growth_columns if column in viable.columns]
    return model_outputs.merge(
        viable[available_growth_columns],
        on=["market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        how="left",
    )


def _build_live_rule_hits(runtime: OnlineRuntimeContainer, snapshots: pd.DataFrame) -> pd.DataFrame:
    exact_hits = runtime.rule_runtime.match_rules(snapshots, runtime.rules_frame)
    if not exact_hits.empty:
        exact_hits = exact_hits.copy()
        exact_hits["rule_match_priority"] = 1
        matched_keys = exact_hits[["market_id", "snapshot_time"]].drop_duplicates()
        remaining = snapshots.merge(
            matched_keys.assign(_matched=True),
            on=["market_id", "snapshot_time"],
            how="left",
        )
        remaining = remaining[remaining["_matched"] != True].drop(columns=["_matched"])
    else:
        remaining = snapshots.copy()

    fallback_hits = build_group_default_rule_hits(remaining, runtime.serving_feature_bundle)
    if exact_hits.empty:
        return fallback_hits.reset_index(drop=True)
    if fallback_hits.empty:
        return exact_hits.reset_index(drop=True)
    return pd.concat([exact_hits, fallback_hits], ignore_index=True, sort=False).reset_index(drop=True)


@dataclass(frozen=True)
class LiveInferenceResult:
    live_filter: LiveFilterResult
    snapshots: pd.DataFrame
    rule_model: RuleModelResult
    feature_contract_summary: dict[str, Any] = field(default_factory=dict)


class CriticalFeatureContractError(RuntimeError):
    def __init__(self, summary: dict[str, Any]):
        self.summary = summary
        missing = ", ".join(summary.get("missing_critical_columns", [])) or "unknown"
        super().__init__(f"Missing critical feature columns: {missing}")


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
            feature_contract_summary={},
        )

    snapshots = _build_live_snapshot_rows(runtime.cfg, live_filter.eligible)
    market_context = _build_market_feature_context(runtime.cfg, live_filter.eligible)
    market_feature_cache = runtime.rule_runtime.build_market_feature_cache(
        market_context,
        _build_market_annotations(live_filter.eligible),
    )
    matched = _build_live_rule_hits(runtime, snapshots)
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
            feature_contract_summary={},
        )

    rule_hits = add_rule_match_reasons(collapse_rule_hits(matched))
    feature_inputs = prepare_feature_inputs(
        rule_hits,
        market_feature_cache,
        runtime.rule_runtime.preprocess_features,
        runtime.serving_feature_bundle,
    )
    try:
        feature_inputs = _ensure_feature_contract(feature_inputs, runtime.feature_contract)
    except CriticalFeatureContractError as exc:
        empty = pd.DataFrame()
        return LiveInferenceResult(
            live_filter=live_filter,
            snapshots=snapshots,
            rule_model=RuleModelResult(
                rule_hits=rule_hits,
                feature_inputs=empty,
                model_outputs=empty,
                viable_candidates=empty,
            ),
            feature_contract_summary=exc.summary,
        )
    feature_contract_summary = dict(feature_inputs.attrs.get("feature_contract_summary", {}))
    predicted = _predict_from_feature_inputs(runtime, rule_hits, feature_inputs)
    viable = runtime.rule_runtime.compute_growth_and_direction(predicted, runtime.rule_runtime.backtest_config)
    model_outputs = _merge_growth_columns(predicted, viable)
    if not viable.empty:
        viable = (
            viable.sort_values(
                by=["market_id", "snapshot_time", "f_star"],
                ascending=[True, True, False],
            )
            .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
            .reset_index(drop=True)
        )
        viable = runtime.rule_runtime.apply_earliest_market_dedup(viable, score_column="f_star")
        viable = viable.sort_values(["batch_id", "f_star"], ascending=[True, False]).reset_index(drop=True)

    return LiveInferenceResult(
        live_filter=live_filter,
        snapshots=snapshots,
        rule_model=RuleModelResult(
            rule_hits=rule_hits,
            feature_inputs=feature_inputs,
            model_outputs=model_outputs,
            viable_candidates=viable,
        ),
        feature_contract_summary=feature_contract_summary,
    )
