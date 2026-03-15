"""Thin shared runtime for execution-parity scoring imports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import sys

import joblib
import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULE_RUNTIME_CACHE: dict[str, "RuleRuntime"] = {}


def _ensure_rule_engine_import_path(cfg: PegConfig) -> None:
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)


@dataclass(frozen=True)
class RuleRuntime:
    backtest_config: Any
    apply_earliest_market_dedup: Any
    build_market_feature_cache: Any
    compute_growth_and_direction: Any
    match_rules: Any
    predict_candidates: Any
    preprocess_features: Any

@dataclass(frozen=True)
class RuleModelResult:
    rule_hits: pd.DataFrame
    feature_inputs: pd.DataFrame
    model_outputs: pd.DataFrame
    viable_candidates: pd.DataFrame


def load_rule_runtime(cfg: PegConfig) -> RuleRuntime:
    cache_key = str(cfg.rule_engine_dir.resolve())
    cached = _RULE_RUNTIME_CACHE.get(cache_key)
    if cached is not None:
        return cached

    _ensure_rule_engine_import_path(cfg)
    from rule_baseline.backtesting.backtest_execution_parity import (  # type: ignore
        ExecutionParityConfig,
        compute_growth_and_direction,
        match_rules,
        predict_candidates,
    )
    from rule_baseline.datasets.snapshots import apply_earliest_market_dedup  # type: ignore
    from rule_baseline.features import build_market_feature_cache, preprocess_features  # type: ignore

    parity_cfg = ExecutionParityConfig()

    runtime = RuleRuntime(
        backtest_config=parity_cfg,
        apply_earliest_market_dedup=apply_earliest_market_dedup,
        build_market_feature_cache=build_market_feature_cache,
        compute_growth_and_direction=compute_growth_and_direction,
        match_rules=match_rules,
        predict_candidates=predict_candidates,
        preprocess_features=preprocess_features,
    )
    _RULE_RUNTIME_CACHE[cache_key] = runtime
    return runtime


def collapse_rule_hits(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    return (
        frame.sort_values(
            by=["market_id", "snapshot_time", "rule_score"],
            ascending=[True, True, False],
        )
        .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
        .reset_index(drop=True)
    )


def add_rule_match_reasons(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    out = frame.copy()

    def build_reason(row: pd.Series) -> str:
        domain = str(row.get("domain") or "UNKNOWN")
        category = str(row.get("category") or "UNKNOWN")
        market_type = str(row.get("market_type") or "UNKNOWN")
        horizon = float(pd.to_numeric(pd.Series([row.get("horizon_hours")]), errors="coerce").fillna(0.0).iloc[0])
        price = float(pd.to_numeric(pd.Series([row.get("price")]), errors="coerce").fillna(0.0).iloc[0])
        h_min = float(pd.to_numeric(pd.Series([row.get("h_min")]), errors="coerce").fillna(0.0).iloc[0])
        h_max = float(pd.to_numeric(pd.Series([row.get("h_max")]), errors="coerce").fillna(0.0).iloc[0])
        price_min = float(pd.to_numeric(pd.Series([row.get("price_min")]), errors="coerce").fillna(0.0).iloc[0])
        price_max = float(pd.to_numeric(pd.Series([row.get("price_max")]), errors="coerce").fillna(0.0).iloc[0])
        return (
            f"domain={domain}; category={category}; market_type={market_type}; "
            f"horizon_hours={horizon:.6f} in [{h_min:.6f}, {h_max:.6f}]; "
            f"price={price:.6f} in [{price_min:.6f}, {price_max:.6f}]"
        )

    out["rule_match_reason"] = out.apply(build_reason, axis=1)
    return out


def prepare_feature_inputs(
    matched: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    preprocess_features: Any,
) -> pd.DataFrame:
    if matched.empty:
        return pd.DataFrame()
    model_input = matched.copy()
    model_input["leaf_id"] = model_input["rule_leaf_id"]
    model_input["direction"] = model_input["rule_direction"]
    model_input["group_key"] = model_input["rule_group_key"]
    return preprocess_features(model_input, market_feature_cache)


def evaluate_matched_snapshots(
    cfg: PegConfig,
    runtime: RuleRuntime,
    matched: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    rules_frame: pd.DataFrame,
) -> RuleModelResult:
    _ = rules_frame
    if matched.empty:
        empty = pd.DataFrame()
        return RuleModelResult(
            rule_hits=empty,
            feature_inputs=empty,
            model_outputs=empty,
            viable_candidates=empty,
        )

    rule_hits = add_rule_match_reasons(collapse_rule_hits(matched))
    feature_inputs = prepare_feature_inputs(
        rule_hits,
        market_feature_cache,
        runtime.preprocess_features,
    )

    payload = joblib.load(cfg.rule_engine_model_path)
    predicted = runtime.predict_candidates(rule_hits, market_feature_cache, payload)
    viable = runtime.compute_growth_and_direction(predicted, runtime.backtest_config)

    if viable.empty:
        model_outputs = predicted.copy()
        for column in ["edge_prob", "direction_model", "f_star", "f_exec", "g_net", "growth_score"]:
            if column not in model_outputs.columns:
                model_outputs[column] = None
        return RuleModelResult(
            rule_hits=rule_hits,
            feature_inputs=feature_inputs,
            model_outputs=model_outputs,
            viable_candidates=viable,
        )

    growth_columns = [
        "market_id",
        "snapshot_time",
        "rule_group_key",
        "rule_leaf_id",
        "edge_prob",
        "direction_model",
        "f_star",
        "f_exec",
        "g_net",
        "growth_score",
    ]
    model_outputs = predicted.merge(
        viable[growth_columns],
        on=["market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        how="left",
    )
    viable = (
        viable.sort_values(
            by=["market_id", "snapshot_time", "growth_score"],
            ascending=[True, True, False],
        )
        .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
        .reset_index(drop=True)
    )
    viable = runtime.apply_earliest_market_dedup(viable, score_column="growth_score")
    viable = viable.sort_values(["batch_id", "growth_score"], ascending=[True, False]).reset_index(drop=True)

    return RuleModelResult(
        rule_hits=rule_hits,
        feature_inputs=feature_inputs,
        model_outputs=model_outputs,
        viable_candidates=viable,
    )
