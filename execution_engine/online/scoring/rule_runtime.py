"""Thin shared runtime for execution-parity scoring imports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULE_RUNTIME_CACHE: dict[str, "RuleRuntime"] = {}
_MODEL_PAYLOAD_CACHE: dict[str, Any] = {}


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
class FeatureContract:
    feature_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    required_critical_columns: tuple[str, ...] = ()
    required_noncritical_columns: tuple[str, ...] = ()
    optional_debug_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuleModelResult:
    rule_hits: pd.DataFrame
    feature_inputs: pd.DataFrame
    model_outputs: pd.DataFrame
    viable_candidates: pd.DataFrame


def load_model_payload(cfg: PegConfig):
    cache_key = str(cfg.rule_engine_model_path.resolve())
    cached = _MODEL_PAYLOAD_CACHE.get(cache_key)
    if cached is not None:
        return cached
    runtime_manifest = cfg.rule_engine_model_path / "runtime_manifest.json"
    if not cfg.rule_engine_model_path.is_dir() or not runtime_manifest.exists():
        raise FileNotFoundError(
            "execution_engine requires a runtime bundle directory at "
            f"{cfg.rule_engine_model_path} (expected q_model_bundle_deploy with runtime_manifest.json)."
        )
    _ensure_rule_engine_import_path(cfg)
    from rule_baseline.models import load_model_artifact  # type: ignore

    payload = load_model_artifact(cfg.rule_engine_model_path)
    if str(getattr(payload, "target_mode", "")) != "q":
        raise ValueError(
            f"execution_engine live runtime only supports q-model artifacts, got target_mode={payload.target_mode!r}"
        )
    _MODEL_PAYLOAD_CACHE[cache_key] = payload
    return payload


def get_feature_contract(payload) -> FeatureContract:
    contract = payload.feature_contract
    feature_columns = tuple(str(value) for value in contract.feature_columns)
    numeric_columns = tuple(str(value) for value in contract.numeric_columns)
    categorical_columns = tuple(str(value) for value in contract.categorical_columns)
    required_critical_columns = tuple(str(value) for value in getattr(contract, "required_critical_columns", ()))
    required_noncritical_columns = tuple(str(value) for value in getattr(contract, "required_noncritical_columns", ()))
    optional_debug_columns = tuple(str(value) for value in getattr(contract, "optional_debug_columns", ()))
    return FeatureContract(
        feature_columns=feature_columns,
        numeric_columns=numeric_columns,
        categorical_columns=categorical_columns,
        required_critical_columns=required_critical_columns,
        required_noncritical_columns=required_noncritical_columns or feature_columns,
        optional_debug_columns=optional_debug_columns,
    )


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
    scored = frame.copy()
    if "rule_match_priority" not in scored.columns:
        scored["rule_match_priority"] = 1
    else:
        scored["rule_match_priority"] = pd.to_numeric(scored["rule_match_priority"], errors="coerce").fillna(1).astype(int)
    return (
        scored.sort_values(
            by=["market_id", "snapshot_time", "rule_match_priority", "edge_lower_bound_full"],
            ascending=[True, True, False, False],
        )
        .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
        .reset_index(drop=True)
    )


def add_rule_match_reasons(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()

    out = frame.copy()
    if "rule_match_reason" in out.columns:
        existing_reason = out["rule_match_reason"].fillna("").astype(str).str.strip()
    else:
        existing_reason = pd.Series("", index=out.index, dtype="object")

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

    out["rule_match_reason"] = existing_reason.where(existing_reason.ne(""), out.apply(build_reason, axis=1))
    return out


def prepare_feature_inputs(
    matched: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    preprocess_features: Any,
    serving_feature_bundle: Any | None = None,
) -> pd.DataFrame:
    if matched.empty:
        return pd.DataFrame()
    model_input = matched.copy()
    model_input["leaf_id"] = model_input["rule_leaf_id"]
    model_input["direction"] = model_input["rule_direction"]
    model_input["group_key"] = model_input["rule_group_key"]
    if serving_feature_bundle is not None:
        from rule_baseline.features.serving import attach_serving_features  # type: ignore

        model_input = attach_serving_features(
            model_input,
            serving_feature_bundle,
            price_column="price",
            horizon_column="horizon_hours",
        )
    return preprocess_features(model_input, market_feature_cache)


def evaluate_matched_snapshots(
    cfg: PegConfig,
    runtime: RuleRuntime,
    matched: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    rules_frame: pd.DataFrame,
    *,
    payload: Any | None = None,
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
        None,
    )

    resolved_payload = payload if payload is not None else load_model_payload(cfg)
    predicted = runtime.predict_candidates(rule_hits, market_feature_cache, resolved_payload)
    viable = runtime.compute_growth_and_direction(predicted, runtime.backtest_config)

    if viable.empty:
        model_outputs = predicted.copy()
        for column in ["edge_prob", "direction_model", "f_star", "edge_final"]:
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
        "edge_final",
    ]
    model_outputs = predicted.merge(
        viable[growth_columns],
        on=["market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        how="left",
    )
    viable = (
        viable.sort_values(
            by=["market_id", "snapshot_time", "f_star"],
            ascending=[True, True, False],
        )
        .drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
        .reset_index(drop=True)
    )
    viable = runtime.apply_earliest_market_dedup(viable, score_column="f_star")
    viable = viable.sort_values(["batch_id", "f_star"], ascending=[True, False]).reset_index(drop=True)

    return RuleModelResult(
        rule_hits=rule_hits,
        feature_inputs=feature_inputs,
        model_outputs=model_outputs,
        viable_candidates=viable,
    )
