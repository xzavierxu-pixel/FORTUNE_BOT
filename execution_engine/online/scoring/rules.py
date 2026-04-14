"""Shared execution-parity rule metadata helpers for the online pipeline."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import List, Tuple
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULES_CACHE: dict[str, pd.DataFrame] = {}
_HORIZON_PROFILE_CACHE: dict[str, "RuleHorizonProfile"] = {}
_GROUP_SERVING_CACHE: dict[str, pd.DataFrame] = {}
_FINE_SERVING_CACHE: dict[str, pd.DataFrame] = {}
_SERVING_DEFAULTS_CACHE: dict[str, dict] = {}


def _build_rule_horizon_mask(
    merged: pd.DataFrame,
    *,
    horizon_column: str,
) -> pd.Series:
    candidate_horizon = pd.to_numeric(merged[horizon_column], errors="coerce")
    rule_horizon_column = None
    if "horizon_hours_rule" in merged.columns:
        rule_horizon_column = "horizon_hours_rule"
    elif "horizon_hours" in merged.columns and horizon_column != "horizon_hours":
        rule_horizon_column = "horizon_hours"
    if rule_horizon_column is not None and horizon_column == "horizon_hours":
        rule_horizon = pd.to_numeric(merged[rule_horizon_column], errors="coerce")
        exact_mask = candidate_horizon.eq(rule_horizon)
        if exact_mask.any():
            return exact_mask.fillna(False)
    h_min = pd.to_numeric(merged["h_min"], errors="coerce")
    h_max = pd.to_numeric(merged["h_max"], errors="coerce")
    return ((candidate_horizon >= h_min) & (candidate_horizon <= h_max)).fillna(False)


def _ensure_rule_engine_import_path(cfg: PegConfig) -> None:
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)


@dataclass(frozen=True)
class RuleHorizonProfile:
    interval_count: int
    intervals: List[Tuple[float, float]]
    min_horizon_hours: float | None
    max_horizon_hours: float | None


@dataclass(frozen=True)
class ServingFeatureBundle:
    fine_features: pd.DataFrame
    group_features: pd.DataFrame
    defaults_manifest: dict


def load_rules_frame(cfg: PegConfig) -> pd.DataFrame:
    cache_key = str(cfg.rule_engine_rules_path.resolve())
    cached = _RULES_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()
    _ensure_rule_engine_import_path(cfg)
    from rule_baseline.backtesting.backtest_execution_parity import load_rules  # type: ignore

    rules = load_rules(cfg.rule_engine_rules_path).reset_index(drop=True)
    _RULES_CACHE[cache_key] = rules.copy()
    return rules.copy()


def load_group_serving_features_frame(cfg: PegConfig) -> pd.DataFrame:
    cache_key = str(cfg.rule_engine_group_serving_features_path.resolve())
    cached = _GROUP_SERVING_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()
    if not cfg.rule_engine_group_serving_features_path.exists():
        empty = pd.DataFrame(columns=["group_key"])
        _GROUP_SERVING_CACHE[cache_key] = empty.copy()
        return empty
    frame = pd.read_parquet(cfg.rule_engine_group_serving_features_path)
    _GROUP_SERVING_CACHE[cache_key] = frame.copy()
    return frame.copy()


def load_fine_serving_features_frame(cfg: PegConfig) -> pd.DataFrame:
    cache_key = str(cfg.rule_engine_fine_serving_features_path.resolve())
    cached = _FINE_SERVING_CACHE.get(cache_key)
    if cached is not None:
        return cached.copy()
    if not cfg.rule_engine_fine_serving_features_path.exists():
        empty = pd.DataFrame(columns=["group_key", "price_bin", "horizon_hours"])
        _FINE_SERVING_CACHE[cache_key] = empty.copy()
        return empty
    frame = pd.read_parquet(cfg.rule_engine_fine_serving_features_path)
    _FINE_SERVING_CACHE[cache_key] = frame.copy()
    return frame.copy()


def load_serving_feature_defaults(cfg: PegConfig) -> dict:
    cache_key = str(cfg.rule_engine_serving_defaults_path.resolve())
    cached = _SERVING_DEFAULTS_CACHE.get(cache_key)
    if cached is not None:
        return dict(cached)
    if not cfg.rule_engine_serving_defaults_path.exists():
        payload = {"fine_feature_defaults": {}, "indicator_defaults": {}}
        _SERVING_DEFAULTS_CACHE[cache_key] = payload
        return dict(payload)
    with cfg.rule_engine_serving_defaults_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    _SERVING_DEFAULTS_CACHE[cache_key] = payload
    return dict(payload)


def load_serving_feature_bundle(cfg: PegConfig) -> ServingFeatureBundle:
    return ServingFeatureBundle(
        fine_features=load_fine_serving_features_frame(cfg),
        group_features=load_group_serving_features_frame(cfg),
        defaults_manifest=load_serving_feature_defaults(cfg),
    )


def attach_serving_features(
    frame: pd.DataFrame,
    bundle: ServingFeatureBundle,
    *,
    price_column: str,
    horizon_column: str,
) -> pd.DataFrame:
    from rule_baseline.features.serving import attach_serving_features as _attach_serving_features  # type: ignore

    return _attach_serving_features(
        frame,
        bundle,
        price_column=price_column,
        horizon_column=horizon_column,
    )


def load_top_rules_frame(cfg: PegConfig) -> pd.DataFrame:
    return load_rules_frame(cfg)


def score_frame_group_rule_coverage(frame: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy().reset_index(drop=True)
    if out.empty:
        out["group_rule_coverage_match_count"] = pd.Series(dtype="int64")
        out["group_rule_coverage_exact_match"] = pd.Series(dtype=bool)
        out["group_rule_key"] = pd.Series(dtype="object")
        return out

    out["group_rule_coverage_match_count"] = 0
    out["group_rule_coverage_exact_match"] = False
    out["group_rule_key"] = (
        out["domain"].astype(str) + "|" + out["category"].astype(str) + "|" + out["market_type"].astype(str)
    )
    if rules.empty:
        return out

    required_rule_cols = ["domain", "category", "market_type"]
    if any(column not in rules.columns for column in required_rule_cols):
        return out

    family_keys = (
        rules[required_rule_cols]
        .dropna()
        .assign(
            group_rule_key=lambda df: (
                df["domain"].astype(str) + "|" + df["category"].astype(str) + "|" + df["market_type"].astype(str)
            )
        )
        ["group_rule_key"]
        .value_counts()
    )
    out["group_rule_coverage_match_count"] = out["group_rule_key"].map(family_keys).fillna(0).astype(int)
    out["group_rule_coverage_exact_match"] = out["group_rule_coverage_match_count"] > 0
    return out


def filter_frame_by_group_rule_coverage(frame: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    scored = score_frame_group_rule_coverage(frame, rules)
    if scored.empty or "group_rule_coverage_exact_match" not in scored.columns:
        return scored
    return scored[scored["group_rule_coverage_exact_match"]].copy().reset_index(drop=True)


def load_rule_horizon_profile(cfg: PegConfig) -> RuleHorizonProfile:
    cache_key = str(cfg.rule_engine_rules_path.resolve())
    cached = _HORIZON_PROFILE_CACHE.get(cache_key)
    if cached is not None:
        return cached
    rules = load_rules_frame(cfg)
    if rules.empty:
        profile = RuleHorizonProfile(interval_count=0, intervals=[], min_horizon_hours=None, max_horizon_hours=None)
        _HORIZON_PROFILE_CACHE[cache_key] = profile
        return profile

    intervals = (
        rules[["h_min", "h_max"]]
        .dropna()
        .drop_duplicates()
        .sort_values(by=["h_min", "h_max"])
        .itertuples(index=False, name=None)
    )
    normalized = [(float(h_min), float(h_max)) for h_min, h_max in intervals]
    if not normalized:
        profile = RuleHorizonProfile(interval_count=0, intervals=[], min_horizon_hours=None, max_horizon_hours=None)
        _HORIZON_PROFILE_CACHE[cache_key] = profile
        return profile
    profile = RuleHorizonProfile(
        interval_count=len(normalized),
        intervals=normalized,
        min_horizon_hours=min(item[0] for item in normalized),
        max_horizon_hours=max(item[1] for item in normalized),
    )
    _HORIZON_PROFILE_CACHE[cache_key] = profile
    return profile


def filter_frame_by_rule_horizons(
    frame: pd.DataFrame,
    profile: RuleHorizonProfile,
    *,
    horizon_column: str,
) -> pd.DataFrame:
    if frame.empty or not profile.intervals or horizon_column not in frame.columns:
        return frame.copy()
    horizon_series = pd.to_numeric(frame[horizon_column], errors="coerce")
    mask = pd.Series(False, index=frame.index)
    for h_min, h_max in profile.intervals:
        mask |= (horizon_series >= h_min) & (horizon_series <= h_max)
    return frame[mask].copy().reset_index(drop=True)


def score_frame_rule_coverage(
    frame: pd.DataFrame,
    rules: pd.DataFrame,
    *,
    horizon_column: str,
    price_column: str,
) -> pd.DataFrame:
    out = frame.copy().reset_index(drop=True)
    if out.empty:
        out["rule_coverage_match_count"] = pd.Series(dtype="int64")
        out["rule_coverage_exact_match"] = pd.Series(dtype=bool)
        return out

    out["rule_coverage_match_count"] = 0
    out["rule_coverage_exact_match"] = False
    if rules.empty or horizon_column not in out.columns or price_column not in out.columns:
        return out

    required_rule_cols = ["domain", "category", "market_type", "h_min", "h_max", "price_min", "price_max"]
    optional_rule_cols = ["horizon_hours"]
    if any(column not in rules.columns for column in required_rule_cols):
        return out

    candidate_rows = out.copy()
    candidate_rows["_row_id"] = candidate_rows.index.astype(int)
    candidate_rows[horizon_column] = pd.to_numeric(candidate_rows[horizon_column], errors="coerce")
    candidate_rows[price_column] = pd.to_numeric(candidate_rows[price_column], errors="coerce")

    rule_bins = rules[required_rule_cols + [column for column in optional_rule_cols if column in rules.columns]].dropna().drop_duplicates().copy()
    merged = candidate_rows.merge(
        rule_bins,
        on=["domain", "category", "market_type"],
        how="inner",
        suffixes=("", "_rule"),
    )
    if merged.empty:
        return out

    horizon_mask = _build_rule_horizon_mask(merged, horizon_column=horizon_column)
    price_mask = (
        (merged[price_column] >= merged["price_min"] - 1e-9)
        & (merged[price_column] <= merged["price_max"] + 1e-9)
    )
    mask = horizon_mask & price_mask
    merged = merged.loc[mask, ["_row_id"]]
    if merged.empty:
        return out

    counts = merged.groupby("_row_id").size()
    out["rule_coverage_match_count"] = out.index.to_series().map(counts).fillna(0).astype(int)
    out["rule_coverage_exact_match"] = out["rule_coverage_match_count"] > 0
    return out


def filter_frame_by_rule_coverage(
    frame: pd.DataFrame,
    rules: pd.DataFrame,
    *,
    horizon_column: str,
    price_column: str,
) -> pd.DataFrame:
    scored = score_frame_rule_coverage(
        frame,
        rules,
        horizon_column=horizon_column,
        price_column=price_column,
    )
    if scored.empty or "rule_coverage_exact_match" not in scored.columns:
        return scored
    return scored[scored["rule_coverage_exact_match"]].copy().reset_index(drop=True)


def build_group_default_rule_hits(
    frame: pd.DataFrame,
    bundle: ServingFeatureBundle,
) -> pd.DataFrame:
    out = frame.copy().reset_index(drop=True)
    if out.empty:
        return out
    group_features = bundle.group_features.copy()
    if group_features.empty or "group_key" not in group_features.columns:
        return pd.DataFrame(columns=list(out.columns))

    out["rule_group_key"] = out["domain"].astype(str) + "|" + out["category"].astype(str) + "|" + out["market_type"].astype(str)
    merge_columns = [
        "group_key",
        "group_decision",
        "group_default_leaf_id",
        "group_default_direction",
        "group_default_q_full",
        "group_default_p_full",
        "group_default_edge_full",
        "group_default_edge_std_full",
        "group_default_edge_lower_bound_full",
        "group_default_rule_score",
        "group_default_n_full",
        "group_unique_markets",
        "group_snapshot_rows",
        "group_median_logloss",
        "group_median_brier",
        "global_group_logloss_q25",
        "global_group_brier_q25",
    ]
    available_columns = [column for column in merge_columns if column in group_features.columns]
    merged = out.merge(
        group_features[available_columns],
        left_on="rule_group_key",
        right_on="group_key",
        how="left",
    )
    if "group_decision" in merged.columns:
        merged = merged[merged["group_decision"].astype(str).eq("keep")].copy()
    merged = merged[merged["group_default_direction"].notna()].copy() if "group_default_direction" in merged.columns else merged.iloc[0:0].copy()
    if merged.empty:
        return merged

    def numeric_series(column: str) -> pd.Series:
        if column in merged.columns:
            return pd.to_numeric(merged[column], errors="coerce")
        return pd.Series(pd.NA, index=merged.index, dtype="float64")

    merged["rule_leaf_id"] = -1
    merged["rule_leaf_key"] = merged.get("group_default_leaf_id", pd.Series("", index=merged.index)).astype(str)
    merged["rule_direction"] = numeric_series("group_default_direction").fillna(0).astype(int)
    merged["q_full"] = numeric_series("group_default_q_full")
    merged["p_full"] = numeric_series("group_default_p_full")
    merged["edge_full"] = numeric_series("group_default_edge_full")
    merged["edge_std_full"] = numeric_series("group_default_edge_std_full")
    merged["edge_lower_bound_full"] = numeric_series("group_default_edge_lower_bound_full")
    merged["rule_score"] = numeric_series("group_default_rule_score")
    merged["n_full"] = numeric_series("group_default_n_full")
    merged["rule_match_priority"] = 0
    merged["rule_match_reason"] = "group_default_fallback"
    return merged.reset_index(drop=True)
