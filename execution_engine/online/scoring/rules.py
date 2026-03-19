"""Shared execution-parity rule metadata helpers for the online pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULES_CACHE: dict[str, pd.DataFrame] = {}
_HORIZON_PROFILE_CACHE: dict[str, "RuleHorizonProfile"] = {}


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


def load_top_rules_frame(cfg: PegConfig) -> pd.DataFrame:
    return load_rules_frame(cfg)


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
    price_midpoint_tolerance: float | None = None,
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
    if any(column not in rules.columns for column in required_rule_cols):
        return out

    candidate_rows = out.copy()
    candidate_rows["_row_id"] = candidate_rows.index.astype(int)
    candidate_rows[horizon_column] = pd.to_numeric(candidate_rows[horizon_column], errors="coerce")
    candidate_rows[price_column] = pd.to_numeric(candidate_rows[price_column], errors="coerce")

    rule_bins = rules[required_rule_cols].dropna().drop_duplicates().copy()
    merged = candidate_rows.merge(
        rule_bins,
        on=["domain", "category", "market_type"],
        how="inner",
    )
    if merged.empty:
        return out

    horizon_mask = (
        (merged[horizon_column] >= merged["h_min"])
        & (merged[horizon_column] <= merged["h_max"])
    )
    if price_midpoint_tolerance is None:
        price_mask = (
            (merged[price_column] >= merged["price_min"] - 1e-9)
            & (merged[price_column] <= merged["price_max"] + 1e-9)
        )
    else:
        midpoint = (pd.to_numeric(merged["price_min"], errors="coerce") + pd.to_numeric(merged["price_max"], errors="coerce")) / 2.0
        price_mask = (pd.to_numeric(merged[price_column], errors="coerce") - midpoint).abs() <= float(price_midpoint_tolerance) + 1e-9
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
