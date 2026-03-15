"""Shared universe eligibility evaluation for the online pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Set

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.scoring.annotations import apply_online_market_annotations
from execution_engine.online.scoring.rules import (
    filter_frame_by_rule_horizons,
    load_rules_frame,
    load_rule_horizon_profile,
    score_frame_rule_coverage,
)
from execution_engine.online.scoring.snapshot_builder import refresh_live_universe_view


@dataclass(frozen=True)
class EligibleUniverseResult:
    frame: pd.DataFrame
    source_market_count: int
    live_universe_market_count: int
    live_universe_filter_breakdown: Dict[str, int]
    after_state_filter_count: int
    horizon_eligible_count: int
    rule_coverage_eligible_count: int


def _build_reference_price(frame: pd.DataFrame) -> pd.Series:
    reference_price = pd.Series(0.0, index=frame.index)
    best_bid = pd.to_numeric(frame.get("best_bid"), errors="coerce").fillna(0.0)
    best_ask = pd.to_numeric(frame.get("best_ask"), errors="coerce").fillna(0.0)
    last_trade = pd.to_numeric(frame.get("last_trade_price"), errors="coerce").fillna(0.0)

    has_book = (best_bid > 0) & (best_ask > 0) & (best_ask >= best_bid)
    reference_price.loc[has_book] = (best_bid.loc[has_book] + best_ask.loc[has_book]) / 2.0
    fallback_mask = (reference_price <= 0) & (last_trade > 0)
    reference_price.loc[fallback_mask] = last_trade.loc[fallback_mask]
    return reference_price


def evaluate_online_universe(
    cfg: PegConfig,
    universe: pd.DataFrame,
    *,
    excluded_market_ids: Set[str] | None = None,
) -> EligibleUniverseResult:
    source_market_count = int(len(universe))
    live_universe, live_universe_breakdown = refresh_live_universe_view(
        universe,
        window_hours=cfg.online_universe_window_hours,
    )
    live_universe_market_count = int(len(live_universe))
    if live_universe.empty:
        return EligibleUniverseResult(
            frame=live_universe,
            source_market_count=source_market_count,
            live_universe_market_count=live_universe_market_count,
            live_universe_filter_breakdown=live_universe_breakdown,
            after_state_filter_count=0,
            horizon_eligible_count=0,
            rule_coverage_eligible_count=0,
        )

    annotated = apply_online_market_annotations(cfg, live_universe)
    if excluded_market_ids:
        annotated = annotated[
            ~annotated["market_id"].astype(str).isin(excluded_market_ids)
        ].copy()
    after_state_filter_count = int(len(annotated))
    if annotated.empty:
        return EligibleUniverseResult(
            frame=annotated.reset_index(drop=True),
            source_market_count=source_market_count,
            live_universe_market_count=live_universe_market_count,
            live_universe_filter_breakdown=live_universe_breakdown,
            after_state_filter_count=after_state_filter_count,
            horizon_eligible_count=0,
            rule_coverage_eligible_count=0,
        )

    horizon_eligible = filter_frame_by_rule_horizons(
        annotated,
        load_rule_horizon_profile(cfg),
        horizon_column="remaining_hours",
    )
    horizon_eligible_count = int(len(horizon_eligible))
    if horizon_eligible.empty:
        return EligibleUniverseResult(
            frame=horizon_eligible.reset_index(drop=True),
            source_market_count=source_market_count,
            live_universe_market_count=live_universe_market_count,
            live_universe_filter_breakdown=live_universe_breakdown,
            after_state_filter_count=after_state_filter_count,
            horizon_eligible_count=horizon_eligible_count,
            rule_coverage_eligible_count=0,
        )

    scored = horizon_eligible.copy()
    scored["rule_reference_price"] = _build_reference_price(scored)
    scored = score_frame_rule_coverage(
        scored,
        load_rules_frame(cfg),
        horizon_column="remaining_hours",
        price_column="rule_reference_price",
    )
    covered = scored[scored["rule_coverage_exact_match"]].copy().reset_index(drop=True)
    rule_coverage_eligible_count = int(len(covered))
    if cfg.online_require_rule_coverage:
        eligible = covered
    elif not covered.empty:
        eligible = covered
    else:
        eligible = scored

    if "remaining_hours" in eligible.columns:
        eligible["remaining_hours_num"] = pd.to_numeric(eligible["remaining_hours"], errors="coerce").fillna(999999.0)
        eligible = eligible.sort_values(
            by=["rule_coverage_match_count", "remaining_hours_num", "end_time_utc", "market_id"],
            ascending=[False, True, True, True],
        ).drop(columns=["remaining_hours_num"])

    return EligibleUniverseResult(
        frame=eligible.reset_index(drop=True),
        source_market_count=source_market_count,
        live_universe_market_count=live_universe_market_count,
        live_universe_filter_breakdown=live_universe_breakdown,
        after_state_filter_count=after_state_filter_count,
        horizon_eligible_count=horizon_eligible_count,
        rule_coverage_eligible_count=rule_coverage_eligible_count,
    )


