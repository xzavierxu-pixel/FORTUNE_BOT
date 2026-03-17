"""Two-stage filtering for the direct online submit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Set

import pandas as pd

from execution_engine.online.scoring.rules import score_frame_rule_coverage
from execution_engine.runtime.config import PegConfig

STRUCTURAL_REJECT = "STRUCTURAL_REJECT"
STATE_REJECT = "STATE_REJECT"
DIRECT_CANDIDATE = "DIRECT_CANDIDATE"

LIVE_ELIGIBLE = "LIVE_ELIGIBLE"
LIVE_PRICE_MISS = "LIVE_PRICE_MISS"
LIVE_STATE_MISSING = "LIVE_STATE_MISSING"
LIVE_STATE_STALE = "LIVE_STATE_STALE"
INVALID_PRICE = "INVALID_PRICE"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
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


def _best_live_mid(row: Dict[str, Any]) -> float:
    mid = _to_float(row.get("mid_price"))
    if mid > 0:
        return mid
    best_bid = _to_float(row.get("best_bid"))
    best_ask = _to_float(row.get("best_ask"))
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        return round((best_bid + best_ask) / 2.0, 6)
    return _to_float(row.get("last_trade_price"))


@dataclass(frozen=True)
class StructuralFilterResult:
    direct_candidates: pd.DataFrame
    rejected: pd.DataFrame
    state_counts: Dict[str, int]


@dataclass(frozen=True)
class LiveFilterResult:
    eligible: pd.DataFrame
    rejected: pd.DataFrame
    state_counts: Dict[str, int]


def _score_reason_counts(frame: pd.DataFrame, column: str) -> Dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {
        str(key): int(value)
        for key, value in frame[column].astype(str).value_counts().sort_index().items()
    }


def apply_structural_coarse_filter(
    cfg: PegConfig,
    markets: pd.DataFrame,
    rules_frame: pd.DataFrame,
    *,
    excluded_market_ids: Set[str] | None = None,
) -> StructuralFilterResult:
    if markets.empty:
        empty = markets.copy()
        if "coarse_filter_state" not in empty.columns:
            empty["coarse_filter_state"] = pd.Series(dtype=str)
        if "coarse_filter_reason" not in empty.columns:
            empty["coarse_filter_reason"] = pd.Series(dtype=str)
        return StructuralFilterResult(direct_candidates=empty, rejected=empty, state_counts={})

    excluded_ids = {str(value) for value in (excluded_market_ids or set()) if str(value)}
    candidates = markets.copy().reset_index(drop=True)
    candidates["remaining_hours"] = pd.to_numeric(candidates.get("remaining_hours"), errors="coerce")
    candidates["coarse_filter_state"] = DIRECT_CANDIDATE
    candidates["coarse_filter_reason"] = "rule_family_horizon_match"

    if "market_id" not in candidates.columns:
        candidates["market_id"] = ""
    if "accepting_orders" not in candidates.columns:
        candidates["accepting_orders"] = True

    missing_end_mask = candidates.get("end_time_utc", pd.Series("", index=candidates.index)).astype(str).str.strip().eq("")
    accepting_orders = candidates["accepting_orders"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "on"})
    remaining = candidates["remaining_hours"]
    slack = max(float(cfg.online_coarse_horizon_slack_hours), 0.0)
    configured_horizon_limit = float(cfg.online_universe_window_hours)
    if not rules_frame.empty and "h_max" in rules_frame.columns:
        rule_horizon_limit = pd.to_numeric(rules_frame["h_max"], errors="coerce").dropna()
        if not rule_horizon_limit.empty:
            configured_horizon_limit = max(configured_horizon_limit, float(rule_horizon_limit.max()))

    candidates.loc[missing_end_mask, ["coarse_filter_state", "coarse_filter_reason"]] = [
        STRUCTURAL_REJECT,
        "missing_end_time",
    ]
    candidates.loc[(remaining <= 0) & candidates["coarse_filter_state"].eq(DIRECT_CANDIDATE), ["coarse_filter_state", "coarse_filter_reason"]] = [
        STRUCTURAL_REJECT,
        "expired_market",
    ]
    outside_horizon_mask = remaining > (configured_horizon_limit + slack)
    candidates.loc[
        outside_horizon_mask & candidates["coarse_filter_state"].eq(DIRECT_CANDIDATE),
        ["coarse_filter_state", "coarse_filter_reason"],
    ] = [STRUCTURAL_REJECT, "outside_trading_horizon"]
    candidates.loc[
        (~accepting_orders) & candidates["coarse_filter_state"].eq(DIRECT_CANDIDATE),
        ["coarse_filter_state", "coarse_filter_reason"],
    ] = [STRUCTURAL_REJECT, "accepting_orders_false"]
    candidates.loc[
        candidates["market_id"].astype(str).isin(excluded_ids) & candidates["coarse_filter_state"].eq(DIRECT_CANDIDATE),
        ["coarse_filter_state", "coarse_filter_reason"],
    ] = [STATE_REJECT, "open_or_pending_market"]

    active = candidates[candidates["coarse_filter_state"] == DIRECT_CANDIDATE].copy()
    if not active.empty and not rules_frame.empty:
        rule_families = rules_frame[["domain", "category", "market_type"]].dropna().drop_duplicates().copy()
        family_keys = set(
            zip(
                rule_families["domain"].astype(str),
                rule_families["category"].astype(str),
                rule_families["market_type"].astype(str),
            )
        )
        active["rule_family_key"] = list(
            zip(
                active["domain"].astype(str),
                active["category"].astype(str),
                active["market_type"].astype(str),
            )
        )
        family_match = active["rule_family_key"].isin(family_keys)
        active.loc[~family_match, ["coarse_filter_state", "coarse_filter_reason"]] = [
            STRUCTURAL_REJECT,
            "rule_family_miss",
        ]

        active_horizon = active[active["coarse_filter_state"] == DIRECT_CANDIDATE].copy()
        if not active_horizon.empty:
            rule_bins = rules_frame[
                ["domain", "category", "market_type", "h_min", "h_max"]
            ].dropna().drop_duplicates()
            merged = active_horizon.merge(
                rule_bins,
                on=["domain", "category", "market_type"],
                how="left",
            )
            horizon_match = (
                (pd.to_numeric(merged["remaining_hours"], errors="coerce") >= pd.to_numeric(merged["h_min"], errors="coerce") - slack)
                & (pd.to_numeric(merged["remaining_hours"], errors="coerce") <= pd.to_numeric(merged["h_max"], errors="coerce") + slack)
            )
            matched_market_ids = set(merged.loc[horizon_match, "market_id"].astype(str))
            miss_mask = ~active_horizon["market_id"].astype(str).isin(matched_market_ids)
            active_horizon.loc[miss_mask, ["coarse_filter_state", "coarse_filter_reason"]] = [
                STRUCTURAL_REJECT,
                "rule_horizon_miss",
            ]
            active.loc[active_horizon.index, ["coarse_filter_state", "coarse_filter_reason"]] = active_horizon[
                ["coarse_filter_state", "coarse_filter_reason"]
            ]

        candidates.loc[active.index, ["coarse_filter_state", "coarse_filter_reason"]] = active[
            ["coarse_filter_state", "coarse_filter_reason"]
        ]

    direct_candidates = candidates[candidates["coarse_filter_state"] == DIRECT_CANDIDATE].copy().reset_index(drop=True)
    rejected = candidates[candidates["coarse_filter_state"] != DIRECT_CANDIDATE].copy().reset_index(drop=True)
    return StructuralFilterResult(
        direct_candidates=direct_candidates,
        rejected=rejected,
        state_counts=_score_reason_counts(candidates, "coarse_filter_state"),
    )


def apply_live_price_filter(
    cfg: PegConfig,
    candidates: pd.DataFrame,
    rules_frame: pd.DataFrame,
    token_state: pd.DataFrame,
) -> LiveFilterResult:
    if candidates.empty:
        empty = candidates.copy()
        if "live_filter_state" not in empty.columns:
            empty["live_filter_state"] = pd.Series(dtype=str)
        if "live_filter_reason" not in empty.columns:
            empty["live_filter_reason"] = pd.Series(dtype=str)
        return LiveFilterResult(eligible=empty, rejected=empty, state_counts={})

    token_state_by_token = {
        str(row.get("token_id") or ""): row
        for row in token_state.to_dict(orient="records")
        if str(row.get("token_id") or "")
    }
    now = _utc_now()
    stage2_rows = []
    provisional_states: Dict[str, str] = {}
    for row in candidates.to_dict(orient="records"):
        token_id = str(row.get("selected_reference_token_id") or "")
        market_id = str(row.get("market_id") or "")
        state_row = token_state_by_token.get(token_id)
        enriched = dict(row)
        enriched["live_filter_state"] = LIVE_ELIGIBLE
        enriched["live_filter_reason"] = "live_rule_match"
        enriched["token_state_age_sec"] = None
        if state_row is None:
            enriched["live_filter_state"] = LIVE_STATE_MISSING
            enriched["live_filter_reason"] = "missing_live_state"
            stage2_rows.append(enriched)
            provisional_states[market_id] = LIVE_STATE_MISSING
            continue

        latest_event_at = _parse_utc(state_row.get("latest_event_at_utc"))
        age_sec = None if latest_event_at is None else max((now - latest_event_at).total_seconds(), 0.0)
        enriched["token_state_age_sec"] = age_sec
        if age_sec is None:
            enriched["live_filter_state"] = LIVE_STATE_MISSING
            enriched["live_filter_reason"] = "missing_live_timestamp"
            stage2_rows.append(enriched)
            provisional_states[market_id] = LIVE_STATE_MISSING
            continue
        if age_sec > float(cfg.online_token_state_max_age_sec):
            enriched["live_filter_state"] = LIVE_STATE_STALE
            enriched["live_filter_reason"] = "stale_live_state"
            stage2_rows.append(enriched)
            provisional_states[market_id] = LIVE_STATE_STALE
            continue

        enriched["best_bid"] = _to_float(state_row.get("best_bid"), default=_to_float(row.get("best_bid")))
        enriched["best_ask"] = _to_float(state_row.get("best_ask"), default=_to_float(row.get("best_ask")))
        enriched["last_trade_price"] = _to_float(
            state_row.get("last_trade_price"),
            default=_to_float(row.get("last_trade_price")),
        )
        enriched["mid_price"] = _best_live_mid(state_row)
        enriched["raw_event_count"] = _to_float(state_row.get("raw_event_count"), default=1.0)
        enriched["tick_size"] = _to_float(
            state_row.get("tick_size"),
            default=_to_float(row.get("order_price_min_tick_size"), default=0.001),
        )
        live_mid = _to_float(enriched.get("mid_price"))
        if not (float(cfg.rule_engine_min_price) < live_mid < float(cfg.rule_engine_max_price)):
            enriched["live_filter_state"] = INVALID_PRICE
            enriched["live_filter_reason"] = "invalid_live_mid_price"
            stage2_rows.append(enriched)
            provisional_states[market_id] = INVALID_PRICE
            continue

        enriched["live_mid_price"] = live_mid
        stage2_rows.append(enriched)
        provisional_states[market_id] = LIVE_ELIGIBLE

    stage2 = pd.DataFrame(stage2_rows)
    if not stage2.empty:
        stage2["_stage2_row_id"] = stage2.index.astype(int)
    live_rule_inputs = stage2[stage2["live_filter_state"] == LIVE_ELIGIBLE].copy()
    if not live_rule_inputs.empty:
        live_rule_inputs = score_frame_rule_coverage(
            live_rule_inputs,
            rules_frame,
            horizon_column="remaining_hours",
            price_column="live_mid_price",
        )
        miss_mask = ~live_rule_inputs["rule_coverage_exact_match"].fillna(False)
        live_rule_inputs.loc[miss_mask, ["live_filter_state", "live_filter_reason"]] = [
            LIVE_PRICE_MISS,
            "live_price_outside_rule_band",
        ]
        for row in live_rule_inputs.to_dict(orient="records"):
            row_id = int(row.get("_stage2_row_id", -1))
            if row_id >= 0:
                for column, value in row.items():
                    stage2.at[row_id, column] = value

    if "_stage2_row_id" in stage2.columns:
        stage2 = stage2.drop(columns=["_stage2_row_id"])

    eligible = stage2[stage2["live_filter_state"] == LIVE_ELIGIBLE].copy().reset_index(drop=True)
    rejected = stage2[stage2["live_filter_state"] != LIVE_ELIGIBLE].copy().reset_index(drop=True)
    return LiveFilterResult(
        eligible=eligible,
        rejected=rejected,
        state_counts=_score_reason_counts(stage2, "live_filter_state"),
    )
