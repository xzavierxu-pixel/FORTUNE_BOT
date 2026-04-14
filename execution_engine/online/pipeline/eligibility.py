"""Two-stage filtering for the direct online submit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Set
import json

import pandas as pd

from execution_engine.online.scoring.rules import score_frame_group_rule_coverage
from execution_engine.runtime.config import PegConfig

STRUCTURAL_REJECT = "STRUCTURAL_REJECT"
STATE_REJECT = "STATE_REJECT"
DIRECT_CANDIDATE = "DIRECT_CANDIDATE"

LIVE_ELIGIBLE = "LIVE_ELIGIBLE"
LIVE_SPREAD_TOO_WIDE = "LIVE_SPREAD_TOO_WIDE"
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


def _parse_resolution_statuses(value: Any) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return set()
        try:
            parsed = json.loads(raw)
            items = parsed if isinstance(parsed, list) else [raw]
        except Exception:
            items = [part.strip() for part in raw.split(",")]
    else:
        items = [value]
    return {str(item).strip().lower() for item in items if str(item).strip()}


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
    candidates["coarse_filter_reason"] = "rule_family_match"

    if "market_id" not in candidates.columns:
        candidates["market_id"] = ""
    if "accepting_orders" not in candidates.columns:
        candidates["accepting_orders"] = True
    if "uma_resolution_statuses" not in candidates.columns:
        candidates["uma_resolution_statuses"] = ""

    missing_end_mask = candidates.get("end_time_utc", pd.Series("", index=candidates.index)).astype(str).str.strip().eq("")
    accepting_orders = candidates["accepting_orders"].astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y", "on"})
    remaining = candidates["remaining_hours"]
    slack = max(float(cfg.online_coarse_horizon_slack_hours), 0.0)
    configured_horizon_limit = float(cfg.online_universe_window_hours)

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
    uma_status_hit = candidates["uma_resolution_statuses"].map(
        lambda value: bool(_parse_resolution_statuses(value) & {"pending", "proposed", "resolved", "disputed"})
    )
    candidates.loc[
        uma_status_hit & candidates["coarse_filter_state"].eq(DIRECT_CANDIDATE),
        ["coarse_filter_state", "coarse_filter_reason"],
    ] = [STRUCTURAL_REJECT, "uma_resolution_status_filtered"]

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
        enriched["live_filter_reason"] = "live_state_ok"
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
        live_spread = None
        if enriched["best_bid"] > 0 and enriched["best_ask"] > 0 and enriched["best_ask"] >= enriched["best_bid"]:
            live_spread = round(enriched["best_ask"] - enriched["best_bid"], 6)
        enriched["spread"] = live_spread
        enriched["mid_price"] = _best_live_mid(state_row)
        enriched["raw_event_count"] = _to_float(state_row.get("raw_event_count"), default=1.0)
        enriched["tick_size"] = _to_float(
            state_row.get("tick_size"),
            default=_to_float(row.get("order_price_min_tick_size"), default=0.001),
        )
        if live_spread is not None and live_spread > 0.5:
            enriched["live_filter_state"] = LIVE_SPREAD_TOO_WIDE
            enriched["live_filter_reason"] = "live_spread_above_threshold"
            stage2_rows.append(enriched)
            provisional_states[market_id] = LIVE_SPREAD_TOO_WIDE
            continue
        live_mid = _to_float(enriched.get("mid_price"))
        if not (float(cfg.rule_engine_min_price) <= live_mid <= float(cfg.rule_engine_max_price)):
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
        live_rule_inputs = score_frame_group_rule_coverage(live_rule_inputs, rules_frame)
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
