"""Event-page source for direct online execution processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Set

import pandas as pd

from execution_engine.integrations.providers.gamma_provider import GammaMarketProvider
from execution_engine.online.scoring.annotations import apply_online_market_annotations
from execution_engine.online.universe.refresh import _build_binary_market_row, _load_rule_baseline_helpers
from execution_engine.runtime.config import PegConfig

EXECUTION_SOURCE_COLUMNS = [
    "market_id",
    "question",
    "description",
    "resolution_source",
    "game_id",
    "remaining_hours",
    "category",
    "category_raw",
    "category_parsed",
    "category_override_flag",
    "domain",
    "domain_parsed",
    "sub_domain",
    "source_url",
    "market_type",
    "outcome_pattern",
    "accepting_orders",
    "volume",
    "best_bid",
    "best_ask",
    "spread",
    "last_trade_price",
    "liquidity",
    "volume24hr",
    "volume1wk",
    "volume24hr_clob",
    "volume1wk_clob",
    "order_price_min_tick_size",
    "neg_risk",
    "rewards_min_size",
    "rewards_max_spread",
    "line",
    "one_hour_price_change",
    "one_day_price_change",
    "one_week_price_change",
    "liquidity_amm",
    "liquidity_clob",
    "group_item_title",
    "market_maker_address",
    "outcome_0_label",
    "outcome_1_label",
    "token_0_id",
    "token_1_id",
    "selected_reference_token_id",
    "selected_reference_outcome_label",
    "selected_reference_side_index",
    "uma_resolution_statuses",
    "start_time_utc",
    "created_at_utc",
    "end_time_utc",
    "source_market_updated_at_utc",
    "first_seen_at_utc",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class EventPageResult:
    page_offset: int
    page_limit: int
    event_count: int
    expanded_market_count: int
    exclusion_breakdown: Dict[str, int]
    markets: pd.DataFrame
    has_more: bool


def _project_execution_frame(markets: pd.DataFrame, first_seen_at_utc: str) -> pd.DataFrame:
    if markets.empty:
        return pd.DataFrame(columns=EXECUTION_SOURCE_COLUMNS)
    out = markets.copy()
    out["first_seen_at_utc"] = first_seen_at_utc
    for column in EXECUTION_SOURCE_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    return out[EXECUTION_SOURCE_COLUMNS].reset_index(drop=True)


def fetch_event_page(
    cfg: PegConfig,
    *,
    offset: int,
    limit: int,
    seen_market_ids: Set[str] | None = None,
) -> EventPageResult:
    provider = GammaMarketProvider(cfg.gamma_base_url, timeout_sec=cfg.clob_request_timeout_sec)
    helpers = _load_rule_baseline_helpers(cfg)
    now = _utc_now()
    first_seen_at_utc = _to_iso(now)
    raw_events = provider.fetch_open_events_page(
        limit=limit,
        offset=offset,
        order="endDate",
        ascending=True,
    )

    rows: List[Dict[str, object]] = []
    exclusion_breakdown: Dict[str, int] = {}
    seen = set(seen_market_ids or set())
    for event in raw_events:
        markets = event.get("markets") or []
        if not isinstance(markets, list):
            continue
        for market in markets:
            if not isinstance(market, dict):
                continue
            parsed, reason = _build_binary_market_row(
                cfg,
                event,
                market,
                now,
                cfg.online_require_two_token_markets,
                helpers,
            )
            if parsed is None:
                bucket = reason or "structure_filtered"
                exclusion_breakdown[bucket] = exclusion_breakdown.get(bucket, 0) + 1
                continue
            market_id = str(parsed.get("market_id") or "")
            if not market_id:
                exclusion_breakdown["missing_market_id"] = exclusion_breakdown.get("missing_market_id", 0) + 1
                continue
            if market_id in seen:
                exclusion_breakdown["duplicate_market_id"] = exclusion_breakdown.get("duplicate_market_id", 0) + 1
                continue
            seen.add(market_id)
            rows.append(parsed)

    markets = pd.DataFrame(rows)
    if not markets.empty:
        markets = apply_online_market_annotations(cfg, markets)
        markets = markets.sort_values(
            by=["remaining_hours", "end_time_utc", "market_id"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

    return EventPageResult(
        page_offset=int(max(offset, 0)),
        page_limit=int(max(limit, 1)),
        event_count=int(len(raw_events)),
        expanded_market_count=int(len(markets)),
        exclusion_breakdown=exclusion_breakdown,
        markets=_project_execution_frame(markets, first_seen_at_utc),
        has_more=bool(raw_events) and len(raw_events) >= max(limit, 1),
    )


def iter_event_pages(cfg: PegConfig, *, seen_market_ids: Set[str] | None = None) -> Iterable[EventPageResult]:
    offset = 0
    page_limit = max(int(cfg.online_gamma_event_page_size), 1)
    seen = set(seen_market_ids or set())
    while True:
        page = fetch_event_page(cfg, offset=offset, limit=page_limit, seen_market_ids=seen)
        for market_id in page.markets.get("market_id", pd.Series(dtype=str)).astype(str).tolist():
            if market_id:
                seen.add(market_id)
        if page.event_count == 0:
            break
        yield page
        if not page.has_more:
            break
        offset += page_limit
