"""Universe refresh logic for the online execution pipeline."""

from __future__ import annotations

from ast import literal_eval
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import json

import pandas as pd

from execution_engine.integrations.providers.gamma_provider import GammaMarketProvider
from execution_engine.runtime.config import PegConfig
from execution_engine.online.execution.positions import load_open_market_ids

UNIVERSE_COLUMNS = [
    "market_id",
    "condition_id",
    "question",
    "description",
    "slug",
    "start_time_utc",
    "end_time_utc",
    "resolution_source",
    "game_id",
    "remaining_hours",
    "category",
    "domain",
    "market_type",
    "accepting_orders",
    "best_bid",
    "best_ask",
    "last_trade_price",
    "liquidity",
    "volume24hr",
    "order_price_min_tick_size",
    "outcome_0_label",
    "outcome_1_label",
    "token_0_id",
    "token_1_id",
    "selected_reference_token_id",
    "selected_reference_outcome_label",
    "selected_reference_side_index",
    "source_market_updated_at_utc",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _parse_maybe_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        for parser in (json.loads, literal_eval):
            try:
                parsed = parser(raw)
            except Exception:
                continue
            if isinstance(parsed, list):
                return parsed
        return []
    return []


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_end_time(value: Any) -> datetime | None:
    try:
        dt = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def _parse_group_title_as_end_time(value: Any) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        dt = pd.to_datetime(value, utc=True, errors="coerce")
    except Exception:
        return None
    if pd.isna(dt):
        return None
    parsed = dt.to_pydatetime()
    # Date-only titles are interpreted as an all-day market end.
    if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0 and len(str(value).strip()) <= 20:
        parsed = parsed.replace(hour=23, minute=59, second=59)
    return parsed


@dataclass(frozen=True)
class UniverseRefreshResult:
    fetched_markets: int
    eligible_markets: int
    excluded_for_expiry: int
    excluded_for_structure: int
    excluded_for_positions: int
    exclusion_breakdown: Dict[str, int]
    current_universe_path: Path
    current_manifest_path: Path
    run_universe_path: Path
    run_manifest_path: Path


def _build_binary_market_row(
    market: Dict[str, Any],
    now: datetime,
    require_two_token_markets: bool,
) -> tuple[Dict[str, Any] | None, str | None]:
    market_id = str(market.get("id") or market.get("market_id") or "")
    if not market_id:
        return None, "missing_market_id"

    outcomes = [str(item) for item in _parse_maybe_list(market.get("outcomes"))]
    token_ids = [str(item) for item in _parse_maybe_list(market.get("clobTokenIds") or market.get("clob_token_ids"))]
    if require_two_token_markets:
        if len(outcomes) != 2 or len(token_ids) != 2:
            return None, "non_binary_market"
    elif len(outcomes) < 2 or len(token_ids) < 2:
        return None, "insufficient_tokens"

    end_time = _parse_end_time(market.get("endDate"))
    if end_time is None or end_time <= now:
        fallback_end_time = _parse_end_time(market.get("closedTime")) or _parse_group_title_as_end_time(
            market.get("groupItemTitle")
        )
        if fallback_end_time is not None:
            end_time = fallback_end_time
    if end_time is None:
        return None, "missing_end_time"

    remaining_hours = (end_time - now).total_seconds() / 3600.0
    if remaining_hours <= 0:
        return None, "expired_market"

    return {
        "market_id": market_id,
        "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
        "question": str(market.get("question") or ""),
        "description": str(market.get("description") or ""),
        "slug": str(market.get("slug") or ""),
        "start_time_utc": str(market.get("startDate") or market.get("createdAt") or ""),
        "end_time_utc": _to_iso(end_time),
        "resolution_source": str(market.get("resolutionSource") or ""),
        "game_id": str(market.get("gameId") or ""),
        "remaining_hours": round(remaining_hours, 6),
        "category": str(market.get("category") or "UNKNOWN"),
        "domain": str(market.get("domain") or "UNKNOWN"),
        "market_type": str(market.get("market_type") or "UNKNOWN"),
        "accepting_orders": _to_bool(market.get("acceptingOrders") or market.get("accepting_orders")),
        "best_bid": _to_float(market.get("bestBid")),
        "best_ask": _to_float(market.get("bestAsk")),
        "last_trade_price": _to_float(market.get("lastTradePrice"), default=0.0),
        "liquidity": _to_float(market.get("liquidity")),
        "volume24hr": _to_float(market.get("volume24hr")),
        "order_price_min_tick_size": _to_float(market.get("orderPriceMinTickSize"), default=0.001),
        "outcome_0_label": outcomes[0],
        "outcome_1_label": outcomes[1],
        "token_0_id": token_ids[0],
        "token_1_id": token_ids[1],
        "selected_reference_token_id": token_ids[0],
        "selected_reference_outcome_label": outcomes[0],
        "selected_reference_side_index": 0,
        "source_market_updated_at_utc": str(market.get("updatedAt") or ""),
    }, None


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame if not frame.empty else pd.DataFrame(columns=UNIVERSE_COLUMNS)
    output.to_csv(path, index=False)


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def refresh_current_universe(cfg: PegConfig, max_markets: int | None = None) -> UniverseRefreshResult:
    provider = GammaMarketProvider(cfg.gamma_base_url, timeout_sec=cfg.clob_request_timeout_sec)
    now = _utc_now()
    window_end = now + pd.Timedelta(hours=cfg.online_universe_window_hours)
    raw_markets = provider.fetch_open_markets(
        limit=cfg.rule_engine_page_size,
        max_markets=max_markets or cfg.rule_engine_max_markets,
        order="endDate",
        ascending=True,
        end_date_min=_to_iso(now),
        end_date_max=_to_iso(window_end),
    )
    opened_market_ids = load_open_market_ids(cfg)

    rows: List[Dict[str, Any]] = []
    excluded_for_expiry = 0
    excluded_for_structure = 0
    excluded_for_positions = 0
    exclusion_breakdown: Dict[str, int] = {}

    for market in raw_markets:
        parsed, reason = _build_binary_market_row(market, now, cfg.online_require_two_token_markets)
        if parsed is None:
            excluded_for_structure += 1
            key = reason or "structure_filtered"
            exclusion_breakdown[key] = exclusion_breakdown.get(key, 0) + 1
            continue
        if parsed["remaining_hours"] > cfg.online_universe_window_hours:
            excluded_for_expiry += 1
            exclusion_breakdown["outside_expiry_window"] = exclusion_breakdown.get("outside_expiry_window", 0) + 1
            continue
        if str(parsed["market_id"]) in opened_market_ids:
            excluded_for_positions += 1
            exclusion_breakdown["opened_position_market"] = exclusion_breakdown.get("opened_position_market", 0) + 1
            continue
        rows.append(parsed)

    universe = pd.DataFrame(rows)
    if not universe.empty:
        universe = universe.sort_values(
            by=["remaining_hours", "end_time_utc", "market_id"],
            ascending=[True, True, True],
        ).reset_index(drop=True)

    manifest = {
        "generated_at_utc": _to_iso(now),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "window_hours": cfg.online_universe_window_hours,
        "require_two_token_markets": cfg.online_require_two_token_markets,
        "fetched_markets": len(raw_markets),
        "eligible_markets": len(universe),
        "excluded_for_expiry": excluded_for_expiry,
        "excluded_for_structure": excluded_for_structure,
        "excluded_for_positions": excluded_for_positions,
        "exclusion_breakdown": exclusion_breakdown,
        "opened_market_ids_count": len(opened_market_ids),
        "source": "gamma_open_markets",
        "current_universe_path": str(cfg.universe_current_path),
        "run_universe_path": str(cfg.run_universe_path),
    }

    _write_frame(cfg.universe_current_path, universe)
    _write_manifest(cfg.universe_current_manifest_path, manifest)
    _write_frame(cfg.run_universe_path, universe)
    _write_manifest(cfg.run_universe_manifest_path, manifest)

    return UniverseRefreshResult(
        fetched_markets=len(raw_markets),
        eligible_markets=len(universe),
        excluded_for_expiry=excluded_for_expiry,
        excluded_for_structure=excluded_for_structure,
        excluded_for_positions=excluded_for_positions,
        exclusion_breakdown=exclusion_breakdown,
        current_universe_path=cfg.universe_current_path,
        current_manifest_path=cfg.universe_current_manifest_path,
        run_universe_path=cfg.run_universe_path,
        run_manifest_path=cfg.run_universe_manifest_path,
    )


