"""Build canonical hourly snapshot inputs for online scoring."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import json

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.execution.positions import load_open_market_ids, load_pending_market_ids
from execution_engine.online.scoring.price_history import (
    ClobPriceHistoryClient,
    build_quote_window_features,
    build_source_host,
    build_latest_live_prices_from_token_state,
    build_historical_price_features,
    merge_price_points,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


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


def load_market_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str)


def _best_price_from_sources(universe_row: Dict[str, Any], token_state_row: Dict[str, Any] | None) -> float:
    if token_state_row is not None:
        best_bid = _to_float(token_state_row.get("best_bid"))
        best_ask = _to_float(token_state_row.get("best_ask"))
        last_trade = _to_float(token_state_row.get("last_trade_price"))
        if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
            return round((best_bid + best_ask) / 2.0, 6)
        if last_trade > 0:
            return round(last_trade, 6)
        if best_ask > 0:
            return round(best_ask, 6)
        if best_bid > 0:
            return round(best_bid, 6)

    best_bid = _to_float(universe_row.get("best_bid"))
    best_ask = _to_float(universe_row.get("best_ask"))
    last_trade = _to_float(universe_row.get("last_trade_price"))
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        return round((best_bid + best_ask) / 2.0, 6)
    if last_trade > 0:
        return round(last_trade, 6)
    if best_ask > 0:
        return round(best_ask, 6)
    if best_bid > 0:
        return round(best_bid, 6)
    return 0.0


def _market_duration_hours(start_time_utc: Any, end_time_utc: Any) -> float | None:
    start_dt = _parse_utc(start_time_utc)
    end_dt = _parse_utc(end_time_utc)
    if start_dt is None or end_dt is None:
        return None
    return round(max((end_dt - start_dt).total_seconds(), 0.0) / 3600.0, 6)


def _state_age_seconds(now: datetime, latest_event_at_utc: Any) -> float | None:
    latest_event_at = _parse_utc(latest_event_at_utc)
    if latest_event_at is None:
        return None
    return max((now - latest_event_at).total_seconds(), 0.0)


def _batch_id(index: int, batch_size: int) -> str:
    return f"batch_{(index // max(batch_size, 1)) + 1:03d}"


def refresh_live_universe_view(
    universe: pd.DataFrame,
    *,
    window_hours: float,
) -> tuple[pd.DataFrame, Dict[str, int]]:
    if universe.empty:
        return universe.copy(), {
            "missing_end_time_market": 0,
            "expired_universe_market": 0,
            "outside_live_window_market": 0,
        }

    now = pd.Timestamp(_utc_now())
    out = universe.copy()
    end_times = pd.to_datetime(out.get("end_time_utc"), utc=True, errors="coerce")
    remaining_hours = (end_times - now).dt.total_seconds() / 3600.0
    out["remaining_hours"] = remaining_hours.round(6)

    valid_end_time = end_times.notna()
    live_window = (remaining_hours > 0) & (remaining_hours <= window_hours)
    keep_mask = valid_end_time & live_window

    breakdown = {
        "missing_end_time_market": int((~valid_end_time).sum()),
        "expired_universe_market": int((valid_end_time & (remaining_hours <= 0)).sum()),
        "outside_live_window_market": int((valid_end_time & (remaining_hours > window_hours)).sum()),
    }
    return out.loc[keep_mask].reset_index(drop=True), breakdown


def build_online_market_context(
    active_markets: pd.DataFrame,
    token_state_by_token: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for row in active_markets.to_dict(orient="records"):
        token_id = str(row.get("selected_reference_token_id") or "")
        token_state = token_state_by_token.get(token_id, {})
        best_bid = _to_float(token_state.get("best_bid"), default=_to_float(row.get("best_bid")))
        best_ask = _to_float(token_state.get("best_ask"), default=_to_float(row.get("best_ask")))
        spread = max(best_ask - best_bid, 0.0) if best_bid > 0 and best_ask > 0 else 0.0
        last_trade = _to_float(token_state.get("last_trade_price"), default=_to_float(row.get("last_trade_price")))
        rows.append(
            {
                "id": str(row.get("market_id") or ""),
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
                "negRisk": bool(str(row.get("neg_risk") or "").strip().lower() in {"1", "true", "yes", "y", "on"}),
                "rewardsMinSize": _to_float(row.get("rewards_min_size")),
                "rewardsMaxSpread": _to_float(row.get("rewards_max_spread")),
                "bestBid": best_bid,
                "bestAsk": best_ask,
                "spread": _to_float(row.get("spread"), default=spread),
                "lastTradePrice": last_trade,
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
                "resolutionSource": str(row.get("resolution_source") or ""),
                "outcomes": json.dumps(
                    [
                        str(row.get("outcome_0_label") or ""),
                        str(row.get("outcome_1_label") or ""),
                    ],
                    ensure_ascii=True,
                ),
                "clobTokenIds": json.dumps(
                    [
                        str(row.get("token_0_id") or ""),
                        str(row.get("token_1_id") or ""),
                    ],
                    ensure_ascii=True,
                ),
                "category": str(row.get("category") or "UNKNOWN"),
                "domain": str(row.get("domain") or "UNKNOWN"),
                "market_type": str(row.get("market_type") or "UNKNOWN"),
            }
        )
    return pd.DataFrame(rows)


@dataclass(frozen=True)
class SnapshotInputBuildResult:
    processed: pd.DataFrame
    snapshots: pd.DataFrame
    active_markets: pd.DataFrame
    raw_inputs: List[Dict[str, Any]]
    processing_counts: Dict[str, int]


def build_snapshot_inputs(
    cfg: PegConfig,
    universe: pd.DataFrame,
    *,
    market_limit: int | None,
    market_offset: int,
) -> SnapshotInputBuildResult:
    now = _utc_now()
    now_ts = int(pd.Timestamp(now).timestamp())
    token_state = load_market_frame(cfg.token_state_current_path)
    token_state_by_token = {
        str(row.get("token_id") or ""): row
        for row in token_state.to_dict(orient="records")
        if str(row.get("token_id") or "")
    }

    opened_market_ids = load_open_market_ids(cfg)
    pending_market_ids = load_pending_market_ids(cfg)
    history_client = ClobPriceHistoryClient(cfg)
    universe_rows = sorted_universe = universe.sort_values(
        by=["remaining_hours", "end_time_utc", "market_id"],
        ascending=[True, True, True],
    ).reset_index(drop=True)
    latest_ws_prices = build_latest_live_prices_from_token_state(
        token_state_by_token,
        (
            str(row.get("selected_reference_token_id") or "")
            for row in universe_rows.to_dict(orient="records")
        ),
        now=now,
    )

    processed_rows: List[Dict[str, Any]] = []
    snapshot_rows: List[Dict[str, Any]] = []
    raw_input_rows: List[Dict[str, Any]] = []
    active_rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {
        "opened_position_market": 0,
        "pending_order_market": 0,
        "market_offset_skip": 0,
        "market_limit_skip": 0,
        "missing_live_state": 0,
        "stale_live_state": 0,
        "invalid_price": 0,
        "missing_history_1h": 0,
        "missing_history_24h": 0,
        "snapshot_built": 0,
    }

    active_index = 0
    for row in universe_rows.to_dict(orient="records"):
        market_id = str(row.get("market_id") or "")
        reason = ""
        batch_id = ""
        if market_id in opened_market_ids:
            reason = "opened_position_market"
        elif market_id in pending_market_ids:
            reason = "pending_order_market"
        else:
            if active_index < market_offset:
                reason = "market_offset_skip"
            elif market_limit is not None and (active_index - market_offset) >= market_limit:
                reason = "market_limit_skip"
            else:
                batch_id = _batch_id(active_index - market_offset, cfg.online_market_batch_size)
            active_index += 1

        token_id = str(row.get("selected_reference_token_id") or "")
        token_state_row = token_state_by_token.get(token_id)
        latest_ws_price = latest_ws_prices.get(token_id)
        age_sec = _state_age_seconds(now, (token_state_row or {}).get("latest_event_at_utc")) if token_state_row else None
        if not reason:
            if token_state_row is None or age_sec is None:
                reason = "missing_live_state"
            elif age_sec > cfg.online_token_state_max_age_sec:
                reason = "stale_live_state"

        price = float(latest_ws_price.price) if latest_ws_price is not None else _best_price_from_sources(row, token_state_row)
        if not reason and not (cfg.rule_engine_min_price < price < cfg.rule_engine_max_price):
            reason = "invalid_price"

        counts[reason or "snapshot_built"] = counts.get(reason or "snapshot_built", 0) + 1
        processed_rows.append(
            {
                "run_id": cfg.run_id,
                "market_id": market_id,
                "batch_id": batch_id,
                "selected_reference_token_id": token_id,
                "selected_reference_outcome_label": str(row.get("selected_reference_outcome_label") or ""),
                "remaining_hours": _to_float(row.get("remaining_hours")),
                "end_time_utc": str(row.get("end_time_utc") or ""),
                "processing_status": "excluded" if reason else "snapshot_built",
                "processing_reason": reason or "snapshot_built",
                "token_state_age_sec": age_sec,
                "reference_price": price,
            }
        )
        if reason:
            continue

        tick_size = _to_float(
            (token_state_row or {}).get("tick_size"),
            default=_to_float(row.get("order_price_min_tick_size"), default=0.001),
        )
        latest_event_ts_ms = _to_int((token_state_row or {}).get("latest_event_timestamp_ms"))
        selected_quote_ts = int(latest_event_ts_ms / 1000) if latest_event_ts_ms else now_ts
        raw_event_count = _to_float((token_state_row or {}).get("raw_event_count"), default=1.0)
        try:
            history_points = history_client.fetch_history(
                token_id,
                start_ts=now_ts - 24 * 3600,
                end_ts=now_ts,
                fidelity_minutes=1,
            )
        except Exception:
            history_points = []
        merged_history = merge_price_points(history_points, latest_ws_price, now_ts=now_ts)
        quote_window = build_quote_window_features(cfg, merged_points=merged_history, target_ts=now_ts)
        end_dt = _parse_utc(row.get("end_time_utc"))
        end_ts = int(end_dt.timestamp()) if end_dt is not None else now_ts
        history_features = build_historical_price_features(
            current_price=price,
            now_ts=now_ts,
            end_ts=end_ts,
            merged_points=merged_history,
        )
        if history_features.get("p_1h") is None:
            counts["missing_history_1h"] = counts.get("missing_history_1h", 0) + 1
        if history_features.get("p_24h") is None:
            counts["missing_history_24h"] = counts.get("missing_history_24h", 0) + 1
        market_duration_hours = _market_duration_hours(row.get("start_time_utc"), row.get("end_time_utc"))

        snapshot_rows.append(
            {
                "run_id": cfg.run_id,
                "batch_id": batch_id,
                "market_id": market_id,
                "price": price,
                "horizon_hours": _to_float(row.get("remaining_hours")),
                "snapshot_time": pd.Timestamp(now),
                "snapshot_date": pd.Timestamp(now).date(),
                "scheduled_end": str(row.get("end_time_utc") or ""),
                "closedTime": str(row.get("end_time_utc") or ""),
                "delta_hours": 0.0,
                "delta_hours_bucket": 0.0,
                "selected_quote_offset_sec": quote_window["selected_quote_offset_sec"],
                "selected_quote_points_in_window": quote_window["selected_quote_points_in_window"],
                "selected_quote_left_gap_sec": quote_window["selected_quote_left_gap_sec"],
                "selected_quote_right_gap_sec": quote_window["selected_quote_right_gap_sec"],
                "selected_quote_local_gap_sec": quote_window["selected_quote_local_gap_sec"],
                "selected_quote_ts": quote_window["selected_quote_ts"] or selected_quote_ts,
                "snapshot_target_ts": quote_window["snapshot_target_ts"],
                "selected_quote_side": quote_window["selected_quote_side"],
                "stale_quote_flag": quote_window["stale_quote_flag"],
                "snapshot_quality_score": quote_window["snapshot_quality_score"],
                "price_in_range_flag": bool(cfg.rule_engine_min_price < price < cfg.rule_engine_max_price),
                "quality_pass": bool(cfg.rule_engine_min_price < price < cfg.rule_engine_max_price),
                "category": str(row.get("category") or "UNKNOWN"),
                "domain": str(row.get("domain") or "UNKNOWN"),
                "market_type": str(row.get("market_type") or "UNKNOWN"),
                "source_host": build_source_host(
                    source_url=row.get("source_url"),
                    resolution_source=row.get("resolution_source"),
                    domain=row.get("domain"),
                ),
                "primary_outcome": str(row.get("outcome_0_label") or ""),
                "secondary_outcome": str(row.get("outcome_1_label") or ""),
                "market_duration_hours": market_duration_hours,
                "duration_is_negative_flag": bool((market_duration_hours or 0.0) < 0.0) if market_duration_hours is not None else False,
                "duration_below_min_horizon_flag": bool((market_duration_hours or 0.0) < 1.0) if market_duration_hours is not None else False,
                "delta_hours_exceeded_flag": False,
                "p_1h": history_features.get("p_1h"),
                "p_2h": history_features.get("p_2h"),
                "p_4h": history_features.get("p_4h"),
                "p_6h": history_features.get("p_6h"),
                "p_12h": history_features.get("p_12h"),
                "p_24h": history_features.get("p_24h"),
                "delta_p_1_2": history_features.get("delta_p_1_2"),
                "delta_p_2_4": history_features.get("delta_p_2_4"),
                "delta_p_4_12": history_features.get("delta_p_4_12"),
                "delta_p_12_24": history_features.get("delta_p_12_24"),
                "term_structure_slope": history_features.get("term_structure_slope"),
                "path_price_mean": history_features.get("path_price_mean"),
                "path_price_std": history_features.get("path_price_std"),
                "path_price_min": history_features.get("path_price_min"),
                "path_price_max": history_features.get("path_price_max"),
                "path_price_range": history_features.get("path_price_range"),
                "price_reversal_flag": history_features.get("price_reversal_flag"),
                "price_acceleration": history_features.get("price_acceleration"),
                "closing_drift": history_features.get("closing_drift"),
                "outcome_0_label": str(row.get("outcome_0_label") or ""),
                "outcome_1_label": str(row.get("outcome_1_label") or ""),
                "token_0_id": str(row.get("token_0_id") or ""),
                "token_1_id": str(row.get("token_1_id") or ""),
                "selected_reference_token_id": token_id,
                "selected_reference_outcome_label": str(row.get("selected_reference_outcome_label") or ""),
                "selected_reference_side_index": _to_int(row.get("selected_reference_side_index"), default=0),
                "best_bid": _to_float((token_state_row or {}).get("best_bid"), default=_to_float(row.get("best_bid"))),
                "best_ask": _to_float((token_state_row or {}).get("best_ask"), default=_to_float(row.get("best_ask"))),
                "mid_price": _to_float((token_state_row or {}).get("mid_price"), default=price),
                "last_trade_price": _to_float((token_state_row or {}).get("last_trade_price"), default=_to_float(row.get("last_trade_price"))),
                "tick_size": tick_size,
                "liquidity": _to_float(row.get("liquidity")),
                "volume24hr": _to_float(row.get("volume24hr")),
                "token_state_age_sec": float(age_sec or 0.0),
                "e_sample": None,
                "r_std": None,
            }
        )
        active_rows.append(row)
        raw_input_rows.append(
            {
                "run_id": cfg.run_id,
                "batch_id": batch_id,
                "market_id": market_id,
                "reference_token_id": token_id,
                "snapshot_time_utc": _to_iso(now),
                "universe_row": row,
                "token_state_row": token_state_row or {},
                "latest_ws_price": None if latest_ws_price is None else {
                    "price": latest_ws_price.price,
                    "event_time_utc": _to_iso(latest_ws_price.event_time),
                    "source_event_type": latest_ws_price.source_event_type,
                },
                "history_point_count": len(history_points),
                "derived_price": price,
            }
        )

    return SnapshotInputBuildResult(
        processed=pd.DataFrame(processed_rows),
        snapshots=pd.DataFrame(snapshot_rows),
        active_markets=pd.DataFrame(active_rows),
        raw_inputs=raw_input_rows,
        processing_counts=counts,
    )


