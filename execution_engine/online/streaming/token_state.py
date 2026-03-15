"""Token subscription targets and latest-state persistence for online streaming."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List
import json

import pandas as pd

from execution_engine.runtime.config import PegConfig

TOKEN_STATE_COLUMNS = [
    "token_id",
    "market_id",
    "market_hash",
    "outcome_label",
    "side_index",
    "end_time_utc",
    "remaining_hours",
    "subscription_source",
    "latest_event_type",
    "latest_event_timestamp_ms",
    "latest_event_at_utc",
    "best_bid",
    "best_bid_size",
    "best_ask",
    "best_ask_size",
    "mid_price",
    "spread",
    "last_trade_price",
    "last_trade_side",
    "last_trade_size",
    "tick_size",
    "book_hash",
    "raw_event_count",
    "book_event_count",
    "price_change_event_count",
    "best_bid_ask_event_count",
    "last_trade_event_count",
    "tick_size_change_event_count",
    "new_market_event_count",
    "market_resolved_event_count",
    "resolved",
    "winning_asset_id",
    "winning_outcome_label",
]


@dataclass(frozen=True)
class TokenSubscriptionTarget:
    token_id: str
    market_id: str
    outcome_label: str
    side_index: int | None
    end_time_utc: str
    remaining_hours: float
    tick_size: float
    subscription_source: str


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


def _write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(path)


def load_reference_targets_from_universe(
    cfg: PegConfig,
    market_limit: int | None = None,
    market_offset: int = 0,
) -> List[TokenSubscriptionTarget]:
    if not cfg.universe_current_path.exists():
        return []

    frame = pd.read_csv(cfg.universe_current_path, dtype=str)
    if frame.empty:
        return []

    if market_offset > 0:
        frame = frame.iloc[market_offset:]
    if market_limit and market_limit > 0:
        frame = frame.head(market_limit)

    targets: List[TokenSubscriptionTarget] = []
    seen_token_ids: set[str] = set()
    for row in frame.to_dict(orient="records"):
        token_id = str(row.get("selected_reference_token_id") or "").strip()
        if not token_id or token_id in seen_token_ids:
            continue
        seen_token_ids.add(token_id)
        targets.append(
            TokenSubscriptionTarget(
                token_id=token_id,
                market_id=str(row.get("market_id") or ""),
                outcome_label=str(row.get("selected_reference_outcome_label") or ""),
                side_index=_to_int(row.get("selected_reference_side_index")),
                end_time_utc=str(row.get("end_time_utc") or ""),
                remaining_hours=_to_float(row.get("remaining_hours")),
                tick_size=_to_float(row.get("order_price_min_tick_size"), default=0.001),
                subscription_source="universe_reference",
            )
        )
    return targets


def build_override_targets(asset_ids: Iterable[str]) -> List[TokenSubscriptionTarget]:
    targets: List[TokenSubscriptionTarget] = []
    seen_token_ids: set[str] = set()
    for asset_id in asset_ids:
        raw_value = str(asset_id).strip()
        if not raw_value:
            continue
        parts = [part.strip() for part in raw_value.split(",") if part.strip()]
        for token_id in parts:
            if token_id in seen_token_ids:
                continue
            seen_token_ids.add(token_id)
            targets.append(
                TokenSubscriptionTarget(
                    token_id=token_id,
                    market_id="",
                    outcome_label="",
                    side_index=None,
                    end_time_utc="",
                    remaining_hours=0.0,
                    tick_size=0.001,
                    subscription_source="explicit_override",
                )
            )
    return targets


def build_initial_token_state(target: TokenSubscriptionTarget) -> Dict[str, Any]:
    return {
        "token_id": target.token_id,
        "market_id": target.market_id,
        "market_hash": "",
        "outcome_label": target.outcome_label,
        "side_index": target.side_index,
        "end_time_utc": target.end_time_utc,
        "remaining_hours": target.remaining_hours,
        "subscription_source": target.subscription_source,
        "latest_event_type": "",
        "latest_event_timestamp_ms": None,
        "latest_event_at_utc": "",
        "best_bid": 0.0,
        "best_bid_size": 0.0,
        "best_ask": 0.0,
        "best_ask_size": 0.0,
        "mid_price": 0.0,
        "spread": 0.0,
        "last_trade_price": 0.0,
        "last_trade_side": "",
        "last_trade_size": 0.0,
        "tick_size": target.tick_size,
        "book_hash": "",
        "raw_event_count": 0,
        "book_event_count": 0,
        "price_change_event_count": 0,
        "best_bid_ask_event_count": 0,
        "last_trade_event_count": 0,
        "tick_size_change_event_count": 0,
        "new_market_event_count": 0,
        "market_resolved_event_count": 0,
        "resolved": False,
        "winning_asset_id": "",
        "winning_outcome_label": "",
    }


def compute_mid_price(best_bid: float, best_ask: float, last_trade_price: float) -> float:
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        return round((best_bid + best_ask) / 2.0, 6)
    if last_trade_price > 0:
        return round(last_trade_price, 6)
    return 0.0


def format_token_state_frame(states: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    if not states:
        return pd.DataFrame(columns=TOKEN_STATE_COLUMNS)
    frame = pd.DataFrame(list(states.values()))
    for column in TOKEN_STATE_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[TOKEN_STATE_COLUMNS]
    return frame.sort_values(
        by=["market_id", "outcome_label", "token_id"],
        ascending=[True, True, True],
        na_position="last",
    ).reset_index(drop=True)


def write_token_state_outputs(
    cfg: PegConfig,
    frame: pd.DataFrame,
    generated_at_utc: str,
) -> None:
    csv_payload = frame.to_csv(index=False)
    _write_text_atomic(cfg.token_state_current_path, csv_payload)
    _write_text_atomic(cfg.run_stream_token_state_path, csv_payload)

    json_payload = {
        "generated_at_utc": generated_at_utc,
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "token_count": int(len(frame)),
        "records": frame.to_dict(orient="records"),
    }
    _write_text_atomic(
        cfg.token_state_current_json_path,
        json.dumps(json_payload, ensure_ascii=True, indent=2),
    )


def serialize_targets(targets: Iterable[TokenSubscriptionTarget]) -> List[Dict[str, Any]]:
    return [asdict(target) for target in targets]

