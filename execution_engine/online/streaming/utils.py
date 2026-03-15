"""Shared helpers for online market streaming."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Sequence

from execution_engine.runtime.config import PegConfig
from execution_engine.online.streaming.token_state import (
    TokenSubscriptionTarget,
    build_override_targets,
    load_reference_targets_from_universe,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def chunked(items: Sequence[TokenSubscriptionTarget], size: int) -> List[List[TokenSubscriptionTarget]]:
    if size <= 0:
        size = len(items) or 1
    return [list(items[index:index + size]) for index in range(0, len(items), size)]


def select_best_level(levels: Any, side: str) -> tuple[float, float]:
    if not isinstance(levels, list) or not levels:
        return 0.0, 0.0

    best_price = None
    best_size = 0.0
    for level in levels:
        if isinstance(level, dict):
            price = to_float(level.get("price"))
            size = to_float(level.get("size"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = to_float(level[0])
            size = to_float(level[1])
        else:
            continue
        if price <= 0:
            continue
        if best_price is None:
            best_price = price
            best_size = size
            continue
        if side == "bid" and price > best_price:
            best_price = price
            best_size = size
        elif side == "ask" and price < best_price:
            best_price = price
            best_size = size
    if best_price is None:
        return 0.0, 0.0
    return float(best_price), float(best_size)


def resolve_stream_targets(
    cfg: PegConfig,
    asset_ids: Iterable[str] | None = None,
    market_limit: int | None = None,
    market_offset: int = 0,
) -> List[TokenSubscriptionTarget]:
    explicit_asset_ids = [str(asset_id).strip() for asset_id in (asset_ids or []) if str(asset_id).strip()]
    if explicit_asset_ids:
        return build_override_targets(explicit_asset_ids)
    return load_reference_targets_from_universe(cfg, market_limit=market_limit, market_offset=market_offset)


