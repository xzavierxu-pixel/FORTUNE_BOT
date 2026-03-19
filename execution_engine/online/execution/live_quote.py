"""Live quote helpers for online order submission."""

from __future__ import annotations

from typing import Any, Dict

from execution_engine.integrations.trading.clob_client import NullClobClient
from execution_engine.shared.time import to_iso, utc_now

MIN_EXECUTION_TICK_SIZE = 0.01


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_price(entry: Any) -> float | None:
    if entry is None:
        return None
    if isinstance(entry, dict):
        value = entry.get("price") or entry.get("p")
        return _to_float(value, default=0.0) or None
    if isinstance(entry, (list, tuple)) and entry:
        return _to_float(entry[0], default=0.0) or None
    return _to_float(entry, default=0.0) or None


def _best_bid_from_levels(levels: Any) -> float | None:
    if not levels:
        return None
    prices = [_extract_price(entry) for entry in levels]
    prices = [price for price in prices if price is not None]
    return max(prices) if prices else None


def _best_ask_from_levels(levels: Any) -> float | None:
    if not levels:
        return None
    prices = [_extract_price(entry) for entry in levels]
    prices = [price for price in prices if price is not None]
    return min(prices) if prices else None


def quote_from_token_state(
    token_state_by_token: Dict[str, Dict[str, Any]],
    token_id: str,
) -> Dict[str, Any] | None:
    row = token_state_by_token.get(token_id)
    if row is None:
        return None
    best_bid = _to_float(row.get("best_bid"))
    best_ask = _to_float(row.get("best_ask"))
    tick_size = max(_to_float(row.get("tick_size"), default=MIN_EXECUTION_TICK_SIZE), MIN_EXECUTION_TICK_SIZE)
    mid = _to_float(row.get("mid_price"))
    if mid <= 0 and best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 and best_ask >= best_bid else None
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "depth_usdc": None,
        "min_order_size": None,
        "tick_size": tick_size,
        "quote_source": "token_state",
        "quote_time_utc": str(row.get("latest_event_at_utc") or ""),
    }


def quote_from_clob(clob_client: Any, token_id: str) -> Dict[str, Any] | None:
    book = clob_client.get_order_book(token_id)
    if not isinstance(book, dict):
        return None
    bids = book.get("bids")
    asks = book.get("asks")
    best_bid = _best_bid_from_levels(bids)
    best_ask = _best_ask_from_levels(asks)
    mid = clob_client.get_midpoint(token_id)
    if mid is None and best_bid is not None and best_ask is not None:
        mid = (best_bid + best_ask) / 2.0
    spread = None
    if best_bid is not None and best_ask is not None:
        spread = best_ask - best_bid
    return {
        "best_bid": float(best_bid or 0.0),
        "best_ask": float(best_ask or 0.0),
        "mid": float(mid or 0.0),
        "spread": float(spread) if spread is not None else None,
        "depth_usdc": None,
        "min_order_size": _to_float(book.get("min_order_size"), default=0.0) or None,
        "tick_size": None,
        "quote_source": "clob",
        "quote_time_utc": to_iso(utc_now()),
    }


def get_live_quote(
    clob_client: Any,
    token_state_by_token: Dict[str, Dict[str, Any]],
    token_id: str,
) -> Dict[str, Any] | None:
    if not isinstance(clob_client, NullClobClient):
        quote = quote_from_clob(clob_client, token_id)
        if quote is not None:
            token_state_quote = quote_from_token_state(token_state_by_token, token_id)
            if token_state_quote is not None and not quote.get("tick_size"):
                quote["tick_size"] = token_state_quote.get("tick_size")
            return quote
    return quote_from_token_state(token_state_by_token, token_id)

