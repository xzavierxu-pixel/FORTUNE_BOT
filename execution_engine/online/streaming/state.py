"""Token-state mutation helpers for the online market stream."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from execution_engine.online.streaming.utils import select_best_level, to_float, to_int, to_iso
from execution_engine.online.streaming.token_state import TokenSubscriptionTarget, build_initial_token_state, compute_mid_price


def get_or_create_state(state_by_token: Dict[str, Dict[str, Any]], token_id: str) -> Dict[str, Any]:
    state = state_by_token.get(token_id)
    if state is not None:
        return state

    target = TokenSubscriptionTarget(
        token_id=token_id,
        market_id="",
        outcome_label="",
        side_index=None,
        end_time_utc="",
        remaining_hours=0.0,
        tick_size=0.001,
        subscription_source="stream_discovered",
    )
    state = build_initial_token_state(target)
    state_by_token[token_id] = state
    return state


def touch_state(state: Dict[str, Any], event_type: str, payload: Dict[str, Any], received_at: datetime) -> None:
    state["latest_event_type"] = event_type
    state["latest_event_timestamp_ms"] = to_int(payload.get("timestamp") or payload.get("ts"))
    state["latest_event_at_utc"] = to_iso(received_at)
    state["market_hash"] = str(payload.get("market") or state.get("market_hash") or "")
    state["raw_event_count"] = int(state.get("raw_event_count", 0)) + 1


def refresh_price_fields(state: Dict[str, Any]) -> None:
    best_bid = to_float(state.get("best_bid"))
    best_ask = to_float(state.get("best_ask"))
    last_trade_price = to_float(state.get("last_trade_price"))
    state["mid_price"] = compute_mid_price(best_bid, best_ask, last_trade_price)
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        state["spread"] = round(best_ask - best_bid, 6)
    else:
        state["spread"] = 0.0


def apply_book_event(state_by_token: Dict[str, Dict[str, Any]], token_id: str, payload: Dict[str, Any], received_at: datetime) -> None:
    state = get_or_create_state(state_by_token, token_id)
    touch_state(state, "book", payload, received_at)
    best_bid, best_bid_size = select_best_level(payload.get("bids"), side="bid")
    best_ask, best_ask_size = select_best_level(payload.get("asks"), side="ask")
    state["best_bid"] = best_bid
    state["best_bid_size"] = best_bid_size
    state["best_ask"] = best_ask
    state["best_ask_size"] = best_ask_size
    state["book_hash"] = str(payload.get("hash") or state.get("book_hash") or "")
    state["book_event_count"] = int(state.get("book_event_count", 0)) + 1
    refresh_price_fields(state)


def apply_price_change_event(state_by_token: Dict[str, Dict[str, Any]], token_id: str, payload: Dict[str, Any], received_at: datetime) -> None:
    change = payload.get("price_change") or {}
    state = get_or_create_state(state_by_token, token_id)
    touch_state(state, "price_change", payload, received_at)
    state["best_bid"] = to_float(change.get("best_bid"), default=to_float(state.get("best_bid")))
    state["best_ask"] = to_float(change.get("best_ask"), default=to_float(state.get("best_ask")))
    state["book_hash"] = str(change.get("hash") or state.get("book_hash") or "")
    state["price_change_event_count"] = int(state.get("price_change_event_count", 0)) + 1
    refresh_price_fields(state)


def apply_best_bid_ask_event(state_by_token: Dict[str, Dict[str, Any]], token_id: str, payload: Dict[str, Any], received_at: datetime) -> None:
    state = get_or_create_state(state_by_token, token_id)
    touch_state(state, "best_bid_ask", payload, received_at)
    state["best_bid"] = to_float(payload.get("best_bid"), default=to_float(state.get("best_bid")))
    state["best_bid_size"] = to_float(payload.get("best_bid_size"), default=to_float(state.get("best_bid_size")))
    state["best_ask"] = to_float(payload.get("best_ask"), default=to_float(state.get("best_ask")))
    state["best_ask_size"] = to_float(payload.get("best_ask_size"), default=to_float(state.get("best_ask_size")))
    state["best_bid_ask_event_count"] = int(state.get("best_bid_ask_event_count", 0)) + 1
    refresh_price_fields(state)


def apply_last_trade_price_event(state_by_token: Dict[str, Dict[str, Any]], token_id: str, payload: Dict[str, Any], received_at: datetime) -> None:
    state = get_or_create_state(state_by_token, token_id)
    touch_state(state, "last_trade_price", payload, received_at)
    state["last_trade_price"] = to_float(payload.get("price"), default=to_float(state.get("last_trade_price")))
    state["last_trade_side"] = str(payload.get("side") or state.get("last_trade_side") or "")
    state["last_trade_size"] = to_float(payload.get("size"), default=to_float(state.get("last_trade_size")))
    state["last_trade_event_count"] = int(state.get("last_trade_event_count", 0)) + 1
    refresh_price_fields(state)


def apply_tick_size_change_event(state_by_token: Dict[str, Dict[str, Any]], token_id: str, payload: Dict[str, Any], received_at: datetime) -> None:
    state = get_or_create_state(state_by_token, token_id)
    touch_state(state, "tick_size_change", payload, received_at)
    next_tick = to_float(payload.get("new_tick_size"), default=to_float(state.get("tick_size"), default=0.001))
    if next_tick > 0:
        state["tick_size"] = next_tick
    state["tick_size_change_event_count"] = int(state.get("tick_size_change_event_count", 0)) + 1


def apply_market_lifecycle_event(
    state_by_token: Dict[str, Dict[str, Any]],
    token_id: str,
    payload: Dict[str, Any],
    received_at: datetime,
    *,
    resolved: bool,
) -> None:
    state = get_or_create_state(state_by_token, token_id)
    event_type = "market_resolved" if resolved else "new_market"
    touch_state(state, event_type, payload, received_at)
    if resolved:
        state["market_resolved_event_count"] = int(state.get("market_resolved_event_count", 0)) + 1
        state["resolved"] = True
        state["winning_asset_id"] = str(payload.get("winning_asset_id") or "")
        if state["winning_asset_id"] and state["winning_asset_id"] == token_id and state.get("outcome_label"):
            state["winning_outcome_label"] = str(state.get("outcome_label") or "")
    else:
        state["new_market_event_count"] = int(state.get("new_market_event_count", 0)) + 1


def ingest_event(
    state_by_token: Dict[str, Dict[str, Any]],
    event_type: str,
    payload: Dict[str, Any],
    received_at: datetime,
) -> bool:
    if event_type == "book":
        token_id = str(payload.get("asset_id") or "")
        if token_id:
            apply_book_event(state_by_token, token_id, payload, received_at)
            return True
        return False

    if event_type == "price_change":
        changed = False
        for change in payload.get("price_changes") or []:
            if not isinstance(change, dict):
                continue
            token_id = str(change.get("asset_id") or "")
            if token_id:
                merged = dict(payload)
                merged["price_change"] = change
                apply_price_change_event(state_by_token, token_id, merged, received_at)
                changed = True
        return changed

    if event_type == "best_bid_ask":
        token_id = str(payload.get("asset_id") or "")
        if token_id:
            apply_best_bid_ask_event(state_by_token, token_id, payload, received_at)
            return True
        return False

    if event_type == "last_trade_price":
        token_id = str(payload.get("asset_id") or "")
        if token_id:
            apply_last_trade_price_event(state_by_token, token_id, payload, received_at)
            return True
        return False

    if event_type == "tick_size_change":
        token_id = str(payload.get("asset_id") or "")
        if token_id:
            apply_tick_size_change_event(state_by_token, token_id, payload, received_at)
            return True
        return False

    if event_type == "new_market":
        changed = False
        for token_id in payload.get("asset_ids") or []:
            apply_market_lifecycle_event(state_by_token, str(token_id), payload, received_at, resolved=False)
            changed = True
        return changed

    if event_type == "market_resolved":
        changed = False
        for token_id in payload.get("asset_ids") or []:
            apply_market_lifecycle_event(state_by_token, str(token_id), payload, received_at, resolved=True)
            changed = True
        return changed

    return False

