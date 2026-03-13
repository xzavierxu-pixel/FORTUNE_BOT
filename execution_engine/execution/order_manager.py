"""Order manager for PEG."""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from ..core.config import PegConfig
from .clob_client import ClobClient, NullClobClient
from .nonce import NonceManager
from .state_machine import TERMINAL_STATES, can_transition
from ..utils.io import append_jsonl, read_jsonl
from ..core.models import DecisionRecord, FillRecord, OrderRecord
from ..utils.logger import log_structured
from ..utils.metrics import increment_metric
from ..utils.time import parse_utc, to_iso, utc_now


def compute_effective_expiration_seconds(signal: Dict[str, object], cfg: PegConfig) -> Tuple[int, Optional[str]]:
    now = utc_now()
    valid_until = signal.get("valid_until_utc")
    decision_end = signal.get("decision_window_end_utc")
    close_time = signal.get("market_close_time_utc")

    expiry_candidates: List[int] = []
    if valid_until:
        expiry_candidates.append(int((parse_utc(str(valid_until)) - now).total_seconds()))
    if decision_end:
        expiry_candidates.append(int((parse_utc(str(decision_end)) - now).total_seconds()))
    if close_time and cfg.min_time_to_close_sec > 0:
        close_delta = int((parse_utc(str(close_time)) - now).total_seconds()) - cfg.min_time_to_close_sec
        expiry_candidates.append(close_delta)

    expiry_candidates.append(int(signal.get("expiration_seconds", cfg.order_ttl_sec)))
    effective = min(expiry_candidates) if expiry_candidates else cfg.order_ttl_sec

    if effective <= 0:
        return 0, "SIGNAL_EXPIRED"
    return effective, None


def _latest_orders_by_id(orders: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    latest: Dict[str, Dict[str, object]] = {}
    for order in orders:
        oid = str(order.get("order_attempt_id", ""))
        if not oid:
            continue
        prior = latest.get(oid)
        if prior is None:
            latest[oid] = order
            continue
        prior_time = prior.get("updated_at_utc") or prior.get("created_at_utc")
        curr_time = order.get("updated_at_utc") or order.get("created_at_utc")
        if prior_time and curr_time and str(curr_time) >= str(prior_time):
            latest[oid] = order
    return latest


def sweep_expired_orders(cfg: PegConfig, clob_client: Optional[ClobClient] = None) -> None:
    orders = read_jsonl(cfg.orders_path)
    if not orders:
        return

    client = clob_client or NullClobClient()
    latest = _latest_orders_by_id(orders)
    now = utc_now()
    for order in latest.values():
        status = str(order.get("status", "")).upper()
        if status in TERMINAL_STATES:
            continue
        created_at = order.get("created_at_utc")
        expiration_seconds = int(order.get("expiration_seconds", cfg.order_ttl_sec))
        if not created_at:
            continue
        try:
            created_dt = parse_utc(str(created_at))
        except ValueError:
            continue
        if created_dt + timedelta(seconds=expiration_seconds) <= now:
            if cfg.dry_run:
                expired = transition_order(order, "EXPIRED", "TTL_EXPIRED")
                append_jsonl(cfg.orders_path, expired)
                log_structured(cfg.logs_path, {"type": "order_state", **expired})
                increment_metric(cfg.metrics_path, "orders_expired", 1)
                continue
            updated = transition_order(order, "CANCEL_REQUESTED", "TTL_EXPIRED")
            append_jsonl(cfg.orders_path, updated)
            log_structured(cfg.logs_path, {"type": "order_state", **updated})
            clob_order_id = order.get("clob_order_id")
            if clob_order_id:
                try:
                    client.cancel_order(str(clob_order_id))
                    canceled = transition_order(updated, "CANCELED", "TTL_EXPIRED")
                    append_jsonl(cfg.orders_path, canceled)
                    log_structured(cfg.logs_path, {"type": "order_state", **canceled})
                    increment_metric(cfg.metrics_path, "orders_canceled", 1)
                except Exception:
                    error = transition_order(updated, "ERROR", "CANCEL_FAILED")
                    append_jsonl(cfg.orders_path, error)
                    log_structured(cfg.logs_path, {"type": "order_state", **error})
                    increment_metric(cfg.metrics_path, "orders_error", 1)
            else:
                expired = transition_order(updated, "EXPIRED", "TTL_EXPIRED")
                append_jsonl(cfg.orders_path, expired)
                log_structured(cfg.logs_path, {"type": "order_state", **expired})
                increment_metric(cfg.metrics_path, "orders_expired", 1)


def transition_order(order: OrderRecord, new_status: str, reason: Optional[str] = None) -> OrderRecord:
    current = str(order.get("status", "")).upper() or "NEW"
    target = new_status.upper()
    if not can_transition(current, target):
        raise ValueError(f"Invalid transition {current} -> {target}")
    updated = dict(order)
    updated["status"] = target
    updated["updated_at_utc"] = to_iso(utc_now())
    if reason:
        updated["status_reason"] = reason
    return updated


def request_cancel(order: OrderRecord, cfg: PegConfig, clob_client: Optional[ClobClient] = None) -> OrderRecord:
    updated = transition_order(order, "CANCEL_REQUESTED")
    if cfg.dry_run:
        return updated

    client = clob_client or NullClobClient()
    clob_order_id = order.get("clob_order_id")
    if not clob_order_id:
        return transition_order(updated, "ERROR", "MISSING_CLOB_ORDER_ID")
    _ = client.cancel_order(str(clob_order_id))
    return transition_order(updated, "CANCELED")


def _extract_field(row: Dict[str, object], names: List[str]) -> Optional[object]:
    for name in names:
        if name in row:
            return row[name]
    return None


def reconcile(cfg: PegConfig, clob_client: Optional[ClobClient] = None) -> None:
    if cfg.dry_run:
        return

    client = clob_client or NullClobClient()
    if isinstance(client, NullClobClient):
        return
    latest_orders = _latest_orders_by_id(read_jsonl(cfg.orders_path))
    if not latest_orders:
        return

    open_orders = client.get_open_orders()
    open_ids = set()
    for order in open_orders:
        oid = _extract_field(order, ["orderID", "order_id", "id"])
        if oid:
            open_ids.add(str(oid))

    fills = client.get_fills()
    seen_fill_ids = set()
    for fill in read_jsonl(cfg.fills_path):
        fill_id = fill.get("fill_id")
        if fill_id:
            seen_fill_ids.add(str(fill_id))

    fills_by_order: Dict[str, float] = {}
    order_by_clob_id: Dict[str, OrderRecord] = {}
    for order in latest_orders.values():
        clob_id = order.get("clob_order_id")
        if clob_id:
            order_by_clob_id[str(clob_id)] = order

    for trade in fills:
        trade_id = _extract_field(trade, ["trade_id", "id", "match_id"])
        clob_order_id = _extract_field(trade, ["order_id", "orderID", "orderId"])
        if not clob_order_id or str(clob_order_id) not in order_by_clob_id:
            continue
        if trade_id and str(trade_id) in seen_fill_ids:
            continue

        order = order_by_clob_id[str(clob_order_id)]
        price_val = _extract_field(trade, ["price", "p", "rate"])
        size_val = _extract_field(trade, ["size", "amount", "qty", "quantity"])
        try:
            price = float(price_val) if price_val is not None else float(order.get("price_limit", 0.0))
            size = float(size_val) if size_val is not None else 0.0
        except (TypeError, ValueError):
            price = float(order.get("price_limit", 0.0))
            size = 0.0
        amount_usdc = price * size if size > 0 else float(order.get("amount_usdc", 0.0))

        fill_record: FillRecord = {
            "fill_id": str(trade_id) if trade_id else f"{clob_order_id}:{to_iso(utc_now())}",
            "order_attempt_id": order.get("order_attempt_id"),
            "clob_order_id": str(clob_order_id),
            "decision_id": order.get("decision_id"),
            "market_id": order.get("market_id"),
            "outcome_index": order.get("outcome_index"),
            "action": order.get("action"),
            "amount_usdc": amount_usdc,
            "price": price,
            "filled_at_utc": str(_extract_field(trade, ["timestamp", "time", "created_at"]) or to_iso(utc_now())),
            "category": order.get("category"),
        }
        append_jsonl(cfg.fills_path, fill_record)
        log_structured(cfg.logs_path, {"type": "fill", **fill_record})
        increment_metric(cfg.metrics_path, "trades_filled", 1)
        fills_by_order[str(order.get("order_attempt_id"))] = fills_by_order.get(
            str(order.get("order_attempt_id")), 0.0
        ) + amount_usdc

    for order in latest_orders.values():
        status = str(order.get("status", "")).upper()
        if status in TERMINAL_STATES:
            continue
        clob_order_id = order.get("clob_order_id")
        attempt_id = str(order.get("order_attempt_id", ""))
        filled_usdc = fills_by_order.get(attempt_id, 0.0)
        if filled_usdc > 0:
            target = "FILLED" if filled_usdc >= float(order.get("amount_usdc", 0.0)) else "PARTIALLY_FILLED"
            updated = transition_order(order, target)
            append_jsonl(cfg.orders_path, updated)
            log_structured(cfg.logs_path, {"type": "order_state", **updated})
            if target == "FILLED":
                increment_metric(cfg.metrics_path, "orders_filled", 1)
            continue
        if clob_order_id and str(clob_order_id) not in open_ids:
            updated = transition_order(order, "CANCEL_REQUESTED", "NOT_IN_OPEN_ORDERS")
            append_jsonl(cfg.orders_path, updated)
            log_structured(cfg.logs_path, {"type": "order_state", **updated})
            canceled = transition_order(updated, "CANCELED", "NOT_IN_OPEN_ORDERS")
            append_jsonl(cfg.orders_path, canceled)
            log_structured(cfg.logs_path, {"type": "order_state", **canceled})
            increment_metric(cfg.metrics_path, "orders_canceled", 1)


def submit_order(
    cfg: PegConfig,
    decision: DecisionRecord,
    signal: Dict[str, object],
    nonce_manager: Optional[NonceManager] = None,
    clob_client: Optional[ClobClient] = None,
    token_id: Optional[str] = None,
) -> OrderRecord:
    now = utc_now()
    expiration_seconds, reason = compute_effective_expiration_seconds(signal, cfg)
    if reason:
        raise ValueError(reason)

    order: OrderRecord = {
        "order_attempt_id": signal.get("order_attempt_id"),
        "decision_id": decision.get("decision_id"),
        "market_id": decision.get("market_id"),
        "outcome_index": decision.get("outcome_index"),
        "action": decision.get("action"),
        "order_type": decision.get("order_type"),
        "price_limit": decision.get("price_limit"),
        "amount_usdc": decision.get("amount_usdc"),
        "expiration_seconds": expiration_seconds,
        "status": "DRY_RUN_SUBMITTED" if cfg.dry_run else "NEW",
        "created_at_utc": to_iso(now),
        "updated_at_utc": to_iso(now),
        "run_id": cfg.run_id,
        "category": decision.get("category"),
    }

    if cfg.dry_run:
        return order

    if not token_id:
        raise ValueError("MISSING_TOKEN_ID")

    client = clob_client or NullClobClient()
    nonce = nonce_manager.next_nonce() if nonce_manager else None
    if nonce is not None:
        order["nonce"] = int(nonce)
    price = float(order["price_limit"])
    if price <= 0:
        raise ValueError("INVALID_PRICE")
    size = float(order["amount_usdc"]) / price

    result = client.place_order(
        {
            "token_id": token_id,
            "side": order["action"],
            "price": price,
            "size": size,
            "nonce": nonce,
            "client_order_id": order.get("order_attempt_id"),
        }
    )
    status_raw = str(result.get("status", "SENT")).upper()
    if status_raw in {"OPEN", "LIVE", "ACTIVE"}:
        order["status"] = "ACKED"
    elif status_raw in {"MATCHED", "FILLED"}:
        order["status"] = "FILLED"
    elif status_raw in {"REJECTED", "REJECT"}:
        order["status"] = "REJECTED"
    else:
        order["status"] = status_raw
    order["updated_at_utc"] = to_iso(utc_now())
    if result.get("order_id"):
        order["clob_order_id"] = result.get("order_id")
    return order
