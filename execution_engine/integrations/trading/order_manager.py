"""Order manager for PEG."""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, List, Optional, Tuple

from execution_engine.runtime.config import PegConfig
from .clob_client import ClobClient, NullClobClient
from .nonce import NonceManager
from .state_machine import TERMINAL_STATES, can_transition
from execution_engine.shared.io import append_jsonl, list_run_artifact_paths, read_jsonl, read_jsonl_many
from execution_engine.runtime.models import DecisionRecord, FillRecord, OrderRecord
from execution_engine.shared.logger import log_structured
from execution_engine.shared.metrics import increment_metric
from execution_engine.shared.time import parse_utc, to_iso, utc_now
from execution_engine.online.execution.positions import load_open_position_rows, rebuild_open_positions_ledger


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
    orders = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
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
    latest_orders = _latest_orders_by_id(read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl")))
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
    for fill in read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "fills.jsonl")):
        fill_id = fill.get("fill_id")
        if fill_id:
            seen_fill_ids.add(str(fill_id))

    fills_by_order: Dict[str, float] = {}
    order_by_clob_id: Dict[str, OrderRecord] = {}
    position_basis: Dict[str, Dict[str, float]] = {}
    for row in load_open_position_rows(cfg):
        market_id = str(row.get("market_id", "") or "")
        token_id = str(row.get("token_id", "") or "")
        outcome_index = int(row.get("outcome_index", 0) or 0)
        if not market_id or not token_id:
            continue
        position_basis[f"{market_id}|{token_id}|{outcome_index}"] = {
            "open_shares": float(row.get("filled_shares", 0.0) or 0.0),
            "open_cost_usdc": float(row.get("filled_amount_usdc", 0.0) or 0.0),
        }
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
        action = str(order.get("action", "") or "").upper()
        outcome_index = int(order.get("outcome_index", 0) or 0)
        position_key = f"{order.get('market_id', '')}|{order.get('token_id', '')}|{outcome_index}"
        pnl_usdc = 0.0
        basis = position_basis.setdefault(position_key, {"open_shares": 0.0, "open_cost_usdc": 0.0})
        shares = size if size > 0 else (amount_usdc / price if price > 0 else 0.0)
        if action == "SELL" and shares > 0 and basis["open_shares"] > 0:
            avg_cost = basis["open_cost_usdc"] / basis["open_shares"] if basis["open_shares"] > 0 else 0.0
            closed_shares = min(basis["open_shares"], shares)
            pnl_usdc = amount_usdc - (avg_cost * closed_shares)
            basis["open_shares"] = max(0.0, basis["open_shares"] - closed_shares)
            basis["open_cost_usdc"] = max(0.0, basis["open_cost_usdc"] - (avg_cost * closed_shares))
        elif action == "BUY" and shares > 0:
            basis["open_shares"] += shares
            basis["open_cost_usdc"] += amount_usdc

        fill_record: FillRecord = {
            "fill_id": str(trade_id) if trade_id else f"{clob_order_id}:{to_iso(utc_now())}",
            "order_attempt_id": order.get("order_attempt_id"),
            "clob_order_id": str(clob_order_id),
            "decision_id": order.get("decision_id"),
            "run_id": order.get("run_id"),
            "market_id": order.get("market_id"),
            "outcome_index": outcome_index,
            "action": order.get("action"),
            "amount_usdc": amount_usdc,
            "price": price,
            "shares": shares,
            "pnl_usdc": pnl_usdc,
            "filled_at_utc": str(_extract_field(trade, ["timestamp", "time", "created_at"]) or to_iso(utc_now())),
            "category": order.get("category"),
            "domain": order.get("domain"),
            "position_side": order.get("position_side"),
            "token_id": order.get("token_id"),
            "outcome_label": order.get("outcome_label"),
            "execution_phase": order.get("execution_phase", "ENTRY"),
            "parent_order_attempt_id": order.get("parent_order_attempt_id"),
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
            elif target == "PARTIALLY_FILLED":
                increment_metric(cfg.metrics_path, "orders_partially_filled", 1)
            continue
        if clob_order_id and str(clob_order_id) not in open_ids:
            updated = transition_order(order, "CANCEL_REQUESTED", "NOT_IN_OPEN_ORDERS")
            append_jsonl(cfg.orders_path, updated)
            log_structured(cfg.logs_path, {"type": "order_state", **updated})
            canceled = transition_order(updated, "CANCELED", "NOT_IN_OPEN_ORDERS")
            append_jsonl(cfg.orders_path, canceled)
            log_structured(cfg.logs_path, {"type": "order_state", **canceled})
            increment_metric(cfg.metrics_path, "orders_canceled", 1)

    rebuild_open_positions_ledger(cfg)


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
        "market_close_time_utc": decision.get("market_close_time_utc"),
        "status": "DRY_RUN_SUBMITTED" if cfg.dry_run else "NEW",
        "created_at_utc": to_iso(now),
        "updated_at_utc": to_iso(now),
        "run_id": cfg.run_id,
        "category": decision.get("category"),
        "domain": decision.get("domain"),
        "market_type": decision.get("market_type"),
        "source_host": decision.get("source_host"),
        "position_side": decision.get("position_side"),
        "rule_group_key": decision.get("rule_group_key"),
        "rule_leaf_id": decision.get("rule_leaf_id"),
        "q_pred": decision.get("q_pred"),
        "growth_score": decision.get("growth_score"),
        "f_exec": decision.get("f_exec"),
        "edge_prob": decision.get("edge_prob"),
        "settlement_key": decision.get("settlement_key"),
        "cluster_key": decision.get("cluster_key"),
        "token_id": decision.get("token_id"),
        "outcome_label": decision.get("outcome_label"),
        "best_bid_at_submit": decision.get("best_bid_at_submit"),
        "best_ask_at_submit": decision.get("best_ask_at_submit"),
        "tick_size": decision.get("tick_size"),
        "execution_phase": decision.get("execution_phase", "ENTRY"),
        "parent_order_attempt_id": decision.get("parent_order_attempt_id"),
    }

    if cfg.dry_run:
        return order

    if not token_id:
        raise ValueError("MISSING_TOKEN_ID")

    client = clob_client or NullClobClient()
    price = float(order["price_limit"])
    if price <= 0:
        raise ValueError("INVALID_PRICE")
    size = float(signal.get("order_size_shares", 0.0) or 0.0)
    if size <= 0:
        size = float(order["amount_usdc"]) / price

    payload = {
        "token_id": token_id,
        "side": order["action"],
        "price": price,
        "size": size,
        "client_order_id": order.get("order_attempt_id"),
    }
    result = client.place_order(payload)
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
