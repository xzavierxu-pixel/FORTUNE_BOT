"""Metrics builders for run summaries and dashboard state."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from execution_engine.runtime.config import PegConfig
from execution_engine.integrations.trading.state_machine import TERMINAL_STATES
from execution_engine.online.reporting.summary_io import (
    count_csv_rows,
    count_jsonl,
    load_json,
    mean,
    safe_div,
    safe_float,
    safe_int,
)
from execution_engine.shared.io import list_artifact_paths_recursive, list_run_artifact_paths, read_jsonl, read_jsonl_many


def _count_csv_rows_many(paths: List[Any]) -> int:
    return sum(count_csv_rows(path) for path in paths)


def build_counts(cfg: PegConfig) -> Dict[str, int]:
    run_submit_attempt_paths = list_artifact_paths_recursive(cfg.data_dir, "submission_attempts.csv")
    run_orders_submitted_paths = list_artifact_paths_recursive(cfg.data_dir, "orders_submitted.jsonl")
    return {
        "decisions": count_jsonl(cfg.decisions_path),
        "orders": count_jsonl(cfg.orders_path),
        "events": count_jsonl(cfg.events_path),
        "fills": count_jsonl(cfg.fills_path),
        "rejections": count_jsonl(cfg.rejections_path),
        "alerts": count_jsonl(cfg.alerts_path),
        "processed_markets": count_csv_rows(cfg.run_snapshot_processed_markets_path),
        "normalized_snapshots": count_csv_rows(cfg.run_snapshot_normalized_path),
        "feature_inputs": count_csv_rows(cfg.run_snapshot_feature_inputs_path),
        "rule_hits": count_csv_rows(cfg.run_snapshot_rule_hits_path),
        "model_outputs": count_csv_rows(cfg.run_snapshot_model_outputs_path),
        "selection_decisions": count_csv_rows(cfg.run_snapshot_selection_path),
        "submission_attempts": _count_csv_rows_many(run_submit_attempt_paths),
        "orders_submitted": sum(count_jsonl(path) for path in run_orders_submitted_paths),
    }


def build_rejection_reasons(cfg: PegConfig) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in read_jsonl(cfg.rejections_path):
        reason = str(row.get("reason_code", "") or "UNKNOWN").strip() or "UNKNOWN"
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def latest_orders_by_attempt(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        order_attempt_id = str(row.get("order_attempt_id", "") or "")
        if not order_attempt_id:
            continue
        prior = latest.get(order_attempt_id)
        current_ts = str(row.get("updated_at_utc") or row.get("created_at_utc") or "")
        prior_ts = str((prior or {}).get("updated_at_utc") or (prior or {}).get("created_at_utc") or "")
        if prior is None or current_ts >= prior_ts:
            latest[order_attempt_id] = row
    return list(latest.values())


def order_status_family(status: str) -> str:
    normalized = status.upper()
    if normalized in {"FILLED", "PARTIALLY_FILLED"}:
        return "executed"
    if normalized in {"CANCELED", "EXPIRED"}:
        return "canceled"
    if normalized in {"REJECTED", "ERROR"}:
        return "failed"
    if normalized in {"NEW", "SENT", "ACKED", "CANCEL_REQUESTED", "DRY_RUN_SUBMITTED"}:
        return "open"
    return "other"


def build_position_snapshot(all_fills: List[Dict[str, Any]]) -> Dict[str, Any]:
    positions: Dict[str, Dict[str, Any]] = {}
    for fill in sorted(all_fills, key=lambda row: str(row.get("filled_at_utc", ""))):
        market_id = str(fill.get("market_id", "") or "")
        outcome_index = safe_int(fill.get("outcome_index"))
        position_side = str(fill.get("position_side", "") or fill.get("action", "") or "UNKNOWN")
        key = f"{market_id}|{outcome_index}|{position_side}"
        amount_usdc = safe_float(fill.get("amount_usdc"))
        price = safe_float(fill.get("price"))
        shares = amount_usdc / price if price > 0 else 0.0
        action = str(fill.get("action", "") or "BUY").upper()

        position = positions.setdefault(
            key,
            {
                "key": key,
                "market_id": market_id,
                "outcome_index": outcome_index,
                "position_side": position_side,
                "category": fill.get("category"),
                "domain": fill.get("domain"),
                "open_shares": 0.0,
                "open_cost_usdc": 0.0,
                "gross_buy_usdc": 0.0,
                "gross_sell_usdc": 0.0,
                "buy_fills": 0,
                "sell_fills": 0,
                "last_fill_at_utc": fill.get("filled_at_utc"),
                "realized_pnl_usdc": 0.0,
            },
        )

        position["last_fill_at_utc"] = fill.get("filled_at_utc")
        if action == "SELL":
            position["sell_fills"] += 1
            position["gross_sell_usdc"] += amount_usdc
            avg_cost = safe_div(position["open_cost_usdc"], position["open_shares"]) if position["open_shares"] > 0 else 0.0
            closed_shares = min(position["open_shares"], shares)
            position["open_shares"] = max(0.0, position["open_shares"] - closed_shares)
            position["open_cost_usdc"] = max(0.0, position["open_cost_usdc"] - avg_cost * closed_shares)
            position["realized_pnl_usdc"] += amount_usdc - (avg_cost * closed_shares)
            continue

        position["buy_fills"] += 1
        position["gross_buy_usdc"] += amount_usdc
        position["open_shares"] += shares
        position["open_cost_usdc"] += amount_usdc

    open_positions: List[Dict[str, Any]] = []
    closed_positions: List[Dict[str, Any]] = []
    for position in positions.values():
        open_cost = safe_float(position["open_cost_usdc"])
        open_shares = safe_float(position["open_shares"])
        avg_entry_price = safe_div(open_cost, open_shares) if open_shares > 0 else 0.0
        snapshot = {
            "key": position["key"],
            "market_id": position["market_id"],
            "outcome_index": position["outcome_index"],
            "position_side": position["position_side"],
            "category": position.get("category"),
            "domain": position.get("domain"),
            "open_shares": round(open_shares, 6),
            "open_cost_usdc": round(open_cost, 4),
            "avg_entry_price": round(avg_entry_price, 6),
            "gross_buy_usdc": round(safe_float(position["gross_buy_usdc"]), 4),
            "gross_sell_usdc": round(safe_float(position["gross_sell_usdc"]), 4),
            "buy_fills": safe_int(position["buy_fills"]),
            "sell_fills": safe_int(position["sell_fills"]),
            "realized_pnl_usdc": round(safe_float(position["realized_pnl_usdc"]), 4),
            "last_fill_at_utc": position.get("last_fill_at_utc"),
        }
        if open_shares > 1e-9:
            open_positions.append(snapshot)
        else:
            closed_positions.append(snapshot)

    open_positions.sort(key=lambda item: (-safe_float(item["open_cost_usdc"]), item["key"]))
    closed_positions.sort(key=lambda item: str(item.get("last_fill_at_utc", "")), reverse=True)

    return {
        "open_positions_count": len(open_positions),
        "closed_positions_count": len(closed_positions),
        "open_position_notional_usdc": round(sum(safe_float(item["open_cost_usdc"]) for item in open_positions), 4),
        "realized_pnl_usdc": round(sum(safe_float(item["realized_pnl_usdc"]) for item in closed_positions), 4),
        "open_positions": open_positions[:20],
        "closed_positions": closed_positions[:20],
    }


def build_execution_metrics(cfg: PegConfig) -> Dict[str, Any]:
    all_orders = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
    all_fills = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "fills.jsonl"))
    run_orders = read_jsonl(cfg.orders_path)
    run_fills = read_jsonl(cfg.fills_path)
    run_dry_orders = read_jsonl_many(list_artifact_paths_recursive(cfg.data_dir, "orders_submitted.jsonl"))

    latest_orders = latest_orders_by_attempt(all_orders)
    run_latest_orders = latest_orders_by_attempt(run_orders)
    latest_status_counts: Dict[str, int] = {}
    run_status_counts: Dict[str, int] = {}
    open_orders: List[Dict[str, Any]] = []
    market_exposure: Dict[str, float] = {}
    category_exposure: Dict[str, float] = {}
    side_exposure: Dict[str, float] = {}
    order_lifecycle_seconds: List[float] = []
    fill_latency_seconds: List[float] = []

    for row in latest_orders:
        status = str(row.get("status", "") or "UNKNOWN").upper()
        latest_status_counts[status] = latest_status_counts.get(status, 0) + 1
        amount = safe_float(row.get("amount_usdc"))
        created_at = str(row.get("created_at_utc", "") or "")
        updated_at = str(row.get("updated_at_utc", "") or row.get("created_at_utc") or "")
        if created_at and updated_at and status in TERMINAL_STATES:
            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                order_lifecycle_seconds.append((updated_dt - created_dt).total_seconds())
            except ValueError:
                pass
        if status not in TERMINAL_STATES:
            open_orders.append(row)
            market_key = f"{row.get('market_id', '')}|{row.get('outcome_index', 0)}|{row.get('action', '')}"
            category_key = str(row.get("category", "") or "UNKNOWN")
            side_key = str(row.get("position_side", "") or str(row.get("action", "") or "UNKNOWN"))
            market_exposure[market_key] = market_exposure.get(market_key, 0.0) + amount
            category_exposure[category_key] = category_exposure.get(category_key, 0.0) + amount
            side_exposure[side_key] = side_exposure.get(side_key, 0.0) + amount

    for row in run_latest_orders:
        status = str(row.get("status", "") or "UNKNOWN").upper()
        run_status_counts[status] = run_status_counts.get(status, 0) + 1

    latest_orders_lookup = {
        str(row.get("order_attempt_id", "")): row
        for row in latest_orders
        if str(row.get("order_attempt_id", "") or "")
    }
    for fill in all_fills:
        attempt_id = str(fill.get("order_attempt_id", "") or "")
        order = latest_orders_lookup.get(attempt_id)
        if order is None:
            continue
        created_at = str(order.get("created_at_utc", "") or "")
        filled_at = str(fill.get("filled_at_utc", "") or "")
        if not created_at or not filled_at:
            continue
        try:
            created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            filled_dt = datetime.fromisoformat(filled_at.replace("Z", "+00:00"))
            fill_latency_seconds.append((filled_dt - created_dt).total_seconds())
        except ValueError:
            continue

    run_submitted_notional = sum(safe_float(row.get("amount_usdc")) for row in run_orders)
    if not run_orders and run_dry_orders:
        run_submitted_notional = sum(
            safe_float(row.get("submitted_amount_usdc") or row.get("amount_usdc"))
            for row in run_dry_orders
        )
    run_fill_notional = sum(safe_float(row.get("amount_usdc")) for row in run_fills)
    total_open_notional = sum(safe_float(row.get("amount_usdc")) for row in open_orders)
    total_fill_notional = sum(safe_float(row.get("amount_usdc")) for row in all_fills)
    position_snapshot = build_position_snapshot(all_fills)

    total_latest_orders = len(latest_orders)
    filled_orders = sum(1 for row in latest_orders if str(row.get("status", "")).upper() == "FILLED")
    partial_orders = sum(1 for row in latest_orders if str(row.get("status", "")).upper() == "PARTIALLY_FILLED")
    canceled_orders = sum(1 for row in latest_orders if order_status_family(str(row.get("status", ""))) == "canceled")
    failed_orders = sum(1 for row in latest_orders if order_status_family(str(row.get("status", ""))) == "failed")

    def top_items(source: Dict[str, float], limit: int = 10) -> List[Dict[str, Any]]:
        return [
            {"key": key, "amount_usdc": round(value, 4)}
            for key, value in sorted(source.items(), key=lambda item: (-item[1], item[0]))[:limit]
        ]

    recent_orders = sorted(
        [
            {
                "order_attempt_id": row.get("order_attempt_id"),
                "decision_id": row.get("decision_id"),
                "market_id": row.get("market_id"),
                "outcome_index": safe_int(row.get("outcome_index")),
                "action": row.get("action"),
                "position_side": row.get("position_side"),
                "amount_usdc": round(safe_float(row.get("amount_usdc")), 4),
                "price_limit": round(safe_float(row.get("price_limit")), 6),
                "status": row.get("status"),
                "status_reason": row.get("status_reason"),
                "updated_at_utc": row.get("updated_at_utc") or row.get("created_at_utc"),
                "run_id": row.get("run_id"),
            }
            for row in latest_orders
        ],
        key=lambda item: str(item.get("updated_at_utc", "")),
        reverse=True,
    )[:25]

    recent_fills = sorted(
        [
            {
                "fill_id": row.get("fill_id"),
                "order_attempt_id": row.get("order_attempt_id"),
                "market_id": row.get("market_id"),
                "action": row.get("action"),
                "position_side": row.get("position_side"),
                "amount_usdc": round(safe_float(row.get("amount_usdc")), 4),
                "price": round(safe_float(row.get("price")), 6),
                "filled_at_utc": row.get("filled_at_utc"),
                "category": row.get("category"),
            }
            for row in all_fills
        ],
        key=lambda item: str(item.get("filled_at_utc", "")),
        reverse=True,
    )[:25]

    return {
        "run_orders_count": len(run_orders) if run_orders else len(run_dry_orders),
        "run_fills_count": len(run_fills),
        "run_submitted_notional_usdc": round(run_submitted_notional, 4),
        "run_filled_notional_usdc": round(run_fill_notional, 4),
        "run_avg_order_usdc": round(run_submitted_notional / (len(run_orders) if run_orders else len(run_dry_orders)), 4)
        if (run_orders or run_dry_orders)
        else 0.0,
        "run_latest_order_status_counts": dict(sorted(run_status_counts.items(), key=lambda item: item[0])),
        "latest_order_status_counts": dict(sorted(latest_status_counts.items(), key=lambda item: item[0])),
        "order_lifecycle": {
            "total_orders": total_latest_orders,
            "filled_orders": filled_orders,
            "partial_orders": partial_orders,
            "canceled_orders": canceled_orders,
            "failed_orders": failed_orders,
            "fill_rate": round(safe_div(filled_orders + partial_orders, total_latest_orders), 4),
            "cancel_rate": round(safe_div(canceled_orders, total_latest_orders), 4),
            "failure_rate": round(safe_div(failed_orders, total_latest_orders), 4),
            "avg_terminal_lifecycle_sec": round(mean(order_lifecycle_seconds), 4),
            "avg_fill_latency_sec": round(mean(fill_latency_seconds), 4),
        },
        "current_open_orders_count": len(open_orders),
        "current_open_notional_usdc": round(total_open_notional, 4),
        "lifetime_filled_notional_usdc": round(total_fill_notional, 4),
        "positions": position_snapshot,
        "top_open_market_exposure": top_items(market_exposure),
        "top_open_category_exposure": top_items(category_exposure),
        "top_open_side_exposure": top_items(side_exposure),
        "recent_orders": recent_orders,
        "recent_fills": recent_fills,
    }


def build_shared_state(cfg: PegConfig) -> Dict[str, Any]:
    market_state = load_json(cfg.market_state_cache_path) or {}
    state_snapshot = load_json(cfg.state_snapshot_path) or {}
    return {
        "market_state_cache_path": str(cfg.market_state_cache_path),
        "state_snapshot_path": str(cfg.state_snapshot_path),
        "pending_market_count": safe_int(market_state.get("pending_market_count")),
        "open_market_count": safe_int(market_state.get("open_market_count")),
        "market_state_latest_order_count": safe_int(market_state.get("latest_order_count")),
        "market_state_fill_count": safe_int(market_state.get("fill_count")),
        "state_open_orders_count": safe_int(state_snapshot.get("open_orders_count")),
        "state_net_exposure_usdc": round(safe_float(state_snapshot.get("net_exposure_usdc")), 4),
        "state_daily_pnl_usdc": round(safe_float(state_snapshot.get("daily_pnl_usdc")), 4),
        "state_market_action_filled_count": len(state_snapshot.get("market_action_filled", []) or []),
        "state_decision_last_seen_count": len(state_snapshot.get("decision_last_seen", {}) or {}),
    }


