"""Helpers for shared online position and market exclusion state."""

from __future__ import annotations

from typing import Any, Dict, List, Set
import json

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.io import list_run_artifact_paths, read_jsonl, read_jsonl_many
from execution_engine.shared.time import to_iso, utc_now

PENDING_ORDER_STATUSES = {
    "NEW",
    "SENT",
    "ACKED",
    "DELAYED",
    "PARTIALLY_FILLED",
    "CANCEL_REQUESTED",
    "DRY_RUN_SUBMITTED",
}


def _latest_by_order_attempt(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
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
    return latest


def _read_market_state_cache(cfg: PegConfig) -> Dict[str, Any] | None:
    path = cfg.market_state_cache_path
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _write_market_state_cache(cfg: PegConfig, payload: Dict[str, Any]) -> None:
    cfg.market_state_cache_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.market_state_cache_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def _write_open_positions(cfg: PegConfig, positions: List[Dict[str, Any]]) -> None:
    cfg.open_positions_path.parent.mkdir(parents=True, exist_ok=True)
    with cfg.open_positions_path.open("w", encoding="utf-8") as handle:
        for row in positions:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def _build_open_positions_from_fills(fills: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    positions_by_key: Dict[str, Dict[str, Any]] = {}
    for fill in sorted(fills, key=lambda row: str(row.get("filled_at_utc") or "")):
        market_id = str(fill.get("market_id", "") or "")
        token_id = str(fill.get("token_id", "") or "")
        outcome_index = int(fill.get("outcome_index", 0) or 0)
        if not market_id or not token_id:
            continue
        key = f"{market_id}|{token_id}|{outcome_index}"
        action = str(fill.get("action", "") or "BUY").upper()
        try:
            shares = float(fill.get("shares", 0.0) or 0.0)
        except (TypeError, ValueError):
            shares = 0.0
        try:
            amount_usdc = float(fill.get("amount_usdc", 0.0) or 0.0)
        except (TypeError, ValueError):
            amount_usdc = 0.0
        try:
            price = float(fill.get("price", 0.0) or 0.0)
        except (TypeError, ValueError):
            price = 0.0

        position = positions_by_key.setdefault(
            key,
            {
                "market_id": market_id,
                "token_id": token_id,
                "outcome_index": outcome_index,
                "outcome_label": str(fill.get("outcome_label", "") or ""),
                "category": str(fill.get("category", "") or ""),
                "domain": str(fill.get("domain", "") or ""),
                "event_id": str(fill.get("event_id", "") or ""),
                "entry_run_id": str(fill.get("run_id", "") or ""),
                "entry_order_attempt_id": str(fill.get("order_attempt_id", "") or ""),
                "opened_at_utc": str(fill.get("filled_at_utc", "") or ""),
                "open_shares": 0.0,
                "open_cost_usdc": 0.0,
                "entry_price": 0.0,
            },
        )

        if action == "SELL":
            if position["open_shares"] <= 0 or shares <= 0:
                continue
            avg_cost = position["open_cost_usdc"] / position["open_shares"] if position["open_shares"] > 0 else 0.0
            closed_shares = min(float(position["open_shares"]), shares)
            position["open_shares"] = max(0.0, float(position["open_shares"]) - closed_shares)
            position["open_cost_usdc"] = max(0.0, float(position["open_cost_usdc"]) - avg_cost * closed_shares)
            if position["open_shares"] <= 1e-9:
                position["open_shares"] = 0.0
                position["open_cost_usdc"] = 0.0
            continue

        position["open_shares"] = float(position["open_shares"]) + max(shares, 0.0)
        position["open_cost_usdc"] = float(position["open_cost_usdc"]) + max(amount_usdc, 0.0)
        if price > 0:
            position["entry_price"] = (
                float(position["open_cost_usdc"]) / float(position["open_shares"])
                if float(position["open_shares"]) > 0
                else price
            )

    positions: List[Dict[str, Any]] = []
    for position in positions_by_key.values():
        open_shares = float(position.get("open_shares", 0.0) or 0.0)
        if open_shares <= 1e-9:
            continue
        positions.append(
            {
                "market_id": str(position.get("market_id") or ""),
                "token_id": str(position.get("token_id") or ""),
                "outcome_index": int(position.get("outcome_index", 0) or 0),
                "outcome_label": str(position.get("outcome_label") or ""),
                "category": str(position.get("category") or ""),
                "domain": str(position.get("domain") or ""),
                "event_id": str(position.get("event_id") or ""),
                "entry_run_id": str(position.get("entry_run_id") or ""),
                "entry_order_attempt_id": str(position.get("entry_order_attempt_id") or ""),
                "entry_price": float(position.get("entry_price", 0.0) or 0.0),
                "filled_amount_usdc": round(float(position.get("open_cost_usdc", 0.0) or 0.0), 6),
                "filled_shares": round(open_shares, 6),
                "opened_at_utc": str(position.get("opened_at_utc") or ""),
                "status": "OPEN",
            }
        )
    positions.sort(key=lambda row: str(row.get("opened_at_utc") or ""))
    return positions


def refresh_market_state_cache(cfg: PegConfig) -> Dict[str, Any]:
    orders = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
    fills = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "fills.jsonl"))
    latest_orders = _latest_by_order_attempt(orders)
    positions = _build_open_positions_from_fills(fills)
    _write_open_positions(cfg, positions)

    pending_market_ids = sorted(
        {
            str(row.get("market_id", "") or "")
            for row in latest_orders.values()
            if str(row.get("status", "") or "").upper() in PENDING_ORDER_STATUSES
            and str(row.get("market_id", "") or "")
        }
    )
    open_market_ids = sorted(
        {
            str(row.get("market_id", "") or "")
            for row in positions
            if str(row.get("market_id", "") or "")
        }
        | {
            str(fill.get("market_id", "") or "")
            for fill in fills
            if str(fill.get("market_id", "") or "")
        }
    )
    held_event_ids = sorted(
        {
            str(row.get("event_id", "") or "")
            for row in positions
            if str(row.get("event_id", "") or "")
        }
    )
    order_status_counts: Dict[str, int] = {}
    for row in latest_orders.values():
        status = str(row.get("status", "") or "UNKNOWN").upper()
        order_status_counts[status] = order_status_counts.get(status, 0) + 1

    payload = {
        "generated_at_utc": to_iso(utc_now()),
        "orders_root_dir": str(cfg.runs_root_dir),
        "open_positions_path": str(cfg.open_positions_path),
        "latest_order_count": int(len(latest_orders)),
        "fill_count": int(len(fills)),
        "open_position_count": int(len(positions)),
        "pending_market_count": int(len(pending_market_ids)),
        "open_market_count": int(len(open_market_ids)),
        "pending_market_ids": pending_market_ids,
        "open_market_ids": open_market_ids,
        "held_event_ids": held_event_ids,
        "order_status_counts": dict(sorted(order_status_counts.items())),
    }
    _write_market_state_cache(cfg, payload)
    return payload


def _load_or_refresh_market_state(cfg: PegConfig) -> Dict[str, Any]:
    cached = _read_market_state_cache(cfg)
    if cached is not None:
        return cached
    return refresh_market_state_cache(cfg)


def load_open_position_rows(cfg: PegConfig) -> List[Dict[str, Any]]:
    rows = read_jsonl(cfg.open_positions_path)
    if cfg.open_positions_path.exists():
        return [row for row in rows if str(row.get("status", "OPEN")).upper() == "OPEN"]
    refresh_market_state_cache(cfg)
    rows = read_jsonl(cfg.open_positions_path)
    return [row for row in rows if str(row.get("status", "OPEN")).upper() == "OPEN"]


def load_open_market_ids(cfg: PegConfig) -> Set[str]:
    payload = _load_or_refresh_market_state(cfg)
    values = payload.get("open_market_ids", [])
    return {str(value) for value in values if str(value)}


def load_pending_market_ids(cfg: PegConfig) -> Set[str]:
    payload = _load_or_refresh_market_state(cfg)
    values = payload.get("pending_market_ids", [])
    return {str(value) for value in values if str(value)}


def load_held_event_ids(cfg: PegConfig) -> Set[str]:
    payload = _load_or_refresh_market_state(cfg)
    values = payload.get("held_event_ids", [])
    return {str(value) for value in values if str(value)}


def rebuild_open_positions_ledger(cfg: PegConfig) -> List[Dict[str, Any]]:
    refresh_market_state_cache(cfg)
    return load_open_position_rows(cfg)

