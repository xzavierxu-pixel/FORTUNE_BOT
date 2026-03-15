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
    fills_by_market: Dict[str, List[Dict[str, Any]]] = {}
    for fill in fills:
        market_id = str(fill.get("market_id", "") or "")
        if not market_id:
            continue
        fills_by_market.setdefault(market_id, []).append(fill)

    positions: List[Dict[str, Any]] = []
    for market_id, market_fills in sorted(fills_by_market.items()):
        market_fills = sorted(
            market_fills,
            key=lambda row: str(row.get("filled_at_utc") or ""),
        )
        first_fill = market_fills[0]
        total_amount_usdc = 0.0
        total_shares = 0.0
        last_price = 0.0
        for fill in market_fills:
            try:
                total_amount_usdc += float(fill.get("amount_usdc", 0.0) or 0.0)
            except (TypeError, ValueError):
                pass
            try:
                total_shares += float(fill.get("shares", 0.0) or 0.0)
            except (TypeError, ValueError):
                pass
            try:
                price = float(fill.get("price", 0.0) or 0.0)
                if price > 0:
                    last_price = price
            except (TypeError, ValueError):
                continue

        positions.append(
            {
                "market_id": market_id,
                "token_id": str(first_fill.get("token_id", "") or ""),
                "outcome_label": str(first_fill.get("outcome_label", "") or ""),
                "entry_run_id": str(first_fill.get("run_id", "") or ""),
                "entry_order_attempt_id": str(first_fill.get("order_attempt_id", "") or ""),
                "entry_price": last_price,
                "filled_amount_usdc": total_amount_usdc,
                "filled_shares": total_shares,
                "opened_at_utc": str(first_fill.get("filled_at_utc", "") or ""),
                "status": "OPEN",
            }
        )
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


def rebuild_open_positions_ledger(cfg: PegConfig) -> List[Dict[str, Any]]:
    refresh_market_state_cache(cfg)
    return load_open_position_rows(cfg)

