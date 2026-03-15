"""Lightweight state store using cached JSON state plus JSONL logs."""

from __future__ import annotations

from typing import Any, Dict, Set
import json

from .config import PegConfig
from execution_engine.integrations.trading.state_machine import TERMINAL_STATES
from execution_engine.shared.io import append_jsonl, list_run_artifact_paths, read_jsonl_many
from execution_engine.shared.time import parse_utc, to_iso, utc_now


def _market_action_key(market_id: str, outcome_index: int, action: str) -> str:
    return f"{market_id}|{outcome_index}|{action}"


def _latest_orders_by_attempt(rows: list[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    latest: Dict[str, Dict[str, object]] = {}
    for order in rows:
        order_attempt_id = str(order.get("order_attempt_id", ""))
        if not order_attempt_id:
            continue
        prior = latest.get(order_attempt_id)
        if prior is None:
            latest[order_attempt_id] = order
            continue
        prior_time = prior.get("updated_at_utc") or prior.get("created_at_utc")
        curr_time = order.get("updated_at_utc") or order.get("created_at_utc")
        if prior_time and curr_time and str(curr_time) >= str(prior_time):
            latest[order_attempt_id] = order
    return latest


def _empty_state_payload(cfg: PegConfig) -> Dict[str, Any]:
    return {
        "generated_at_utc": to_iso(utc_now()),
        "orders_root_dir": str(cfg.runs_root_dir),
        "decision_last_seen": {},
        "market_action_filled": [],
        "open_orders_count": 0,
        "net_exposure_usdc": 0.0,
        "market_exposure_usdc": {},
        "category_exposure_usdc": {},
        "daily_pnl_usdc": 0.0,
        "latest_order_count": 0,
        "fill_count": 0,
    }


def _read_state_snapshot(cfg: PegConfig) -> Dict[str, Any] | None:
    path = cfg.state_snapshot_path
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if str(payload.get("orders_root_dir", "")) != str(cfg.runs_root_dir):
        return None
    return payload


def _write_state_snapshot(cfg: PegConfig, payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload)
    payload["generated_at_utc"] = to_iso(utc_now())
    payload["orders_root_dir"] = str(cfg.runs_root_dir)
    cfg.state_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.state_snapshot_path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return payload


def build_state_snapshot(cfg: PegConfig) -> Dict[str, Any]:
    orders = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "orders.jsonl"))
    fills = read_jsonl_many(list_run_artifact_paths(cfg.runs_root_dir, "fills.jsonl"))
    latest_by_order = _latest_orders_by_attempt(orders)

    decision_last_seen: Dict[str, str] = {}
    market_action_filled: Set[str] = set()
    open_orders_count = 0
    net_exposure_usdc = 0.0
    market_exposure_usdc: Dict[str, float] = {}
    category_exposure_usdc: Dict[str, float] = {}
    daily_pnl_usdc = 0.0
    today = utc_now().date()

    for order in latest_by_order.values():
        decision_id = str(order.get("decision_id", "") or "")
        status = str(order.get("status", "")).upper()
        market_id = str(order.get("market_id", "") or "")
        outcome_index = int(order.get("outcome_index", 0))
        action = str(order.get("action", "") or "")
        amount_usdc = float(order.get("amount_usdc", 0.0))
        category = str(order.get("category", "")).strip()

        if decision_id:
            timestamp = str(order.get("updated_at_utc") or order.get("created_at_utc") or "")
            if timestamp:
                decision_last_seen[decision_id] = timestamp

        if status and status not in TERMINAL_STATES:
            open_orders_count += 1
            net_exposure_usdc += amount_usdc
            if market_id and action:
                key = _market_action_key(market_id, outcome_index, action)
                market_exposure_usdc[key] = market_exposure_usdc.get(key, 0.0) + amount_usdc
            if category:
                category_exposure_usdc[category] = category_exposure_usdc.get(category, 0.0) + amount_usdc

    for fill in fills:
        market_id = str(fill.get("market_id", "") or "")
        outcome_index = int(fill.get("outcome_index", 0))
        action = str(fill.get("action", "") or "")
        amount_usdc = float(fill.get("amount_usdc", 0.0))
        category = str(fill.get("category", "")).strip()
        pnl = float(fill.get("pnl_usdc", 0.0))

        if market_id and action:
            key = _market_action_key(market_id, outcome_index, action)
            market_exposure_usdc[key] = market_exposure_usdc.get(key, 0.0) + amount_usdc
            market_action_filled.add(key)
        if category:
            category_exposure_usdc[category] = category_exposure_usdc.get(category, 0.0) + amount_usdc
        net_exposure_usdc += amount_usdc

        filled_at = fill.get("filled_at_utc")
        if filled_at:
            try:
                if parse_utc(str(filled_at)).date() == today:
                    daily_pnl_usdc += pnl
            except ValueError:
                continue

    payload = {
        "decision_last_seen": dict(sorted(decision_last_seen.items())),
        "market_action_filled": sorted(market_action_filled),
        "open_orders_count": int(open_orders_count),
        "net_exposure_usdc": float(net_exposure_usdc),
        "market_exposure_usdc": {key: float(value) for key, value in sorted(market_exposure_usdc.items())},
        "category_exposure_usdc": {key: float(value) for key, value in sorted(category_exposure_usdc.items())},
        "daily_pnl_usdc": float(daily_pnl_usdc),
        "latest_order_count": int(len(latest_by_order)),
        "fill_count": int(len(fills)),
    }
    return _write_state_snapshot(cfg, payload)


def refresh_state_snapshot(cfg: PegConfig) -> Dict[str, Any]:
    return build_state_snapshot(cfg)


class StateStore:
    def __init__(self, cfg: PegConfig) -> None:
        self.cfg = cfg
        self.decision_ids: Set[str] = set()
        self.decision_last_seen: Dict[str, object] = {}
        self.market_action_filled: Set[str] = set()
        self.open_orders_count = 0
        self.net_exposure_usdc = 0.0
        self.market_exposure_usdc: Dict[str, float] = {}
        self.category_exposure_usdc: Dict[str, float] = {}
        self.daily_pnl_usdc = 0.0
        self._load_existing()

    def _load_existing(self) -> None:
        payload = _read_state_snapshot(self.cfg)
        if payload is None:
            payload = build_state_snapshot(self.cfg)
        self._apply_snapshot(payload)

    def _apply_snapshot(self, payload: Dict[str, Any]) -> None:
        self.decision_last_seen = {}
        for decision_id, timestamp in (payload.get("decision_last_seen", {}) or {}).items():
            try:
                self.decision_last_seen[str(decision_id)] = parse_utc(str(timestamp))
            except ValueError:
                continue
        self.decision_ids = set(self.decision_last_seen.keys())
        self.market_action_filled = {
            str(value)
            for value in (payload.get("market_action_filled", []) or [])
            if str(value)
        }
        self.open_orders_count = int(payload.get("open_orders_count", 0) or 0)
        self.net_exposure_usdc = float(payload.get("net_exposure_usdc", 0.0) or 0.0)
        self.market_exposure_usdc = {
            str(key): float(value)
            for key, value in (payload.get("market_exposure_usdc", {}) or {}).items()
        }
        self.category_exposure_usdc = {
            str(key): float(value)
            for key, value in (payload.get("category_exposure_usdc", {}) or {}).items()
        }
        self.daily_pnl_usdc = float(payload.get("daily_pnl_usdc", 0.0) or 0.0)

    def _snapshot_payload(self) -> Dict[str, Any]:
        return {
            "decision_last_seen": {
                key: to_iso(value)
                for key, value in sorted(self.decision_last_seen.items())
            },
            "market_action_filled": sorted(self.market_action_filled),
            "open_orders_count": int(self.open_orders_count),
            "net_exposure_usdc": float(self.net_exposure_usdc),
            "market_exposure_usdc": {
                key: float(value)
                for key, value in sorted(self.market_exposure_usdc.items())
            },
            "category_exposure_usdc": {
                key: float(value)
                for key, value in sorted(self.category_exposure_usdc.items())
            },
            "daily_pnl_usdc": float(self.daily_pnl_usdc),
            "latest_order_count": 0,
            "fill_count": 0,
        }

    def persist_snapshot(self) -> Dict[str, Any]:
        return _write_state_snapshot(self.cfg, self._snapshot_payload())

    def _apply_order_record(self, order: Dict[str, object]) -> None:
        decision_id = order.get("decision_id")
        if decision_id:
            self.decision_ids.add(str(decision_id))

        status = str(order.get("status", "")).upper()
        market_id = str(order.get("market_id", ""))
        outcome_index = int(order.get("outcome_index", 0))
        action = str(order.get("action", ""))
        amount_usdc = float(order.get("amount_usdc", 0.0))
        category = str(order.get("category", "")).strip()

        if status and status not in TERMINAL_STATES:
            self.open_orders_count += 1
            self.net_exposure_usdc += amount_usdc
            if market_id and action:
                key = _market_action_key(market_id, outcome_index, action)
                self.market_exposure_usdc[key] = self.market_exposure_usdc.get(key, 0.0) + amount_usdc
            if category:
                self.category_exposure_usdc[category] = self.category_exposure_usdc.get(category, 0.0) + amount_usdc
        elif status == "FILLED":
            if market_id and action:
                self.market_action_filled.add(_market_action_key(market_id, outcome_index, action))

    def seen_decision(self, decision_id: str) -> bool:
        return decision_id in self.decision_ids

    def seen_recent_decision(self, decision_id: str, window_sec: int) -> bool:
        if window_sec <= 0:
            return False
        last_seen = self.decision_last_seen.get(decision_id)
        if not last_seen:
            return False
        try:
            delta = (utc_now() - last_seen).total_seconds()
        except TypeError:
            return False
        return delta <= window_sec

    def seen_market_action(self, market_id: str, outcome_index: int, action: str) -> bool:
        key = _market_action_key(market_id, outcome_index, action)
        return key in self.market_action_filled

    def record_decision(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.decisions_path, record)
        decision_id = str(record.get("decision_id", ""))
        if decision_id:
            self.decision_ids.add(decision_id)

    def record_order(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.orders_path, record)
        self._apply_order_record(record)
        decision_id = record.get("decision_id")
        if decision_id:
            timestamp = record.get("updated_at_utc") or record.get("created_at_utc")
            if timestamp:
                try:
                    self.decision_last_seen[str(decision_id)] = parse_utc(str(timestamp))
                except ValueError:
                    pass
        self.persist_snapshot()

    def record_rejection(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.rejections_path, record)

    def record_event(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.events_path, record)

    def record_fill(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.fills_path, record)
        market_id = str(record.get("market_id", "") or "")
        outcome_index = int(record.get("outcome_index", 0))
        action = str(record.get("action", "") or "")
        amount_usdc = float(record.get("amount_usdc", 0.0) or 0.0)
        category = str(record.get("category", "") or "").strip()
        pnl = float(record.get("pnl_usdc", 0.0) or 0.0)
        if market_id and action:
            key = _market_action_key(market_id, outcome_index, action)
            self.market_action_filled.add(key)
            self.market_exposure_usdc[key] = self.market_exposure_usdc.get(key, 0.0) + amount_usdc
        if category:
            self.category_exposure_usdc[category] = self.category_exposure_usdc.get(category, 0.0) + amount_usdc
        self.net_exposure_usdc += amount_usdc
        self.daily_pnl_usdc += pnl
        self.persist_snapshot()

    def get_market_exposure(self, market_id: str, outcome_index: int, action: str) -> float:
        return self.market_exposure_usdc.get(_market_action_key(market_id, outcome_index, action), 0.0)

    def get_category_exposure(self, category: str) -> float:
        return self.category_exposure_usdc.get(category, 0.0)

    def current_daily_pnl(self) -> float:
        return self.daily_pnl_usdc
