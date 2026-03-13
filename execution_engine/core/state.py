"""Lightweight state store using JSONL logs under execution_engine/data/."""

from __future__ import annotations

from typing import Dict, Set

from .config import PegConfig
from ..execution.state_machine import TERMINAL_STATES
from ..utils.io import append_jsonl, read_jsonl
from ..utils.time import parse_utc, utc_now


def _market_action_key(market_id: str, outcome_index: int, action: str) -> str:
    return f"{market_id}|{outcome_index}|{action}"


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
        orders = read_jsonl(self.cfg.orders_path)
        latest_by_order: Dict[str, Dict[str, object]] = {}
        for order in orders:
            order_attempt_id = str(order.get("order_attempt_id", ""))
            if not order_attempt_id:
                continue
            prior = latest_by_order.get(order_attempt_id)
            if prior is None:
                latest_by_order[order_attempt_id] = order
                continue
            prior_time = prior.get("updated_at_utc") or prior.get("created_at_utc")
            curr_time = order.get("updated_at_utc") or order.get("created_at_utc")
            if prior_time and curr_time and str(curr_time) >= str(prior_time):
                latest_by_order[order_attempt_id] = order

        for order in latest_by_order.values():
            self._apply_order_record(order)
            decision_id = order.get("decision_id")
            if decision_id:
                timestamp = order.get("updated_at_utc") or order.get("created_at_utc")
                if timestamp:
                    try:
                        self.decision_last_seen[str(decision_id)] = parse_utc(str(timestamp))
                    except ValueError:
                        pass

        fills = read_jsonl(self.cfg.fills_path)
        today = utc_now().date()
        for fill in fills:
            market_id = str(fill.get("market_id", ""))
            outcome_index = int(fill.get("outcome_index", 0))
            action = str(fill.get("action", ""))
            amount_usdc = float(fill.get("amount_usdc", 0.0))
            category = str(fill.get("category", "")).strip()
            pnl = float(fill.get("pnl_usdc", 0.0))

            if market_id and action:
                key = _market_action_key(market_id, outcome_index, action)
                self.market_exposure_usdc[key] = self.market_exposure_usdc.get(key, 0.0) + amount_usdc
                self.market_action_filled.add(key)
            if category:
                self.category_exposure_usdc[category] = self.category_exposure_usdc.get(category, 0.0) + amount_usdc
            self.net_exposure_usdc += amount_usdc

            filled_at = fill.get("filled_at_utc")
            if filled_at:
                try:
                    if parse_utc(str(filled_at)).date() == today:
                        self.daily_pnl_usdc += pnl
                except ValueError:
                    continue

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
        else:
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

    def record_rejection(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.rejections_path, record)

    def record_event(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.events_path, record)

    def record_fill(self, record: Dict[str, object]) -> None:
        append_jsonl(self.cfg.fills_path, record)

    def get_market_exposure(self, market_id: str, outcome_index: int, action: str) -> float:
        return self.market_exposure_usdc.get(_market_action_key(market_id, outcome_index, action), 0.0)

    def get_category_exposure(self, category: str) -> float:
        return self.category_exposure_usdc.get(category, 0.0)

    def current_daily_pnl(self) -> float:
        return self.daily_pnl_usdc
