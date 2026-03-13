"""PEG runner: merge signals, perform checks, write decisions/orders/rejections."""

from __future__ import annotations

from typing import Dict, List, Optional
import time

from .validation import check_price_and_liquidity
from .validation import check_basic_risk
from .config import PegConfig, load_config
from .decision import merge_rule_and_llm
from ..execution.clob_client import NullClobClient, build_clob_client
from ..execution.nonce import NonceManager
from ..execution.order_manager import reconcile, submit_order, sweep_expired_orders
from ..utils.io import index_by, read_jsonl
from .models import SignalPayload, ensure_ids
from ..utils.alerts import record_alert
from ..utils.logger import log_structured
from ..utils.metrics import increment_metric
from ..connectors.balance_provider import ClobBalanceProvider, FileBalanceProvider
from ..connectors.price_provider import ClobMidPriceProvider, FileMidPriceProvider
from ..connectors.token_mapper import TokenMapper
from .state import StateStore
from ..utils.time import to_iso, utc_now

_ALERT_REASONS = {
    "CIRCUIT_OPEN",
    "BALANCE_INSUFFICIENT",
    "DAILY_LOSS_LIMIT",
    "OPEN_ORDERS_LIMIT",
}


def _key(signal: SignalPayload) -> str:
    return f"{signal.get('market_id')}|{signal.get('outcome_index')}|{signal.get('action')}"


def _normalize_signal(signal: SignalPayload, cfg: PegConfig) -> SignalPayload:
    now = utc_now()
    signal.setdefault("order_type", "LIMIT")
    signal.setdefault("amount_usdc", cfg.order_usdc)
    signal.setdefault("expiration_seconds", cfg.order_ttl_sec)
    signal.setdefault("created_at_utc", to_iso(now))
    signal.setdefault("decision_window_start_utc", to_iso(now))
    signal.setdefault("decision_window_end_utc", _safe_extend_seconds(to_iso(now), cfg.signal_ttl_sec_max))
    signal.setdefault("valid_until_utc", _safe_extend_seconds(to_iso(now), cfg.signal_ttl_sec_max))
    return ensure_ids(signal)


def _safe_extend_seconds(ts_iso: str, seconds: int) -> str:
    from datetime import timedelta
    from ..utils.time import parse_utc

    dt = parse_utc(ts_iso)
    return to_iso(dt + timedelta(seconds=seconds))


def _ensure_time_window(signal: SignalPayload, cfg: PegConfig) -> SignalPayload:
    now_iso = to_iso(utc_now())
    if not signal.get("decision_window_start_utc"):
        signal["decision_window_start_utc"] = now_iso
    if not signal.get("decision_window_end_utc"):
        signal["decision_window_end_utc"] = _safe_extend_seconds(
            signal["decision_window_start_utc"], cfg.signal_ttl_sec_max
        )
    if not signal.get("valid_until_utc"):
        signal["valid_until_utc"] = _safe_extend_seconds(now_iso, cfg.signal_ttl_sec_max)
    return signal


def _load_signals(cfg: PegConfig) -> List[SignalPayload]:
    return read_jsonl(cfg.rule_signals_path)


def _load_llm_index(cfg: PegConfig) -> Dict[str, SignalPayload]:
    llm_rows = read_jsonl(cfg.llm_signals_path)
    return index_by(llm_rows, _key)


def _price_check(cfg: PegConfig, provider, price_key: str, reference_mid: float) -> Optional[str]:
    for attempt in range(cfg.price_refresh_retries + 1):
        try:
            mid_now, spread_now, depth = provider.get(price_key)
        except KeyError:
            return "MID_PRICE_MISSING"

        ok, reason = check_price_and_liquidity(
            float(reference_mid), mid_now, spread_now, depth, cfg
        )
        if ok:
            return None

        if attempt < cfg.price_refresh_retries:
            time.sleep(cfg.price_refresh_backoff_sec)
            if hasattr(provider, "_load"):
                provider._load()
        else:
            return reason

    return "PRICE_CHECK_FAILED"


def _log_rejection(cfg: PegConfig, record: Dict[str, object]) -> None:
    state_record = dict(record)
    state_record.setdefault("created_at_utc", to_iso(utc_now()))
    log_structured(cfg.logs_path, {"type": "rejection", **state_record})
    increment_metric(cfg.metrics_path, "rejections_count", 1)
    if state_record.get("reason_code") in _ALERT_REASONS:
        record_alert(cfg.alerts_path, {"type": "alert", **state_record})


def run_once(cfg: PegConfig) -> None:
    cfg.ensure_dirs()

    clob_client = build_clob_client(cfg)
    use_clob = not isinstance(clob_client, NullClobClient)
    token_mapper = TokenMapper(cfg.gamma_base_url, cfg.token_cache_path, cfg.token_cache_ttl_sec, cfg.clob_request_timeout_sec)

    sweep_expired_orders(cfg, clob_client)
    reconcile(cfg, clob_client)

    state = StateStore(cfg)
    nonce_manager = NonceManager(cfg.nonce_path)

    price_mode = cfg.price_source.lower().strip()
    if price_mode == "clob" and not use_clob:
        price_mode = "file"

    if price_mode == "clob":
        price_provider = ClobMidPriceProvider(clob_client)
    else:
        price_provider = FileMidPriceProvider(cfg.mid_prices_path)

    if cfg.balance_source.lower().strip() == "clob" and use_clob:
        balance_provider = ClobBalanceProvider(clob_client)
    else:
        balance_provider = FileBalanceProvider(cfg.balances_path)

    rule_signals = _load_signals(cfg)
    llm_index = _load_llm_index(cfg)

    for raw_rule in rule_signals:
        rule_signal = _ensure_time_window(_normalize_signal(raw_rule, cfg), cfg)
        llm_signal = llm_index.get(_key(rule_signal))

        decision, reason = merge_rule_and_llm(rule_signal, llm_signal, cfg)
        if decision is None:
            rejection = {
                "decision_id": rule_signal.get("decision_id"),
                "market_id": rule_signal.get("market_id"),
                "outcome_index": rule_signal.get("outcome_index"),
                "action": rule_signal.get("action"),
                "reason_code": reason,
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": rule_signal.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        state.record_decision(decision)
        state.record_event(
            {
                "event_time_utc": to_iso(utc_now()),
                "event_type": "DECISION_CREATED",
                "decision_id": decision.get("decision_id"),
                "order_attempt_id": rule_signal.get("order_attempt_id"),
                "payload": decision,
            }
        )
        log_structured(cfg.logs_path, {"type": "decision", **decision})
        increment_metric(cfg.metrics_path, "decisions_count", 1)

        ok, risk_reason = check_basic_risk(rule_signal, state, cfg, balance_provider)
        if not ok:
            rejection = {
                "decision_id": decision.get("decision_id"),
                "market_id": decision.get("market_id"),
                "outcome_index": decision.get("outcome_index"),
                "action": decision.get("action"),
                "reason_code": risk_reason,
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": decision.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        token_id = None
        if use_clob:
            try:
                token_id = token_mapper.get_token_id(
                    str(decision.get("market_id")), int(decision.get("outcome_index", 0))
                )
            except KeyError:
                rejection = {
                    "decision_id": decision.get("decision_id"),
                    "market_id": decision.get("market_id"),
                    "outcome_index": decision.get("outcome_index"),
                    "action": decision.get("action"),
                    "reason_code": "TOKEN_ID_MISSING",
                    "created_at_utc": to_iso(utc_now()),
                }
                state.record_rejection(rejection)
                state.record_event(
                    {
                        "event_time_utc": to_iso(utc_now()),
                        "event_type": "REJECTION",
                        "decision_id": decision.get("decision_id"),
                        "order_attempt_id": rule_signal.get("order_attempt_id"),
                        "payload": rejection,
                    }
                )
                _log_rejection(cfg, rejection)
                continue

        reference_mid = rule_signal.get("reference_mid_price")
        if reference_mid is None:
            rejection = {
                "decision_id": decision.get("decision_id"),
                "market_id": decision.get("market_id"),
                "outcome_index": decision.get("outcome_index"),
                "action": decision.get("action"),
                "reason_code": "MISSING_REFERENCE_PRICE",
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": decision.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        price_key = token_id if price_mode == "clob" else str(decision.get("market_id"))
        if price_key is None:
            rejection = {
                "decision_id": decision.get("decision_id"),
                "market_id": decision.get("market_id"),
                "outcome_index": decision.get("outcome_index"),
                "action": decision.get("action"),
                "reason_code": "PRICE_SOURCE_UNAVAILABLE",
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": decision.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        price_reason = _price_check(cfg, price_provider, price_key, float(reference_mid))
        if price_reason:
            rejection = {
                "decision_id": decision.get("decision_id"),
                "market_id": decision.get("market_id"),
                "outcome_index": decision.get("outcome_index"),
                "action": decision.get("action"),
                "reason_code": price_reason,
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": decision.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        try:
            order = submit_order(cfg, decision, rule_signal, nonce_manager, clob_client, token_id)
        except ValueError as exc:
            rejection = {
                "decision_id": decision.get("decision_id"),
                "market_id": decision.get("market_id"),
                "outcome_index": decision.get("outcome_index"),
                "action": decision.get("action"),
                "reason_code": str(exc),
                "created_at_utc": to_iso(utc_now()),
            }
            state.record_rejection(rejection)
            state.record_event(
                {
                    "event_time_utc": to_iso(utc_now()),
                    "event_type": "REJECTION",
                    "decision_id": decision.get("decision_id"),
                    "order_attempt_id": rule_signal.get("order_attempt_id"),
                    "payload": rejection,
                }
            )
            _log_rejection(cfg, rejection)
            continue

        state.record_order(order)
        state.record_event(
            {
                "event_time_utc": to_iso(utc_now()),
                "event_type": "ORDER_SUBMITTED",
                "decision_id": decision.get("decision_id"),
                "order_attempt_id": order.get("order_attempt_id"),
                "payload": order,
            }
        )
        log_structured(cfg.logs_path, {"type": "order", **order})
        increment_metric(cfg.metrics_path, "orders_sent", 1)


def main() -> None:
    cfg = load_config()
    run_once(cfg)


if __name__ == "__main__":
    main()
