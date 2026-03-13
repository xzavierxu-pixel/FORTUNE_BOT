"""Demo runner for PEG (file-based, DRY_RUN)."""

from __future__ import annotations

import argparse
from pathlib import Path
import json
import os
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from execution_engine.core.config import load_config
from execution_engine.utils.io import append_jsonl
from execution_engine.core.engine import run_once
from execution_engine.utils.time import to_iso, utc_now


def _reset_file(path: Path) -> None:
    if path.exists():
        path.unlink()


def write_demo_inputs(cfg) -> None:
    cfg.ensure_dirs()
    _reset_file(cfg.rule_signals_path)
    _reset_file(cfg.llm_signals_path)
    _reset_file(cfg.decisions_path)
    _reset_file(cfg.orders_path)
    _reset_file(cfg.events_path)
    _reset_file(cfg.fills_path)
    _reset_file(cfg.rejections_path)
    _reset_file(cfg.logs_path)
    _reset_file(cfg.alerts_path)
    _reset_file(cfg.metrics_path)
    _reset_file(cfg.token_cache_path)

    now = utc_now()
    valid_until = to_iso(now)
    decision_end = to_iso(now)
    market_close = to_iso(now)

    # Extend windows
    from datetime import timedelta
    valid_until = to_iso(now + timedelta(seconds=cfg.signal_ttl_sec_max))
    decision_end = to_iso(now + timedelta(seconds=cfg.signal_ttl_sec_max))
    market_close = to_iso(now + timedelta(hours=2))

    rule_signal = {
        "source": "rule_baseline_demo",
        "source_run_id": cfg.run_id,
        "market_id": "demo_market_1",
        "outcome_index": 0,
        "action": "BUY",
        "order_type": "LIMIT",
        "price_limit": 0.45,
        "reference_mid_price": 0.45,
        "reference_price_time_utc": to_iso(now),
        "amount_usdc": cfg.order_usdc,
        "expiration_seconds": cfg.order_ttl_sec,
        "strategy_ref_id": "demo_strategy",
        "created_at_utc": to_iso(now),
        "valid_until_utc": valid_until,
        "decision_window_start_utc": to_iso(now),
        "decision_window_end_utc": decision_end,
        "market_close_time_utc": market_close,
        "confidence": "high",
        "reasoning_ref": "demo",
        "category": "demo",
    }

    llm_signal = {
        "source": "llm_demo",
        "source_run_id": cfg.run_id,
        "market_id": "demo_market_1",
        "outcome_index": 0,
        "action": "BUY",
        "order_type": "LIMIT",
        "price_limit": 0.45,
        "reference_mid_price": 0.45,
        "reference_price_time_utc": to_iso(now),
        "amount_usdc": cfg.order_usdc,
        "expiration_seconds": cfg.order_ttl_sec,
        "strategy_ref_id": "demo_strategy",
        "created_at_utc": to_iso(now),
        "valid_until_utc": valid_until,
        "decision_window_start_utc": to_iso(now),
        "decision_window_end_utc": decision_end,
        "market_close_time_utc": market_close,
        "confidence": "high",
        "reasoning_ref": "demo",
        "category": "demo",
    }

    append_jsonl(cfg.rule_signals_path, rule_signal)
    append_jsonl(cfg.llm_signals_path, llm_signal)

    mid_prices = {
        "demo_market_1": {"mid": 0.45, "spread": 0.02, "depth_usdc": 100}
    }
    with cfg.mid_prices_path.open("w", encoding="utf-8") as handle:
        json.dump(mid_prices, handle)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    write_demo_inputs(cfg)
    run_once(cfg)

    print(f"Demo complete. Outputs in {cfg.data_dir}")


if __name__ == "__main__":
    main()
