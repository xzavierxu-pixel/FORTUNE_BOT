"""Adapter to convert rule baseline candidates into PEG rule_signals.jsonl."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable, List

from execution_engine.core.config import load_config
from execution_engine.utils.io import append_jsonl
from execution_engine.core.models import ensure_ids
from execution_engine.utils.time import parse_utc, to_iso, utc_now


def _extend_seconds(ts_iso: str, seconds: int) -> str:
    from datetime import timedelta

    return to_iso(parse_utc(ts_iso) + timedelta(seconds=seconds))


def _read_rows(path: Path) -> Iterable[Dict[str, str]]:
    if path.suffix.lower() == ".jsonl":
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield __import__("json").loads(line)
        return

    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def _build_signal(row: Dict[str, str], cfg) -> Dict[str, object]:
    now = utc_now()
    action = row.get("action") or ("BUY" if float(row.get("direction", 1)) >= 0 else "SELL")
    outcome_index = int(row.get("outcome_index", 0))
    price_limit = float(row.get("price_limit", row.get("price", 0.5)))
    reference_mid = float(row.get("reference_mid_price", price_limit))

    now_iso = to_iso(now)
    signal = {
        "source": row.get("source", "rule_baseline"),
        "source_run_id": row.get("source_run_id", cfg.run_id),
        "market_id": row.get("market_id"),
        "outcome_index": outcome_index,
        "action": action,
        "order_type": "LIMIT",
        "price_limit": price_limit,
        "reference_mid_price": reference_mid,
        "reference_price_time_utc": row.get("reference_price_time_utc", now_iso),
        "amount_usdc": cfg.order_usdc,
        "expiration_seconds": cfg.order_ttl_sec,
        "strategy_ref_id": row.get("strategy_ref_id", "rule_baseline"),
        "created_at_utc": row.get("created_at_utc", now_iso),
        "valid_until_utc": row.get("valid_until_utc", _extend_seconds(now_iso, cfg.signal_ttl_sec_max)),
        "decision_window_start_utc": row.get("decision_window_start_utc", now_iso),
        "decision_window_end_utc": row.get("decision_window_end_utc", _extend_seconds(now_iso, cfg.signal_ttl_sec_max)),
        "market_close_time_utc": row.get("market_close_time_utc", _extend_seconds(now_iso, 3600)),
        "confidence": row.get("confidence", "high"),
        "reasoning_ref": row.get("reasoning_ref", "rule_baseline"),
        "category": row.get("category", ""),
    }
    return ensure_ids(signal)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    cfg = load_config()
    input_path = Path(args.input) if args.input else cfg.rule_candidates_path
    output_path = Path(args.output) if args.output else cfg.rule_signals_path

    if not input_path.exists():
        raise FileNotFoundError(f"Rule candidates not found: {input_path}")

    count = 0
    for row in _read_rows(input_path):
        signal = _build_signal(row, cfg)
        append_jsonl(output_path, signal)
        count += 1

    print(f"Wrote {count} rule signals to {output_path}")


if __name__ == "__main__":
    main()
