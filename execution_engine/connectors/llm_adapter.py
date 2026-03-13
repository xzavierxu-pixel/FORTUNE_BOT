"""Adapter to convert LLM snapshot results into PEG llm_signals.jsonl."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, Optional
from dataclasses import replace

from execution_engine.core.config import load_config
from execution_engine.utils.io import append_jsonl
from execution_engine.core.models import ensure_ids
from execution_engine.utils.time import parse_utc, to_iso, utc_now

_BOXED_RE = re.compile(r"\\boxed\{(.*)\}", re.DOTALL)


def _extend_seconds(ts_iso: str, seconds: int) -> str:
    from datetime import timedelta

    return to_iso(parse_utc(ts_iso) + timedelta(seconds=seconds))


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    candidate = text.strip()
    match = _BOXED_RE.search(candidate)
    if match:
        candidate = match.group(1)
    # Find first JSON object
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    snippet = candidate[start : end + 1]
    try:
        return json.loads(snippet)
    except json.JSONDecodeError:
        return None


def _build_signal(payload: Dict[str, Any], metadata: Dict[str, Any], cfg) -> Optional[Dict[str, object]]:
    market_id = payload.get("market_id") or metadata.get("market_id")
    if market_id is None:
        return None

    target_idx = payload.get("target_outcome_index")
    if target_idx is None:
        return None

    market_price_o0 = payload.get("market_price_o0")
    if market_price_o0 is None:
        return None

    target_idx = int(target_idx)
    price_limit = float(market_price_o0)
    if target_idx == 1:
        price_limit = 1.0 - float(market_price_o0)

    now = utc_now()
    now_iso = to_iso(now)

    signal = {
        "source": "llm_miroflow",
        "source_run_id": cfg.run_id,
        "market_id": str(market_id),
        "outcome_index": target_idx,
        "action": "BUY",
        "order_type": "LIMIT",
        "price_limit": price_limit,
        "reference_mid_price": float(market_price_o0),
        "reference_price_time_utc": now_iso,
        "amount_usdc": cfg.order_usdc,
        "expiration_seconds": cfg.order_ttl_sec,
        "strategy_ref_id": "llm_miroflow",
        "created_at_utc": now_iso,
        "valid_until_utc": _extend_seconds(now_iso, cfg.signal_ttl_sec_max),
        "decision_window_start_utc": now_iso,
        "decision_window_end_utc": _extend_seconds(now_iso, cfg.signal_ttl_sec_max),
        "market_close_time_utc": _extend_seconds(now_iso, 3600),
        "confidence": payload.get("confidence", "high"),
        "reasoning_ref": payload.get("reasoning_ref", "llm_miroflow"),
        "category": payload.get("category") or metadata.get("category") or metadata.get("broad_category") or "",
    }
    return ensure_ids(signal)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    cfg = load_config()
    if args.run_id:
        cfg = replace(cfg, run_id=args.run_id)

    if args.snapshot_dir:
        snapshot_dir = Path(args.snapshot_dir)
    else:
        if not args.run_id:
            raise ValueError("Provide --snapshot-dir or --run-id")
        snapshot_dir = Path(cfg.llm_snapshot_dir) / args.run_id

    results_path = snapshot_dir / "benchmark_results.jsonl"
    if not results_path.exists():
        raise FileNotFoundError(f"benchmark_results.jsonl not found: {results_path}")

    output_path = Path(args.output) if args.output else cfg.llm_signals_path

    count = 0
    with results_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            boxed = row.get("model_boxed_answer") or ""
            payload = _extract_json(str(boxed))
            if payload is None:
                continue
            metadata = row.get("metadata", {}) or {}
            signal = _build_signal(payload, metadata, cfg)
            if signal is None:
                continue
            append_jsonl(output_path, signal)
            count += 1

    print(f"Wrote {count} LLM signals to {output_path}")


if __name__ == "__main__":
    main()
