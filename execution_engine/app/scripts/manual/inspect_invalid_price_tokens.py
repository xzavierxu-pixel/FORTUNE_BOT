#!/usr/bin/env python3
"""Inspect INVALID_PRICE tokens and report their bid/ask/price state."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


ROOT = repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution_engine.online.execution.live_quote import _best_ask_from_levels, _best_bid_from_levels
from execution_engine.online.streaming.manager import stream_market_data
from execution_engine.runtime.config import load_config
from execution_engine.shared.time import to_iso, utc_now


def import_py_clob_client() -> Any:
    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        local_clone = ROOT / "py-clob-client"
        if str(local_clone) not in sys.path:
            sys.path.insert(0, str(local_clone))
        from py_clob_client.client import ClobClient
    return ClobClient


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if hasattr(value, "dict"):
        try:
            return to_jsonable(value.dict())
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return {str(key): to_jsonable(item) for key, item in vars(value).items()}
        except Exception:
            pass
    return value


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def fetch_gamma_market_direct(base_url: str, market_id: str, timeout_sec: int) -> dict[str, Any] | None:
    params = urllib.parse.urlencode({"id": market_id})
    url = f"{base_url.rstrip('/')}/markets?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "PEG/0.3"})
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
    except Exception:
        return None
    data = json.loads(payload)
    markets = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
    for market in markets:
        if str(market.get("id") or market.get("market_id") or "") == market_id:
            return market
    return None


def compute_live_mid_from_row(row: dict[str, Any]) -> tuple[float, str]:
    mid = to_float(row.get("mid_price"))
    if mid > 0:
        return round(mid, 6), "mid_price"
    best_bid = to_float(row.get("best_bid"))
    best_ask = to_float(row.get("best_ask"))
    if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        return round((best_bid + best_ask) / 2.0, 6), "bid_ask_mid"
    last_trade = to_float(row.get("last_trade_price"))
    if last_trade > 0:
        return round(last_trade, 6), "last_trade_price"
    return 0.0, "none"


def classify_invalid_state(mid: float, cfg: Any) -> tuple[bool, str]:
    if mid <= float(cfg.rule_engine_min_price):
        return True, "mid_at_or_below_min_price"
    if mid >= float(cfg.rule_engine_max_price):
        return True, "mid_at_or_above_max_price"
    return False, "price_in_valid_range"


def summarize_clob_book(client: Any, token_id: str) -> dict[str, Any]:
    try:
        raw_book = client.get_order_book(token_id)
        midpoint = client.get_midpoint(token_id)
    except Exception as exc:
        return {"found": False, "error": str(exc)}
    book = to_jsonable(raw_book)
    if not isinstance(book, dict):
        return {"found": False, "error": "non_dict_book"}
    bids = book.get("bids")
    asks = book.get("asks")
    best_bid = _best_bid_from_levels(bids)
    best_ask = _best_ask_from_levels(asks)
    spread = None
    if best_bid is not None and best_ask is not None:
        spread = round(best_ask - best_bid, 6)
    return {
        "found": True,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": midpoint,
        "spread": spread,
        "top_bid_raw": bids[0] if bids else None,
        "top_ask_raw": asks[0] if asks else None,
    }


def load_invalid_price_events(events_path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with events_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            payload = json.loads(raw)
            if str(payload.get("event_type") or "") != "CANDIDATE_STATE":
                continue
            if str(payload.get("candidate_state") or "") != "INVALID_PRICE":
                continue
            token_id = str(payload.get("token_id") or "")
            market_id = str(payload.get("market_id") or "")
            if not token_id or not market_id:
                continue
            rows.append(
                {
                    "event_time_utc": str(payload.get("event_time_utc") or ""),
                    "market_id": market_id,
                    "batch_id": str(payload.get("batch_id") or ""),
                    "token_id": token_id,
                    "candidate_state": "INVALID_PRICE",
                    "reason": str(payload.get("reason") or ""),
                }
            )
    return rows


def dedupe_invalid_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (row["market_id"], row["token_id"])
        deduped[key] = row
    return list(deduped.values())


def parse_args() -> argparse.Namespace:
    default_run_dir = Path(r"C:\var\lib\fortune_bot\execution_engine_data\runs\2026-03-19\EXP_30P_20260319")
    parser = argparse.ArgumentParser(description="Inspect INVALID_PRICE tokens from a submit-window run.")
    parser.add_argument("--run-dir", default=str(default_run_dir))
    parser.add_argument("--duration-sec", type=int, default=8)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--gamma-base-url", default="https://gamma-api.polymarket.com")
    parser.add_argument("--clob-host", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument(
        "--output-csv",
        default=str(ROOT / "execution_engine" / "data" / "tests" / "invalid_price_token_inspection.csv"),
    )
    parser.add_argument(
        "--output-json",
        default=str(ROOT / "execution_engine" / "data" / "tests" / "invalid_price_token_inspection.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir)
    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        raise FileNotFoundError(f"events file not found: {events_path}")

    invalid_rows = dedupe_invalid_rows(load_invalid_price_events(events_path))
    if args.limit > 0:
        invalid_rows = invalid_rows[: args.limit]
    token_ids = [row["token_id"] for row in invalid_rows]

    os.environ.setdefault("PEG_RUN_MODE", "manual_invalid_price_probe")
    os.environ.setdefault("PEG_RUN_ID", "INVALID_PRICE_PROBE")
    os.environ.setdefault("PEG_BASE_DATA_DIR", str(ROOT / "execution_engine" / "data"))
    cfg = load_config()

    stream_result = asyncio.run(
        stream_market_data(
            cfg,
            asset_ids=token_ids,
            duration_sec=max(int(args.duration_sec), 1),
        )
    )
    websocket_by_token = {
        str(row.get("token_id") or ""): row
        for row in stream_result.token_state_records
        if str(row.get("token_id") or "")
    }

    ClobClient = import_py_clob_client()
    clob_client = ClobClient(args.clob_host)

    output_rows: list[dict[str, Any]] = []
    for row in invalid_rows:
        token_id = row["token_id"]
        market_id = row["market_id"]
        websocket_row = websocket_by_token.get(token_id, {})
        computed_mid, mid_source = compute_live_mid_from_row(websocket_row)
        still_invalid, invalid_reason_now = classify_invalid_state(computed_mid, cfg)
        gamma_market = fetch_gamma_market_direct(args.gamma_base_url, market_id, args.timeout_sec)
        clob_summary = summarize_clob_book(clob_client, token_id)

        output_rows.append(
            {
                "market_id": market_id,
                "token_id": token_id,
                "batch_id": row["batch_id"],
                "invalid_event_time_utc": row["event_time_utc"],
                "invalid_reason_at_run": row["reason"],
                "ws_best_bid": websocket_row.get("best_bid"),
                "ws_best_bid_size": websocket_row.get("best_bid_size"),
                "ws_best_ask": websocket_row.get("best_ask"),
                "ws_best_ask_size": websocket_row.get("best_ask_size"),
                "ws_mid_price": websocket_row.get("mid_price"),
                "ws_last_trade_price": websocket_row.get("last_trade_price"),
                "ws_spread": websocket_row.get("spread"),
                "ws_latest_event_type": websocket_row.get("latest_event_type"),
                "ws_latest_event_at_utc": websocket_row.get("latest_event_at_utc"),
                "derived_live_mid": computed_mid,
                "derived_live_mid_source": mid_source,
                "derived_invalid_now": still_invalid,
                "derived_invalid_reason_now": invalid_reason_now,
                "min_valid_price": float(cfg.rule_engine_min_price),
                "max_valid_price": float(cfg.rule_engine_max_price),
                "clob_best_bid": clob_summary.get("best_bid"),
                "clob_best_ask": clob_summary.get("best_ask"),
                "clob_midpoint": clob_summary.get("midpoint"),
                "clob_spread": clob_summary.get("spread"),
                "gamma_best_bid_market": None if gamma_market is None else gamma_market.get("bestBid"),
                "gamma_best_ask_market": None if gamma_market is None else gamma_market.get("bestAsk"),
                "gamma_last_trade_price_market": None if gamma_market is None else gamma_market.get("lastTradePrice"),
                "gamma_question": None if gamma_market is None else gamma_market.get("question"),
            }
        )

    output_frame = pd.DataFrame(output_rows)
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_frame.to_csv(output_csv, index=False)
    output_json.write_text(
        json.dumps(
            {
                "generated_at_utc": to_iso(utc_now()),
                "run_dir": str(run_dir),
                "invalid_token_count": len(output_rows),
                "stream_duration_sec": int(args.duration_sec),
                "stream_manifest_path": str(stream_result.run_manifest_path),
                "stream_token_state_path": str(stream_result.run_token_state_path),
                "rows": to_jsonable(output_rows),
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"wrote_csv={output_csv}")
    print(f"wrote_json={output_json}")
    print(f"invalid_token_count={len(output_rows)}")


if __name__ == "__main__":
    main()
