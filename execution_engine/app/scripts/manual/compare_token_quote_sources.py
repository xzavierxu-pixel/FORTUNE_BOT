#!/usr/bin/env python3
"""Compare Gamma, websocket token state, and CLOB quotes for token0/token1."""

from __future__ import annotations

import argparse
import asyncio
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


def parse_maybe_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    raw = str(value).strip()
    if not raw:
        return []
    for parser in (json.loads,):
        try:
            parsed = parser(raw)
        except Exception:
            continue
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return [raw]


def summarize_clob_book(client: Any, token_id: str) -> dict[str, Any]:
    try:
        raw_book = client.get_order_book(token_id)
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
        "spread": spread,
        "top_bid_raw": bids[0] if bids else None,
        "top_ask_raw": asks[0] if asks else None,
    }


def to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def diff_or_none(left: Any, right: Any) -> float | None:
    left_f = to_float(left)
    right_f = to_float(right)
    if left_f is None or right_f is None:
        return None
    return round(left_f - right_f, 6)


def build_default_market_ids() -> list[str]:
    return [
        "1596939",
        "1596940",
        "1596941",
        "1646320",
        "1633843",
        "1633844",
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare Gamma/websocket/CLOB bid-ask-spread across token0/token1.")
    parser.add_argument("--market-id", action="append", default=build_default_market_ids())
    parser.add_argument("--duration-sec", type=int, default=8)
    parser.add_argument("--gamma-base-url", default="https://gamma-api.polymarket.com")
    parser.add_argument("--clob-host", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument(
        "--output-csv",
        default=str(ROOT / "execution_engine" / "data" / "tests" / "token_quote_source_comparison.csv"),
    )
    parser.add_argument(
        "--output-json",
        default=str(ROOT / "execution_engine" / "data" / "tests" / "token_quote_source_comparison.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    os.environ.setdefault("PEG_RUN_MODE", "manual_quote_compare")
    os.environ.setdefault("PEG_RUN_ID", "QUOTE_SOURCE_COMPARE")
    os.environ.setdefault("PEG_BASE_DATA_DIR", str(ROOT / "execution_engine" / "data"))
    cfg = load_config()

    market_payloads: list[dict[str, Any]] = []
    token_ids: list[str] = []
    for market_id in args.market_id:
        raw_market = fetch_gamma_market_direct(args.gamma_base_url, str(market_id), args.timeout_sec)
        if raw_market is None:
            continue
        outcomes = parse_maybe_list(raw_market.get("outcomes"))
        clob_token_ids = parse_maybe_list(raw_market.get("clobTokenIds"))
        if len(clob_token_ids) < 2:
            continue
        market_payloads.append(
            {
                "market_id": str(raw_market.get("id") or raw_market.get("market_id") or market_id),
                "question": str(raw_market.get("question") or ""),
                "gamma_best_bid_market": raw_market.get("bestBid"),
                "gamma_best_ask_market": raw_market.get("bestAsk"),
                "gamma_spread_market": raw_market.get("spread"),
                "token_0_id": clob_token_ids[0],
                "token_1_id": clob_token_ids[1],
                "outcome_0_label": outcomes[0] if len(outcomes) > 0 else "",
                "outcome_1_label": outcomes[1] if len(outcomes) > 1 else "",
            }
        )
        token_ids.extend(clob_token_ids[:2])

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

    detail_rows: list[dict[str, Any]] = []
    for market in market_payloads:
        for side_index, side_label in ((0, "token_0"), (1, "token_1")):
            token_id = str(market[f"token_{side_index}_id"])
            websocket_row = websocket_by_token.get(token_id, {})
            clob_summary = summarize_clob_book(clob_client, token_id)
            detail_rows.append(
                {
                    "market_id": market["market_id"],
                    "question": market["question"],
                    "token_side": side_label,
                    "token_id": token_id,
                    "outcome_label": market[f"outcome_{side_index}_label"],
                    "gamma_best_bid_market": market["gamma_best_bid_market"],
                    "gamma_best_ask_market": market["gamma_best_ask_market"],
                    "gamma_spread_market": market["gamma_spread_market"],
                    "gamma_market_level_shared": True,
                    "websocket_best_bid": websocket_row.get("best_bid"),
                    "websocket_best_ask": websocket_row.get("best_ask"),
                    "websocket_spread": websocket_row.get("spread"),
                    "websocket_best_bid_size": websocket_row.get("best_bid_size"),
                    "websocket_best_ask_size": websocket_row.get("best_ask_size"),
                    "websocket_latest_event_type": websocket_row.get("latest_event_type"),
                    "websocket_latest_event_at_utc": websocket_row.get("latest_event_at_utc"),
                    "clob_best_bid": clob_summary.get("best_bid"),
                    "clob_best_ask": clob_summary.get("best_ask"),
                    "clob_spread": clob_summary.get("spread"),
                    "clob_top_bid_raw": clob_summary.get("top_bid_raw"),
                    "clob_top_ask_raw": clob_summary.get("top_ask_raw"),
                    "websocket_minus_clob_best_bid": diff_or_none(websocket_row.get("best_bid"), clob_summary.get("best_bid")),
                    "websocket_minus_clob_best_ask": diff_or_none(websocket_row.get("best_ask"), clob_summary.get("best_ask")),
                    "websocket_minus_clob_spread": diff_or_none(websocket_row.get("spread"), clob_summary.get("spread")),
                }
            )

    detail_frame = pd.DataFrame(detail_rows)
    output_csv = Path(args.output_csv)
    output_json = Path(args.output_json)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    detail_frame.to_csv(output_csv, index=False)
    output_json.write_text(
        json.dumps(
            {
                "generated_at_utc": to_iso(utc_now()),
                "stream_duration_sec": int(args.duration_sec),
                "market_count": len(market_payloads),
                "row_count": len(detail_rows),
                "stream_manifest_path": str(stream_result.run_manifest_path),
                "stream_token_state_path": str(stream_result.run_token_state_path),
                "rows": detail_rows,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote_csv={output_csv}")
    print(f"wrote_json={output_json}")
    print(f"market_count={len(market_payloads)}")
    print(f"row_count={len(detail_rows)}")


if __name__ == "__main__":
    main()
