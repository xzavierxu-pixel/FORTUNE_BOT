#!/usr/bin/env python3
"""Inspect where submit-attempt best bid / ask values come from."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


ROOT = repo_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from execution_engine.integrations.providers.gamma_provider import GammaMarketProvider
from execution_engine.online.execution.live_quote import _best_ask_from_levels, _best_bid_from_levels
from execution_engine.shared.time import to_iso, utc_now


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


def import_py_clob_client() -> Any:
    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        local_clone = ROOT / "py-clob-client"
        if str(local_clone) not in sys.path:
            sys.path.insert(0, str(local_clone))
        from py_clob_client.client import ClobClient
    return ClobClient


def load_attempt_rows(path: Path, sample_size: int) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    abnormal_rows = [
        row
        for row in rows
        if str(row.get("status") or "").upper() == "ABNORMAL_TOP_OF_BOOK"
    ]
    return abnormal_rows[: max(sample_size, 1)]


def load_attempt_rows_by_status(path: Path, status: str, sample_size: int) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    target = str(status or "").upper()
    matched_rows = [row for row in rows if str(row.get("status") or "").upper() == target]
    return matched_rows[: max(sample_size, 1)]


def fetch_gamma_market_by_id(
    provider: GammaMarketProvider,
    market_id: str,
    *,
    page_limit: int = 200,
    max_pages: int = 100,
) -> dict[str, Any] | None:
    offset = 0
    for _ in range(max_pages):
        events = provider.fetch_open_events_page(limit=page_limit, offset=offset, order="endDate", ascending=True)
        if not events:
            break
        for event in events:
            markets = event.get("markets") or []
            if not isinstance(markets, list):
                continue
            for market in markets:
                if not isinstance(market, dict):
                    continue
                raw_market_id = str(market.get("id") or market.get("market_id") or "")
                if raw_market_id == market_id:
                    return market
        if len(events) < page_limit:
            break
        offset += page_limit
    return None


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


def gamma_presence(value: Any) -> bool:
    return value not in (None, "")


def summarize_gamma_market(raw_market: dict[str, Any] | None) -> dict[str, Any]:
    if raw_market is None:
        return {"found": False}
    return {
        "found": True,
        "market_id": str(raw_market.get("id") or raw_market.get("market_id") or ""),
        "question_raw": raw_market.get("question"),
        "bestBid_present": gamma_presence(raw_market.get("bestBid")),
        "bestAsk_present": gamma_presence(raw_market.get("bestAsk")),
        "spread_present": gamma_presence(raw_market.get("spread")),
        "lastTradePrice_present": gamma_presence(raw_market.get("lastTradePrice")),
        "bestBid_raw": raw_market.get("bestBid"),
        "bestAsk_raw": raw_market.get("bestAsk"),
        "spread_raw": raw_market.get("spread"),
        "lastTradePrice_raw": raw_market.get("lastTradePrice"),
        "acceptingOrders_raw": raw_market.get("acceptingOrders"),
        "active_raw": raw_market.get("active"),
        "closed_raw": raw_market.get("closed"),
        "archived_raw": raw_market.get("archived"),
        "enableOrderBook_raw": raw_market.get("enableOrderBook"),
        "liquidityNum_raw": raw_market.get("liquidityNum"),
        "volumeNum_raw": raw_market.get("volumeNum"),
        "volume24hrNum_raw": raw_market.get("volume24hrNum"),
        "endDate_raw": raw_market.get("endDate"),
        "endDateIso_raw": raw_market.get("endDateIso"),
        "gameStartTime_raw": raw_market.get("gameStartTime"),
        "secondsDelay_raw": raw_market.get("secondsDelay"),
        "umaResolutionStatuses_raw": raw_market.get("umaResolutionStatuses"),
        "orderPriceMinTickSize_raw": raw_market.get("orderPriceMinTickSize"),
        "outcomes_raw": raw_market.get("outcomes"),
        "clobTokenIds_raw": raw_market.get("clobTokenIds"),
    }


def summarize_clob_book(client: Any, token_id: str) -> dict[str, Any]:
    try:
        raw_book = client.get_order_book(token_id)
        midpoint = client.get_midpoint(token_id)
    except Exception as exc:
        return {"found": False, "error": str(exc)}

    book = to_jsonable(raw_book)
    bids = book.get("bids") if isinstance(book, dict) else None
    asks = book.get("asks") if isinstance(book, dict) else None
    best_bid = _best_bid_from_levels(bids)
    best_ask = _best_ask_from_levels(asks)
    return {
        "found": True,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "midpoint": midpoint,
        "spread": (best_ask - best_bid) if best_bid is not None and best_ask is not None else None,
        "bids_present": bool(bids),
        "asks_present": bool(asks),
        "top_bid_raw": to_jsonable(bids[0]) if bids else None,
        "top_ask_raw": to_jsonable(asks[0]) if asks else None,
        "book": book,
    }


def build_market_sample(
    market_id: str,
    token_id: str,
    *,
    label: str,
    gamma_base_url: str,
    timeout_sec: int,
    gamma_page_limit: int,
    gamma_max_pages: int,
    gamma_provider: GammaMarketProvider,
    clob_client: Any,
    attempt_row: dict[str, str] | None = None,
) -> dict[str, Any]:
    raw_market = fetch_gamma_market_direct(gamma_base_url, market_id, timeout_sec)
    if raw_market is None:
        raw_market = fetch_gamma_market_by_id(
            gamma_provider,
            market_id,
            page_limit=gamma_page_limit,
            max_pages=gamma_max_pages,
        )
    gamma_summary = summarize_gamma_market(raw_market)
    clob_summary = summarize_clob_book(clob_client, token_id)
    return {
        "label": label,
        "market_id": market_id,
        "token_id": token_id,
        "attempt_status": attempt_row.get("status") if attempt_row else None,
        "attempt_best_bid": attempt_row.get("best_bid") if attempt_row else None,
        "attempt_best_ask": attempt_row.get("best_ask") if attempt_row else None,
        "attempt_tick_size": attempt_row.get("tick_size") if attempt_row else None,
        "attempt_quote_source": attempt_row.get("quote_source") if attempt_row else None,
        "gamma": gamma_summary,
        "clob": clob_summary,
        "observations": {
            "attempt_values_are_not_execution_defaults": (
                attempt_row is not None
                and str(attempt_row.get("best_bid") or "") not in {"", "0", "0.0"}
                and str(attempt_row.get("best_ask") or "") not in {"", "0", "0.0"}
            ),
            "gamma_default_when_missing_is_zero": True,
            "execution_code_synthesizes_mid_not_best_bid_ask": True,
        },
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    attempts_path = Path(args.attempts_csv)
    sample_rows = load_attempt_rows(attempts_path, args.sample_size)
    region_rows = load_attempt_rows_by_status(attempts_path, "REGION_RESTRICTED", args.region_sample_size)
    gamma = GammaMarketProvider(args.gamma_base_url, timeout_sec=args.timeout_sec)
    ClobClient = import_py_clob_client()
    clob = ClobClient(args.clob_host)

    samples: list[dict[str, Any]] = []
    for row in sample_rows:
        market_id = str(row.get("market_id") or "")
        token_id = str(row.get("token_id") or "")
        samples.append(
            build_market_sample(
                market_id,
                token_id,
                label="abnormal_attempt",
                gamma_base_url=args.gamma_base_url,
                timeout_sec=args.timeout_sec,
                gamma_page_limit=args.gamma_page_limit,
                gamma_max_pages=args.gamma_max_pages,
                gamma_provider=gamma,
                clob_client=clob,
                attempt_row=row,
            )
        )

    region_samples: list[dict[str, Any]] = []
    for row in region_rows:
        market_id = str(row.get("market_id") or "")
        token_id = str(row.get("token_id") or "")
        region_samples.append(
            build_market_sample(
                market_id,
                token_id,
                label="region_restricted_attempt",
                gamma_base_url=args.gamma_base_url,
                timeout_sec=args.timeout_sec,
                gamma_page_limit=args.gamma_page_limit,
                gamma_max_pages=args.gamma_max_pages,
                gamma_provider=gamma,
                clob_client=clob,
                attempt_row=row,
            )
        )

    normal_samples: list[dict[str, Any]] = []
    for raw_pair in args.normal_market_pairs:
        market_id, token_id = raw_pair.split(":", 1)
        normal_samples.append(
            build_market_sample(
                market_id,
                token_id,
                label="normal_market_probe",
                gamma_base_url=args.gamma_base_url,
                timeout_sec=args.timeout_sec,
                gamma_page_limit=args.gamma_page_limit,
                gamma_max_pages=args.gamma_max_pages,
                gamma_provider=gamma,
                clob_client=clob,
            )
        )

    return {
        "generated_at_utc": to_iso(utc_now()),
        "attempts_csv": str(attempts_path),
        "abnormal_sample_size": len(samples),
        "region_sample_size": len(region_samples),
        "normal_sample_size": len(normal_samples),
        "selection_rule": "first_abnormal_top_of_book_rows + first_region_restricted_rows + explicit_normal_market_pairs",
        "code_defaults": {
            "gamma_best_bid_default_when_missing": 0.0,
            "gamma_best_ask_default_when_missing": 0.0,
            "live_quote_best_bid_default_when_missing": 0.0,
            "live_quote_best_ask_default_when_missing": 0.0,
            "note": "The code synthesizes mid from bid/ask, but does not synthesize 0.01/0.99 boundary bids or asks.",
        },
        "abnormal_samples": samples,
        "region_restricted_samples": region_samples,
        "normal_samples": normal_samples,
    }


def parse_args() -> argparse.Namespace:
default_attempts = ROOT / "execution_engine" / "data" / "runs" / "2026-03-19" / "EXP20260319_SUBMIT_FULL_01" / "submit_window" / "submission_attempts.csv"
    default_normal_market_pairs = [
        "1633842:13859161527131805596386232961909653339330952192194039757131667984173198775581",
        "1633843:106795652436815554164476106798549407654605074130170338834752776791190474957501",
        "1633844:44266141247233860650640550097894265955318798466481305291628526789289274584292",
        "1633845:21964169500207926682044514716780557594265676876952144701055692109438200792768",
        "1633846:46308182264340614372835850313399712116903957113304725180556643033078534445265",
    ]
    parser = argparse.ArgumentParser(description="Inspect Gamma/CLOB sources for best bid / ask values.")
    parser.add_argument("--attempts-csv", default=str(default_attempts))
    parser.add_argument("--sample-size", type=int, default=3)
    parser.add_argument("--region-sample-size", type=int, default=1)
    parser.add_argument("--gamma-base-url", default="https://gamma-api.polymarket.com")
    parser.add_argument("--clob-host", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-sec", type=int, default=20)
    parser.add_argument("--gamma-page-limit", type=int, default=200)
    parser.add_argument("--gamma-max-pages", type=int, default=100)
    parser.add_argument("--normal-market-pairs", nargs="*", default=default_normal_market_pairs)
    parser.add_argument(
        "--output-json",
        default=str(ROOT / "execution_engine" / "data" / "tests" / "bid_ask_source_inspection.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    report = build_report(args)
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(to_jsonable(report), ensure_ascii=True, indent=2), encoding="utf-8")
    print(f"wrote_report={output_path}")
    print(f"abnormal_sample_size={report['abnormal_sample_size']}")
    print(f"region_sample_size={report['region_sample_size']}")
    print(f"normal_sample_size={report['normal_sample_size']}")


if __name__ == "__main__":
    main()
