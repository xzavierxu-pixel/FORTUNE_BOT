"""Helpers for combining local websocket state with CLOB price-history backfill."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List
import json
import math
import urllib.parse
import urllib.request

from execution_engine.runtime.config import PegConfig


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_received_at(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _mid_from_book(payload: Dict[str, Any]) -> float | None:
    best_bid = None
    best_ask = None
    for level in payload.get("bids") or []:
        price = _to_float((level or {}).get("price"))
        if price is not None:
            best_bid = price if best_bid is None else max(best_bid, price)
    for level in payload.get("asks") or []:
        price = _to_float((level or {}).get("price"))
        if price is not None and price > 0:
            best_ask = price if best_ask is None else min(best_ask, price)
    if best_bid is not None and best_ask is not None and best_ask >= best_bid:
        return round((best_bid + best_ask) / 2.0, 6)
    if best_ask is not None:
        return round(best_ask, 6)
    if best_bid is not None:
        return round(best_bid, 6)
    last_trade = _to_float(payload.get("last_trade_price"))
    return round(last_trade, 6) if last_trade is not None else None


def _price_from_payload(payload: Dict[str, Any]) -> float | None:
    event_type = str(payload.get("event_type") or payload.get("type") or "")
    if event_type == "book":
        return _mid_from_book(payload)
    if event_type == "best_bid_ask":
        best_bid = _to_float(payload.get("best_bid"))
        best_ask = _to_float(payload.get("best_ask"))
        if best_bid is not None and best_ask is not None and best_ask >= best_bid:
            return round((best_bid + best_ask) / 2.0, 6)
        return round(best_ask, 6) if best_ask is not None else round(best_bid, 6) if best_bid is not None else None
    if event_type == "last_trade_price":
        price = _to_float(payload.get("price"))
        return round(price, 6) if price is not None else None
    if event_type == "price_change":
        changes = payload.get("price_changes") or []
        for change in changes:
            if not isinstance(change, dict):
                continue
            best_bid = _to_float(change.get("best_bid"))
            best_ask = _to_float(change.get("best_ask"))
            if best_bid is not None and best_ask is not None and best_ask >= best_bid:
                return round((best_bid + best_ask) / 2.0, 6)
    return None


@dataclass(frozen=True)
class LatestWsPrice:
    token_id: str
    event_time: datetime
    price: float
    source_event_type: str


@dataclass(frozen=True)
class PricePoint:
    ts: int
    price: float
    source: str


class ClobPriceHistoryClient:
    def __init__(self, cfg: PegConfig) -> None:
        self.cfg = cfg
        self._cache: Dict[tuple[str, int, int, int], List[PricePoint]] = {}

    def fetch_history(
        self,
        token_id: str,
        *,
        start_ts: int,
        end_ts: int,
        fidelity_minutes: int = 5,
    ) -> List[PricePoint]:
        key = (str(token_id), int(start_ts), int(end_ts), int(fidelity_minutes))
        cached = self._cache.get(key)
        if cached is not None:
            return list(cached)

        params = urllib.parse.urlencode(
            {
                "market": str(token_id),
                "startTs": str(int(start_ts)),
                "endTs": str(int(end_ts)),
                "fidelity": str(max(int(fidelity_minutes), 1)),
            }
        )
        url = f"{self.cfg.clob_host.rstrip('/')}/prices-history?{params}"
        request = urllib.request.Request(url, headers={"User-Agent": "PEG/0.3"})
        with urllib.request.urlopen(request, timeout=self.cfg.clob_request_timeout_sec) as response:
            payload = response.read().decode("utf-8")
        data = json.loads(payload)
        history = data.get("history", []) if isinstance(data, dict) else []
        points: List[PricePoint] = []
        for item in history:
            if not isinstance(item, dict):
                continue
            ts = item.get("t")
            price = _to_float(item.get("p"))
            if ts is None or price is None:
                continue
            try:
                points.append(PricePoint(ts=int(ts), price=float(price), source="clob_prices_history"))
            except (TypeError, ValueError):
                continue
        points.sort(key=lambda point: point.ts)
        self._cache[key] = list(points)
        return points


def load_latest_ws_prices(
    cfg: PegConfig,
    token_ids: Iterable[str],
    *,
    now: datetime,
    lookback_hours: int = 48,
) -> Dict[str, LatestWsPrice]:
    target_ids = {str(token_id) for token_id in token_ids if str(token_id)}
    if not target_ids:
        return {}

    latest_by_token: Dict[str, LatestWsPrice] = {}
    base_dir = Path(cfg.shared_ws_raw_dir)
    for hour_offset in range(max(int(lookback_hours), 1) + 1):
        hour_dt = now - timedelta(hours=hour_offset)
        hour_dir = base_dir / hour_dt.strftime("%Y-%m-%d") / hour_dt.strftime("%H")
        if not hour_dir.exists():
            continue
        for path in sorted(hour_dir.glob("shard_*.jsonl"), reverse=True):
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for raw_line in reversed(lines):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
                payload = record.get("payload")
                if not isinstance(payload, dict):
                    continue
                token_id = str(payload.get("asset_id") or "")
                if token_id not in target_ids or token_id in latest_by_token:
                    continue
                price = _price_from_payload(payload)
                if price is None or price <= 0:
                    continue
                event_time = _parse_received_at(record.get("received_at_utc"))
                if event_time is None:
                    ts_raw = payload.get("timestamp")
                    try:
                        event_time = datetime.fromtimestamp(int(ts_raw) / 1000.0, tz=timezone.utc)
                    except (TypeError, ValueError, OSError):
                        continue
                latest_by_token[token_id] = LatestWsPrice(
                    token_id=token_id,
                    event_time=event_time,
                    price=float(price),
                    source_event_type=str(payload.get("event_type") or payload.get("type") or ""),
                )
                if len(latest_by_token) == len(target_ids):
                    return latest_by_token
    return latest_by_token


def merge_price_points(
    history_points: Iterable[PricePoint],
    latest_ws_price: LatestWsPrice | None,
    *,
    now_ts: int,
) -> List[PricePoint]:
    merged: Dict[int, PricePoint] = {}
    for point in history_points:
        merged[int(point.ts)] = point
    if latest_ws_price is not None:
        merged[int(now_ts)] = PricePoint(ts=int(now_ts), price=float(latest_ws_price.price), source="ws_raw")
    return [merged[key] for key in sorted(merged)]


def _latest_price_at_or_before(points: List[PricePoint], target_ts: int) -> float | None:
    selected = None
    for point in points:
        if point.ts <= target_ts:
            selected = point.price
        else:
            break
    return selected


def _safe_diff(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)


def _safe_mean(values: List[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _safe_std(values: List[float]) -> float | None:
    if len(values) < 2:
        return 0.0 if values else None
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    return round(math.sqrt(max(variance, 0.0)), 6)


def build_historical_price_features(
    *,
    current_price: float,
    now_ts: int,
    end_ts: int,
    merged_points: List[PricePoint],
) -> Dict[str, float | None]:
    horizons = [1, 2, 4, 6, 12, 24]
    features: Dict[str, float | None] = {}
    horizon_prices: Dict[int, float | None] = {}
    for hour in horizons:
        target_ts = int(end_ts - hour * 3600)
        price = _latest_price_at_or_before(merged_points, target_ts) if target_ts <= now_ts else None
        horizon_prices[hour] = price
        features[f"p_{hour}h"] = price

    features["delta_p_1_2"] = _safe_diff(horizon_prices[1], horizon_prices[2])
    features["delta_p_2_4"] = _safe_diff(horizon_prices[2], horizon_prices[4])
    features["delta_p_4_12"] = _safe_diff(horizon_prices[4], horizon_prices[12])
    features["delta_p_12_24"] = _safe_diff(horizon_prices[12], horizon_prices[24])

    p_1h = horizon_prices[1]
    p_2h = horizon_prices[2]
    p_4h = horizon_prices[4]
    p_12h = horizon_prices[12]
    p_24h = horizon_prices[24]

    if p_1h is not None and p_24h is not None:
        features["term_structure_slope"] = round(p_1h - p_24h, 6)
    else:
        features["term_structure_slope"] = None

    ordered_horizon_prices = [features.get(f"p_{hour}h") for hour in horizons]
    path_prices = [float(price) for price in ordered_horizon_prices if price is not None]
    features["path_price_mean"] = _safe_mean(path_prices)
    features["path_price_std"] = _safe_std(path_prices)
    features["path_price_min"] = round(min(path_prices), 6) if path_prices else None
    features["path_price_max"] = round(max(path_prices), 6) if path_prices else None
    features["path_price_range"] = (
        round(features["path_price_max"] - features["path_price_min"], 6)
        if features["path_price_max"] is not None and features["path_price_min"] is not None
        else None
    )

    if p_1h is not None and p_2h is not None and p_12h is not None and p_24h is not None:
        short_leg = p_1h - p_2h
        long_leg = p_12h - p_24h
        features["price_reversal_flag"] = 1.0 if short_leg * long_leg < 0 else 0.0
        features["price_acceleration"] = round(short_leg - long_leg, 6)
    else:
        features["price_reversal_flag"] = 0.0
        features["price_acceleration"] = 0.0

    features["closing_drift"] = round(current_price - p_24h, 6) if p_24h is not None else None
    return features
