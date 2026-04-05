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
import bisect
import sys

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


def latest_price_from_token_state(row: Dict[str, Any], *, now: datetime) -> LatestWsPrice | None:
    if not isinstance(row, dict):
        return None
    token_id = str(row.get("token_id") or "").strip()
    if not token_id:
        return None

    best_bid = _to_float(row.get("best_bid"))
    best_ask = _to_float(row.get("best_ask"))
    mid_price = _to_float(row.get("mid_price"))
    last_trade_price = _to_float(row.get("last_trade_price"))
    price = None
    if best_bid is not None and best_ask is not None and best_ask >= best_bid:
        price = round((best_bid + best_ask) / 2.0, 6)
    elif mid_price is not None and mid_price > 0:
        price = round(mid_price, 6)
    elif last_trade_price is not None and last_trade_price > 0:
        price = round(last_trade_price, 6)
    elif best_ask is not None and best_ask > 0:
        price = round(best_ask, 6)
    elif best_bid is not None and best_bid > 0:
        price = round(best_bid, 6)
    if price is None or price <= 0:
        return None

    event_time = _parse_received_at(row.get("latest_event_at_utc")) or now
    return LatestWsPrice(
        token_id=token_id,
        event_time=event_time,
        price=float(price),
        source_event_type=str(row.get("latest_event_type") or "token_state"),
    )


def build_latest_live_prices_from_token_state(
    token_state_by_token: Dict[str, Dict[str, Any]],
    token_ids: Iterable[str],
    *,
    now: datetime,
) -> Dict[str, LatestWsPrice]:
    latest_by_token: Dict[str, LatestWsPrice] = {}
    for token_id in {str(value) for value in token_ids if str(value)}:
        latest = latest_price_from_token_state(token_state_by_token.get(token_id, {}), now=now)
        if latest is not None:
            latest_by_token[token_id] = latest
    return latest_by_token


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


def _load_rule_snapshot_config(cfg: PegConfig) -> tuple[int, int, int]:
    rule_engine_dir = str(getattr(cfg, "rule_engine_dir", "") or "")
    if rule_engine_dir and rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)
    from rule_baseline.utils import config as rule_config  # type: ignore

    return (
        int(rule_config.SNAP_WINDOW_SEC),
        int(rule_config.STALE_QUOTE_MAX_OFFSET_SEC),
        int(rule_config.STALE_QUOTE_MAX_GAP_SEC),
    )


def build_source_host(
    *,
    source_url: Any,
    resolution_source: Any,
    domain: Any,
) -> str:
    for candidate in (source_url, resolution_source):
        raw = str(candidate or "").strip()
        if not raw or raw.upper() == "UNKNOWN":
            continue
        if "://" not in raw:
            raw = f"http://{raw}"
        parsed = urllib.parse.urlparse(raw)
        host = (parsed.netloc or "").strip().lower()
        if host:
            return host

    fallback = str(domain or "").strip().lower()
    return fallback if fallback else "UNKNOWN"


def _find_prices_batch(
    timestamps: List[int],
    prices: List[float],
    target_ts_list: List[int],
    window_sec: int,
) -> List[Dict[str, float | int | str | bool | None]]:
    if not timestamps:
        return [
            {
                "price": None,
                "selected_ts": None,
                "point_side": None,
                "offset_sec": None,
                "points_in_window": 0,
                "left_gap_sec": None,
                "right_gap_sec": None,
                "local_gap_sec": None,
                "stale_quote_flag": True,
            }
            for _ in target_ts_list
        ]

    results: List[Dict[str, float | int | str | bool | None]] = []
    length = len(timestamps)
    for target_ts in target_ts_list:
        idx = bisect.bisect_left(timestamps, target_ts)
        left_window = bisect.bisect_left(timestamps, target_ts - window_sec)
        right_window = bisect.bisect_right(timestamps, target_ts + window_sec)
        points_in_window = max(right_window - left_window, 0)
        best_price = None
        best_ts = None
        point_side = None
        offset_sec = None
        local_gap_sec = None
        min_diff = float("inf")
        if idx > 0:
            ts_left = timestamps[idx - 1]
            diff = abs(ts_left - target_ts)
            if diff <= window_sec:
                min_diff = diff
                best_price = prices[idx - 1]
                best_ts = ts_left
                point_side = "left"
                offset_sec = diff
        if idx < length:
            ts_right = timestamps[idx]
            diff = abs(ts_right - target_ts)
            if diff <= window_sec and diff < min_diff:
                best_price = prices[idx]
                best_ts = ts_right
                point_side = "right"
                offset_sec = diff

        if best_ts is not None:
            best_idx = bisect.bisect_left(timestamps, best_ts)
            candidate_gaps = []
            if best_idx > 0:
                candidate_gaps.append(best_ts - timestamps[best_idx - 1])
            if best_idx + 1 < length:
                candidate_gaps.append(timestamps[best_idx + 1] - best_ts)
            local_gap_sec = min(candidate_gaps) if candidate_gaps else None

        left_gap_sec = target_ts - timestamps[idx - 1] if idx > 0 else None
        right_gap_sec = timestamps[idx] - target_ts if idx < length else None
        stale_quote_flag = best_price is None
        results.append(
            {
                "price": best_price,
                "selected_ts": best_ts,
                "point_side": point_side,
                "offset_sec": offset_sec,
                "points_in_window": points_in_window,
                "left_gap_sec": left_gap_sec,
                "right_gap_sec": right_gap_sec,
                "local_gap_sec": local_gap_sec,
                "stale_quote_flag": stale_quote_flag,
            }
        )
    return results


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


def build_quote_window_features(
    cfg: PegConfig,
    *,
    merged_points: List[PricePoint],
    target_ts: int,
) -> Dict[str, float | int | str | bool | None]:
    snap_window_sec, stale_offset_sec, stale_gap_sec = _load_rule_snapshot_config(cfg)
    quote_meta = _find_prices_batch(
        [int(point.ts) for point in merged_points],
        [float(point.price) for point in merged_points],
        [int(target_ts)],
        window_sec=snap_window_sec,
    )[0]
    offset_sec = quote_meta.get("offset_sec")
    local_gap_sec = quote_meta.get("local_gap_sec")
    stale_quote_flag = (
        quote_meta.get("price") is None
        or (offset_sec is not None and float(offset_sec) > stale_offset_sec)
        or (local_gap_sec is not None and float(local_gap_sec) > stale_gap_sec)
    )
    points_in_window = float(quote_meta.get("points_in_window") or 0.0)
    offset_for_quality = min(max(float(offset_sec or 0.0), 0.0), float(snap_window_sec))
    snapshot_quality_score = (
        1.0 - offset_for_quality / max(float(snap_window_sec), 1.0)
    ) * (1.0 + math.log1p(max(points_in_window, 0.0)))
    return {
        "selected_quote_ts": quote_meta.get("selected_ts"),
        "snapshot_target_ts": int(target_ts),
        "selected_quote_side": quote_meta.get("point_side") or "UNKNOWN",
        "selected_quote_offset_sec": float(offset_sec) if offset_sec is not None else None,
        "selected_quote_points_in_window": points_in_window,
        "selected_quote_left_gap_sec": float(quote_meta["left_gap_sec"]) if quote_meta.get("left_gap_sec") is not None else None,
        "selected_quote_right_gap_sec": float(quote_meta["right_gap_sec"]) if quote_meta.get("right_gap_sec") is not None else None,
        "selected_quote_local_gap_sec": float(local_gap_sec) if local_gap_sec is not None else None,
        "stale_quote_flag": bool(stale_quote_flag),
        "snapshot_quality_score": float(snapshot_quality_score),
    }


def build_historical_price_features(
    *,
    current_price: float,
    now_ts: int,
    end_ts: int,
    merged_points: List[PricePoint],
) -> Dict[str, float | None]:
    horizons = [1, 2, 4, 6, 12, 24]
    features: Dict[str, float | None] = {}
    timestamps = [int(point.ts) for point in merged_points]
    prices = [float(point.price) for point in merged_points]
    target_times = [int(end_ts - hour * 3600) for hour in horizons]
    quote_points = _find_prices_batch(timestamps, prices, target_times, window_sec=300)
    horizon_prices: Dict[int, float | None] = {}
    for hour, quote_meta, target_ts in zip(horizons, quote_points, target_times):
        price = float(quote_meta["price"]) if quote_meta["price"] is not None and target_ts <= now_ts else None
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
