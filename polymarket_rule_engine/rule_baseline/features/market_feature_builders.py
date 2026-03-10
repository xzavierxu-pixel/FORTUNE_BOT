from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from rule_baseline.utils import config
from rule_baseline.utils.feature_util import (
    CATEGORIES,
    FINANCE_THRESHOLD_KEYWORDS,
    HIGH_AMBIGUITY_KEYWORDS,
    MONTH_NAMES,
    OUTCOME_NEG,
    OUTCOME_POS,
    PLAYER_PROP_KEYWORDS,
    STRONG_POS,
    TEAM_TOTAL_KEYWORDS,
    WEAK_POS,
    build_year_tokens,
)

THRESHOLD_PATTERN = re.compile(r"([-+]?\d+(?:\.\d+)?)")
YEAR_TOKENS = build_year_tokens()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return default
        return float(value)
    except Exception:
        return default


def _parse_tokens(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _hash_text_embedding(text: str, dim: int) -> dict[str, float]:
    bucket = np.zeros(dim, dtype=float)
    for token in text.split():
        digest = hashlib.sha1(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % dim
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        bucket[index] += sign
    norm = float(np.linalg.norm(bucket))
    if norm > 0:
        bucket = bucket / norm
    return {f"text_embed_{idx:02d}": float(value) for idx, value in enumerate(bucket)}


def _extract_numeric_thresholds(text: str) -> tuple[float, float]:
    matches = [float(match) for match in THRESHOLD_PATTERN.findall(text)]
    if not matches:
        return 0.0, 0.0
    return float(max(matches)), float(min(matches))


def extract_market_features(market: dict[str, Any]) -> dict[str, float]:
    vol = _to_float(market.get("volume", market.get("volumeNum", 0.0)))
    liq = _to_float(market.get("liquidity", market.get("liquidityNum", 0.0)))
    v24 = _to_float(market.get("volume24hr", 0.0))
    v1w = _to_float(market.get("volume1wk", 0.0))
    best_bid = _to_float(market.get("bestBid", 0.0))
    best_ask = _to_float(market.get("bestAsk", 0.0))
    spread = _to_float(market.get("spread", 0.0))
    last_trade = _to_float(market.get("lastTradePrice", 0.0))
    liquidity_clob = _to_float(market.get("liquidityClob", 0.0))
    liquidity_amm = _to_float(market.get("liquidityAmm", 0.0))
    volume24_clob = _to_float(market.get("volume24hrClob", 0.0))
    volume1w_clob = _to_float(market.get("volume1wkClob", 0.0))
    price_change_1h = _to_float(market.get("oneHourPriceChange", 0.0))
    price_change_1d = _to_float(market.get("oneDayPriceChange", 0.0))
    price_change_1w = _to_float(market.get("oneWeekPriceChange", 0.0))
    line_value = _to_float(market.get("line", 0.0))

    question = str(market.get("question", market.get("title", "")) or "").lower()
    description = str(market.get("description", "") or "").lower()
    text = f"{question} {description}".strip()
    words = question.split()

    features: dict[str, float] = {}

    features["log_vol"] = np.log1p(vol)
    features["log_liq"] = np.log1p(liq)
    features["log_v24"] = np.log1p(v24)
    features["log_v1w"] = np.log1p(v1w)
    features["vol_ratio_24"] = min(v24 / max(vol, 1.0), 1.0)
    features["vol_ratio_1w"] = min(v1w / max(vol, 1.0), 1.0)
    features["liq_ratio"] = min(liq / max(vol, 1.0), 5.0)
    features["daily_weekly"] = min(v24 * 7.0 / max(v1w, 1.0), 10.0) if v1w > 0 else 1.0
    features["vol_tier_ultra"] = float(vol > 500_000)
    features["vol_tier_high"] = float(100_000 < vol <= 500_000)
    features["vol_tier_med"] = float(10_000 < vol <= 100_000)
    features["vol_tier_low"] = float(vol <= 10_000)
    features["activity"] = min(1.0, np.log1p(vol) / 17.0)
    features["engagement"] = (features["vol_ratio_24"] + features["vol_ratio_1w"]) / 2.0
    features["momentum"] = features["vol_ratio_24"] - features["vol_ratio_1w"] / 7.0
    features["best_bid"] = best_bid
    features["best_ask"] = best_ask
    features["mid_price"] = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else last_trade
    features["quoted_spread"] = spread if spread > 0 else max(best_ask - best_bid, 0.0)
    features["quoted_spread_pct"] = features["quoted_spread"] / max(features["mid_price"], 1e-6)
    features["book_imbalance"] = (liq - vol) / max(liq + vol, 1.0)
    features["log_liquidity_clob"] = np.log1p(liquidity_clob)
    features["log_liquidity_amm"] = np.log1p(liquidity_amm)
    features["clob_share_liquidity"] = liquidity_clob / max(liq, 1.0)
    features["clob_share_volume24"] = volume24_clob / max(v24, 1.0)
    features["clob_share_volume1w"] = volume1w_clob / max(v1w, 1.0)
    features["price_change_1h"] = price_change_1h
    features["price_change_1d"] = price_change_1d
    features["price_change_1w"] = price_change_1w
    features["price_change_accel"] = price_change_1h - price_change_1d / 24.0
    features["line_value"] = line_value
    features["has_line"] = float(line_value != 0.0)

    q_len = len(words)
    q_chars = len(question)
    features["q_len"] = float(min(q_len, 50))
    features["q_chars"] = float(min(q_chars, 300))
    features["avg_word_len"] = float(np.mean([len(word) for word in words])) if words else 0.0
    features["max_word_len"] = float(max([len(word) for word in words])) if words else 0.0
    features["word_diversity"] = len(set(words)) / max(q_len, 1)
    features["num_count"] = float(sum(1 for char in question if char.isdigit()))
    features["has_number"] = float(any(char.isdigit() for char in question))
    features["has_year"] = float(any(year in question for year in YEAR_TOKENS))
    features["has_percent"] = float("%" in question or "percent" in question)
    features["has_dollar"] = float("$" in question or "dollar" in question)
    features["has_million"] = float("million" in question or "billion" in question)
    features["has_date"] = float(any(month in question for month in MONTH_NAMES))
    features["starts_will"] = float(question.startswith("will"))
    features["starts_can"] = float(question.startswith("can"))
    features["has_by"] = float(" by " in question)
    features["has_before"] = float("before" in question or "by end" in question)
    features["has_after"] = float("after" in question)
    features["has_above_below"] = float("above" in question or "below" in question)
    features["is_binary"] = float(len(_parse_tokens(market.get("tokens") or market.get("outcomes"))) == 2)
    features["has_or"] = float(" or " in question)
    features["has_and"] = float(" and " in question)
    features["cap_ratio"] = sum(1 for char in question if char.isupper()) / max(q_chars, 1)
    features["punct_count"] = float(sum(1 for char in question if char in "?!.,")) 
    threshold_max, threshold_min = _extract_numeric_thresholds(question)
    features["threshold_max"] = threshold_max
    features["threshold_min"] = threshold_min
    features["threshold_span"] = max(threshold_max - threshold_min, 0.0)
    features["is_player_prop"] = float(any(token in text for token in PLAYER_PROP_KEYWORDS))
    features["is_team_total"] = float(any(token in text for token in TEAM_TOTAL_KEYWORDS))
    features["is_finance_threshold"] = float(any(token in text for token in FINANCE_THRESHOLD_KEYWORDS))
    features["is_date_based"] = float(features["has_date"] or features["has_before"] or features["has_after"])
    features["is_high_ambiguity"] = float(any(token in text for token in HIGH_AMBIGUITY_KEYWORDS))

    strong_pos = sum(1 for token in STRONG_POS if token in text)
    weak_pos = sum(1 for token in WEAK_POS if token in text)
    out_pos = sum(1 for token in OUTCOME_POS if token in text)
    out_neg = sum(1 for token in OUTCOME_NEG if token in text)

    features["strong_pos"] = float(min(strong_pos, 5))
    features["weak_pos"] = float(min(weak_pos, 5))
    features["outcome_pos"] = float(min(out_pos, 5))
    features["outcome_neg"] = float(min(out_neg, 5))
    features["sentiment"] = (out_pos - out_neg) / max(out_pos + out_neg, 1)
    features["sentiment_abs"] = abs(out_pos - out_neg) / max(out_pos + out_neg, 1)
    features["total_sentiment"] = float(min(out_pos + out_neg, 10))
    features["certainty"] = (strong_pos - weak_pos) / max(strong_pos + weak_pos, 1)
    features["pos_ratio"] = out_pos / max(out_pos + out_neg, 1)
    features["neg_ratio"] = out_neg / max(out_pos + out_neg, 1)
    features["sentiment_vol"] = features["sentiment"] * features["log_vol"]
    features["sentiment_activity"] = features["sentiment"] * features["activity"]

    cat_count = 0
    max_matches = 0
    for category, keywords in CATEGORIES.items():
        matches = sum(1 for keyword in keywords if keyword in text)
        features[f"cat_{category}"] = float(matches > 0)
        features[f"cat_{category}_str"] = float(min(matches, 5))
        if matches > 0:
            cat_count += 1
        max_matches = max(max_matches, matches)
    features["cat_count"] = float(cat_count)
    features["primary_cat_str"] = float(max_matches)

    days = 30
    try:
        end_value = market.get("endDate") or market.get("end_date_iso")
        start_value = market.get("createdAt") or market.get("created_at") or market.get("creationDate") or market.get("startDate")
        if end_value and start_value:
            end_dt = datetime.fromisoformat(str(end_value).replace("Z", "+00:00"))
            start_dt = datetime.fromisoformat(str(start_value).replace("Z", "+00:00"))
            days = max(1, (end_dt - start_dt).days)
    except Exception:
        pass

    features["log_duration"] = np.log1p(days)
    features["dur_very_short"] = float(days <= 3)
    features["dur_short"] = float(3 < days <= 7)
    features["dur_medium"] = float(7 < days <= 30)
    features["dur_long"] = float(30 < days <= 90)
    features["dur_very_long"] = float(days > 90)
    features["vol_per_day"] = vol / max(days, 1)
    features["log_vol_per_day"] = np.log1p(vol / max(days, 1))

    features["vol_x_sentiment"] = features["log_vol"] * features["sentiment"]
    features["activity_x_catcount"] = features["activity"] * cat_count
    features["engagement_x_duration"] = features["engagement"] * features["log_duration"]
    features["sentiment_x_duration"] = features["sentiment"] * features["log_duration"]
    features["vol_x_diversity"] = features["log_vol"] * features["word_diversity"]
    features.update(_hash_text_embedding(text, config.TEXT_EMBED_DIM))

    return features


def build_market_feature_frame(raw_markets: pd.DataFrame) -> pd.DataFrame:
    if raw_markets.empty:
        return pd.DataFrame(columns=["market_id"])

    records: list[dict[str, Any]] = []
    for market in raw_markets.to_dict("records"):
        row = {"market_id": str(market.get("id", market.get("market_id", "")))}
        row.update(extract_market_features(market))
        records.append(row)

    feature_frame = pd.DataFrame(records)
    feature_frame = feature_frame.drop_duplicates(subset=["market_id"]).reset_index(drop=True)
    return feature_frame
