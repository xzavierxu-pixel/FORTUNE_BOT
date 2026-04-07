from __future__ import annotations

import numpy as np
import pandas as pd

from rule_baseline.features.annotation_normalization import CACHE_CATEGORY_SOURCE, SNAPSHOT_CATEGORY_SOURCE
from rule_baseline.features.market_feature_builders import build_market_feature_frame

DEFAULT_FEATURE_VARIANT = "interaction_features"


def build_market_feature_cache(
    raw_markets: pd.DataFrame,
    market_annotations: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if raw_markets.empty:
        return pd.DataFrame(columns=["market_id"])

    raw = raw_markets.copy()
    raw["market_id"] = raw["market_id"].astype(str)
    if "id" in raw.columns:
        raw["id"] = raw["id"].astype(str)

    base_columns = [
        "market_id",
        "question",
        "description",
        "volume",
        "liquidity",
        "volume24hr",
        "volume1wk",
        "volume24hrClob",
        "volume1wkClob",
        "orderPriceMinTickSize",
        "negRisk",
        "rewardsMinSize",
        "rewardsMaxSpread",
        "bestBid",
        "bestAsk",
        "spread",
        "lastTradePrice",
        "line",
        "oneHourPriceChange",
        "oneDayPriceChange",
        "oneWeekPriceChange",
        "liquidityAmm",
        "liquidityClob",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
        "startDate",
        "endDate",
        "closedTime",
    ]
    base_columns = [column for column in base_columns if column in raw.columns]
    cache = raw[base_columns].copy()

    feature_frame = build_market_feature_frame(raw)
    cache = cache.merge(feature_frame, on="market_id", how="left")

    annotations = market_annotations.copy() if market_annotations is not None else pd.DataFrame(columns=["market_id"])
    if not annotations.empty:
        annotations["market_id"] = annotations["market_id"].astype(str)
        cache = cache.merge(annotations, on="market_id", how="left", suffixes=("", "_annotation"))

    if "source_host" not in cache.columns:
        cache["source_host"] = cache.get("domain", "UNKNOWN")

    fill_unknown_columns = [
        "domain",
        "domain_parsed",
        "category",
        "category_raw",
        "category_parsed",
        "market_type",
        "sub_domain",
        "source_host",
        "source_url",
        "outcome_pattern",
        "negRisk",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
    ]
    for column in fill_unknown_columns:
        if column not in cache.columns:
            cache[column] = "UNKNOWN"
        cache[column] = cache[column].fillna("UNKNOWN")

    numeric_columns = [
        "orderPriceMinTickSize",
        "rewardsMinSize",
        "rewardsMaxSpread",
        "volume",
        "liquidity",
        "volume24hr",
        "volume1wk",
        "volume24hrClob",
        "volume1wkClob",
        "bestBid",
        "bestAsk",
        "spread",
        "lastTradePrice",
        "line",
        "oneHourPriceChange",
        "oneDayPriceChange",
        "oneWeekPriceChange",
        "liquidityAmm",
        "liquidityClob",
    ]
    for column in numeric_columns:
        if column in cache.columns:
            cache[column] = pd.to_numeric(cache[column], errors="coerce").fillna(0.0)

    if "startDate" in cache.columns and "closedTime" in cache.columns:
        start_time = pd.to_datetime(cache["startDate"], utc=True, errors="coerce")
        closed_time = pd.to_datetime(cache["closedTime"], utc=True, errors="coerce")
        cache["market_duration_hours"] = (closed_time - start_time).dt.total_seconds() / 3600.0
        cache["market_duration_hours"] = cache["market_duration_hours"].fillna(0.0)
    else:
        cache["market_duration_hours"] = 0.0

    return cache.drop_duplicates(subset=["market_id"]).reset_index(drop=True)


def preprocess_features(
    df: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    if "market_id" not in out.columns:
        raise ValueError("Input feature frame must include market_id.")

    out["market_id"] = out["market_id"].astype(str)
    existing_category_source = (
        out.get("category_source", pd.Series(SNAPSHOT_CATEGORY_SOURCE, index=out.index))
        .astype("string")
        .fillna(SNAPSHOT_CATEGORY_SOURCE)
        .replace("", SNAPSHOT_CATEGORY_SOURCE)
    )
    if "category" in out.columns:
        out = out.rename(columns={"category": "snapshot_category"})

    if not market_feature_cache.empty:
        feature_cache = market_feature_cache.copy()
        feature_cache["market_id"] = feature_cache["market_id"].astype(str)
        out = out.merge(feature_cache, on="market_id", how="left", suffixes=("", "_market"))

    if "snapshot_category" in out.columns:
        snapshot_category = out["snapshot_category"]
        snapshot_available = snapshot_category.astype("string").fillna("").str.strip() != ""
        if "category" in out.columns:
            cache_category = out["category"]
            cache_available = (
                cache_category.astype("string").fillna("").str.strip().replace("UNKNOWN", "") != ""
            )
            out["category"] = cache_category.where(cache_available, snapshot_category)
            out["category_source"] = existing_category_source.where(~cache_available, CACHE_CATEGORY_SOURCE)
        else:
            out["category"] = snapshot_category
            out["category_source"] = existing_category_source.where(snapshot_available, SNAPSHOT_CATEGORY_SOURCE)
        out = out.drop(columns=["snapshot_category"])
    elif "category" in out.columns:
        cache_available = out["category"].astype("string").fillna("").str.strip().replace("UNKNOWN", "") != ""
        out["category_source"] = existing_category_source.where(~cache_available, CACHE_CATEGORY_SOURCE)
    else:
        out["category_source"] = existing_category_source

    if "price" in out.columns:
        out["price"] = pd.to_numeric(out["price"], errors="coerce").fillna(0.0)
    if "horizon_hours" in out.columns:
        out["horizon_hours"] = pd.to_numeric(out["horizon_hours"], errors="coerce").fillna(0.0)
    out["log_horizon"] = np.log1p(out["horizon_hours"].clip(lower=0))

    if "q_smooth" not in out.columns and "q_full" in out.columns:
        out["q_smooth"] = out["q_full"]
    if "q_smooth" in out.columns:
        out["q_smooth"] = pd.to_numeric(out["q_smooth"], errors="coerce").fillna(0.5)
    else:
        out["q_smooth"] = 0.5

    if "rule_score" in out.columns:
        out["rule_score"] = pd.to_numeric(out["rule_score"], errors="coerce").fillna(0.0)
    else:
        out["rule_score"] = 0.0

    out = apply_feature_variant(out, feature_variant=DEFAULT_FEATURE_VARIANT)

    numeric_columns = [
        "volume",
        "liquidity",
        "volume24hr",
        "volume1wk",
        "volume24hrClob",
        "volume1wkClob",
        "bestBid",
        "bestAsk",
        "spread",
        "lastTradePrice",
        "line",
        "oneHourPriceChange",
        "oneDayPriceChange",
        "oneWeekPriceChange",
        "liquidityAmm",
        "liquidityClob",
        "selected_quote_offset_sec",
        "selected_quote_points_in_window",
        "selected_quote_left_gap_sec",
        "selected_quote_right_gap_sec",
        "selected_quote_local_gap_sec",
        "snapshot_target_ts",
        "selected_quote_ts",
        "abs_price_q_gap",
        "abs_price_center_gap",
        "horizon_q_gap",
        "spread_over_liquidity",
        "quote_staleness_x_horizon",
        "rule_score_x_q_full",
    ]
    for column in numeric_columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)

    categorical_columns = [
        "domain",
        "domain_parsed",
        "category",
        "category_raw",
        "category_parsed",
        "market_type",
        "sub_domain",
        "source_host",
        "outcome_pattern",
        "negRisk",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
        "primary_outcome",
        "secondary_outcome",
        "winning_outcome_label",
        "selected_quote_side",
        "question_market",
        "description_market",
        "groupItemTitle_market",
        "gameId_market",
        "marketMakerAddress_market",
        "startDate_market",
        "endDate_market",
        "closedTime_market",
        "domain_parsed_market",
        "sub_domain_market",
        "source_url_market",
        "category_raw_market",
        "category_parsed_market",
        "outcome_pattern_market",
        "source_host_market",
    ]
    for column in categorical_columns:
        if column not in out.columns:
            out[column] = "UNKNOWN"
        out[column] = out[column].astype("string").fillna("UNKNOWN").astype("category")

    if "category_override_flag" in out.columns:
        out["category_override_flag"] = out["category_override_flag"].fillna(False).astype(bool)
    out["category_source"] = (
        out.get("category_source", pd.Series(SNAPSHOT_CATEGORY_SOURCE, index=out.index))
        .astype("string")
        .fillna(SNAPSHOT_CATEGORY_SOURCE)
        .replace("", SNAPSHOT_CATEGORY_SOURCE)
    )

    if "y" in out.columns:
        out["y"] = pd.to_numeric(out["y"], errors="coerce").fillna(0).astype(int)

    return out


def _safe_numeric(series: pd.Series | None, default: float = 0.0) -> pd.Series:
    if series is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(series, errors="coerce").fillna(default).astype(float)


def _safe_text(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="string")
    return series.astype("string").fillna("")


def apply_feature_variant(
    df_feat: pd.DataFrame,
    *,
    feature_variant: str = DEFAULT_FEATURE_VARIANT,
) -> pd.DataFrame:
    out = df_feat.copy()
    if feature_variant == "baseline":
        return out

    # Shared experiment/training feature contract. Training must use the exact
    # same augmentation path as experiments to avoid contract drift.
    price = _safe_numeric(out.get("price"), default=0.0)
    q_anchor = _safe_numeric(out.get("q_smooth"), default=0.5)
    horizon = _safe_numeric(out.get("horizon_hours"), default=0.0)
    log_horizon = _safe_numeric(out.get("log_horizon"), default=0.0) if "log_horizon" in out.columns else pd.Series(dtype=float)
    if log_horizon.empty:
        log_horizon = pd.Series(np.log1p(horizon.clip(lower=0.0)), index=out.index)
    liquidity = _safe_numeric(out.get("liquidity"), default=0.0)
    spread = _safe_numeric(out.get("spread"), default=0.0)
    quote_offset = _safe_numeric(out.get("selected_quote_offset_sec"), default=0.0)
    rule_score = _safe_numeric(out.get("rule_score"), default=0.0)
    edge_lower = _safe_numeric(out.get("edge_lower_bound_full"), default=0.0) if "edge_lower_bound_full" in out.columns else pd.Series(0.0, index=out.index)
    edge_std = _safe_numeric(out.get("edge_std_full"), default=0.0) if "edge_std_full" in out.columns else pd.Series(0.0, index=out.index)

    out["abs_price_q_gap"] = (price - q_anchor).abs()
    out["abs_price_center_gap"] = (price - 0.5).abs()
    out["horizon_q_gap"] = horizon * out["abs_price_q_gap"]
    out["log_horizon_x_liquidity"] = pd.Series(np.log1p(liquidity.clip(lower=0.0)), index=out.index) * _safe_numeric(log_horizon, default=0.0)
    out["spread_over_liquidity"] = spread / (liquidity.abs() + 1.0)
    out["quote_staleness_x_horizon"] = quote_offset * (horizon + 1.0)
    out["rule_score_x_q_full"] = rule_score * q_anchor
    out["edge_lower_bound_over_std"] = edge_lower / edge_std.replace(0.0, pd.NA).fillna(1.0)

    if feature_variant == "market_structure_v2":
        best_bid = _safe_numeric(out.get("bestBid"), default=0.0)
        best_ask = _safe_numeric(out.get("bestAsk"), default=0.0)
        mid_price = ((best_bid + best_ask) / 2.0).replace([np.inf, -np.inf], 0.0).fillna(0.0)
        liquidity_clob = _safe_numeric(out.get("liquidityClob"), default=0.0)
        volume24_clob = _safe_numeric(out.get("volume24hrClob"), default=0.0)
        volume1w_clob = _safe_numeric(out.get("volume1wkClob"), default=0.0)
        rewards_max_spread = _safe_numeric(out.get("rewardsMaxSpread"), default=0.0)
        out["book_mid_gap"] = (price - mid_price).abs()
        out["spread_to_mid_ratio"] = spread / (mid_price.abs() + 1e-6)
        out["quote_quality_score"] = 1.0 / (1.0 + out["spread_to_mid_ratio"].abs() + quote_offset / 60.0)
        out["liquidity_pressure"] = liquidity_clob / (liquidity.abs() + 1.0)
        out["clob_turnover_24h"] = volume24_clob / (liquidity_clob.abs() + 1.0)
        out["clob_turnover_1w"] = volume1w_clob / (liquidity_clob.abs() + 1.0)
        out["uncertainty_normalized_edge"] = edge_lower / (edge_std.abs() + 1e-6)
        out["rule_confidence_gap"] = rule_score - out["abs_price_q_gap"]
        out["reward_spread_alignment"] = rewards_max_spread - spread
        out["horizon_term_structure"] = log_horizon * out["abs_price_center_gap"]
        return out

    if feature_variant == "interaction_plus_textlite":
        question = _safe_text(out.get("question_market"))
        description = _safe_text(out.get("description_market"))
        combined = (question + " " + description).astype("string")
        out["question_length_chars"] = question.str.len().astype(float)
        out["description_length_chars"] = description.str.len().astype(float)
        out["text_has_year"] = combined.str.contains(r"\b20\d{2}\b", regex=True, na=False).astype(int)
        out["text_has_date_word"] = combined.str.contains(
            r"\b(january|february|march|april|may|june|july|august|september|october|november|december|today|tomorrow)\b",
            regex=True,
            case=False,
            na=False,
        ).astype(int)
        out["text_has_percent"] = combined.str.contains(r"%|percent", regex=True, case=False, na=False).astype(int)
        out["text_has_currency"] = combined.str.contains(r"\$|usd|dollar|million|billion", regex=True, case=False, na=False).astype(int)
        out["text_has_deadline_word"] = combined.str.contains(
            r"\b(before|after|by|end of|deadline|close|closing)\b",
            regex=True,
            case=False,
            na=False,
        ).astype(int)

    return out
