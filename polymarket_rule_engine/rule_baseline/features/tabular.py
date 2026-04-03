from __future__ import annotations

import numpy as np
import pandas as pd

from rule_baseline.features.market_feature_builders import build_market_feature_frame


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
    if "category" in out.columns:
        out = out.rename(columns={"category": "snapshot_category"})

    if not market_feature_cache.empty:
        feature_cache = market_feature_cache.copy()
        feature_cache["market_id"] = feature_cache["market_id"].astype(str)
        out = out.merge(feature_cache, on="market_id", how="left", suffixes=("", "_market"))

    if "snapshot_category" in out.columns:
        if "category" in out.columns:
            out["category"] = out["category"].fillna(out["snapshot_category"])
        else:
            out["category"] = out["snapshot_category"]
        out = out.drop(columns=["snapshot_category"])

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

    if "y" in out.columns:
        out["y"] = pd.to_numeric(out["y"], errors="coerce").fillna(0).astype(int)

    return out
