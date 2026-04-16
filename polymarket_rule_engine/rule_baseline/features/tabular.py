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

    group_rule_numeric_columns = [
        "q_full",
        "p_full",
        "edge_full",
        "edge_std_full",
        "edge_lower_bound_full",
        "n_full",
        "h_min",
        "h_max",
        "horizon_hours_rule",
        "group_unique_markets",
        "group_snapshot_rows",
        "global_total_unique_markets",
        "global_total_snapshot_rows",
        "group_market_share_global",
        "group_snapshot_share_global",
        "group_median_logloss",
        "group_median_brier",
        "global_group_logloss_q25",
        "global_group_brier_q25",
    ]
    serving_numeric_exclusions = ("leaf_id", "group_decision", "domain", "category", "market_type", "_key")
    dynamic_serving_numeric_columns = [
        column
        for column in out.columns
        if (column.startswith("group_feature_") or column.startswith("fine_feature_"))
        and not column.endswith(serving_numeric_exclusions)
    ]
    for column in group_rule_numeric_columns:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)
    for column in dynamic_serving_numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)

    if "group_decision" not in out.columns:
        out["group_decision"] = "UNKNOWN"
    if "domain" not in out.columns:
        out["domain"] = "UNKNOWN"
    if "category" not in out.columns:
        out["category"] = "UNKNOWN"
    if "market_type" not in out.columns:
        out["market_type"] = "UNKNOWN"

    # Structural keys for hierarchical grouping beyond the full group_key.
    out["domain_category_key"] = out["domain"].astype(str) + "|" + out["category"].astype(str)
    out["domain_market_type_key"] = out["domain"].astype(str) + "|" + out["market_type"].astype(str)
    out["category_market_type_key"] = out["category"].astype(str) + "|" + out["market_type"].astype(str)
    out["domain_is_unknown"] = out["domain"].astype(str).eq("UNKNOWN").astype(float)

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
        "quote_staleness_x_horizon",
        "rule_score_x_q_full",
        "q_full",
        "p_full",
        "edge_full",
        "edge_std_full",
        "edge_lower_bound_full",
        "n_full",
        "h_min",
        "h_max",
        "horizon_hours_rule",
        "group_unique_markets",
        "group_snapshot_rows",
        "global_total_unique_markets",
        "global_total_snapshot_rows",
        "group_market_share_global",
        "group_snapshot_share_global",
        "group_median_logloss",
        "group_median_brier",
        "global_group_logloss_q25",
        "global_group_brier_q25",
        "group_logloss_gap_q25",
        "group_brier_gap_q25",
        "group_quality_pass_q25",
        "group_quality_fail_q25",
        "rule_edge_over_std",
        "rule_edge_over_logloss",
        "rule_edge_over_brier",
        "group_market_density",
        "group_snapshot_density",
        "group_rule_score_x_edge_lower",
        "edge_lower_bound_over_std",
        "domain_is_unknown",
        "rule_price_center",
        "rule_price_width",
        "rule_horizon_center",
        "rule_horizon_width",
        "rule_edge_buffer",
        "rule_confidence_ratio",
        "rule_support_log1p",
        "rule_snapshot_support_log1p",
        "group_share_x_logloss_gap",
        "group_share_x_brier_gap",
        "group_match_found",
        "fine_match_found",
        "used_group_fallback_only",
    ]
    numeric_columns.extend(dynamic_serving_numeric_columns)
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
        "group_decision",
        "domain_category_key",
        "domain_market_type_key",
        "category_market_type_key",
    ]
    categorical_columns.extend(
        [
            column
            for column in out.columns
            if (column.startswith("group_feature_") or column.startswith("fine_feature_"))
            and column.endswith(serving_numeric_exclusions)
        ]
    )
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
    edge_full = _safe_numeric(out.get("edge_full"), default=0.0) if "edge_full" in out.columns else pd.Series(0.0, index=out.index)
    n_full = _safe_numeric(out.get("n_full"), default=0.0) if "n_full" in out.columns else pd.Series(0.0, index=out.index)
    group_unique_markets = _safe_numeric(out.get("group_unique_markets"), default=0.0) if "group_unique_markets" in out.columns else pd.Series(0.0, index=out.index)
    group_snapshot_rows = _safe_numeric(out.get("group_snapshot_rows"), default=0.0) if "group_snapshot_rows" in out.columns else pd.Series(0.0, index=out.index)
    group_market_share = _safe_numeric(out.get("group_market_share_global"), default=0.0) if "group_market_share_global" in out.columns else pd.Series(0.0, index=out.index)
    group_snapshot_share = _safe_numeric(out.get("group_snapshot_share_global"), default=0.0) if "group_snapshot_share_global" in out.columns else pd.Series(0.0, index=out.index)
    group_median_logloss = _safe_numeric(out.get("group_median_logloss"), default=0.0) if "group_median_logloss" in out.columns else pd.Series(0.0, index=out.index)
    group_median_brier = _safe_numeric(out.get("group_median_brier"), default=0.0) if "group_median_brier" in out.columns else pd.Series(0.0, index=out.index)
    global_logloss_q25 = _safe_numeric(out.get("global_group_logloss_q25"), default=0.0) if "global_group_logloss_q25" in out.columns else pd.Series(0.0, index=out.index)
    global_brier_q25 = _safe_numeric(out.get("global_group_brier_q25"), default=0.0) if "global_group_brier_q25" in out.columns else pd.Series(0.0, index=out.index)
    h_min = _safe_numeric(out.get("h_min"), default=0.0) if "h_min" in out.columns else pd.Series(0.0, index=out.index)
    h_max = _safe_numeric(out.get("h_max"), default=0.0) if "h_max" in out.columns else pd.Series(0.0, index=out.index)
    p_full = _safe_numeric(out.get("p_full"), default=0.0) if "p_full" in out.columns else pd.Series(0.0, index=out.index)

    out["abs_price_q_gap"] = (price - q_anchor).abs()
    out["abs_price_center_gap"] = (price - 0.5).abs()
    out["horizon_q_gap"] = horizon * out["abs_price_q_gap"]
    out["quote_staleness_x_horizon"] = quote_offset * (horizon + 1.0)
    out["rule_score_x_q_full"] = rule_score * q_anchor
    out["edge_lower_bound_over_std"] = edge_lower / edge_std.replace(0.0, pd.NA).fillna(1.0)
    out["group_logloss_gap_q25"] = group_median_logloss - global_logloss_q25
    out["group_brier_gap_q25"] = group_median_brier - global_brier_q25
    out["group_quality_pass_q25"] = (
        (group_median_logloss >= global_logloss_q25) | (group_median_brier >= global_brier_q25)
    ).astype(float)
    out["group_quality_fail_q25"] = (
        (group_median_logloss < global_logloss_q25) & (group_median_brier < global_brier_q25)
    ).astype(float)
    out["rule_edge_over_std"] = edge_full / (edge_std.abs() + 1e-6)
    out["rule_edge_over_logloss"] = edge_full / (group_median_logloss.abs() + 1e-6)
    out["rule_edge_over_brier"] = edge_full / (group_median_brier.abs() + 1e-6)
    out["group_market_density"] = group_unique_markets / (n_full + 1.0)
    out["group_snapshot_density"] = group_snapshot_rows / (group_unique_markets + 1.0)
    out["group_rule_score_x_edge_lower"] = rule_score * edge_lower
    out["rule_price_center"] = (p_full + price) / 2.0
    out["rule_price_width"] = (_safe_numeric(out.get("price_max"), default=0.0) - _safe_numeric(out.get("price_min"), default=0.0)).clip(lower=0.0)
    out["rule_horizon_center"] = (h_min + h_max) / 2.0
    out["rule_horizon_width"] = (h_max - h_min).clip(lower=0.0)
    out["rule_edge_buffer"] = edge_full - edge_lower
    out["rule_confidence_ratio"] = edge_lower / (edge_full.abs() + 1e-6)
    out["rule_support_log1p"] = np.log1p(n_full.clip(lower=0.0))
    out["rule_snapshot_support_log1p"] = np.log1p(group_snapshot_rows.clip(lower=0.0))
    out["group_share_x_logloss_gap"] = group_market_share * out["group_logloss_gap_q25"]
    out["group_share_x_brier_gap"] = group_snapshot_share * out["group_brier_gap_q25"]

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
