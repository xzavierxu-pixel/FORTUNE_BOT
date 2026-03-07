from __future__ import annotations

import numpy as np
import pandas as pd

from rule_baseline.utils import config
from rule_baseline.utils.data_processing import load_domain_features, load_raw_markets, load_snapshots
from rule_baseline.utils.research_context import TemporalSplit, assign_dataset_split, compute_temporal_split

DEFAULT_PRICE_MIN = 0.01
DEFAULT_PRICE_MAX = 0.99
DEFAULT_PRICE_BIN_STEP = 0.03


def _series_or_default(df: pd.DataFrame, column: str, default_value) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series(default_value, index=df.index)


def add_term_structure_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "market_id" not in df.columns:
        return df

    pivot = (
        df.pivot_table(index="market_id", columns="horizon_hours", values="price", aggfunc="first")
        .rename(columns={horizon: f"p_{int(horizon)}h" for horizon in config.HORIZONS if horizon in df["horizon_hours"].dropna().unique()})
        .reset_index()
    )
    out = df.merge(pivot, on="market_id", how="left")

    price_cols = [f"p_{horizon}h" for horizon in config.HORIZONS if f"p_{horizon}h" in out.columns]
    for left, right in [(1, 2), (2, 4), (4, 12), (12, 24)]:
        left_col = f"p_{left}h"
        right_col = f"p_{right}h"
        if left_col in out.columns and right_col in out.columns:
            out[f"delta_p_{left}_{right}"] = out[left_col] - out[right_col]

    if "p_1h" in out.columns and "p_24h" in out.columns:
        out["term_structure_slope"] = out["p_1h"] - out["p_24h"]
    else:
        out["term_structure_slope"] = np.nan

    out["path_price_mean"] = out[price_cols].mean(axis=1) if price_cols else np.nan
    out["path_price_std"] = out[price_cols].std(axis=1) if price_cols else np.nan
    out["path_price_min"] = out[price_cols].min(axis=1) if price_cols else np.nan
    out["path_price_max"] = out[price_cols].max(axis=1) if price_cols else np.nan
    out["path_price_range"] = out["path_price_max"] - out["path_price_min"]

    if "p_1h" in out.columns and "p_2h" in out.columns and "p_12h" in out.columns and "p_24h" in out.columns:
        short_leg = out["p_1h"] - out["p_2h"]
        long_leg = out["p_12h"] - out["p_24h"]
        out["price_reversal_flag"] = (short_leg * long_leg < 0).astype(float)
        out["price_acceleration"] = short_leg - long_leg
    else:
        out["price_reversal_flag"] = 0.0
        out["price_acceleration"] = 0.0

    if "p_24h" in out.columns:
        out["closing_drift"] = out["price"] - out["p_24h"]
    else:
        out["closing_drift"] = np.nan

    return out


def load_research_snapshots(
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
    max_rows: int | None = None,
    recent_days: int | None = None,
) -> pd.DataFrame:
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    domain_features = load_domain_features(config.MARKET_DOMAIN_FEATURES_PATH)

    context_cols = ["market_id", "domain", "category", "market_type"]
    snapshots = snapshots.merge(
        domain_features[context_cols],
        on="market_id",
        how="left",
        suffixes=("", "_domain"),
    )
    if "category_domain" in snapshots.columns:
        snapshots["category"] = snapshots["category_domain"].fillna(snapshots["category"])
        snapshots = snapshots.drop(columns=["category_domain"])

    raw_context_cols = [column for column in ["market_id", "startDate", "endDate", "closedTime"] if column in raw_markets.columns]
    snapshots = snapshots.merge(raw_markets[raw_context_cols], on="market_id", how="left", suffixes=("", "_market"))

    for column in ["startDate", "endDate", "closedTime", "closedTime_market"]:
        if column in snapshots.columns:
            snapshots[column] = pd.to_datetime(snapshots[column], utc=True, errors="coerce")
    if "closedTime_market" in snapshots.columns:
        if "closedTime" in snapshots.columns:
            snapshots["closedTime"] = snapshots["closedTime"].fillna(snapshots["closedTime_market"])
        else:
            snapshots["closedTime"] = snapshots["closedTime_market"]
        snapshots = snapshots.drop(columns=["closedTime_market"])
    if "closedTime" not in snapshots.columns:
        raise ValueError("Research snapshots are missing required 'closedTime' after market merge.")

    if max_rows is not None:
        snapshots = snapshots.sort_values("closedTime").tail(max_rows).copy()

    if recent_days is not None and recent_days > 0:
        cutoff = snapshots["closedTime"].max() - pd.Timedelta(days=recent_days)
        snapshots = snapshots[snapshots["closedTime"] >= cutoff].copy()

    snapshots["domain"] = snapshots.get("domain", "UNKNOWN").fillna("UNKNOWN").astype(str)
    snapshots["category"] = snapshots.get("category", "UNKNOWN").fillna("UNKNOWN").astype(str)
    snapshots["market_type"] = snapshots.get("market_type", "UNKNOWN").fillna("UNKNOWN").astype(str)
    snapshots["price"] = pd.to_numeric(snapshots["price"], errors="coerce")
    snapshots["horizon_hours"] = pd.to_numeric(snapshots["horizon_hours"], errors="coerce")
    snapshots["delta_hours"] = pd.to_numeric(snapshots["delta_hours"], errors="coerce")
    snapshots["selected_quote_offset_sec"] = pd.to_numeric(
        _series_or_default(snapshots, "selected_quote_offset_sec", 0.0),
        errors="coerce",
    ).fillna(0.0)
    snapshots["selected_quote_points_in_window"] = pd.to_numeric(
        _series_or_default(snapshots, "selected_quote_points_in_window", 0.0),
        errors="coerce",
    ).fillna(0.0)
    snapshots["stale_quote_flag"] = _series_or_default(snapshots, "stale_quote_flag", False).fillna(False).astype(bool)
    snapshots["selected_quote_side"] = _series_or_default(snapshots, "selected_quote_side", "UNKNOWN").fillna("UNKNOWN").astype(str)

    snapshots["market_duration_hours"] = (
        snapshots["closedTime"] - snapshots["startDate"]
    ).dt.total_seconds() / 3600.0
    snapshots["market_duration_hours"] = snapshots["market_duration_hours"].fillna(np.nan)
    snapshots["duration_is_negative_flag"] = snapshots["market_duration_hours"] < 0
    snapshots["duration_below_min_horizon_flag"] = snapshots["market_duration_hours"] < min(config.HORIZONS)
    snapshots["delta_hours_exceeded_flag"] = snapshots["delta_hours"] > config.MAX_ALLOWED_RESOLVE_DELTA_HOURS
    snapshots["delta_hours_bucket"] = snapshots["delta_hours"].fillna(999.0).round(2).clip(lower=0.0, upper=999.0)
    snapshots["price_in_range_flag"] = snapshots["price"].between(min_price, max_price, inclusive="left")
    snapshots["quality_pass"] = snapshots["price_in_range_flag"].fillna(False)
    snapshots["snapshot_quality_score"] = (
        1.0
        - snapshots["selected_quote_offset_sec"].clip(lower=0.0, upper=float(config.SNAP_WINDOW_SEC)) / max(float(config.SNAP_WINDOW_SEC), 1.0)
    ) * (1.0 + np.log1p(snapshots["selected_quote_points_in_window"].clip(lower=0.0)))

    snapshots["e_sample"] = snapshots["y"] - snapshots["price"]
    p_clip = snapshots["price"].clip(0.001, 0.999)
    snapshots["r_std"] = snapshots["e_sample"] / np.sqrt(p_clip * (1.0 - p_clip))
    return add_term_structure_features(snapshots)


def build_rule_bins(
    df: pd.DataFrame,
    price_bin_step: float = DEFAULT_PRICE_BIN_STEP,
) -> pd.DataFrame:
    out = df.copy()
    price_bins = np.arange(0, 1.0 + price_bin_step, price_bin_step)
    price_labels = [f"{round(value, 2)}-{round(value + price_bin_step, 2)}" for value in price_bins[:-1]]

    horizon_edges = [0] + sorted(config.HORIZONS) + [1000]
    horizon_labels = [f"<{horizon_edges[1]}h"]
    for index in range(1, len(horizon_edges) - 2):
        horizon_labels.append(f"{horizon_edges[index]}-{horizon_edges[index + 1]}h")
    horizon_labels.append(f">{horizon_edges[-2]}h")

    out["price_bin"] = pd.cut(out["price"], bins=price_bins, labels=price_labels, right=False)
    out["horizon_bin"] = pd.cut(out["horizon_hours"], bins=horizon_edges, labels=horizon_labels, right=False)
    return out.dropna(subset=["price_bin", "horizon_bin"]).copy()


def prepare_rule_training_frame(
    max_rows: int | None = None,
    recent_days: int | None = None,
) -> tuple[pd.DataFrame, TemporalSplit]:
    df = load_research_snapshots(max_rows=max_rows, recent_days=recent_days)
    print(f"[INFO] Snapshot rows before quality_pass filter: {len(df)}")
    df = df[df["quality_pass"]].copy()
    print(f"[INFO] Snapshot rows after quality_pass filter: {len(df)}")
    df = build_rule_bins(df)
    split = compute_temporal_split(df, date_col="closedTime")
    df = assign_dataset_split(df, split, date_col="closedTime")
    df = df[df["dataset_split"].isin(["train", "valid", "test"])].copy()
    return df, split


def apply_earliest_market_dedup(
    df: pd.DataFrame,
    score_column: str,
    market_column: str = "market_id",
    time_column: str = "snapshot_time",
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.sort_values(
        [market_column, time_column, score_column],
        ascending=[True, True, False],
    )
    return out.drop_duplicates(subset=[market_column], keep="first").reset_index(drop=True)
