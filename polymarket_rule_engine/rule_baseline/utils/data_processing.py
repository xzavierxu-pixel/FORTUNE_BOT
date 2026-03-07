from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from rule_baseline.utils import config
from rule_baseline.utils.ensemble_features import build_market_feature_frame
from rule_baseline.utils.raw_batches import rebuild_canonical_merged


def load_snapshots(path: Path | None = None) -> pd.DataFrame:
    target = path or config.SNAPSHOTS_PATH
    if not target.exists():
        raise FileNotFoundError(f"Snapshots file not found at {target}")

    print(f"[INFO] Loading snapshots from {target}...")
    df = pd.read_csv(target, low_memory=False)

    df["scheduled_end"] = pd.to_datetime(df["scheduled_end"], utc=True, format="mixed")
    df["resolve_time"] = pd.to_datetime(df["resolve_time"], utc=True, format="mixed")
    df["snapshot_time"] = df["scheduled_end"] - pd.to_timedelta(df["horizon_hours"], unit="h")
    df["snapshot_date"] = df["snapshot_time"].dt.date
    df["market_id"] = df["market_id"].astype(str)
    df["y"] = df["y"].astype(int)
    return df


def load_raw_markets(path: Path | None = None, rebuild: bool = False) -> pd.DataFrame:
    target = path or config.RAW_MERGED_PATH
    if rebuild or not target.exists():
        rebuild_canonical_merged()

    if not target.exists():
        print(f"[WARN] Merged raw markets not found at {target}.")
        return pd.DataFrame(columns=["market_id"])

    print(f"[INFO] Loading merged raw markets from {target}...")
    df = pd.read_csv(target, low_memory=False)
    if "id" not in df.columns:
        raise ValueError(f"Merged raw markets at {target} are missing the 'id' column.")
    df["id"] = df["id"].astype(str)
    df["market_id"] = df["id"]
    return df


def load_domain_features(path: Path | None = None) -> pd.DataFrame:
    target = path or config.MARKET_DOMAIN_FEATURES_PATH
    if not target.exists():
        print(f"[WARN] Domain features not found at {target}.")
        return pd.DataFrame(columns=["market_id", "domain", "category", "market_type"])

    print(f"[INFO] Loading domain features from {target}...")
    df = pd.read_csv(target)
    df["market_id"] = df["market_id"].astype(str)
    return df


def build_market_feature_cache(
    raw_markets: pd.DataFrame,
    domain_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if raw_markets.empty:
        return pd.DataFrame(columns=["market_id"])

    raw = raw_markets.copy()
    raw["market_id"] = raw["market_id"].astype(str)
    if "id" in raw.columns:
        raw["id"] = raw["id"].astype(str)

    base_cols = [
        "market_id",
        "question",
        "description",
        "volume",
        "liquidity",
        "volume24hr",
        "volume1wk",
        "sportsMarketType",
        "marketType",
        "orderPriceMinTickSize",
        "negRisk",
        "rewardsMinSize",
        "rewardsMaxSpread",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
        "startDate",
        "endDate",
    ]
    base_cols = [column for column in base_cols if column in raw.columns]
    cache = raw[base_cols].copy()

    feature_frame = build_market_feature_frame(raw)
    cache = cache.merge(feature_frame, on="market_id", how="left")

    domain = domain_features.copy() if domain_features is not None else pd.DataFrame(columns=["market_id"])
    if not domain.empty:
        domain["market_id"] = domain["market_id"].astype(str)
        cache = cache.merge(domain, on="market_id", how="left", suffixes=("", "_domain"))

    if "category" not in cache.columns:
        if "category_domain" in cache.columns:
            cache.rename(columns={"category_domain": "category"}, inplace=True)
        else:
            cache["category"] = "UNKNOWN"

    if "market_type" not in cache.columns:
        cache["market_type"] = cache.get("marketType", "UNKNOWN")

    if "domain" not in cache.columns:
        cache["domain"] = "UNKNOWN"

    if "source_host" not in cache.columns:
        cache["source_host"] = cache.get("domain", "UNKNOWN")

    if "outcome_pattern" not in cache.columns:
        cache["outcome_pattern"] = "UNKNOWN"

    numeric_static = ["orderPriceMinTickSize", "rewardsMinSize", "rewardsMaxSpread"]
    for column in numeric_static:
        if column in cache.columns:
            cache[column] = pd.to_numeric(cache[column], errors="coerce").fillna(0.0)

    if "startDate" in cache.columns and "endDate" in cache.columns:
        start = pd.to_datetime(cache["startDate"], utc=True, errors="coerce")
        end = pd.to_datetime(cache["endDate"], utc=True, errors="coerce")
        cache["market_duration_hours"] = (end - start).dt.total_seconds() / 3600.0
        cache["market_duration_hours"] = cache["market_duration_hours"].fillna(0.0)
    else:
        cache["market_duration_hours"] = 0.0

    fill_unknown = [
        "domain",
        "category",
        "market_type",
        "sub_domain",
        "source_host",
        "outcome_pattern",
        "sportsMarketType",
        "marketType",
        "negRisk",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
    ]
    for column in fill_unknown:
        if column not in cache.columns:
            cache[column] = "UNKNOWN"
        cache[column] = cache[column].fillna("UNKNOWN")

    cache = cache.drop_duplicates(subset=["market_id"]).reset_index(drop=True)
    return cache


def preprocess_features(
    df: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
) -> pd.DataFrame:
    if df.empty:
        return df

    out_df = df.copy()
    if "market_id" not in out_df.columns:
        raise ValueError("Input feature frame must include market_id.")

    out_df["market_id"] = out_df["market_id"].astype(str)
    if "category" in out_df.columns:
        out_df = out_df.rename(columns={"category": "snapshot_category"})

    if not market_feature_cache.empty:
        meta = market_feature_cache.copy()
        meta["market_id"] = meta["market_id"].astype(str)
        out_df = out_df.merge(meta, on="market_id", how="left", suffixes=("", "_market"))

    if "snapshot_category" in out_df.columns:
        if "category" in out_df.columns:
            out_df["category"] = out_df["category"].fillna(out_df["snapshot_category"])
        else:
            out_df["category"] = out_df["snapshot_category"]
        out_df = out_df.drop(columns=["snapshot_category"])

    if "price" in out_df.columns:
        out_df["price"] = pd.to_numeric(out_df["price"], errors="coerce").fillna(0.0)
    if "horizon_hours" in out_df.columns:
        out_df["horizon_hours"] = pd.to_numeric(out_df["horizon_hours"], errors="coerce").fillna(0.0)

    out_df["log_horizon"] = np.log1p(out_df["horizon_hours"].clip(lower=0))

    if "q_smooth" in out_df.columns:
        out_df["q_smooth"] = pd.to_numeric(out_df["q_smooth"], errors="coerce").fillna(0.5)
    else:
        out_df["q_smooth"] = 0.5

    if "rule_score" in out_df.columns:
        out_df["rule_score"] = pd.to_numeric(out_df["rule_score"], errors="coerce").fillna(0.0)
    else:
        out_df["rule_score"] = 0.0

    categorical_cols = [
        "domain",
        "category",
        "market_type",
        "sub_domain",
        "source_host",
        "outcome_pattern",
        "sportsMarketType",
        "marketType",
        "negRisk",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
    ]
    for column in categorical_cols:
        if column not in out_df.columns:
            out_df[column] = "UNKNOWN"
        out_df[column] = out_df[column].astype("string").fillna("UNKNOWN").astype("category")

    if "y" in out_df.columns:
        out_df["y"] = pd.to_numeric(out_df["y"], errors="coerce").fillna(0).astype(int)

    return out_df


def compute_temporal_split(df: pd.DataFrame, date_col: str = "resolve_time") -> tuple[pd.Timestamp, pd.Timestamp]:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")

    reference_end = pd.to_datetime(df[date_col], utc=True, errors="coerce").max()
    if pd.isna(reference_end):
        raise ValueError(f"Unable to infer rolling split boundaries from '{date_col}'.")

    _, train_end, valid_start = config.compute_split_boundaries(reference_end.to_pydatetime())
    return pd.Timestamp(train_end), pd.Timestamp(valid_start)
