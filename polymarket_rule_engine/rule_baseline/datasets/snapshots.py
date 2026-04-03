from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from rule_baseline.datasets.splits import TemporalSplit, assign_dataset_split, compute_artifact_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged

DEFAULT_PRICE_MIN = 0.01
DEFAULT_PRICE_MAX = 0.99
DEFAULT_PRICE_BIN_STEP = 0.03
PRICE_BIN_STEP_OPTIONS = (0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1)
MIN_MARKETS_PER_PRICE_BIN = 15
RULE_BIN_GROUP_COLUMNS = ["domain", "category", "market_type"]


def _stage_summary(name: str, df: pd.DataFrame) -> dict:
    unique_markets = 0
    if "market_id" in df.columns and not df.empty:
        unique_markets = int(df["market_id"].astype(str).nunique())
    return {
        "stage": name,
        "snapshot_rows": int(len(df)),
        "unique_markets": unique_markets,
    }


def _raw_market_summary(name: str, df: pd.DataFrame, reason_column: str | None = None) -> dict:
    if df.empty:
        return {
            "stage": name,
            "row_count": 0,
            "unique_markets": 0,
            "reason_counts": {},
        }

    market_column = "market_id" if "market_id" in df.columns else "id" if "id" in df.columns else None
    unique_markets = int(df[market_column].astype(str).nunique()) if market_column else int(len(df))
    reason_counts: dict[str, int] = {}
    if reason_column and reason_column in df.columns:
        reason_counts = df[reason_column].fillna("UNKNOWN").astype(str).value_counts().to_dict()

    return {
        "stage": name,
        "row_count": int(len(df)),
        "unique_markets": unique_markets,
        "reason_counts": {str(key): int(value) for key, value in reason_counts.items()},
    }


def load_snapshots(path: Path | None = None) -> pd.DataFrame:
    target = path or config.SNAPSHOTS_PATH
    if not target.exists():
        raise FileNotFoundError(f"Snapshots file not found at {target}")

    print(f"[INFO] Loading snapshots from {target}...")
    snapshots = pd.read_csv(target, low_memory=False)

    if "scheduled_end" in snapshots.columns:
        snapshots["scheduled_end"] = pd.to_datetime(snapshots["scheduled_end"], utc=True, format="mixed", errors="coerce")
    if "closedTime" not in snapshots.columns:
        raise ValueError(f"Snapshots file at {target} is missing required 'closedTime' column.")
    snapshots["closedTime"] = pd.to_datetime(snapshots["closedTime"], utc=True, format="mixed", errors="coerce")

    snapshots["snapshot_time"] = snapshots["closedTime"] - pd.to_timedelta(snapshots["horizon_hours"], unit="h")
    snapshots["snapshot_target_ts"] = (snapshots["snapshot_time"].astype("int64") // 10**9).astype("int64")
    snapshots["snapshot_date"] = snapshots["snapshot_time"].dt.date
    snapshots["market_id"] = snapshots["market_id"].astype(str)
    snapshots["y"] = snapshots["y"].astype(int)
    return snapshots


def load_raw_markets(path: Path | None = None, rebuild: bool = False) -> pd.DataFrame:
    target = path or config.RAW_MERGED_PATH
    if rebuild or not target.exists():
        rebuild_canonical_merged()

    if not target.exists():
        print(f"[WARN] Merged raw markets not found at {target}.")
        return pd.DataFrame(columns=["market_id"])

    print(f"[INFO] Loading merged raw markets from {target}...")
    raw_markets = pd.read_csv(target, low_memory=False)
    if "id" not in raw_markets.columns:
        raise ValueError(f"Merged raw markets at {target} are missing the 'id' column.")
    raw_markets["id"] = raw_markets["id"].astype(str)
    raw_markets["market_id"] = raw_markets["id"]
    return raw_markets


def _series_or_default(df: pd.DataFrame, column: str, default_value) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series(default_value, index=df.index)


def _apply_market_annotations(snapshots: pd.DataFrame, market_annotations: pd.DataFrame) -> pd.DataFrame:
    out = snapshots.copy()
    if market_annotations.empty:
        for column in ["domain", "category", "market_type"]:
            if column not in out.columns:
                out[column] = "UNKNOWN"
        return out

    annotation_columns = [
        "market_id",
        "domain",
        "domain_parsed",
        "sub_domain",
        "source_url",
        "category",
        "category_raw",
        "category_parsed",
        "category_override_flag",
        "market_type",
        "outcome_pattern",
    ]
    available_columns = [column for column in annotation_columns if column in market_annotations.columns]
    out = out.merge(
        market_annotations[available_columns],
        on="market_id",
        how="left",
        suffixes=("", "_annotation"),
    )

    for column in ["domain", "category", "market_type", "sub_domain", "source_url", "source_host", "outcome_pattern"]:
        annotation_column = f"{column}_annotation"
        if annotation_column in out.columns:
            if column in out.columns:
                out[column] = out[annotation_column].fillna(out[column])
            else:
                out[column] = out[annotation_column]
            out = out.drop(columns=[annotation_column])

    for column in ["domain", "category", "market_type"]:
        out[column] = out.get(column, "UNKNOWN").fillna("UNKNOWN").astype(str)
    return out


def _apply_raw_market_context(snapshots: pd.DataFrame, raw_markets: pd.DataFrame) -> pd.DataFrame:
    out = snapshots.copy()
    if raw_markets.empty:
        return out

    raw_context_columns = [
        column
        for column in [
            "market_id",
            "question",
            "description",
            "startDate",
            "endDate",
            "closedTime",
            "groupItemTitle",
            "gameId",
            "marketMakerAddress",
        ]
        if column in raw_markets.columns
    ]
    if not raw_context_columns:
        return out

    out = out.merge(raw_markets[raw_context_columns], on="market_id", how="left", suffixes=("", "_market"))
    for column in ["question", "description", "startDate", "endDate", "closedTime", "groupItemTitle", "gameId", "marketMakerAddress"]:
        market_column = f"{column}_market"
        if market_column in out.columns:
            if column in out.columns:
                out[column] = out[column].fillna(out[market_column])
            else:
                out[column] = out[market_column]
            out = out.drop(columns=[market_column])
    return out


def add_term_structure_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "market_id" not in df.columns:
        return df

    observed_horizons = df["horizon_hours"].dropna().unique() if "horizon_hours" in df.columns else []
    pivot = (
        df.pivot_table(index="market_id", columns="horizon_hours", values="price", aggfunc="first")
        .rename(columns={horizon: f"p_{int(horizon)}h" for horizon in config.HORIZONS if horizon in observed_horizons})
        .reset_index()
    )
    out = df.merge(pivot, on="market_id", how="left")

    price_columns = [f"p_{horizon}h" for horizon in config.HORIZONS if f"p_{horizon}h" in out.columns]
    current_horizon = pd.to_numeric(out.get("horizon_hours"), errors="coerce")
    # Only horizons at or before the current decision time are observable.
    for horizon in config.HORIZONS:
        column = f"p_{horizon}h"
        if column in out.columns:
            out.loc[current_horizon.notna() & (current_horizon > horizon), column] = np.nan

    for left, right in [(1, 2), (2, 4), (4, 12), (12, 24)]:
        left_column = f"p_{left}h"
        right_column = f"p_{right}h"
        if left_column in out.columns and right_column in out.columns:
            out[f"delta_p_{left}_{right}"] = out[left_column] - out[right_column]

    if "p_1h" in out.columns and "p_24h" in out.columns:
        out["term_structure_slope"] = out["p_1h"] - out["p_24h"]
    else:
        out["term_structure_slope"] = np.nan

    out["path_price_mean"] = out[price_columns].mean(axis=1) if price_columns else np.nan
    out["path_price_std"] = out[price_columns].std(axis=1) if price_columns else np.nan
    out["path_price_min"] = out[price_columns].min(axis=1) if price_columns else np.nan
    out["path_price_max"] = out[price_columns].max(axis=1) if price_columns else np.nan
    out["path_price_range"] = out["path_price_max"] - out["path_price_min"]

    if {"p_1h", "p_2h", "p_12h", "p_24h"}.issubset(out.columns):
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


def _filter_tradable_price_range(
    df: pd.DataFrame,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
) -> pd.DataFrame:
    return df[df["price"].ge(min_price) & df["price"].le(max_price)].copy()


def _select_price_bin_step(
    market_count: int,
    min_markets_per_bin: int = MIN_MARKETS_PER_PRICE_BIN,
    step_options: tuple[float, ...] = PRICE_BIN_STEP_OPTIONS,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
) -> float:
    tradable_range = max_price - min_price
    if market_count <= 0 or tradable_range <= 0:
        return step_options[-1]

    for step in step_options:
        bin_count = max(1, int(np.ceil(tradable_range / step)))
        if market_count / bin_count >= min_markets_per_bin:
            return step
    return step_options[-1]


def _build_price_bin_edges(
    price_bin_step: float,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
) -> np.ndarray:
    edges = np.arange(min_price, max_price, price_bin_step, dtype=float)
    if len(edges) == 0 or not np.isclose(edges[0], min_price):
        edges = np.insert(edges, 0, min_price)
    if edges[-1] < max_price:
        edges = np.append(edges, max_price)
    # Keep right-open bins while still admitting the upper boundary into the final bin.
    edges[-1] = np.nextafter(edges[-1], np.inf)
    return edges


def build_snapshot_base(
    snapshots: pd.DataFrame,
    raw_markets: pd.DataFrame | None = None,
    market_annotations: pd.DataFrame | None = None,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
) -> pd.DataFrame:
    out = snapshots.copy()
    out["market_id"] = out["market_id"].astype(str)

    if market_annotations is not None:
        annotations = market_annotations.copy()
        annotations["market_id"] = annotations["market_id"].astype(str)
        out = _apply_market_annotations(out, annotations)

    if raw_markets is not None:
        raw = raw_markets.copy()
        raw["market_id"] = raw["market_id"].astype(str)
        out = _apply_raw_market_context(out, raw)

    for column in ["startDate", "endDate", "closedTime"]:
        if column in out.columns:
            out[column] = pd.to_datetime(out[column], utc=True, errors="coerce")
    if "closedTime" not in out.columns:
        raise ValueError("Snapshot base is missing required 'closedTime' column.")

    out["domain"] = out.get("domain", "UNKNOWN").fillna("UNKNOWN").astype(str)
    out["category"] = out.get("category", "UNKNOWN").fillna("UNKNOWN").astype(str)
    out["market_type"] = out.get("market_type", "UNKNOWN").fillna("UNKNOWN").astype(str)
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["horizon_hours"] = pd.to_numeric(out["horizon_hours"], errors="coerce")
    out["delta_hours"] = pd.to_numeric(out["delta_hours"], errors="coerce")
    out = _filter_tradable_price_range(out, min_price=min_price, max_price=max_price)
    out["selected_quote_offset_sec"] = pd.to_numeric(
        _series_or_default(out, "selected_quote_offset_sec", 0.0),
        errors="coerce",
    ).fillna(0.0)
    out["selected_quote_points_in_window"] = pd.to_numeric(
        _series_or_default(out, "selected_quote_points_in_window", 0.0),
        errors="coerce",
    ).fillna(0.0)
    out["stale_quote_flag"] = _series_or_default(out, "stale_quote_flag", False).fillna(False).astype(bool)
    out["selected_quote_side"] = _series_or_default(out, "selected_quote_side", "UNKNOWN").fillna("UNKNOWN").astype(str)

    out["market_duration_hours"] = (out["closedTime"] - out["startDate"]).dt.total_seconds() / 3600.0
    out["market_duration_hours"] = out["market_duration_hours"].fillna(np.nan)
    out["duration_is_negative_flag"] = out["market_duration_hours"] < 0
    out["duration_below_min_horizon_flag"] = out["market_duration_hours"] < min(config.HORIZONS)
    out["delta_hours_exceeded_flag"] = out["delta_hours"] > config.MAX_ALLOWED_RESOLVE_DELTA_HOURS
    out["delta_hours_bucket"] = out["delta_hours"].fillna(999.0).round(2).clip(lower=0.0, upper=999.0)
    out["price_in_range_flag"] = out["price"].between(min_price, max_price, inclusive="both")
    out["quality_pass"] = out["price_in_range_flag"].fillna(False)
    out["snapshot_quality_score"] = (
        1.0
        - out["selected_quote_offset_sec"].clip(lower=0.0, upper=float(config.SNAP_WINDOW_SEC)) / max(float(config.SNAP_WINDOW_SEC), 1.0)
    ) * (1.0 + np.log1p(out["selected_quote_points_in_window"].clip(lower=0.0)))

    out["e_sample"] = out["y"] - out["price"]
    out["r_std"] = out["e_sample"] / np.sqrt(out["price"] * (1.0 - out["price"]))
    return add_term_structure_features(out)


def load_research_snapshots(
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
    max_rows: int | None = None,
    recent_days: int | None = None,
) -> pd.DataFrame:
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)

    if max_rows is not None:
        snapshots = snapshots.sort_values("closedTime").tail(max_rows).copy()

    if recent_days is not None and recent_days > 0:
        cutoff = snapshots["closedTime"].max() - pd.Timedelta(days=recent_days)
        snapshots = snapshots[snapshots["closedTime"] >= cutoff].copy()

    return build_snapshot_base(
        snapshots=snapshots,
        raw_markets=raw_markets,
        market_annotations=market_annotations,
        min_price=min_price,
        max_price=max_price,
    )


def build_rule_bins(
    df: pd.DataFrame,
    price_bin_step: float = DEFAULT_PRICE_BIN_STEP,
    bin_source_df: pd.DataFrame | None = None,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
) -> pd.DataFrame:
    out = df.copy()
    reference = bin_source_df.copy() if bin_source_df is not None else out.copy()
    horizon_edges = [0] + sorted(config.HORIZONS) + [1000]
    horizon_labels = [f"<{horizon_edges[1]}h"]
    for index in range(1, len(horizon_edges) - 2):
        horizon_labels.append(f"{horizon_edges[index]}-{horizon_edges[index + 1]}h")
    horizon_labels.append(f">{horizon_edges[-2]}h")

    out["price_bin"] = pd.Series(pd.NA, index=out.index, dtype="object")
    group_columns = [column for column in RULE_BIN_GROUP_COLUMNS if column in out.columns]
    grouped = out.groupby(group_columns, observed=False, sort=False) if group_columns else [(None, out)]
    for _, group_df in grouped:
        if group_columns:
            reference_mask = pd.Series(True, index=reference.index)
            for column in group_columns:
                reference_mask &= reference[column].astype(str) == str(group_df.iloc[0][column])
            reference_group_df = reference[reference_mask]
        else:
            reference_group_df = reference
        if reference_group_df.empty:
            continue
        market_count = (
            int(reference_group_df["market_id"].nunique())
            if "market_id" in reference_group_df.columns
            else int(len(reference_group_df))
        )
        step = (
            _select_price_bin_step(market_count, min_price=min_price, max_price=max_price)
            if price_bin_step == DEFAULT_PRICE_BIN_STEP
            else price_bin_step
        )
        price_bins = _build_price_bin_edges(step, min_price=min_price, max_price=max_price)
        price_labels = [f"{left:.2f}-{right:.2f}" for left, right in zip(price_bins[:-1], price_bins[1:])]
        out.loc[group_df.index, "price_bin"] = pd.cut(
            group_df["price"],
            bins=price_bins,
            labels=price_labels,
            right=False,
        ).astype("object")
    out["horizon_bin"] = pd.cut(out["horizon_hours"], bins=horizon_edges, labels=horizon_labels, right=False)
    return out.dropna(subset=["price_bin", "horizon_bin"]).copy()


def prepare_rule_training_frame(
    artifact_mode: str = "offline",
    max_rows: int | None = None,
    recent_days: int | None = None,
    split_reference_end: str | None = None,
    history_start_override: str | None = None,
    min_price: float = DEFAULT_PRICE_MIN,
    max_price: float = DEFAULT_PRICE_MAX,
    price_bin_step: float = DEFAULT_PRICE_BIN_STEP,
) -> tuple[pd.DataFrame, TemporalSplit, dict]:
    snapshots_raw = load_snapshots(config.SNAPSHOTS_PATH)
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    raw_quarantine = pd.DataFrame()
    if config.RAW_MARKET_QUARANTINE_PATH.exists():
        raw_quarantine = pd.read_csv(config.RAW_MARKET_QUARANTINE_PATH, low_memory=False)

    if max_rows is not None:
        snapshots_raw = snapshots_raw.sort_values("closedTime").tail(max_rows).copy()

    if recent_days is not None and recent_days > 0:
        cutoff = snapshots_raw["closedTime"].max() - pd.Timedelta(days=recent_days)
        snapshots_raw = snapshots_raw[snapshots_raw["closedTime"] >= cutoff].copy()

    snapshots = build_snapshot_base(
        snapshots=snapshots_raw,
        raw_markets=raw_markets,
        market_annotations=market_annotations,
        min_price=min_price,
        max_price=max_price,
    )
    raw_seen_rows = int(len(raw_markets) + len(raw_quarantine))
    raw_seen_unique_markets = int(
        pd.concat(
            [
                raw_markets["market_id"].astype(str) if "market_id" in raw_markets.columns else pd.Series(dtype="object"),
                raw_quarantine["market_id"].astype(str) if "market_id" in raw_quarantine.columns else pd.Series(dtype="object"),
            ],
            ignore_index=True,
        ).nunique()
    )

    funnel_summary = {
        "raw_market_funnel": [
            {
                "stage": "raw_markets_seen",
                "row_count": raw_seen_rows,
                "unique_markets": raw_seen_unique_markets,
                "reason_counts": {},
            },
            _raw_market_summary("after_raw_filter", raw_markets),
            _raw_market_summary("raw_markets_quarantine", raw_quarantine, reason_column="reject_reason"),
        ],
        "snapshot_funnel": [],
    }
    funnel_summary["snapshot_funnel"].append(_stage_summary("snapshots_loaded", snapshots_raw))
    funnel_summary["snapshot_funnel"].append(_stage_summary("after_price_range", snapshots))
    print(f"[INFO] Snapshot rows before quality_pass filter: {len(snapshots)}")
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    print(f"[INFO] Snapshot rows after quality_pass filter: {len(snapshots)}")
    funnel_summary["snapshot_funnel"].append(_stage_summary("after_quality_pass", snapshots))
    split = compute_artifact_split(
        snapshots,
        artifact_mode=artifact_mode,
        date_col="closedTime",
        reference_end=split_reference_end,
        history_start_override=history_start_override,
    )
    snapshots = assign_dataset_split(snapshots, split, date_col="closedTime")
    allowed_splits = ["train", "valid", "test"] if artifact_mode == "offline" else ["train", "valid"]
    snapshots = snapshots[snapshots["dataset_split"].isin(allowed_splits)].copy()
    funnel_summary["snapshot_funnel"].append(_stage_summary("after_dataset_split", snapshots))
    bin_source = snapshots.copy() if artifact_mode == "online" else snapshots[snapshots["dataset_split"].isin(["train", "valid"])].copy()
    snapshots = build_rule_bins(
        snapshots,
        price_bin_step=price_bin_step,
        bin_source_df=bin_source,
        min_price=min_price,
        max_price=max_price,
    )
    funnel_summary["snapshot_funnel"].append(
        _stage_summary("after_rule_bins_with_train_valid_reference", snapshots)
    )
    return snapshots, split, funnel_summary


def apply_earliest_market_dedup(
    df: pd.DataFrame,
    score_column: str,
    market_column: str = "market_id",
    time_column: str = "snapshot_time",
) -> pd.DataFrame:
    if df.empty:
        return df

    ordered = df.sort_values([market_column, time_column, score_column], ascending=[True, True, False])
    return ordered.drop_duplicates(subset=[market_column], keep="first").reset_index(drop=True)
