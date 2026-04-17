from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


HISTORY_WINDOWS = {
    "expanding": None,
    "recent_90days": pd.Timedelta(days=90),
}
LEVEL_DEFINITIONS = {
    "global": [],
    "domain": ["domain"],
    "category": ["category"],
    "market_type": ["market_type"],
    "domain_x_category": ["domain", "category"],
    "domain_x_market_type": ["domain", "market_type"],
    "category_x_market_type": ["category", "market_type"],
    "full_group": ["domain", "category", "market_type"],
}
HISTORY_ARTIFACT_FILENAMES = {
    level_name: f"history_features_{level_name}.parquet"
    for level_name in LEVEL_DEFINITIONS
}


def build_group_key(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["domain"].astype(str)
        + "|"
        + frame["category"].astype(str)
        + "|"
        + frame["market_type"].astype(str)
    )


def prepare_history_quality_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "closedTime" in out.columns:
        out["closedTime"] = pd.to_datetime(out["closedTime"], utc=True, errors="coerce")
    if "snapshot_time" in out.columns:
        out["snapshot_time"] = pd.to_datetime(out["snapshot_time"], utc=True, errors="coerce")
    out["group_key"] = build_group_key(out)
    y = pd.to_numeric(out["y"], errors="coerce").clip(0.0, 1.0)
    p = pd.to_numeric(out["price"], errors="coerce").clip(1e-6, 1 - 1e-6)
    out["row_logloss"] = -(y * np.log(p) + (1.0 - y) * np.log(1.0 - p))
    out["row_brier"] = np.square(y - p)
    out["row_bias"] = y - p
    out["row_abs_bias"] = np.abs(out["row_bias"])
    out["horizon_hours"] = pd.to_numeric(out["horizon_hours"], errors="coerce")
    return out


def history_sort_columns(frame: pd.DataFrame) -> list[str]:
    return ["closedTime"] if "closedTime" in frame.columns else []


def windowed_history_slice(
    frame: pd.DataFrame,
    *,
    group_column: str,
    window_span: pd.Timedelta | None,
) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    sort_columns = history_sort_columns(frame)
    ordered = frame.sort_values(sort_columns).copy() if sort_columns else frame.copy()
    if window_span is None or not sort_columns:
        return ordered
    time_column = sort_columns[0]
    max_closed_time = ordered.groupby(group_column, observed=True)[time_column].transform("max")
    cutoff = max_closed_time - window_span
    return ordered[ordered[time_column].ge(cutoff)].copy()


def build_level_key(frame: pd.DataFrame, level_name: str, level_columns: list[str]) -> pd.Series:
    if level_name == "global":
        return pd.Series("__GLOBAL__", index=frame.index, dtype="object")
    if level_name == "full_group":
        return build_group_key(frame)
    return frame[level_columns].astype(str).agg("|".join, axis=1)


def summarize_history_features(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    quality = prepare_history_quality_frame(df)
    feature_frames: dict[str, pd.DataFrame] = {}
    for level_name, level_columns in LEVEL_DEFINITIONS.items():
        level_frame = quality.copy()
        level_frame["level_key"] = build_level_key(level_frame, level_name, level_columns)
        stats_by_window: list[pd.DataFrame] = []
        for window_name, window_span in HISTORY_WINDOWS.items():
            window_frame = windowed_history_slice(
                level_frame,
                group_column="level_key",
                window_span=window_span,
            )
            grouped = (
                window_frame.groupby("level_key", observed=True)
                .agg(
                    snapshot_count=("market_id", "size"),
                    market_count=("market_id", "nunique"),
                    bias_mean=("row_bias", "mean"),
                    bias_std=("row_bias", "std"),
                    bias_p50=("row_bias", "median"),
                    abs_bias_mean=("row_abs_bias", "mean"),
                    abs_bias_p25=("row_abs_bias", lambda values: values.quantile(0.25)),
                    abs_bias_p50=("row_abs_bias", "median"),
                    abs_bias_p75=("row_abs_bias", lambda values: values.quantile(0.75)),
                    abs_bias_p90=("row_abs_bias", lambda values: values.quantile(0.90)),
                    brier_mean=("row_brier", "mean"),
                    brier_p25=("row_brier", lambda values: values.quantile(0.25)),
                    brier_p50=("row_brier", "median"),
                    brier_p75=("row_brier", lambda values: values.quantile(0.75)),
                    brier_p90=("row_brier", lambda values: values.quantile(0.90)),
                    brier_std=("row_brier", "std"),
                    logloss_mean=("row_logloss", "mean"),
                    logloss_p25=("row_logloss", lambda values: values.quantile(0.25)),
                    logloss_p50=("row_logloss", "median"),
                    logloss_p75=("row_logloss", lambda values: values.quantile(0.75)),
                    logloss_p90=("row_logloss", lambda values: values.quantile(0.90)),
                    logloss_std=("row_logloss", "std"),
                )
                .reset_index()
            )
            grouped = grouped.rename(
                columns={
                    "snapshot_count": f"{level_name}_{window_name}_snapshot_count",
                    "market_count": f"{level_name}_{window_name}_market_count",
                    "bias_mean": f"{level_name}_{window_name}_bias_mean",
                    "bias_std": f"{level_name}_{window_name}_bias_std",
                    "bias_p50": f"{level_name}_{window_name}_bias_p50",
                    "abs_bias_mean": f"{level_name}_{window_name}_abs_bias_mean",
                    "abs_bias_p25": f"{level_name}_{window_name}_abs_bias_p25",
                    "abs_bias_p50": f"{level_name}_{window_name}_abs_bias_p50",
                    "abs_bias_p75": f"{level_name}_{window_name}_abs_bias_p75",
                    "abs_bias_p90": f"{level_name}_{window_name}_abs_bias_p90",
                    "brier_mean": f"{level_name}_{window_name}_brier_mean",
                    "brier_p25": f"{level_name}_{window_name}_brier_p25",
                    "brier_p50": f"{level_name}_{window_name}_brier_p50",
                    "brier_p75": f"{level_name}_{window_name}_brier_p75",
                    "brier_p90": f"{level_name}_{window_name}_brier_p90",
                    "brier_std": f"{level_name}_{window_name}_brier_std",
                    "logloss_mean": f"{level_name}_{window_name}_logloss_mean",
                    "logloss_p25": f"{level_name}_{window_name}_logloss_p25",
                    "logloss_p50": f"{level_name}_{window_name}_logloss_p50",
                    "logloss_p75": f"{level_name}_{window_name}_logloss_p75",
                    "logloss_p90": f"{level_name}_{window_name}_logloss_p90",
                    "logloss_std": f"{level_name}_{window_name}_logloss_std",
                }
            )
            stats_by_window.append(grouped)
        merged_level = stats_by_window[0]
        for extra in stats_by_window[1:]:
            merged_level = merged_level.merge(extra, on="level_key", how="outer")
        metric_columns = [column for column in merged_level.columns if column != "level_key"]
        for column in metric_columns:
            merged_level[column] = pd.to_numeric(merged_level[column], errors="coerce").fillna(0.0)
        feature_frames[level_name] = merged_level
    return feature_frames


def write_history_feature_artifacts(
    history_feature_frames: dict[str, pd.DataFrame],
    history_artifact_paths: dict[str, Path],
) -> None:
    for level_name, frame in history_feature_frames.items():
        path = history_artifact_paths[level_name]
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)


def load_history_feature_artifacts(history_artifact_paths: dict[str, Path]) -> dict[str, pd.DataFrame]:
    loaded: dict[str, pd.DataFrame] = {}
    for level_name, path in history_artifact_paths.items():
        if not path.exists():
            raise FileNotFoundError(f"History feature artifact not found for {level_name}: {path}")
        loaded[level_name] = pd.read_parquet(path)
    return loaded


def validate_materialized_history_artifacts(history_artifact_paths: dict[str, Path]) -> None:
    missing = [
        f"{level_name}: {path}"
        for level_name, path in history_artifact_paths.items()
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing materialized history artifacts after generation: "
            + ", ".join(missing)
        )
