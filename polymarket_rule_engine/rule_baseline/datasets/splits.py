from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from rule_baseline.utils import config


@dataclass(frozen=True)
class TemporalSplit:
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    valid_start: pd.Timestamp
    valid_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp

    def to_dict(self) -> dict[str, str]:
        return {key: value.isoformat() for key, value in asdict(self).items()}


def compute_temporal_split(df: pd.DataFrame, date_col: str = "closedTime") -> TemporalSplit:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")

    reference_end = pd.to_datetime(df[date_col], utc=True, errors="coerce").max()
    if pd.isna(reference_end):
        raise ValueError(f"Unable to infer split boundaries from '{date_col}'.")

    split_values = config.compute_three_way_split_boundaries(reference_end.to_pydatetime())
    return TemporalSplit(*(pd.Timestamp(value) for value in split_values))


def assign_dataset_split(
    df: pd.DataFrame,
    split: TemporalSplit,
    date_col: str = "closedTime",
    output_col: str = "dataset_split",
) -> pd.DataFrame:
    out = df.copy()
    timestamps = pd.to_datetime(out[date_col], utc=True, errors="coerce")
    out[output_col] = "discard"
    out.loc[(timestamps >= split.train_start) & (timestamps <= split.train_end), output_col] = "train"
    out.loc[(timestamps >= split.valid_start) & (timestamps <= split.valid_end), output_col] = "valid"
    out.loc[(timestamps >= split.test_start) & (timestamps <= split.test_end), output_col] = "test"
    return out


def build_walk_forward_splits(
    df: pd.DataFrame,
    date_col: str = "closedTime",
    n_windows: int = 3,
    validation_days: int = config.VALIDATION_DAYS,
    test_days: int = config.TEST_DAYS,
    step_days: int | None = None,
    min_train_days: int = 90,
) -> list[TemporalSplit]:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")
    if n_windows <= 0:
        return []

    timestamps = pd.to_datetime(df[date_col], utc=True, errors="coerce")
    reference_end = timestamps.max()
    reference_start = timestamps.min()
    if pd.isna(reference_end) or pd.isna(reference_start):
        raise ValueError(f"Unable to infer walk-forward boundaries from '{date_col}'.")

    walk_step_days = step_days or test_days
    splits: list[TemporalSplit] = []
    for reverse_index in range(n_windows - 1, -1, -1):
        test_end = reference_end - pd.Timedelta(days=walk_step_days * reverse_index)
        test_start = test_end - pd.Timedelta(days=test_days) + pd.Timedelta(seconds=1)
        valid_end = test_start - pd.Timedelta(seconds=1)
        valid_start = valid_end - pd.Timedelta(days=validation_days) + pd.Timedelta(seconds=1)
        train_start = pd.Timestamp(config.history_start())
        train_end = valid_start - pd.Timedelta(seconds=1)

        if train_start > train_end:
            continue
        if train_end < reference_start + pd.Timedelta(days=min_train_days):
            continue

        splits.append(
            TemporalSplit(
                train_start=train_start,
                train_end=train_end,
                valid_start=valid_start,
                valid_end=valid_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
    return splits


def compute_train_valid_boundary(df: pd.DataFrame, date_col: str = "closedTime") -> tuple[pd.Timestamp, pd.Timestamp]:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")

    reference_end = pd.to_datetime(df[date_col], utc=True, errors="coerce").max()
    if pd.isna(reference_end):
        raise ValueError(f"Unable to infer rolling split boundaries from '{date_col}'.")

    _, train_end, valid_start = config.compute_split_boundaries(reference_end.to_pydatetime())
    return pd.Timestamp(train_end), pd.Timestamp(valid_start)
