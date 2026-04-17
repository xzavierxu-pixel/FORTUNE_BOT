from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd

from rule_baseline.utils import config
from rule_baseline.workflow.pipeline_config import SplitConfig


@dataclass(frozen=True)
class TemporalSplit:
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    valid_start: pd.Timestamp | None
    valid_end: pd.Timestamp | None
    test_start: pd.Timestamp | None = None
    test_end: pd.Timestamp | None = None

    def to_dict(self) -> dict[str, str | None]:
        out: dict[str, str | None] = {}
        for key, value in asdict(self).items():
            out[key] = value.isoformat() if value is not None else None
        return out


def compute_temporal_split(
    df: pd.DataFrame,
    date_col: str = "closedTime",
    reference_end: str | pd.Timestamp | None = None,
    history_start_override: str | pd.Timestamp | None = None,
) -> TemporalSplit:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")

    inferred_reference_end = pd.to_datetime(df[date_col], utc=True, errors="coerce").max()
    effective_reference_end = (
        pd.Timestamp(config.parse_utc_datetime(reference_end))
        if reference_end is not None
        else inferred_reference_end
    )
    if pd.isna(effective_reference_end):
        raise ValueError(f"Unable to infer split boundaries from '{date_col}'.")

    split_values = config.compute_three_way_split_boundaries(
        effective_reference_end.to_pydatetime(),
        history_start_override=history_start_override,
    )
    return TemporalSplit(*(pd.Timestamp(value) for value in split_values))


def compute_train_valid_split(
    df: pd.DataFrame,
    date_col: str = "closedTime",
    validation_days: int = config.ONLINE_VALIDATION_DAYS,
    reference_end: str | pd.Timestamp | None = None,
    history_start_override: str | pd.Timestamp | None = None,
) -> TemporalSplit:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")

    inferred_reference_end = pd.to_datetime(df[date_col], utc=True, errors="coerce").max()
    effective_reference_end = (
        pd.Timestamp(config.parse_utc_datetime(reference_end))
        if reference_end is not None
        else inferred_reference_end
    )
    if pd.isna(effective_reference_end):
        raise ValueError(f"Unable to infer split boundaries from '{date_col}'.")

    train_start, train_end, valid_start = config.compute_split_boundaries(
        effective_reference_end.to_pydatetime(),
        validation_days=validation_days,
        history_start_override=history_start_override,
    )
    return TemporalSplit(
        train_start=pd.Timestamp(train_start),
        train_end=pd.Timestamp(train_end),
        valid_start=pd.Timestamp(valid_start),
        valid_end=pd.Timestamp(effective_reference_end),
    )


def compute_artifact_split(
    df: pd.DataFrame,
    artifact_mode: str,
    date_col: str = "closedTime",
    online_validation_days: int = config.ONLINE_VALIDATION_DAYS,
    reference_end: str | pd.Timestamp | None = None,
    history_start_override: str | pd.Timestamp | None = None,
) -> TemporalSplit:
    if artifact_mode == "offline":
        return compute_train_valid_split(
            df,
            date_col=date_col,
            validation_days=config.VALIDATION_DAYS,
            reference_end=reference_end,
            history_start_override=history_start_override,
        )
    if artifact_mode == "online":
        return compute_train_valid_split(
            df,
            date_col=date_col,
            validation_days=online_validation_days,
            reference_end=reference_end,
            history_start_override=history_start_override,
        )
    raise ValueError(f"Unsupported artifact mode: {artifact_mode}")


def temporal_split_from_config(split_config: SplitConfig) -> TemporalSplit:
    return TemporalSplit(
        train_start=pd.Timestamp(split_config.train_start),
        train_end=pd.Timestamp(split_config.train_end),
        valid_start=pd.Timestamp(split_config.valid_start) if split_config.valid_start is not None else None,
        valid_end=pd.Timestamp(split_config.valid_end) if split_config.valid_end is not None else None,
        test_start=pd.Timestamp(split_config.test_start) if split_config.test_start is not None else None,
        test_end=pd.Timestamp(split_config.test_end) if split_config.test_end is not None else None,
    )


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
    if split.valid_start is not None and split.valid_end is not None:
        out.loc[(timestamps >= split.valid_start) & (timestamps <= split.valid_end), output_col] = "valid"
    if split.test_start is not None and split.test_end is not None:
        out.loc[(timestamps >= split.test_start) & (timestamps <= split.test_end), output_col] = "test"
    return out


def assign_configured_dataset_split(
    df: pd.DataFrame,
    split_config: SplitConfig,
    *,
    date_col: str = "closedTime",
    output_col: str = "dataset_split",
) -> pd.DataFrame:
    return assign_dataset_split(
        df,
        temporal_split_from_config(split_config),
        date_col=date_col,
        output_col=output_col,
    )


def select_preferred_split(
    df: pd.DataFrame,
    preferred_splits: tuple[str, ...] = ("test", "valid", "train"),
    split_col: str = "dataset_split",
) -> tuple[str, pd.DataFrame]:
    if split_col not in df.columns:
        raise ValueError(f"Dataframe is missing required split column '{split_col}'.")

    for split_name in preferred_splits:
        candidate = df[df[split_col] == split_name].copy()
        if not candidate.empty:
            return split_name, candidate
    return "empty", df.iloc[0:0].copy()


def build_walk_forward_splits(
    df: pd.DataFrame,
    date_col: str = "closedTime",
    n_windows: int = 3,
    validation_days: int = config.VALIDATION_DAYS,
    test_days: int = config.TEST_DAYS,
    step_days: int | None = None,
    min_train_days: int = 90,
    reference_end: str | pd.Timestamp | None = None,
    history_start_override: str | pd.Timestamp | None = None,
) -> list[TemporalSplit]:
    if date_col not in df.columns:
        raise ValueError(f"Dataframe is missing required date column '{date_col}'.")
    if n_windows <= 0:
        return []

    timestamps = pd.to_datetime(df[date_col], utc=True, errors="coerce")
    inferred_reference_end = timestamps.max()
    reference_end = (
        pd.Timestamp(config.parse_utc_datetime(reference_end))
        if reference_end is not None
        else inferred_reference_end
    )
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
        train_start = pd.Timestamp(config.resolve_history_start(history_start_override))
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
