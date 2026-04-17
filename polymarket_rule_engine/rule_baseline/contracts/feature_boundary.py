from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


_METADATA_COLUMNS = frozenset(
    {
        "market_id",
        "snapshot_time",
        "snapshot_date",
        "scheduled_end",
        "closedTime",
        "snapshot_target_ts",
        "selected_quote_ts",
        "question",
        "description",
        "source_url",
        "source_host",
        "batch_id",
        "batch_fetched_at",
        "batch_window_start",
        "batch_window_end",
        "price_bin",
        "horizon_bin",
        "r_std",
        "e_sample",
        "delta_hours",
        "delta_hours_bucket",
        "domain_market",
        "market_type_market",
        "domain_domain",
        "market_type_domain",
        "sub_domain",
        "outcome_pattern",
        "primary_token_id",
        "secondary_token_id",
        "primary_outcome",
        "secondary_outcome",
        "selected_quote_side",
        "groupItemTitle",
        "gameId",
        "marketMakerAddress",
        "startDate",
        "endDate",
        "leaf_id",
        "token_0_id",
        "token_1_id",
        "selected_reference_token_id",
        "outcome_0_label",
        "outcome_1_label",
        "selected_reference_outcome_label",
        "selected_reference_side_index",
        "winning_outcome_index",
        "winning_outcome_label",
        "source_url_market",
        "sub_domain_market",
        "outcome_pattern_market",
        "groupItemTitle_market",
        "gameId_market",
        "marketMakerAddress_market",
        "domain_parsed_market",
        "category_raw_market",
        "category_parsed_market",
        "startDate_market",
        "endDate_market",
        "closedTime_market",
        "description_market",
        "question_market",
        "source_host_market",
        "domain_category_key",
        "domain_market_type_key",
        "category_market_type_key",
    }
)

_MONITORING_COLUMNS = frozenset(
    {
        "group_match_found",
        "fine_match_found",
        "used_group_fallback_only",
        "group_decision",
        "stale_quote_flag",
        "category_override_flag",
        "quality_pass",
        "delta_hours_exceeded_flag",
    }
)

_CONTROL_COLUMNS = frozenset(
    {
        "dataset_split",
        "y",
        "trade_value_true",
        "expected_pnl_target",
        "expected_roi_target",
        "residual_q_target",
        "price_in_range_flag",
        "publish_split",
    }
)

_EXCLUDED_MODEL_PREFIXES = (
    "rule_score",
    "group_feature_global_expanding_",
    "group_feature_global_recent_90days_",
)

_EXCLUDED_MODEL_CONTAINS = (
    "_rule_score",
    "rule_score_",
)

_EXCLUDED_MODEL_COLUMNS = frozenset(
    {
        "rule_price_width",
        "group_market_share_global",
        "group_snapshot_share_global",
        "group_logloss_gap_q25",
        "group_brier_gap_q25",
        "group_quality_pass_q25",
        "group_quality_fail_q25",
        "group_share_x_logloss_gap",
        "group_share_x_brier_gap",
        "group_feature_group_default_rule_price_width",
        "group_feature_group_default_direction",
        "group_feature_fine_match_found_default",
        "group_feature_group_match_found_default",
    }
)


@dataclass(frozen=True)
class FeatureRoleClassification:
    model_feature_columns: list[str]
    metadata_columns: list[str]
    monitoring_columns: list[str]
    control_columns: list[str]


def _is_excluded_model_column(column: str) -> bool:
    if column in _EXCLUDED_MODEL_COLUMNS:
        return True
    if any(column.startswith(prefix) for prefix in _EXCLUDED_MODEL_PREFIXES):
        return True
    return any(token in column for token in _EXCLUDED_MODEL_CONTAINS)


def classify_feature_columns(columns: list[str] | tuple[str, ...]) -> FeatureRoleClassification:
    model_columns: list[str] = []
    metadata_columns: list[str] = []
    monitoring_columns: list[str] = []
    control_columns: list[str] = []

    for column in columns:
        if column in _CONTROL_COLUMNS:
            control_columns.append(column)
        elif column in _MONITORING_COLUMNS:
            monitoring_columns.append(column)
        elif column in _METADATA_COLUMNS:
            metadata_columns.append(column)
        elif _is_excluded_model_column(column):
            metadata_columns.append(column)
        else:
            model_columns.append(column)

    return FeatureRoleClassification(
        model_feature_columns=model_columns,
        metadata_columns=metadata_columns,
        monitoring_columns=monitoring_columns,
        control_columns=control_columns,
    )


def _is_binary_series(series: pd.Series) -> bool:
    normalized = pd.to_numeric(series, errors="coerce").dropna()
    if normalized.empty:
        return False
    unique_values = set(normalized.astype(float).unique().tolist())
    return unique_values.issubset({0.0, 1.0})


def apply_model_feature_gates(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str] | tuple[str, ...],
    train_mask: pd.Series,
    min_binary_prevalence: float = 0.01,
    max_binary_prevalence: float = 0.99,
) -> tuple[list[str], dict[str, Any]]:
    train_frame = frame.loc[train_mask, list(feature_columns)].copy()
    kept: list[str] = []
    dropped_zero_variance: list[str] = []
    dropped_binary_prevalence: list[str] = []
    binary_prevalence: dict[str, float] = {}
    cardinality_warnings: dict[str, int] = {}

    for column in feature_columns:
        if column not in train_frame.columns:
            continue
        series = train_frame[column]
        non_null = series.dropna()
        nunique = int(non_null.nunique(dropna=True))
        if nunique <= 1:
            dropped_zero_variance.append(column)
            continue
        if _is_binary_series(series):
            prevalence = float(pd.to_numeric(series, errors="coerce").fillna(0.0).mean())
            binary_prevalence[column] = prevalence
            if prevalence < min_binary_prevalence or prevalence > max_binary_prevalence:
                dropped_binary_prevalence.append(column)
                continue
        else:
            cardinality_warnings[column] = nunique
        kept.append(column)

    diagnostics = {
        "train_rows": int(len(train_frame)),
        "candidate_model_feature_count": int(len(feature_columns)),
        "kept_model_feature_count": int(len(kept)),
        "dropped_zero_variance": sorted(dropped_zero_variance),
        "dropped_binary_prevalence": sorted(dropped_binary_prevalence),
        "binary_prevalence_by_column": {key: binary_prevalence[key] for key in sorted(binary_prevalence)},
        "cardinality_warning_by_column": {key: cardinality_warnings[key] for key in sorted(cardinality_warnings)},
        "thresholds": {
            "min_binary_prevalence": float(min_binary_prevalence),
            "max_binary_prevalence": float(max_binary_prevalence),
        },
    }
    return kept, diagnostics
