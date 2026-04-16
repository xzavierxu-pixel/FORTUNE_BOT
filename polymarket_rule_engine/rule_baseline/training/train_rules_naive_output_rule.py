from __future__ import annotations

import argparse
import hashlib
import os
import sys
from math import sqrt

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import (
    RULE_TRAIN_PRICE_BIN_STEP,
    RULE_TRAIN_PRICE_MAX,
    RULE_TRAIN_PRICE_MIN,
    prepare_rule_training_frame,
)
from rule_baseline.history.history_features import (
    LEVEL_DEFINITIONS,
    load_history_feature_artifacts,
    prepare_history_quality_frame,
    summarize_history_features,
    validate_materialized_history_artifacts,
    write_history_feature_artifacts,
)
from rule_baseline.audits.rule_generation_audit import build_rule_generation_audit_payload, write_rule_generation_audit
from rule_baseline.utils import config

MIN_GROUP_UNIQUE_MARKETS = 15
GROUP_THRESHOLD_QUANTILE = 0.25
TRAIN_PRICE_MIN = RULE_TRAIN_PRICE_MIN
TRAIN_PRICE_MAX = RULE_TRAIN_PRICE_MAX
RULE_PRICE_BIN_STEP = RULE_TRAIN_PRICE_BIN_STEP

BASE_GROUP_COLUMNS = ["domain", "category", "market_type"]
RULE_ROW_COLUMNS = ["domain", "category", "market_type", "price_bin", "horizon_hours"]
RULE_SCHEMA_COLUMNS = [
    "group_key",
    "domain",
    "category",
    "market_type",
    "leaf_id",
    "price_min",
    "price_max",
    "h_min",
    "h_max",
    "direction",
    "q_full",
    "p_full",
    "edge_full",
    "edge_std_full",
    "edge_lower_bound_full",
    "rule_score",
    "n_full",
    "horizon_hours",
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
    "group_decision",
]
FINE_SERVING_COLUMNS = [
    "group_key",
    "price_bin",
    "horizon_hours",
    "leaf_id",
    "direction",
    "q_full",
    "p_full",
    "edge_full",
    "edge_std_full",
    "edge_lower_bound_full",
    "rule_score",
    "n_full",
    "rule_price_center",
    "rule_price_width",
    "rule_horizon_center",
    "rule_horizon_width",
    "rule_edge_buffer",
    "rule_confidence_ratio",
    "rule_support_log1p",
    "rule_snapshot_support_log1p",
    "hist_price_x_full_group_expanding_bias",
    "hist_price_x_full_group_recent_90days_bias",
    "hist_price_x_full_group_expanding_logloss",
    "tail_risk_x_price",
    "rule_edge_minus_full_group_expanding_bias",
    "rule_edge_minus_recent_90days_bias",
    "rule_score_minus_full_group_expanding_logloss",
    "rule_score_minus_recent_90days_logloss",
    "rule_edge_over_full_group_logloss",
    "rule_edge_minus_domain_expanding_bias",
    "rule_edge_minus_category_expanding_bias",
    "rule_edge_minus_market_type_expanding_bias",
    "rule_edge_minus_domain_x_category_expanding_bias",
    "rule_edge_minus_domain_x_market_type_expanding_bias",
    "rule_edge_minus_category_x_market_type_expanding_bias",
    "rule_score_minus_domain_expanding_logloss",
    "rule_score_minus_category_expanding_logloss",
    "rule_score_minus_market_type_expanding_logloss",
    "rule_score_minus_domain_x_category_expanding_logloss",
    "rule_score_minus_domain_x_market_type_expanding_logloss",
    "rule_score_minus_category_x_market_type_expanding_logloss",
    "price_x_full_group_expanding_abs_bias_tail_spread",
    "rule_full_group_key_matched_rule_count",
    "rule_full_group_key_max_edge_full",
    "rule_full_group_key_max_edge_lower_bound_full",
    "rule_full_group_key_max_rule_score",
    "rule_full_group_key_mean_edge_full",
    "rule_full_group_key_mean_edge_lower_bound_full",
    "rule_full_group_key_mean_rule_score",
    "rule_full_group_key_sum_n_full",
    "rule_domain_matched_rule_count",
    "rule_domain_max_edge_full",
    "rule_domain_max_edge_lower_bound_full",
    "rule_domain_max_rule_score",
    "rule_domain_mean_edge_full",
    "rule_domain_mean_edge_lower_bound_full",
    "rule_domain_mean_rule_score",
    "rule_domain_sum_n_full",
    "rule_category_matched_rule_count",
    "rule_category_max_edge_full",
    "rule_category_max_edge_lower_bound_full",
    "rule_category_max_rule_score",
    "rule_category_mean_edge_full",
    "rule_category_mean_edge_lower_bound_full",
    "rule_category_mean_rule_score",
    "rule_category_sum_n_full",
    "rule_market_type_matched_rule_count",
    "rule_market_type_max_edge_full",
    "rule_market_type_max_edge_lower_bound_full",
    "rule_market_type_max_rule_score",
    "rule_market_type_mean_edge_full",
    "rule_market_type_mean_edge_lower_bound_full",
    "rule_market_type_mean_rule_score",
    "rule_market_type_sum_n_full",
]
FINE_DEFAULT_AGGREGATIONS = {
    "q_full": "weighted_mean",
    "p_full": "weighted_mean",
    "edge_full": "weighted_mean",
    "edge_std_full": "weighted_mean",
    "edge_lower_bound_full": "weighted_mean",
    "rule_score": "weighted_mean",
    "n_full": "sum",
    "rule_price_center": "mean",
    "rule_price_width": "mean",
    "rule_horizon_center": "mean",
    "rule_horizon_width": "mean",
    "rule_edge_buffer": "weighted_mean",
    "rule_confidence_ratio": "weighted_mean",
    "rule_support_log1p": "mean",
    "rule_snapshot_support_log1p": "mean",
    "hist_price_x_full_group_expanding_bias": "weighted_mean",
    "hist_price_x_full_group_recent_90days_bias": "weighted_mean",
    "hist_price_x_full_group_expanding_logloss": "weighted_mean",
    "tail_risk_x_price": "weighted_mean",
    "rule_edge_minus_full_group_expanding_bias": "weighted_mean",
    "rule_edge_minus_recent_90days_bias": "weighted_mean",
    "rule_score_minus_full_group_expanding_logloss": "weighted_mean",
    "rule_score_minus_recent_90days_logloss": "weighted_mean",
    "rule_edge_over_full_group_logloss": "weighted_mean",
    "rule_edge_minus_domain_expanding_bias": "weighted_mean",
    "rule_edge_minus_category_expanding_bias": "weighted_mean",
    "rule_edge_minus_market_type_expanding_bias": "weighted_mean",
    "rule_edge_minus_domain_x_category_expanding_bias": "weighted_mean",
    "rule_edge_minus_domain_x_market_type_expanding_bias": "weighted_mean",
    "rule_edge_minus_category_x_market_type_expanding_bias": "weighted_mean",
    "rule_score_minus_domain_expanding_logloss": "weighted_mean",
    "rule_score_minus_category_expanding_logloss": "weighted_mean",
    "rule_score_minus_market_type_expanding_logloss": "weighted_mean",
    "rule_score_minus_domain_x_category_expanding_logloss": "weighted_mean",
    "rule_score_minus_domain_x_market_type_expanding_logloss": "weighted_mean",
    "rule_score_minus_category_x_market_type_expanding_logloss": "weighted_mean",
    "price_x_full_group_expanding_abs_bias_tail_spread": "weighted_mean",
    "rule_full_group_key_matched_rule_count": "mean",
    "rule_full_group_key_max_edge_full": "max",
    "rule_full_group_key_max_edge_lower_bound_full": "max",
    "rule_full_group_key_max_rule_score": "max",
    "rule_full_group_key_mean_edge_full": "weighted_mean",
    "rule_full_group_key_mean_edge_lower_bound_full": "weighted_mean",
    "rule_full_group_key_mean_rule_score": "weighted_mean",
    "rule_full_group_key_sum_n_full": "sum",
    "rule_domain_matched_rule_count": "mean",
    "rule_domain_max_edge_full": "max",
    "rule_domain_max_edge_lower_bound_full": "max",
    "rule_domain_max_rule_score": "max",
    "rule_domain_mean_edge_full": "weighted_mean",
    "rule_domain_mean_edge_lower_bound_full": "weighted_mean",
    "rule_domain_mean_rule_score": "weighted_mean",
    "rule_domain_sum_n_full": "sum",
    "rule_category_matched_rule_count": "mean",
    "rule_category_max_edge_full": "max",
    "rule_category_max_edge_lower_bound_full": "max",
    "rule_category_max_rule_score": "max",
    "rule_category_mean_edge_full": "weighted_mean",
    "rule_category_mean_edge_lower_bound_full": "weighted_mean",
    "rule_category_mean_rule_score": "weighted_mean",
    "rule_category_sum_n_full": "sum",
    "rule_market_type_matched_rule_count": "mean",
    "rule_market_type_max_edge_full": "max",
    "rule_market_type_max_edge_lower_bound_full": "max",
    "rule_market_type_max_rule_score": "max",
    "rule_market_type_mean_edge_full": "weighted_mean",
    "rule_market_type_mean_edge_lower_bound_full": "weighted_mean",
    "rule_market_type_mean_rule_score": "weighted_mean",
    "rule_market_type_sum_n_full": "sum",
}


def summarize_rule_selection(
    snapshots_df: pd.DataFrame,
    report_df: pd.DataFrame,
    rules_df: pd.DataFrame,
) -> dict:
    if snapshots_df.empty:
        return {
            "selected_rule_count": int(len(rules_df)),
            "rule_bucket_status_counts": {},
            "selection_status_market_impact": [],
            "after_rule_selection": {
                "snapshot_rows": 0,
                "unique_markets": 0,
            },
        }

    snapshots_keyed = snapshots_df.copy()
    snapshots_keyed["group_key"] = (
        snapshots_keyed["domain"].astype(str)
        + "|"
        + snapshots_keyed["category"].astype(str)
        + "|"
        + snapshots_keyed["market_type"].astype(str)
    )
    snapshot_status_df = snapshots_keyed.merge(
        report_df[["group_key", "selection_status"]],
        on="group_key",
        how="left",
    )
    snapshot_status_df["selection_status"] = snapshot_status_df["selection_status"].fillna("missing_rule_status")

    status_market_impact: list[dict] = []
    status_counts = snapshot_status_df["selection_status"].value_counts().to_dict()
    total_snapshot_rows = int(len(snapshots_df))
    total_unique_markets = int(snapshots_df["market_id"].astype(str).nunique())
    for status, snapshot_rows in status_counts.items():
        status_slice = snapshot_status_df[snapshot_status_df["selection_status"] == status]
        unique_markets = int(status_slice["market_id"].astype(str).nunique())
        status_market_impact.append(
            {
                "selection_status": str(status),
                "snapshot_rows": int(snapshot_rows),
                "unique_markets": unique_markets,
                "snapshot_rows_delta": int(snapshot_rows) - total_snapshot_rows,
                "unique_markets_delta": unique_markets - total_unique_markets,
            }
        )

    selected_snapshots = snapshot_status_df[snapshot_status_df["selection_status"] == "keep"].copy()
    return {
        "selected_rule_count": int(len(rules_df)),
        "rule_bucket_status_counts": {
            str(key): int(value) for key, value in report_df["selection_status"].value_counts().to_dict().items()
        }
        if not report_df.empty
        else {},
        "selection_status_market_impact": sorted(
            status_market_impact,
            key=lambda item: (item["selection_status"] != "keep", -item["snapshot_rows"], item["selection_status"]),
        ),
        "after_rule_selection": {
            "snapshot_rows": int(len(selected_snapshots)),
            "unique_markets": int(selected_snapshots["market_id"].astype(str).nunique()),
        },
    }


def with_stage_deltas(stages: list[dict]) -> list[dict]:
    enriched: list[dict] = []
    previous_snapshots: int | None = None
    previous_markets: int | None = None

    for stage in stages:
        current = dict(stage)
        snapshot_rows = int(current.get("snapshot_rows", 0))
        unique_markets = int(current.get("unique_markets", 0))
        current["snapshot_rows_delta"] = None if previous_snapshots is None else snapshot_rows - previous_snapshots
        current["unique_markets_delta"] = None if previous_markets is None else unique_markets - previous_markets
        enriched.append(current)
        previous_snapshots = snapshot_rows
        previous_markets = unique_markets

    return enriched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train naive rule buckets with a simple raw-frequency estimator."
    )
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional tail-sample row cap for fast debugging.")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help="Optional rolling window for quick debugging without touching the full history.",
    )
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    return parser.parse_args()


def edge_sign(value: float, eps: float = 1e-6) -> int:
    if abs(value) < eps:
        return 0
    return 1 if value > 0 else -1


def parse_bounds(price_label: str, horizon_label: str) -> tuple[float, float, int, int]:
    price_min, price_max = (float(item) for item in price_label.split("-"))

    if horizon_label.startswith("<"):
        horizon_min = 0
        horizon_max = int(horizon_label.replace("<", "").replace("h", ""))
    elif horizon_label.startswith(">"):
        horizon_min = int(horizon_label.replace(">", "").replace("h", ""))
        horizon_max = 1000
    else:
        horizon_min, horizon_max = (int(item) for item in horizon_label.replace("h", "").split("-"))
    return price_min, price_max, horizon_min, horizon_max


def stable_leaf_id(group_key: str, price_label: str, horizon_label: str) -> int:
    digest = hashlib.sha1(f"{group_key}|{price_label}|{horizon_label}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16)


def wilson_interval(successes: float, n_obs: float, z_value: float = 1.96) -> tuple[float, float]:
    if not np.isfinite(n_obs) or n_obs <= 0:
        return 0.0, 1.0
    n_obs = float(n_obs)
    p_hat = float(successes) / n_obs
    denom = 1.0 + z_value**2 / n_obs
    center = (p_hat + z_value**2 / (2.0 * n_obs)) / denom
    radius = z_value * sqrt((p_hat * (1.0 - p_hat) + z_value**2 / (4.0 * n_obs)) / n_obs) / denom
    return max(0.0, center - radius), min(1.0, center + radius)


def direction_adjusted_edge(q_value: float, p_mean: float, direction: int) -> float:
    return float(direction * (q_value - p_mean))


def direction_adjusted_stat(value: float, direction: int) -> float:
    return float(direction * value)


def _build_price_bin_labels(price_min: pd.Series, price_max: pd.Series) -> pd.Series:
    return price_min.map(lambda value: f"{float(value):.2f}") + "-" + price_max.map(lambda value: f"{float(value):.2f}")


def _build_group_key_frame(group_keys: pd.Series) -> pd.DataFrame:
    frame = pd.DataFrame({"group_key": pd.Index(group_keys.dropna().astype(str).unique())})
    parts = frame["group_key"].str.split("|", n=2, expand=True, regex=False)
    frame["domain"] = parts[0].fillna("UNKNOWN")
    frame["category"] = parts[1].fillna("UNKNOWN")
    frame["market_type"] = parts[2].fillna("UNKNOWN")
    return frame.reset_index(drop=True)


def build_fine_serving_features(
    rules_df: pd.DataFrame,
    group_features: pd.DataFrame | None = None,
) -> pd.DataFrame:
    if rules_df.empty:
        return pd.DataFrame(columns=FINE_SERVING_COLUMNS)
    fine = rules_df.copy()
    fine["price_bin"] = _build_price_bin_labels(fine["price_min"], fine["price_max"])
    fine["rule_price_center"] = (pd.to_numeric(fine["price_min"], errors="coerce") + pd.to_numeric(fine["price_max"], errors="coerce")) / 2.0
    fine["rule_price_width"] = pd.to_numeric(fine["price_max"], errors="coerce") - pd.to_numeric(fine["price_min"], errors="coerce")
    fine["rule_horizon_center"] = (pd.to_numeric(fine["h_min"], errors="coerce") + pd.to_numeric(fine["h_max"], errors="coerce")) / 2.0
    fine["rule_horizon_width"] = pd.to_numeric(fine["h_max"], errors="coerce") - pd.to_numeric(fine["h_min"], errors="coerce")
    fine["rule_edge_buffer"] = pd.to_numeric(fine["edge_full"], errors="coerce") - pd.to_numeric(
        fine["edge_lower_bound_full"], errors="coerce"
    )
    fine["rule_confidence_ratio"] = np.divide(
        pd.to_numeric(fine["edge_lower_bound_full"], errors="coerce"),
        pd.to_numeric(fine["edge_std_full"], errors="coerce").replace(0.0, np.nan),
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    fine["rule_support_log1p"] = np.log1p(pd.to_numeric(fine["n_full"], errors="coerce").clip(lower=0.0))
    fine["rule_snapshot_support_log1p"] = fine["rule_support_log1p"]
    if group_features is not None and not group_features.empty:
        interaction_columns = [
            "group_key",
            "full_group_expanding_bias_mean",
            "full_group_recent_90days_bias_mean",
            "full_group_expanding_logloss_mean",
            "full_group_recent_90days_logloss_mean",
            "full_group_expanding_logloss_tail_spread",
            "full_group_expanding_abs_bias_tail_spread",
            "domain_expanding_bias_mean",
            "category_expanding_bias_mean",
            "market_type_expanding_bias_mean",
            "domain_x_category_expanding_bias_mean",
            "domain_x_market_type_expanding_bias_mean",
            "category_x_market_type_expanding_bias_mean",
            "domain_expanding_logloss_mean",
            "category_expanding_logloss_mean",
            "market_type_expanding_logloss_mean",
            "domain_x_category_expanding_logloss_mean",
            "domain_x_market_type_expanding_logloss_mean",
            "category_x_market_type_expanding_logloss_mean",
        ]
        available_columns = [column for column in interaction_columns if column in group_features.columns]
        fine = fine.merge(group_features[available_columns], on="group_key", how="left")
        fine = _build_matched_rule_aggregate_features(fine)
        fine["hist_price_x_full_group_expanding_bias"] = (
            fine["rule_price_center"] * pd.to_numeric(fine.get("full_group_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["hist_price_x_full_group_recent_90days_bias"] = (
            fine["rule_price_center"] * pd.to_numeric(fine.get("full_group_recent_90days_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["hist_price_x_full_group_expanding_logloss"] = (
            fine["rule_price_center"] * pd.to_numeric(fine.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["tail_risk_x_price"] = (
            fine["rule_price_center"] * pd.to_numeric(fine.get("full_group_expanding_logloss_tail_spread"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_full_group_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("full_group_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_recent_90days_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("full_group_recent_90days_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_full_group_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_recent_90days_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("full_group_recent_90days_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_over_full_group_logloss"] = np.divide(
            pd.to_numeric(fine["edge_full"], errors="coerce"),
            pd.to_numeric(fine.get("full_group_expanding_logloss_mean"), errors="coerce").replace(0.0, np.nan),
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        fine["rule_edge_minus_domain_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("domain_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_category_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("category_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_market_type_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("market_type_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_domain_x_category_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("domain_x_category_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_domain_x_market_type_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("domain_x_market_type_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_edge_minus_category_x_market_type_expanding_bias"] = (
            pd.to_numeric(fine["edge_full"], errors="coerce")
            - pd.to_numeric(fine.get("category_x_market_type_expanding_bias_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_domain_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("domain_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_category_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("category_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_market_type_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("market_type_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_domain_x_category_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("domain_x_category_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_domain_x_market_type_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("domain_x_market_type_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["rule_score_minus_category_x_market_type_expanding_logloss"] = (
            pd.to_numeric(fine["rule_score"], errors="coerce")
            - pd.to_numeric(fine.get("category_x_market_type_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        )
        fine["price_x_full_group_expanding_abs_bias_tail_spread"] = (
            fine["rule_price_center"]
            * pd.to_numeric(fine.get("full_group_expanding_abs_bias_tail_spread"), errors="coerce").fillna(0.0)
        )
    else:
        for column in [
            "hist_price_x_full_group_expanding_bias",
            "hist_price_x_full_group_recent_90days_bias",
            "hist_price_x_full_group_expanding_logloss",
            "tail_risk_x_price",
            "rule_edge_minus_full_group_expanding_bias",
            "rule_edge_minus_recent_90days_bias",
            "rule_score_minus_full_group_expanding_logloss",
            "rule_score_minus_recent_90days_logloss",
            "rule_edge_over_full_group_logloss",
            "rule_edge_minus_domain_expanding_bias",
            "rule_edge_minus_category_expanding_bias",
            "rule_edge_minus_market_type_expanding_bias",
            "rule_edge_minus_domain_x_category_expanding_bias",
            "rule_edge_minus_domain_x_market_type_expanding_bias",
            "rule_edge_minus_category_x_market_type_expanding_bias",
            "rule_score_minus_domain_expanding_logloss",
            "rule_score_minus_category_expanding_logloss",
            "rule_score_minus_market_type_expanding_logloss",
            "rule_score_minus_domain_x_category_expanding_logloss",
            "rule_score_minus_domain_x_market_type_expanding_logloss",
            "rule_score_minus_category_x_market_type_expanding_logloss",
            "price_x_full_group_expanding_abs_bias_tail_spread",
            "rule_full_group_key_matched_rule_count",
            "rule_full_group_key_max_edge_full",
            "rule_full_group_key_max_edge_lower_bound_full",
            "rule_full_group_key_max_rule_score",
            "rule_full_group_key_mean_edge_full",
            "rule_full_group_key_mean_edge_lower_bound_full",
            "rule_full_group_key_mean_rule_score",
            "rule_full_group_key_sum_n_full",
            "rule_domain_matched_rule_count",
            "rule_domain_max_edge_full",
            "rule_domain_max_edge_lower_bound_full",
            "rule_domain_max_rule_score",
            "rule_domain_mean_edge_full",
            "rule_domain_mean_edge_lower_bound_full",
            "rule_domain_mean_rule_score",
            "rule_domain_sum_n_full",
            "rule_category_matched_rule_count",
            "rule_category_max_edge_full",
            "rule_category_max_edge_lower_bound_full",
            "rule_category_max_rule_score",
            "rule_category_mean_edge_full",
            "rule_category_mean_edge_lower_bound_full",
            "rule_category_mean_rule_score",
            "rule_category_sum_n_full",
            "rule_market_type_matched_rule_count",
            "rule_market_type_max_edge_full",
            "rule_market_type_max_edge_lower_bound_full",
            "rule_market_type_max_rule_score",
            "rule_market_type_mean_edge_full",
            "rule_market_type_mean_edge_lower_bound_full",
            "rule_market_type_mean_rule_score",
            "rule_market_type_sum_n_full",
        ]:
            fine[column] = 0.0
    return fine.reindex(columns=FINE_SERVING_COLUMNS).sort_values(
        ["group_key", "horizon_hours", "price_bin"], ascending=[True, True, True]
    ).reset_index(drop=True)


def _aggregate_group_default(group_frame: pd.DataFrame, column: str, how: str) -> float:
    series = pd.to_numeric(group_frame[column], errors="coerce")
    if how == "sum":
        return float(series.fillna(0.0).sum())
    if how == "max":
        return float(series.max()) if not series.dropna().empty else 0.0
    if how == "weighted_mean":
        weights = pd.to_numeric(group_frame["n_full"], errors="coerce").fillna(0.0).clip(lower=0.0)
        total_weight = float(weights.sum())
        if total_weight <= 0.0:
            return float(series.fillna(0.0).mean()) if not series.dropna().empty else 0.0
        return float(np.average(series.fillna(0.0), weights=weights))
    return float(series.fillna(0.0).mean()) if not series.dropna().empty else 0.0


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return np.divide(
        pd.to_numeric(numerator, errors="coerce").fillna(0.0),
        pd.to_numeric(denominator, errors="coerce").replace(0.0, np.nan),
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _build_matched_rule_aggregate_features(fine: pd.DataFrame) -> pd.DataFrame:
    aggregate_specs = [
        ("matched_rule_count", "edge_full", "size"),
        ("max_edge_full", "edge_full", "max"),
        ("max_edge_lower_bound_full", "edge_lower_bound_full", "max"),
        ("max_rule_score", "rule_score", "max"),
        ("mean_edge_full", "edge_full", "mean"),
        ("mean_edge_lower_bound_full", "edge_lower_bound_full", "mean"),
        ("mean_rule_score", "rule_score", "mean"),
        ("sum_n_full", "n_full", "sum"),
    ]
    grain_specs = [
        ("full_group_key", ["group_key"]),
        ("domain", ["domain"]),
        ("category", ["category"]),
        ("market_type", ["market_type"]),
    ]
    fine_with_aggregates = fine.copy()
    shared_keys = ["price_bin", "horizon_hours", "direction"]
    for grain_name, grain_columns in grain_specs:
        grouped = (
            fine.groupby(grain_columns + shared_keys, observed=True)
            .agg(
                **{
                    f"rule_{grain_name}_{feature_name}": (source_column, reducer)
                    for feature_name, source_column, reducer in aggregate_specs
                }
            )
            .reset_index()
        )
        fine_with_aggregates = fine_with_aggregates.merge(
            grouped,
            on=grain_columns + shared_keys,
            how="left",
        )
    return fine_with_aggregates


def build_group_serving_features(
    rules_df: pd.DataFrame,
    history_feature_frames: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    if rules_df.empty:
        return pd.DataFrame(columns=["group_key"]), {}
    group_features = _build_group_key_frame(rules_df["group_key"])
    group_metrics = (
        rules_df.groupby("group_key", observed=True)
        .agg(
            group_unique_markets=("group_unique_markets", "first"),
            group_snapshot_rows=("group_snapshot_rows", "first"),
            global_total_unique_markets=("global_total_unique_markets", "first"),
            global_total_snapshot_rows=("global_total_snapshot_rows", "first"),
            group_market_share_global=("group_market_share_global", "first"),
            group_snapshot_share_global=("group_snapshot_share_global", "first"),
            group_median_logloss=("group_median_logloss", "first"),
            group_median_brier=("group_median_brier", "first"),
            global_group_logloss_q25=("global_group_logloss_q25", "first"),
            global_group_brier_q25=("global_group_brier_q25", "first"),
            group_decision=("group_decision", "first"),
        )
        .reset_index()
    )
    group_features = group_features.merge(group_metrics, on="group_key", how="left")
    group_features["domain_is_unknown"] = group_features["domain"].astype(str).eq("UNKNOWN").astype(int)
    group_features["domain_category_key"] = (
        group_features["domain"].astype(str) + "|" + group_features["category"].astype(str)
    )
    group_features["domain_market_type_key"] = (
        group_features["domain"].astype(str) + "|" + group_features["market_type"].astype(str)
    )
    group_features["category_market_type_key"] = (
        group_features["category"].astype(str) + "|" + group_features["market_type"].astype(str)
    )

    for level_name, level_columns in LEVEL_DEFINITIONS.items():
        level_features = history_feature_frames[level_name].copy()
        merge_key_column = "level_key"
        if level_name == "global":
            group_features[merge_key_column] = "__GLOBAL__"
        elif level_name == "full_group":
            group_features[merge_key_column] = group_features["group_key"]
        elif level_name == "domain":
            group_features[merge_key_column] = group_features["domain"].astype(str)
        elif level_name == "category":
            group_features[merge_key_column] = group_features["category"].astype(str)
        elif level_name == "market_type":
            group_features[merge_key_column] = group_features["market_type"].astype(str)
        elif level_name == "domain_x_category":
            group_features[merge_key_column] = group_features["domain"].astype(str) + "|" + group_features["category"].astype(str)
        elif level_name == "domain_x_market_type":
            group_features[merge_key_column] = group_features["domain"].astype(str) + "|" + group_features["market_type"].astype(str)
        elif level_name == "category_x_market_type":
            group_features[merge_key_column] = group_features["category"].astype(str) + "|" + group_features["market_type"].astype(str)
        group_features = group_features.merge(level_features, on=merge_key_column, how="left")
        group_features = group_features.drop(columns=[merge_key_column])

    group_features["full_group_recent_90days_vs_expanding_bias_gap"] = (
        pd.to_numeric(group_features.get("full_group_recent_90days_bias_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_bias_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_recent_90days_vs_expanding_abs_bias_gap"] = (
        pd.to_numeric(group_features.get("full_group_recent_90days_abs_bias_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_abs_bias_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_recent_90days_vs_expanding_brier_gap"] = (
        pd.to_numeric(group_features.get("full_group_recent_90days_brier_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_brier_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_recent_90days_vs_expanding_logloss_gap"] = (
        pd.to_numeric(group_features.get("full_group_recent_90days_logloss_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_expanding_abs_bias_tail_spread"] = (
        pd.to_numeric(group_features.get("full_group_expanding_abs_bias_p90"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_abs_bias_p50"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_expanding_brier_tail_spread"] = (
        pd.to_numeric(group_features.get("full_group_expanding_brier_p90"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_brier_p50"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_expanding_logloss_tail_spread"] = (
        pd.to_numeric(group_features.get("full_group_expanding_logloss_p90"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_expanding_logloss_p50"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_recent_90days_logloss_tail_spread"] = (
        pd.to_numeric(group_features.get("full_group_recent_90days_logloss_p90"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("full_group_recent_90days_logloss_p50"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_expanding_logloss_tail_x_market_share"] = (
        group_features["full_group_expanding_logloss_tail_spread"]
        * pd.to_numeric(group_features["group_market_share_global"], errors="coerce").fillna(0.0)
    )
    group_features["full_group_expanding_abs_bias_tail_x_snapshot_share"] = (
        group_features["full_group_expanding_abs_bias_tail_spread"]
        * pd.to_numeric(group_features["group_snapshot_share_global"], errors="coerce").fillna(0.0)
    )
    group_features["full_group_vs_domain_logloss_gap"] = (
        pd.to_numeric(group_features.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("domain_expanding_logloss_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_vs_category_logloss_gap"] = (
        pd.to_numeric(group_features.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("category_expanding_logloss_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_vs_market_type_logloss_gap"] = (
        pd.to_numeric(group_features.get("full_group_expanding_logloss_mean"), errors="coerce").fillna(0.0)
        - pd.to_numeric(group_features.get("market_type_expanding_logloss_mean"), errors="coerce").fillna(0.0)
    )
    group_features["full_group_recent_90days_vs_expanding_bias_zscore"] = _safe_ratio(
        group_features["full_group_recent_90days_vs_expanding_bias_gap"],
        group_features.get("full_group_expanding_bias_std"),
    )
    group_features["full_group_recent_90days_vs_expanding_logloss_zscore"] = _safe_ratio(
        group_features["full_group_recent_90days_vs_expanding_logloss_gap"],
        group_features.get("full_group_expanding_logloss_std"),
    )
    group_features["full_group_recent_90days_tail_instability_ratio"] = _safe_ratio(
        group_features["full_group_recent_90days_logloss_tail_spread"],
        group_features["full_group_expanding_logloss_tail_spread"],
    )

    fine_serving_features = build_fine_serving_features(rules_df, group_features=group_features)

    fine_defaults_manifest: dict[str, dict[str, object]] = {}
    grouped_fine_features = fine_serving_features.groupby("group_key", observed=True)
    for feature_name, aggregation_name in FINE_DEFAULT_AGGREGATIONS.items():
        fallback_column = f"group_default_{feature_name}"
        group_features[fallback_column] = group_features["group_key"].map(
            lambda value: _aggregate_group_default(grouped_fine_features.get_group(value), feature_name, aggregation_name)
        )
        fine_defaults_manifest[feature_name] = {
            "fallback_scope": "group_key",
            "group_column": fallback_column,
            "aggregation": aggregation_name,
        }

    group_direction = rules_df.groupby("group_key", observed=True)["edge_full"].sum().map(edge_sign)
    group_features["group_default_direction"] = group_features["group_key"].map(group_direction).fillna(0).astype(int)
    fine_defaults_manifest["direction"] = {
        "fallback_scope": "group_key",
        "group_column": "group_default_direction",
        "aggregation": "signed_sum_edge",
    }
    group_features["group_default_leaf_id"] = "__GROUP_DEFAULT__|" + group_features["group_key"].astype(str)
    fine_defaults_manifest["leaf_id"] = {
        "fallback_scope": "group_key",
        "group_column": "group_default_leaf_id",
        "aggregation": "sentinel",
    }
    group_features["fine_match_found_default"] = 0
    group_features["group_match_found_default"] = 1
    return group_features.sort_values("group_key").reset_index(drop=True), fine_defaults_manifest


def build_group_decisions(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    quality = prepare_history_quality_frame(df)
    global_total_unique_markets = int(quality["market_id"].astype(str).nunique()) if not quality.empty else 0
    global_total_snapshot_rows = int(len(quality))
    group_stats = (
        quality.groupby(BASE_GROUP_COLUMNS + ["group_key"], observed=True)
        .agg(
            group_unique_markets=("market_id", "nunique"),
            group_snapshot_rows=("market_id", "size"),
            group_median_logloss=("row_logloss", "median"),
            group_median_brier=("row_brier", "median"),
            group_wins=("y", "sum"),
            group_price_mean=("price", "mean"),
        )
        .reset_index()
    )
    group_stats["global_total_unique_markets"] = global_total_unique_markets
    group_stats["global_total_snapshot_rows"] = global_total_snapshot_rows
    group_stats["group_market_share_global"] = group_stats["group_unique_markets"] / max(global_total_unique_markets, 1)
    group_stats["group_snapshot_share_global"] = group_stats["group_snapshot_rows"] / max(global_total_snapshot_rows, 1)
    group_stats["group_direction"] = (
        group_stats["group_wins"] / group_stats["group_snapshot_rows"] - group_stats["group_price_mean"]
    ).map(edge_sign)
    eligible = group_stats[group_stats["group_unique_markets"] >= MIN_GROUP_UNIQUE_MARKETS].copy()
    thresholds = {
        "global_group_logloss_q25": float(eligible["group_median_logloss"].quantile(GROUP_THRESHOLD_QUANTILE))
        if not eligible.empty
        else float("nan"),
        "global_group_brier_q25": float(eligible["group_median_brier"].quantile(GROUP_THRESHOLD_QUANTILE))
        if not eligible.empty
        else float("nan"),
    }
    group_stats["selection_status"] = "keep"
    insufficient_mask = group_stats["group_unique_markets"] < MIN_GROUP_UNIQUE_MARKETS
    group_stats.loc[insufficient_mask, "selection_status"] = "insufficient_data"
    drop_mask = (
        ~insufficient_mask
        & (group_stats["group_median_logloss"] < thresholds["global_group_logloss_q25"])
        & (group_stats["group_median_brier"] < thresholds["global_group_brier_q25"])
    )
    group_stats.loc[drop_mask, "selection_status"] = "drop"
    return group_stats, thresholds


def _derive_horizon_bounds(horizon_hours: float, supported_hours: list[float]) -> tuple[float, float]:
    hours = sorted(float(value) for value in supported_hours)
    current = float(horizon_hours)
    idx = hours.index(current)
    if idx == 0:
        lower = 0.0
    else:
        lower = (hours[idx - 1] + current) / 2.0
    if idx == len(hours) - 1:
        upper = 1000.0
    else:
        upper = (current + hours[idx + 1]) / 2.0
    return float(lower), float(upper)


def evaluate_rule_candidate(row: pd.Series, artifact_mode: str) -> tuple[dict, str]:
    _ = artifact_mode
    group_unique_markets = float(row.get("group_unique_markets", np.nan))
    if not np.isfinite(group_unique_markets) or group_unique_markets < MIN_GROUP_UNIQUE_MARKETS:
        return {}, "insufficient_data"
    if str(row.get("selection_status", "keep")) != "keep":
        return {}, str(row.get("selection_status", "drop"))

    n_full = float(row.get("n_full", np.nan))
    wins_full = float(row.get("wins_full", np.nan))
    p_full = float(row.get("p_full", np.nan))
    edge_std_full_raw = float(row.get("edge_std_full_raw", np.nan))
    if not np.isfinite(n_full) or n_full <= 0:
        return {}, "empty_rule_row"

    q_full = wins_full / n_full
    direction = edge_sign(q_full - p_full)
    if direction == 0:
        direction = int(row.get("group_direction", 0))
    if direction == 0:
        return {}, "ambiguous_direction"

    group_key = str(row["group_key"])
    price_label = str(row["price_bin"])
    horizon_hours = int(float(row["horizon_hours"]))
    leaf_id = stable_leaf_id(group_key, price_label, str(horizon_hours))
    price_min, price_max = (float(item) for item in price_label.split("-"))
    h_min, h_max = _derive_horizon_bounds(horizon_hours, supported_hours=sorted(config.HORIZONS))
    edge_full = direction_adjusted_edge(q_full, p_full, direction)
    edge_std_full = direction_adjusted_stat(edge_std_full_raw, direction)
    q_lower, q_upper = wilson_interval(wins_full, n_full)
    if direction >= 0:
        edge_lower_bound_full = q_lower - p_full
    else:
        edge_lower_bound_full = p_full - q_upper

    return {
        "group_key": group_key,
        "domain": row["domain"],
        "category": row["category"],
        "market_type": row["market_type"],
        "leaf_id": leaf_id,
        "price_min": float(price_min),
        "price_max": float(price_max),
        "h_min": float(h_min),
        "h_max": float(h_max),
        "direction": int(direction),
        "q_full": float(q_full),
        "p_full": float(p_full),
        "edge_full": float(edge_full),
        "edge_std_full": float(edge_std_full),
        "edge_lower_bound_full": float(edge_lower_bound_full),
        "rule_score": float(edge_lower_bound_full),
        "n_full": int(n_full),
        "horizon_hours": int(horizon_hours),
        "group_unique_markets": int(row["group_unique_markets"]),
        "group_snapshot_rows": int(row["group_snapshot_rows"]),
        "global_total_unique_markets": int(row["global_total_unique_markets"]),
        "global_total_snapshot_rows": int(row["global_total_snapshot_rows"]),
        "group_market_share_global": float(row["group_market_share_global"]),
        "group_snapshot_share_global": float(row["group_snapshot_share_global"]),
        "group_median_logloss": float(row["group_median_logloss"]),
        "group_median_brier": float(row["group_median_brier"]),
        "global_group_logloss_q25": float(row["global_group_logloss_q25"]),
        "global_group_brier_q25": float(row["global_group_brier_q25"]),
        "group_decision": str(row["selection_status"]),
    }, "selected"


def build_rules(df: pd.DataFrame, artifact_mode: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    quality = prepare_history_quality_frame(df)
    group_stats, thresholds = build_group_decisions(quality)
    kept_quality = quality.merge(
        group_stats[["group_key", "selection_status"]],
        on="group_key",
        how="left",
    )
    kept_quality = kept_quality[kept_quality["selection_status"] == "keep"].copy()
    grid = (
        kept_quality.groupby(RULE_ROW_COLUMNS + ["group_key"], observed=True)
        .agg(
            n_full=("y", "size"),
            wins_full=("y", "sum"),
            p_full=("price", "mean"),
            edge_std_full_raw=("r_std", "mean"),
        )
        .reset_index()
    )
    grid = grid.merge(
        group_stats[
            [
                "group_key",
                "group_unique_markets",
                "group_snapshot_rows",
                "global_total_unique_markets",
                "global_total_snapshot_rows",
                "group_market_share_global",
                "group_snapshot_share_global",
                "group_median_logloss",
                "group_median_brier",
                "group_direction",
                "selection_status",
            ]
        ],
        on="group_key",
        how="left",
    )
    grid["global_group_logloss_q25"] = thresholds["global_group_logloss_q25"]
    grid["global_group_brier_q25"] = thresholds["global_group_brier_q25"]
    selected_rules: list[dict] = []
    for _, row in grid.iterrows():
        rule_candidate, status = evaluate_rule_candidate(row, artifact_mode)
        if rule_candidate:
            selected_rules.append(rule_candidate)
    report_df = group_stats.copy()
    rules_df = pd.DataFrame(selected_rules)
    if not rules_df.empty:
        rules_df = rules_df.sort_values(
            ["group_unique_markets", "group_key", "horizon_hours", "price_min"],
            ascending=[False, True, True, True],
        ).reset_index(drop=True)
        rules_df = rules_df.reindex(columns=RULE_SCHEMA_COLUMNS)

    if not report_df.empty:
        report_df = report_df.sort_values(
            ["selection_status", "group_unique_markets", "group_snapshot_rows"],
            ascending=[True, False, False],
        ).reset_index(drop=True)

    return rules_df, report_df


def empty_rules_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=RULE_SCHEMA_COLUMNS)


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    rule_training_mode = "full_history_unified"

    df, split, funnel_summary = prepare_rule_training_frame(
        artifact_mode=args.artifact_mode,
        max_rows=args.max_rows,
        recent_days=args.recent_days,
        split_reference_end=args.split_reference_end,
        history_start_override=args.history_start,
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        price_bin_step=RULE_PRICE_BIN_STEP,
    )
    rules_df, report_df = build_rules(df, args.artifact_mode)
    history_feature_frames = summarize_history_features(df)
    write_history_feature_artifacts(history_feature_frames, artifact_paths.history_feature_paths)
    validate_materialized_history_artifacts(artifact_paths.history_feature_paths)
    history_feature_frames = load_history_feature_artifacts(artifact_paths.history_feature_paths)
    rule_selection_summary = summarize_rule_selection(df, report_df, rules_df)
    snapshot_funnel = with_stage_deltas(
        funnel_summary["snapshot_funnel"] + [{"stage": "after_rule_selection", **rule_selection_summary["after_rule_selection"]}]
    )

    if rules_df.empty:
        rules_df = empty_rules_frame()

    group_serving_features, fine_defaults_manifest = build_group_serving_features(
        rules_df,
        history_feature_frames,
    )
    fine_serving_features = build_fine_serving_features(rules_df, group_features=group_serving_features)

    rules_df.to_csv(artifact_paths.rules_path, index=False)
    group_serving_features.to_parquet(artifact_paths.group_serving_features_path, index=False)
    fine_serving_features.to_parquet(artifact_paths.fine_serving_features_path, index=False)
    report_df.to_csv(artifact_paths.rule_report_path, index=False)
    write_json(
        artifact_paths.serving_feature_defaults_path,
        {
            "fallback_policy": "group_key_aggregates",
            "fine_feature_defaults": fine_defaults_manifest,
            "indicator_defaults": {
                "fine_match_found": 0,
                "group_match_found": 0,
                "used_group_fallback_only": 0,
            },
        },
    )

    split_summary = {
        "artifact_mode": args.artifact_mode,
        "total_rows": int(len(df)),
        "rows_by_split": df["dataset_split"].value_counts().to_dict(),
        "boundaries": split.to_dict(),
        "quality_pass_rows": int(len(df)),
    }
    write_json(artifact_paths.split_summary_path, split_summary)
    write_json(
        artifact_paths.rule_training_summary_path,
        {
            "artifact_mode": args.artifact_mode,
            "selected_rules": int(len(rules_df)),
            "report_rows": int(len(report_df)),
            "group_serving_rows": int(len(group_serving_features)),
            "fine_serving_rows": int(len(fine_serving_features)),
            "selection_status_counts": report_df["selection_status"].value_counts().to_dict() if not report_df.empty else {},
            "boundaries": split.to_dict(),
            "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
            "method": {
                "estimator": "raw_frequency",
                "rule_training_mode": rule_training_mode,
                "bayesian_smoothing": False,
                "prior_mean": False,
                "benjamini_hochberg": False,
            },
        },
    )
    write_json(
        artifact_paths.rule_funnel_summary_path,
        {
            "artifact_mode": args.artifact_mode,
            "rule_training_mode": rule_training_mode,
            "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
            "boundaries": split.to_dict(),
            "raw_market_funnel": funnel_summary["raw_market_funnel"],
            "snapshot_funnel": snapshot_funnel,
            "rule_selection": {
                "selected_rule_count": rule_selection_summary["selected_rule_count"],
                "rule_bucket_status_counts": rule_selection_summary["rule_bucket_status_counts"],
                "selection_status_market_impact": rule_selection_summary["selection_status_market_impact"],
            },
        },
    )
    write_rule_generation_audit(
        artifact_paths=artifact_paths,
        payload=build_rule_generation_audit_payload(
            artifact_paths=artifact_paths,
            rules_df=rules_df,
            report_df=report_df,
            group_serving_features=group_serving_features,
            fine_serving_features=fine_serving_features,
            rule_funnel_summary={
                "artifact_mode": args.artifact_mode,
                "rule_training_mode": rule_training_mode,
                "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
                "boundaries": split.to_dict(),
                "raw_market_funnel": funnel_summary["raw_market_funnel"],
                "snapshot_funnel": snapshot_funnel,
                "rule_selection": {
                    "selected_rule_count": rule_selection_summary["selected_rule_count"],
                    "rule_bucket_status_counts": rule_selection_summary["rule_bucket_status_counts"],
                    "selection_status_market_impact": rule_selection_summary["selection_status_market_impact"],
                },
            },
            split_summary=split_summary,
            rule_training_summary={
                "artifact_mode": args.artifact_mode,
                "selected_rules": int(len(rules_df)),
                "report_rows": int(len(report_df)),
                "group_serving_rows": int(len(group_serving_features)),
                "fine_serving_rows": int(len(fine_serving_features)),
                "selection_status_counts": report_df["selection_status"].value_counts().to_dict() if not report_df.empty else {},
                "boundaries": split.to_dict(),
                "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
                "method": {
                    "estimator": "raw_frequency",
                    "rule_training_mode": rule_training_mode,
                    "bayesian_smoothing": False,
                    "prior_mean": False,
                    "benjamini_hochberg": False,
                },
            },
            debug_filters={"max_rows": args.max_rows, "recent_days": args.recent_days},
        ),
    )

    print(f"[INFO] Saved {len(rules_df)} rules to {artifact_paths.rules_path}")
    print(f"[INFO] Saved {len(history_feature_frames)} history artifacts to {artifact_paths.edge_dir}")
    print(f"[INFO] Saved {len(group_serving_features)} group serving rows to {artifact_paths.group_serving_features_path}")
    print(f"[INFO] Saved {len(fine_serving_features)} fine serving rows to {artifact_paths.fine_serving_features_path}")
    print(f"[INFO] Saved full rule report to {artifact_paths.rule_report_path}")
    print(f"[INFO] Saved split summary to {artifact_paths.split_summary_path}")
    print(f"[INFO] Saved rule funnel summary to {artifact_paths.rule_funnel_summary_path}")
    print(f"[INFO] Saved rule generation audit to {artifact_paths.rule_generation_audit_markdown_path}")


if __name__ == "__main__":
    main()
