from __future__ import annotations

import argparse
import json
import os
import sys
import time
from copy import deepcopy

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import load_online_parity_snapshots, load_raw_markets
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache, preprocess_features
from rule_baseline.features.serving import (
    ServingFeatureBundle,
    attach_serving_features,
    build_price_bin,
    round_horizon_hours,
)
from rule_baseline.models import (
    SUPPORTED_AUTOGLOUON_CALIBRATION_MODES,
    fit_autogluon_q_model,
    compute_trade_value_from_q as _compute_trade_value_from_q,
    fit_model_payload,
    fit_regression_payload,
    infer_q_from_trade_value as _infer_q_from_trade_value,
    predict_probabilities,
    predict_regression,
)
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged
from rule_baseline.features.annotation_normalization import build_normalization_manifest, normalize_market_annotations
from rule_baseline.features.snapshot_semantics import FEATURE_SEMANTICS_VERSION, online_feature_columns, split_feature_contract_columns
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config

TRAIN_PRICE_MIN = 0.2
TRAIN_PRICE_MAX = 0.8
DEFAULT_SNAPSHOT_CALIBRATION_MODE = "none"
DEFAULT_SNAPSHOT_PRESETS = "medium_quality"
DEFAULT_SNAPSHOT_TIME_LIMIT = 300
DEFAULT_SNAPSHOT_HYPERPARAMETER_PROFILE = "gbm_cat"
DEFAULT_SNAPSHOT_REFIT_FULL = True
TRAIN_FEATURES_PARQUET_PATH = config.PROCESSED_DIR / "train.parquet"
VALID_FEATURES_PARQUET_PATH = config.PROCESSED_DIR / "valid.parquet"
TEST_FEATURES_PARQUET_PATH = config.PROCESSED_DIR / "test.parquet"
FEATURE_EXPORT_MANIFEST_PATH = config.PROCESSED_DIR / "feature_export_manifest.json"

DROP_COLS = {
    "y",
    "closedTime",
    "market_id",
    "snapshot_time",
    "scheduled_end",
    "snapshot_date",
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
    "dataset_split",
    "quality_pass",
    "delta_hours_exceeded_flag",
    "trade_value_true",
    "expected_pnl_target",
    "expected_roi_target",
    "residual_q_target",
    "winning_outcome_index",
    "winning_outcome_label",
    "leaf_id",
    "bestBid",
    "bestAsk",
    "spread",
    "lastTradePrice",
    "best_bid",
    "best_ask",
    "mid_price",
    "quoted_spread",
    "quoted_spread_pct",
    "book_imbalance",
    "price_change_1h",
    "price_change_1d",
    "price_change_1w",
    "line_value",
    "volume24hrClob",
    "volume1wkClob",
    "domain_parsed",
    "domain_parsed_market",
    "source_host_market",
    "category_raw_market",
    "category_parsed_market",
    "category_override_flag_market",
    "category_source",
    "is_date_based",
    "vol_x_sentiment",
    "startDate_market",
    "endDate_market",
    "closedTime_market",
    "duration_is_negative_flag",
    "duration_below_min_horizon_flag",
    "price_in_range_flag",
    "liquidity",
    "log_horizon_x_liquidity",
    "negRisk",
    "liquidityAmm",
    "liquidityClob",
    "source_url_market",
    "sub_domain_market",
    "outcome_pattern_market",
    "groupItemTitle_market",
    "gameId_market",
    "marketMakerAddress_market",
    "log_liq",
    "liq_ratio",
    "log_liquidity_clob",
    "log_liquidity_amm",
    "clob_share_liquidity",
    "has_percent",
    "has_million",
    "has_before",
    "has_after",
    "is_binary",
    "cap_ratio",
    "strong_pos",
    "cat_finance",
    "dur_very_long",
    "description_market",
    "question_market",
    "volume",
    "spread_over_liquidity",
    "volume24hr",
    "volume1wk",
    "oneHourPriceChange",
    "oneDayPriceChange",
    "oneWeekPriceChange",
    "log_vol",
    "log_v24",
    "log_v1w",
    "vol_ratio_24",
    "vol_ratio_1w",
    "daily_weekly",
    "vol_tier_ultra",
    "vol_tier_high",
    "vol_tier_med",
    "vol_tier_low",
    "activity",
    "engagement",
    "momentum",
    "clob_share_volume24",
    "clob_share_volume1w",
    "price_change_accel",
    "sentiment_vol",
    "vol_per_day",
    "log_vol_per_day",
    "vol_x_sentiment",
    "activity_x_catcount",
}

PREDICTOR_HYPERPARAMETER_PROFILES: dict[str, dict[str, object]] = {
    "default": {
        "GBM": {},
        "CAT": {},
        "XGB": {},
    },
    "plan_full": {
        "GBM": {},
        "CAT": {},
        "XGB": {},
        "LR": {},
        "EBM": {},
    },
    "gbm_only": {
        "GBM": {},
    },
    "cat_only": {
        "CAT": {},
    },
    "gbm_cat": {
        "GBM": {},
        "CAT": {},
    },
    "gbm_cat_lr": {
        "GBM": {},
        "CAT": {},
        "LR": {},
    },
    "lr_only": {
        "LR": {},
    },
    "gbm_compact": {
        "GBM": {
            "num_boost_round": 300,
            "learning_rate": 0.03,
            "num_leaves": 31,
            "min_data_in_leaf": 100,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
        },
    },
    "gbm_compact_lr": {
        "GBM": {
            "num_boost_round": 300,
            "learning_rate": 0.03,
            "num_leaves": 31,
            "min_data_in_leaf": 100,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
        },
        "LR": {},
    },
    "gbm_compact_cat_lr": {
        "GBM": {
            "num_boost_round": 300,
            "learning_rate": 0.03,
            "num_leaves": 31,
            "min_data_in_leaf": 100,
            "feature_fraction": 0.8,
            "bagging_fraction": 0.8,
            "bagging_freq": 1,
        },
        "CAT": {
            "depth": 6,
            "learning_rate": 0.03,
            "iterations": 400,
            "l2_leaf_reg": 8.0,
        },
        "LR": {},
    },
}


def normalize_predictor_presets(raw_value: str) -> str | list[str]:
    value = raw_value.strip()
    if "," not in value:
        return value
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the snapshot ensemble model with strict split isolation.")
    parser.add_argument("--pipeline-config", type=str, default=None)
    parser.add_argument(
        "--calibration-mode",
        choices=sorted(SUPPORTED_AUTOGLOUON_CALIBRATION_MODES),
        default=DEFAULT_SNAPSHOT_CALIBRATION_MODE,
        help="Calibration strategy for probability outputs.",
    )
    parser.add_argument(
        "--target-mode",
        choices=["q", "residual_q", "expected_pnl", "expected_roi"],
        default="q",
        help="Training target for the main snapshot model.",
    )
    parser.add_argument("--predictor-presets", type=str, default=DEFAULT_SNAPSHOT_PRESETS)
    parser.add_argument("--predictor-time-limit", type=int, default=DEFAULT_SNAPSHOT_TIME_LIMIT)
    parser.add_argument(
        "--predictor-hyperparameters-profile",
        choices=sorted(PREDICTOR_HYPERPARAMETER_PROFILES),
        default=DEFAULT_SNAPSHOT_HYPERPARAMETER_PROFILE,
        help="Named AutoGluon model-family profile to train.",
    )
    parser.add_argument("--random-seed", type=int, default=21)
    parser.add_argument("--grouped-calibration-column", type=str, default="horizon_hours")
    parser.add_argument("--grouped-calibration-min-rows", type=int, default=20)
    parser.add_argument("--num-bag-folds", type=int, default=None)
    parser.add_argument("--num-bag-sets", type=int, default=None)
    parser.add_argument("--num-stack-levels", type=int, default=None)
    parser.add_argument("--auto-stack", dest="auto_stack", action="store_true")
    parser.add_argument("--no-auto-stack", dest="auto_stack", action="store_false")
    parser.set_defaults(auto_stack=None)
    parser.add_argument("--refit-full", dest="refit_full", action="store_true")
    parser.add_argument("--no-refit-full", dest="refit_full", action="store_false")
    parser.set_defaults(refit_full=DEFAULT_SNAPSHOT_REFIT_FULL)
    return parser.parse_args()


def load_rules(path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Rules file not found at {path}. Run train_rules_naive_output_rule.py first.")

    rules = pd.read_csv(path)
    required = [
        "domain",
        "category",
        "market_type",
        "price_min",
        "price_max",
        "h_min",
        "h_max",
        "q_full",
        "edge_lower_bound_full",
        "direction",
        "leaf_id",
        "group_key",
    ]
    missing = [column for column in required if column not in rules.columns]
    if missing:
        raise ValueError(f"Rules file is missing required columns: {missing}")

    for column in ["domain", "category", "market_type"]:
        rules[column] = rules[column].fillna("UNKNOWN").astype(str)
    return rules


def _build_rule_lookup_frame(rules: pd.DataFrame) -> pd.DataFrame:
    lookup = rules.copy()
    lookup["price_bin"] = (
        pd.to_numeric(lookup["price_min"], errors="coerce").map(lambda value: f"{float(value):.2f}")
        + "-"
        + pd.to_numeric(lookup["price_max"], errors="coerce").map(lambda value: f"{float(value):.2f}")
    )
    lookup["rounded_horizon_hours"] = round_horizon_hours(lookup["horizon_hours"])
    return lookup[
        [
            "domain",
            "category",
            "market_type",
            "price_bin",
            "rounded_horizon_hours",
            "leaf_id",
            "price_min",
            "price_max",
            "h_min",
            "h_max",
            "horizon_hours",
            "q_full",
            "p_full",
            "edge_full",
            "edge_std_full",
            "edge_lower_bound_full",
            "direction",
            "group_key",
            "n_full",
        ]
    ].copy()


def match_snapshots_to_rules(
    snapshots: pd.DataFrame,
    market_annotations: pd.DataFrame,
    rules: pd.DataFrame,
) -> pd.DataFrame:
    context = snapshots.copy()
    required_columns = {"domain", "category", "market_type"}
    if required_columns.difference(context.columns):
        context = context.merge(
            market_annotations[["market_id", "domain", "category", "market_type"]],
            on="market_id",
            how="left",
            suffixes=("", "_annotation"),
        )
        for column in ["domain", "category", "market_type"]:
            annotation_column = f"{column}_annotation"
            if annotation_column in context.columns:
                if column in context.columns:
                    context[column] = context[annotation_column].fillna(context[column])
                else:
                    context[column] = context[annotation_column]
                context = context.drop(columns=[annotation_column])

    context["domain"] = context.get("domain", "UNKNOWN").fillna("UNKNOWN").astype(str)
    context["category"] = context.get("category", "UNKNOWN").fillna("UNKNOWN").astype(str)
    context["market_type"] = context.get("market_type", "UNKNOWN").fillna("UNKNOWN").astype(str)
    context["price_bin"] = build_price_bin(context["price"])
    context["rounded_horizon_hours"] = round_horizon_hours(context["horizon_hours"])
    rule_lookup = _build_rule_lookup_frame(rules)

    matched = context.merge(
        rule_lookup,
        on=["domain", "category", "market_type", "price_bin", "rounded_horizon_hours"],
        how="inner",
        suffixes=("", "_rule"),
    )
    if matched.empty:
        return pd.DataFrame()

    matched = matched.sort_values(["market_id", "snapshot_time", "edge_lower_bound_full"], ascending=[True, True, False])
    matched = matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    return matched.drop(columns=["rounded_horizon_hours"], errors="ignore")


def build_feature_table(
    snapshots: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    market_annotations: pd.DataFrame,
    rules: pd.DataFrame,
    serving_feature_bundle: ServingFeatureBundle | None = None,
) -> pd.DataFrame:
    matched = match_snapshots_to_rules(snapshots, market_annotations, rules)
    if matched.empty:
        return pd.DataFrame()
    if serving_feature_bundle is not None:
        matched = attach_serving_features(
            matched,
            serving_feature_bundle,
            price_column="price",
            horizon_column="horizon_hours",
        )
    return preprocess_features(matched, market_feature_cache)


def load_serving_feature_bundle(artifact_paths) -> ServingFeatureBundle | None:
    if not artifact_paths.group_serving_features_path.exists():
        return None
    if not artifact_paths.fine_serving_features_path.exists():
        return None
    if not artifact_paths.serving_feature_defaults_path.exists():
        return None
    with artifact_paths.serving_feature_defaults_path.open("r", encoding="utf-8") as file:
        defaults_manifest = json.load(file)
    return ServingFeatureBundle(
        fine_features=pd.read_parquet(artifact_paths.fine_serving_features_path),
        group_features=pd.read_parquet(artifact_paths.group_serving_features_path),
        defaults_manifest=defaults_manifest,
    )


def probability_metrics(df: pd.DataFrame) -> dict[str, float | int | None]:
    if df.empty:
        return {"rows": 0, "logloss_price": None, "logloss_model": None, "brier_price": None, "brier_model": None, "auc_price": None, "auc_model": None}

    y = df["y"].astype(int).values
    p = df["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    q = df["q_pred"].astype(float).clip(1e-6, 1 - 1e-6).values

    metrics = {
        "rows": int(len(df)),
        "logloss_price": float(log_loss(y, p)),
        "logloss_model": float(log_loss(y, q)),
        "brier_price": float(brier_score_loss(y, p)),
        "brier_model": float(brier_score_loss(y, q)),
        "auc_price": None,
        "auc_model": None,
    }
    if df["y"].nunique() > 1:
        metrics["auc_price"] = float(roc_auc_score(y, p))
        metrics["auc_model"] = float(roc_auc_score(y, q))
    return metrics


def trade_value_metrics(df: pd.DataFrame) -> dict[str, float | int | None]:
    if df.empty or "trade_value_pred" not in df.columns or "trade_value_true" not in df.columns:
        return {"rows": 0, "mae_trade_value": None, "rmse_trade_value": None, "sign_accuracy": None}

    prediction = df["trade_value_pred"].astype(float)
    truth = df["trade_value_true"].astype(float)
    residual = prediction - truth
    return {
        "rows": int(len(df)),
        "mae_trade_value": float(np.abs(residual).mean()),
        "rmse_trade_value": float(np.sqrt(np.mean(np.square(residual)))),
        "sign_accuracy": float((np.sign(prediction) == np.sign(truth)).mean()),
    }


def compute_trade_value_from_q(df: pd.DataFrame, q_pred: pd.Series | pd.Index | np.ndarray) -> np.ndarray:
    return _compute_trade_value_from_q(df, q_pred, direction_column="direction")


def infer_q_from_trade_value(df: pd.DataFrame, trade_value_pred: np.ndarray) -> np.ndarray:
    return _infer_q_from_trade_value(df, trade_value_pred, direction_column="direction")


def add_training_targets(df_feat: pd.DataFrame) -> pd.DataFrame:
    out = df_feat.copy()
    price = out["price"].astype(float).clip(1e-6, 1 - 1e-6)
    direction = out["direction"].astype(int)
    out["trade_value_true"] = np.where(
        direction > 0,
        out["y"].astype(float) / price - 1.0 - config.FEE_RATE,
        (price - out["y"].astype(float)) / np.maximum(1.0 - price, 1e-6) - config.FEE_RATE,
    )
    out["expected_pnl_target"] = out["trade_value_true"]
    out["expected_roi_target"] = out["trade_value_true"]
    out["residual_q_target"] = out["y"].astype(float) - out["price"].astype(float)
    return out


def save_model(payload: dict, model_path) -> None:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, model_path)
    print(f"[INFO] Saved ensemble payload to {model_path}")


def load_feature_export_manifest(path=FEATURE_EXPORT_MANIFEST_PATH) -> dict:
    if not path.exists():
        raise FileNotFoundError(
            f"Feature export manifest not found at {path}. Run export_features.py before train_snapshot_model.py."
        )
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_exported_feature_frames(
    train_path=TRAIN_FEATURES_PARQUET_PATH,
    valid_path=VALID_FEATURES_PARQUET_PATH,
    test_path=TEST_FEATURES_PARQUET_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    missing = [str(path) for path in [train_path, valid_path, test_path] if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing exported parquet feature files. Run export_features.py first. Missing: "
            + ", ".join(missing)
        )
    return pd.read_parquet(train_path), pd.read_parquet(valid_path), pd.read_parquet(test_path)


def export_predictions(
    model_artifact,
    df_feat: pd.DataFrame,
    artifact_paths,
    artifact_mode: str,
    target_mode: str,
    publish_split: str,
    fail_if_publish_split_empty: bool,
) -> dict:
    if target_mode == "q":
        q_pred = model_artifact.predict(df_feat)
        trade_value_pred = compute_trade_value_from_q(df_feat, q_pred)
    elif target_mode == "residual_q":
        residual_pred = predict_regression(model_artifact, df_feat)
        q_pred = np.clip(df_feat["price"].astype(float).values + residual_pred, 0.0, 1.0)
        trade_value_pred = compute_trade_value_from_q(df_feat, q_pred)
    else:
        trade_value_pred = predict_regression(model_artifact, df_feat)
        q_pred = infer_q_from_trade_value(df_feat, trade_value_pred)

    export_columns = [
        "market_id",
        "snapshot_time",
        "snapshot_date",
        "closedTime",
        "scheduled_end",
        "horizon_hours",
        "price",
        "y",
        "domain",
        "category",
        "market_type",
        "leaf_id",
        "direction",
        "group_key",
        "q_smooth",
        "dataset_split",
        "quality_pass",
        "delta_hours_exceeded_flag",
    ]
    export_columns = [column for column in export_columns if column in df_feat.columns]
    out = df_feat[export_columns].copy()
    out["q_pred"] = q_pred
    out["trade_value_true"] = df_feat["trade_value_true"].astype(float).values
    out["trade_value_pred"] = trade_value_pred
    out["edge_prob"] = out["q_pred"] - out["price"]
    out.to_csv(artifact_paths.predictions_full_path, index=False)

    if artifact_mode == "offline":
        publish_df = out[out["dataset_split"] == publish_split].copy()
        if publish_df.empty and fail_if_publish_split_empty:
            raise RuntimeError(f"Configured publish split '{publish_split}' is empty.")
    else:
        publish_df = out[out["dataset_split"] == publish_split].copy()
        if publish_df.empty and fail_if_publish_split_empty:
            raise RuntimeError(f"Configured publish split '{publish_split}' is empty.")
    publish_df.to_csv(artifact_paths.predictions_path, index=False)

    metrics_by_split = {
        split_name: {**probability_metrics(split_df), **trade_value_metrics(split_df)}
        for split_name, split_df in out.groupby("dataset_split", observed=False)
        if split_name in {"train", "valid", "test"}
    }
    metrics_by_split["published"] = {**probability_metrics(publish_df), **trade_value_metrics(publish_df)}
    return metrics_by_split


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    predictor_presets = normalize_predictor_presets(args.predictor_presets)
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)
    if pipeline_config.artifact_mode == "online" and args.target_mode != "q":
        raise ValueError("Online production artifacts support only target_mode='q'.")
    manifest = load_feature_export_manifest()
    if manifest.get("artifact_mode") != pipeline_config.artifact_mode:
        raise ValueError(
            f"Exported feature artifact_mode={manifest.get('artifact_mode')} does not match train_snapshot_model artifact_mode={pipeline_config.artifact_mode}."
        )
    df_train, df_valid, df_test = load_exported_feature_frames()
    df_feat = pd.concat([df_train, df_valid, df_test], ignore_index=True)
    feature_columns = list(manifest["feature_columns"])
    required_critical_columns = list(manifest["required_critical_columns"])
    required_noncritical_columns = list(manifest["required_noncritical_columns"])
    normalization_manifest = dict(manifest.get("normalization_manifest") or {})
    split = manifest["split_boundaries"]

    if df_train.empty:
        raise RuntimeError("Empty training split.")

    if args.target_mode == "q":
        predictor_hyperparameters = deepcopy(
            PREDICTOR_HYPERPARAMETER_PROFILES[args.predictor_hyperparameters_profile]
        )
        training_started = time.perf_counter()
        model_artifact = fit_autogluon_q_model(
            df_train=df_train,
            df_valid=df_valid,
            feature_columns=feature_columns,
            required_critical_columns=required_critical_columns,
            required_noncritical_columns=required_noncritical_columns,
            feature_semantics_version=FEATURE_SEMANTICS_VERSION,
            normalization_manifest=normalization_manifest,
            calibration_mode=args.calibration_mode,
            grouped_calibration_column=args.grouped_calibration_column,
            grouped_calibration_min_rows=args.grouped_calibration_min_rows,
            bundle_dir=artifact_paths.model_bundle_dir,
            full_bundle_dir=artifact_paths.full_model_bundle_dir,
            artifact_mode=pipeline_config.artifact_mode,
            split_boundaries=split,
            predictor_presets=predictor_presets,
            time_limit=args.predictor_time_limit,
            random_seed=args.random_seed,
            predictor_hyperparameters=predictor_hyperparameters,
            num_bag_folds=args.num_bag_folds,
            num_bag_sets=args.num_bag_sets,
            num_stack_levels=args.num_stack_levels,
            auto_stack=args.auto_stack,
            refit_full=args.refit_full,
            deploy_optimized=(pipeline_config.artifact_mode == "online"),
            calibration_holdout_policy="explicit_valid_split",
        )
        training_wall_clock_sec = time.perf_counter() - training_started
        print(f"[INFO] Saved AutoGluon q deployment bundle to {artifact_paths.model_bundle_dir}")
        print(f"[INFO] Saved AutoGluon q full training bundle to {artifact_paths.full_model_bundle_dir}")
    elif args.target_mode == "residual_q":
        if pipeline_config.artifact_mode != "offline":
            raise ValueError("Research target_mode='residual_q' is only supported in offline mode.")
        model_artifact = fit_regression_payload(
            df_train,
            feature_columns=feature_columns,
            target_column="residual_q_target",
        )
    else:
        if pipeline_config.artifact_mode != "offline":
            raise ValueError(f"Research target_mode='{args.target_mode}' is only supported in offline mode.")
        target_column = "expected_pnl_target" if args.target_mode == "expected_pnl" else "expected_roi_target"
        model_artifact = fit_regression_payload(
            df_train,
            feature_columns=feature_columns,
            target_column=target_column,
        )

    if args.target_mode != "q":
        model_artifact["artifact_mode"] = pipeline_config.artifact_mode
        model_artifact["target_mode"] = args.target_mode
        model_artifact["split_boundaries"] = split
        save_model(model_artifact, artifact_paths.legacy_model_path)

    metrics_by_split = export_predictions(
        model_artifact,
        df_feat,
        artifact_paths,
        pipeline_config.artifact_mode,
        args.target_mode,
        pipeline_config.publish.prediction_publish_split,
        pipeline_config.publish.fail_if_publish_split_empty,
    )
    training_summary = {
        "artifact_mode": pipeline_config.artifact_mode,
        "calibration_mode": args.calibration_mode,
        "target_mode": args.target_mode,
        "feature_count": len(feature_columns),
        "rows_by_split": df_feat["dataset_split"].value_counts().to_dict(),
        "metrics_by_split": metrics_by_split,
        "boundaries": split,
        "publish_split": pipeline_config.publish.prediction_publish_split,
        "debug_filters": {"max_rows": pipeline_config.max_rows, "recent_days": pipeline_config.recent_days},
        "feature_export_manifest_path": str(FEATURE_EXPORT_MANIFEST_PATH),
        "feature_export_rows": manifest.get("rows", {}),
    }
    if args.target_mode == "q":
        training_summary["model_artifact"] = {
            "backend": "autogluon_q_bundle",
            "bundle_dir": str(artifact_paths.model_bundle_dir),
            "deploy_bundle_dir": str(artifact_paths.model_bundle_dir),
            "full_bundle_dir": str(artifact_paths.full_model_bundle_dir),
            "predictor_presets": predictor_presets,
            "predictor_hyperparameters_profile": args.predictor_hyperparameters_profile,
            "predictor_time_limit": args.predictor_time_limit,
            "random_seed": int(args.random_seed),
            "num_bag_folds": args.num_bag_folds,
            "num_bag_sets": args.num_bag_sets,
            "num_stack_levels": args.num_stack_levels,
            "auto_stack": args.auto_stack,
            "refit_full": bool(args.refit_full),
            "grouped_calibration_column": args.grouped_calibration_column,
            "grouped_calibration_min_rows": int(args.grouped_calibration_min_rows),
            "runtime_manifest": model_artifact.runtime_manifest,
            "feature_semantics_version": model_artifact.runtime_manifest.get("feature_semantics_version"),
            "normalization_manifest": model_artifact.runtime_manifest.get("normalization_manifest"),
            "calibrator_meta": model_artifact.calibrator_meta,
            "deployment_summary_path": str(artifact_paths.model_bundle_dir / "metadata" / "deployment_summary.json"),
            "full_training_summary_path": str(artifact_paths.full_model_bundle_dir / "metadata" / "deployment_summary.json"),
            "training_wall_clock_sec": float(training_wall_clock_sec),
        }
    else:
        training_summary["model_artifact"] = {
            "backend": "legacy_payload",
            "path": str(artifact_paths.legacy_model_path),
        }
    write_json(
        artifact_paths.model_training_summary_path,
        training_summary,
    )
    write_json(
        artifact_paths.docs_model_training_summary_path,
        training_summary,
    )

    print(f"[INFO] Saved published predictions to {artifact_paths.predictions_path}")
    print(f"[INFO] Saved full diagnostic predictions to {artifact_paths.predictions_full_path}")
    print(f"[INFO] Saved training summary to {artifact_paths.model_training_summary_path}")


if __name__ == "__main__":
    main()
