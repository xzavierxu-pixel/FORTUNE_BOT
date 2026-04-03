from __future__ import annotations

import argparse
import os
import sys

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache, preprocess_features
from rule_baseline.models import fit_model_payload, fit_regression_payload, predict_probabilities, predict_regression
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged

TRAIN_PRICE_MIN = 0.2
TRAIN_PRICE_MAX = 0.8

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
    "is_date_based",
    "vol_x_sentiment",
    "cat_entertainment_str",
    "startDate_market",
    "endDate_market",
    "closedTime_market",
    "duration_is_negative_flag",
    "duration_below_min_horizon_flag",
    "price_in_range_flag",
    "liquidity",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train the snapshot ensemble model with strict split isolation.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument(
        "--calibration-mode",
        choices=[
            "valid_isotonic",
            "valid_sigmoid",
            "domain_valid_isotonic",
            "horizon_valid_isotonic",
            "cv_isotonic",
            "cv_sigmoid",
            "none",
        ],
        default="horizon_valid_isotonic",
        help="Calibration strategy for probability outputs.",
    )
    parser.add_argument(
        "--target-mode",
        choices=["q", "residual_q", "expected_pnl", "expected_roi"],
        default="q",
        help="Training target for the main snapshot model.",
    )
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
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
        "rule_score",
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

    merged = context.merge(
        rules[
            [
                "domain",
                "category",
                "market_type",
                "leaf_id",
                "price_min",
                "price_max",
                "h_min",
                "h_max",
                "q_full",
                "rule_score",
                "direction",
                "group_key",
            ]
        ],
        on=["domain", "category", "market_type"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()

    mask = (
        (merged["price"] >= merged["price_min"] - 1e-9)
        & (merged["price"] <= merged["price_max"] + 1e-9)
        & (merged["horizon_hours"] >= merged["h_min"])
        & (merged["horizon_hours"] <= merged["h_max"])
    )
    matched = merged[mask].copy()
    if matched.empty:
        return pd.DataFrame()

    matched = matched.sort_values(["market_id", "snapshot_time", "rule_score"], ascending=[True, True, False])
    return matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)


def build_feature_table(
    snapshots: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    market_annotations: pd.DataFrame,
    rules: pd.DataFrame,
) -> pd.DataFrame:
    matched = match_snapshots_to_rules(snapshots, market_annotations, rules)
    if matched.empty:
        return pd.DataFrame()
    return preprocess_features(matched, market_feature_cache)


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
    direction = df["direction"].astype(int).values
    price = df["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    q_value = np.asarray(q_pred, dtype=float).clip(1e-6, 1 - 1e-6)
    return np.where(
        direction > 0,
        q_value / price - 1.0 - config.FEE_RATE,
        (price - q_value) / np.maximum(1.0 - price, 1e-6) - config.FEE_RATE,
    )


def infer_q_from_trade_value(df: pd.DataFrame, trade_value_pred: np.ndarray) -> np.ndarray:
    direction = df["direction"].astype(int).values
    price = df["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    q_pred = np.where(
        direction > 0,
        price * (trade_value_pred + 1.0 + config.FEE_RATE),
        price - (1.0 - price) * (trade_value_pred + config.FEE_RATE),
    )
    return np.clip(q_pred, 0.0, 1.0)


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


def export_predictions(payload: dict, df_feat: pd.DataFrame, artifact_paths, artifact_mode: str) -> dict:
    target_mode = payload.get("target_mode", "q")
    if target_mode == "q":
        q_pred = predict_probabilities(payload, df_feat)
        trade_value_pred = compute_trade_value_from_q(df_feat, q_pred)
    elif target_mode == "residual_q":
        residual_pred = predict_regression(payload, df_feat)
        q_pred = np.clip(df_feat["price"].astype(float).values + residual_pred, 0.0, 1.0)
        trade_value_pred = compute_trade_value_from_q(df_feat, q_pred)
    else:
        trade_value_pred = predict_regression(payload, df_feat)
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
        "rule_score",
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

    publish_df = out[out["dataset_split"] == "test"].copy() if artifact_mode == "offline" else out.copy()
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
    artifact_paths = build_artifact_paths(args.artifact_mode)

    rebuild_canonical_merged()
    snapshots = load_research_snapshots(
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        max_rows=args.max_rows,
        recent_days=args.recent_days,
    )
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_artifact_split(
        snapshots,
        artifact_mode=args.artifact_mode,
        reference_end=args.split_reference_end,
        history_start_override=args.history_start,
    )
    snapshots = assign_dataset_split(snapshots, split)
    allowed_splits = ["train", "valid", "test"] if args.artifact_mode == "offline" else ["train", "valid"]
    snapshots = snapshots[snapshots["dataset_split"].isin(allowed_splits)].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)

    df_feat = build_feature_table(snapshots, market_feature_cache, market_annotations, rules)
    if df_feat.empty:
        raise RuntimeError("No feature rows available after rule matching.")
    df_feat = add_training_targets(df_feat)

    feature_columns = [column for column in df_feat.columns if column not in DROP_COLS]
    if args.artifact_mode == "offline":
        df_train = df_feat[df_feat["dataset_split"] == "train"].copy()
        df_valid = df_feat[df_feat["dataset_split"] == "valid"].copy()
    else:
        df_train = df_feat.copy()
        df_valid = pd.DataFrame(columns=df_feat.columns)

    if df_train.empty:
        raise RuntimeError("Empty training split.")

    if args.target_mode == "q":
        payload = fit_model_payload(
            df_train,
            df_valid,
            feature_columns=feature_columns,
            target_column="y",
            calibration_mode=args.calibration_mode,
        )
    elif args.target_mode == "residual_q":
        payload = fit_regression_payload(
            df_train,
            feature_columns=feature_columns,
            target_column="residual_q_target",
        )
    else:
        target_column = "expected_pnl_target" if args.target_mode == "expected_pnl" else "expected_roi_target"
        payload = fit_regression_payload(
            df_train,
            feature_columns=feature_columns,
            target_column=target_column,
        )

    payload["artifact_mode"] = args.artifact_mode
    payload["target_mode"] = args.target_mode
    payload["split_boundaries"] = split.to_dict()
    save_model(payload, artifact_paths.model_path)

    metrics_by_split = export_predictions(payload, df_feat, artifact_paths, args.artifact_mode)
    write_json(
        artifact_paths.model_training_summary_path,
        {
            "artifact_mode": args.artifact_mode,
            "calibration_mode": args.calibration_mode,
            "target_mode": args.target_mode,
            "feature_count": len(feature_columns),
            "rows_by_split": df_feat["dataset_split"].value_counts().to_dict(),
            "metrics_by_split": metrics_by_split,
            "boundaries": split.to_dict(),
            "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
        },
    )

    print(f"[INFO] Saved published predictions to {artifact_paths.predictions_path}")
    print(f"[INFO] Saved full diagnostic predictions to {artifact_paths.predictions_full_path}")
    print(f"[INFO] Saved training summary to {artifact_paths.model_training_summary_path}")


if __name__ == "__main__":
    main()
