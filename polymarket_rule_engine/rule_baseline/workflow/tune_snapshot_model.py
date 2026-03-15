from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.analysis.compare_baseline_families import compute_slice_metrics, run_flat_backtest
from rule_baseline.backtesting.backtest_execution_parity import (
    ExecutionParityConfig,
    compute_capital_timing_audit,
    compute_filter_breakdown,
    compute_summary,
    run_execution_parity_backtest,
)
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.models import DEFAULT_CLASSIFIER_PARAMS, fit_model_payload, predict_probabilities
from rule_baseline.training.train_snapshot_model import (
    DROP_COLS,
    add_training_targets,
    build_feature_table,
    compute_trade_value_from_q,
    load_rules,
    probability_metrics,
    trade_value_metrics,
)
from rule_baseline.utils import config

TOP_K = 50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tune snapshot ensemble hyperparameters under a fixed offline split.")
    parser.add_argument("--artifact-mode", choices=["offline"], default="offline")
    parser.add_argument("--target-mode", choices=["q"], default="q")
    parser.add_argument("--split-reference-end", type=str, required=True)
    parser.add_argument("--history-start", type=str, required=True)
    parser.add_argument("--top-k", type=int, default=TOP_K)
    parser.add_argument("--top-stage2", type=int, default=3)
    parser.add_argument("--top-execution-backtests", type=int, default=5)
    parser.add_argument("--run-label", type=str, default=None)
    return parser.parse_args()


def build_stage1_model_grid() -> list[dict]:
    return [
        {"name": "baseline", "params": {}},
        {
            "name": "conservative_tree",
            "params": {
                "xgb": {"max_depth": 4, "min_child_weight": 10, "reg_lambda": 5.0},
                "lgbm": {"num_leaves": 31, "min_child_samples": 100, "reg_lambda": 5.0},
                "cat": {"depth": 5, "l2_leaf_reg": 10.0},
            },
        },
        {
            "name": "strong_regularization",
            "params": {
                "xgb": {"reg_lambda": 10.0, "min_child_weight": 12},
                "lgbm": {"reg_lambda": 10.0, "min_child_samples": 150},
                "cat": {"l2_leaf_reg": 12.0},
            },
        },
        {
            "name": "slow_learning",
            "params": {
                "xgb": {"learning_rate": 0.02, "n_estimators": 800},
                "lgbm": {"learning_rate": 0.02, "n_estimators": 800},
                "cat": {"learning_rate": 0.02, "iterations": 800},
            },
        },
        {
            "name": "low_subsample",
            "params": {
                "xgb": {"subsample": 0.7, "colsample_bytree": 0.7, "reg_lambda": 5.0},
                "lgbm": {"subsample": 0.7, "colsample_bytree": 0.7, "reg_lambda": 5.0, "min_child_samples": 100},
                "cat": {"l2_leaf_reg": 8.0},
            },
        },
        {
            "name": "high_child_samples",
            "params": {
                "xgb": {"min_child_weight": 15, "max_depth": 5},
                "lgbm": {"min_child_samples": 200, "num_leaves": 31},
                "cat": {"depth": 5, "l2_leaf_reg": 8.0},
            },
        },
        {
            "name": "higher_capacity",
            "params": {
                "xgb": {"max_depth": 8, "min_child_weight": 5},
                "lgbm": {"num_leaves": 127, "min_child_samples": 25},
                "cat": {"depth": 8, "l2_leaf_reg": 5.0},
            },
        },
        {
            "name": "faster_learning",
            "params": {
                "xgb": {"learning_rate": 0.05, "n_estimators": 300},
                "lgbm": {"learning_rate": 0.05, "n_estimators": 300},
                "cat": {"learning_rate": 0.05, "iterations": 300},
            },
        },
    ]


def build_stage1_calibration_grid() -> list[str]:
    return ["none", "valid_sigmoid", "valid_isotonic"]


def build_stage2_calibration_grid() -> list[str]:
    return ["horizon_valid_isotonic", "domain_valid_isotonic"]


def make_run_label(label: str | None) -> str:
    if label:
        return label
    return datetime.now(timezone.utc).strftime("tuning_%Y%m%dT%H%M%SZ")


def prepare_dataset(args: argparse.Namespace):
    artifact_paths = build_artifact_paths(args.artifact_mode)
    snapshots = load_research_snapshots()
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_artifact_split(
        snapshots,
        artifact_mode=args.artifact_mode,
        reference_end=args.split_reference_end,
        history_start_override=args.history_start,
    )
    snapshots = assign_dataset_split(snapshots, split)
    snapshots = snapshots[snapshots["dataset_split"].isin(["train", "valid", "test"])].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)

    df_feat = build_feature_table(snapshots, market_feature_cache, market_annotations, rules)
    if df_feat.empty:
        raise RuntimeError("No feature rows available after rule matching.")
    df_feat = add_training_targets(df_feat)

    feature_columns = [column for column in df_feat.columns if column not in DROP_COLS]
    df_train = df_feat[df_feat["dataset_split"] == "train"].copy()
    df_valid = df_feat[df_feat["dataset_split"] == "valid"].copy()
    return artifact_paths, split, market_feature_cache, rules, snapshots, df_feat, df_train, df_valid, feature_columns


def build_prediction_frame(df_feat: pd.DataFrame, q_pred: np.ndarray) -> pd.DataFrame:
    out = df_feat.copy()
    out["q_pred"] = q_pred
    out["trade_value_pred"] = compute_trade_value_from_q(out, q_pred)
    out["edge_prob"] = out["q_pred"] - out["price"]
    out["rule_direction"] = out["direction"].astype(int)
    out["rule_group_key"] = out["group_key"]
    out["rule_leaf_id"] = out["leaf_id"]
    out["signed_edge_true"] = np.where(
        out["rule_direction"] > 0,
        out["y"] - out["price"],
        out["price"] - out["y"],
    )
    out["tradeable_label"] = (out["signed_edge_true"] > 0).astype(int)
    out["signed_edge_pred"] = np.where(
        out["rule_direction"] > 0,
        out["q_pred"] - out["price"],
        out["price"] - out["q_pred"],
    )
    out["score_model"] = out["signed_edge_pred"]
    out["trade_model"] = out["score_model"] > 0
    return out


def evaluate_prediction_frame(df_pred: pd.DataFrame, top_k: int) -> dict[str, float | int | None]:
    metrics: dict[str, float | int | None] = {}
    for split_name in ["train", "valid", "test"]:
        split_df = df_pred[df_pred["dataset_split"] == split_name].copy()
        prob = probability_metrics(split_df)
        trade = trade_value_metrics(split_df)
        rank = compute_slice_metrics(split_df, "score_model", "trade_model", top_k)
        for source in [prob, trade, rank]:
            for key, value in source.items():
                metrics[f"{split_name}_{key}"] = value
    flat_backtest = run_flat_backtest(df_pred, "score_model", "trade_model")
    for key, value in flat_backtest.items():
        metrics[f"flat_{key}"] = value
    return metrics


def score_stage1_row(row: pd.Series) -> tuple:
    valid_logloss = row.get("valid_logloss_model", np.inf)
    valid_brier = row.get("valid_brier_model", np.inf)
    valid_topk = row.get("valid_top_k_mean_signed_edge", -np.inf)
    valid_precision = row.get("valid_precision_at_k", -np.inf)
    return (
        float(valid_logloss if pd.notna(valid_logloss) else np.inf),
        float(valid_brier if pd.notna(valid_brier) else np.inf),
        -float(valid_topk if pd.notna(valid_topk) else -np.inf),
        -float(valid_precision if pd.notna(valid_precision) else -np.inf),
    )


def run_experiment(
    experiment_name: str,
    calibration_mode: str,
    model_hyperparams: dict,
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    df_feat: pd.DataFrame,
    feature_columns: list[str],
    top_k: int,
) -> tuple[dict, dict]:
    payload = fit_model_payload(
        df_train,
        df_valid,
        feature_columns=feature_columns,
        target_column="y",
        calibration_mode=calibration_mode,
        model_hyperparams=model_hyperparams,
    )
    q_pred = predict_probabilities(payload, df_feat)
    df_pred = build_prediction_frame(df_feat, q_pred)
    metrics = evaluate_prediction_frame(df_pred, top_k)
    row = {
        "experiment_name": experiment_name,
        "calibration_mode": calibration_mode,
        "model_name": experiment_name.split("__")[0],
        "model_hyperparams": json.dumps(model_hyperparams, ensure_ascii=False, sort_keys=True),
        "feature_count": len(feature_columns),
    }
    row.update(metrics)
    return row, payload


def run_execution_backtest_for_payload(
    snapshots: pd.DataFrame,
    rules: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    payload: dict,
    split_boundaries: dict,
    experiment_name: str,
    calibration_mode: str,
) -> dict[str, float | int | None]:
    cfg = ExecutionParityConfig()
    test_snapshots = snapshots[snapshots["dataset_split"] == "test"].copy()
    filter_breakdown, candidates = compute_filter_breakdown(test_snapshots, rules, market_feature_cache, payload, cfg)
    if candidates.empty:
        summary: dict[str, float | int | None] = {"total_trades": 0, "candidate_rows": 0}
    else:
        equity_df, trades_df, skipped_df, daily_df = run_execution_parity_backtest(candidates, cfg)
        summary = compute_summary(equity_df, trades_df, cfg)
        summary.update(compute_capital_timing_audit(trades_df))
        summary["candidate_rows"] = int(len(candidates))
        summary["candidate_markets"] = int(candidates["market_id"].nunique())
        summary["skipped_candidates"] = int(len(skipped_df))
        summary["active_entry_days"] = int((daily_df["executed_count"] > 0).sum()) if not daily_df.empty else 0
    summary["experiment_name"] = experiment_name
    summary["calibration_mode"] = calibration_mode
    summary["split_boundaries"] = json.dumps(split_boundaries, ensure_ascii=False, sort_keys=True)
    for key, value in filter_breakdown.items():
        summary[f"filter_{key}"] = value
    return summary


def save_results(output_dir: Path, name: str, rows: list[dict]) -> Path:
    path = output_dir / name
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def load_existing_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    return df.to_dict("records")


def main() -> None:
    args = parse_args()
    run_label = make_run_label(args.run_label)
    artifact_paths, split, market_feature_cache, rules, snapshots, df_feat, df_train, df_valid, feature_columns = prepare_dataset(args)

    tuning_dir = artifact_paths.analysis_dir / "tuning" / run_label
    tuning_dir.mkdir(parents=True, exist_ok=True)

    stage1_path = tuning_dir / "stage1_results.csv"
    stage2_path = tuning_dir / "stage2_results.csv"
    combined_path = tuning_dir / "all_results.csv"
    execution_path = tuning_dir / "execution_backtests.csv"

    stage1_rows = load_existing_rows(stage1_path)
    stage1_completed = {str(row["experiment_name"]) for row in stage1_rows if "experiment_name" in row}
    for model_spec in build_stage1_model_grid():
        for calibration_mode in build_stage1_calibration_grid():
            experiment_name = f"{model_spec['name']}__{calibration_mode}"
            if experiment_name in stage1_completed:
                print(f"[TUNING] Stage 1 already done: {experiment_name}")
                continue
            print(f"[TUNING] Stage 1: {experiment_name}")
            row, _ = run_experiment(
                experiment_name=experiment_name,
                calibration_mode=calibration_mode,
                model_hyperparams=model_spec["params"],
                df_train=df_train,
                df_valid=df_valid,
                df_feat=df_feat,
                feature_columns=feature_columns,
                top_k=args.top_k,
            )
            stage1_rows.append(row)
            stage1_completed.add(experiment_name)
            save_results(tuning_dir, "stage1_results.csv", stage1_rows)

    stage1_df = pd.DataFrame(stage1_rows)
    stage1_df = stage1_df.sort_values(
        by=["valid_logloss_model", "valid_brier_model", "valid_top_k_mean_signed_edge", "valid_precision_at_k"],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)
    stage1_path = save_results(tuning_dir, "stage1_results.csv", stage1_df.to_dict("records"))

    top_stage2_models = (
        stage1_df.assign(_score=stage1_df.apply(score_stage1_row, axis=1))
        .sort_values("_score")
        .drop_duplicates(subset=["model_name"], keep="first")
        .head(args.top_stage2)
    )

    stage2_rows = load_existing_rows(stage2_path)
    stage2_completed = {str(row["experiment_name"]) for row in stage2_rows if "experiment_name" in row}
    model_specs = {spec["name"]: spec for spec in build_stage1_model_grid()}
    for _, candidate_row in top_stage2_models.iterrows():
        model_name = str(candidate_row["model_name"])
        model_spec = model_specs[model_name]
        for calibration_mode in build_stage2_calibration_grid():
            experiment_name = f"{model_name}__{calibration_mode}"
            if experiment_name in stage2_completed:
                print(f"[TUNING] Stage 2 already done: {experiment_name}")
                continue
            print(f"[TUNING] Stage 2: {experiment_name}")
            row, _ = run_experiment(
                experiment_name=experiment_name,
                calibration_mode=calibration_mode,
                model_hyperparams=model_spec["params"],
                df_train=df_train,
                df_valid=df_valid,
                df_feat=df_feat,
                feature_columns=feature_columns,
                top_k=args.top_k,
            )
            stage2_rows.append(row)
            stage2_completed.add(experiment_name)
            save_results(tuning_dir, "stage2_results.csv", stage2_rows)

    combined_rows = stage1_rows + stage2_rows
    combined_df = pd.DataFrame(combined_rows)
    combined_df = combined_df.sort_values(
        by=["valid_logloss_model", "valid_brier_model", "valid_top_k_mean_signed_edge", "valid_precision_at_k"],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)
    combined_path = save_results(tuning_dir, "all_results.csv", combined_df.to_dict("records"))

    execution_rows = load_existing_rows(execution_path)
    execution_completed = {str(row["experiment_name"]) for row in execution_rows if "experiment_name" in row}
    top_execution_df = combined_df.head(args.top_execution_backtests)
    model_specs = {spec["name"]: spec for spec in build_stage1_model_grid()}
    for _, row in top_execution_df.iterrows():
        experiment_name = str(row["experiment_name"])
        if experiment_name in execution_completed:
            print(f"[TUNING] Execution parity already done: {experiment_name}")
            continue
        calibration_mode = str(row["calibration_mode"])
        print(f"[TUNING] Execution parity: {experiment_name}")
        model_name = str(row["model_name"])
        model_spec = model_specs[model_name]
        _, payload = run_experiment(
            experiment_name=experiment_name,
            calibration_mode=calibration_mode,
            model_hyperparams=model_spec["params"],
            df_train=df_train,
            df_valid=df_valid,
            df_feat=df_feat,
            feature_columns=feature_columns,
            top_k=args.top_k,
        )
        execution_rows.append(
            run_execution_backtest_for_payload(
                snapshots=snapshots,
                rules=rules,
                market_feature_cache=market_feature_cache,
                payload=payload,
                split_boundaries=split.to_dict(),
                experiment_name=experiment_name,
                calibration_mode=calibration_mode,
            )
        )
        execution_completed.add(experiment_name)
        save_results(tuning_dir, "execution_backtests.csv", execution_rows)
    execution_df = pd.DataFrame(execution_rows).sort_values(
        by=["total_roi", "sharpe_ratio", "win_rate"],
        ascending=[False, False, False],
    )
    execution_path = save_results(tuning_dir, "execution_backtests.csv", execution_df.to_dict("records"))

    summary = {
        "run_label": run_label,
        "artifact_mode": args.artifact_mode,
        "target_mode": args.target_mode,
        "top_k": args.top_k,
        "top_stage2": args.top_stage2,
        "top_execution_backtests": args.top_execution_backtests,
        "split_boundaries": split.to_dict(),
        "history_start": args.history_start,
        "split_reference_end": args.split_reference_end,
        "default_classifier_params": DEFAULT_CLASSIFIER_PARAMS,
        "stage1_models": [spec["name"] for spec in build_stage1_model_grid()],
        "stage1_calibrations": build_stage1_calibration_grid(),
        "stage2_calibrations": build_stage2_calibration_grid(),
        "artifacts": {
            "stage1_results": str(stage1_path),
            "all_results": str(combined_path),
            "execution_backtests": str(execution_path),
        },
    }
    write_json(tuning_dir / "run_summary.json", summary)

    print(f"[TUNING] Saved stage 1 results to {stage1_path}")
    print(f"[TUNING] Saved combined results to {combined_path}")
    print(f"[TUNING] Saved execution backtests to {execution_path}")
    print(f"[TUNING] Saved run summary to {tuning_dir / 'run_summary.json'}")


if __name__ == "__main__":
    main()
