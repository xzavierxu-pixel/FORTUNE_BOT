from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.datasets.snapshots import load_raw_markets, load_research_snapshots
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import apply_feature_variant, build_market_feature_cache
from rule_baseline.models import fit_autogluon_q_model
from rule_baseline.training.train_snapshot_model import (
    DROP_COLS,
    add_training_targets,
    build_feature_table,
    load_rules,
    probability_metrics,
)
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged


SEED_SWEEP_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": f"seed_{seed}",
        "kind": "seed_sweep",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": seed,
    }
    for seed in [7, 13, 21, 42, 77, 101, 202, 314]
]

SEARCH_DESIGN_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": "search_gbm_cat_bag5_stack1_300_seed42",
        "kind": "search_design",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 300,
        "random_seed": 42,
        "num_bag_folds": 5,
        "num_stack_levels": 1,
        "predictor_hyperparameters": {
            "GBM": [{}, {"extra_trees": True, "ag_args": {"name_suffix": "XT"}}],
            "CAT": [{}],
        },
    },
    {
        "experiment": "search_gbm_cat_xgb_bag5_stack1_300_seed42",
        "kind": "search_design",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 300,
        "random_seed": 42,
        "num_bag_folds": 5,
        "num_stack_levels": 1,
        "predictor_hyperparameters": {
            "GBM": [{}, {"extra_trees": True, "ag_args": {"name_suffix": "XT"}}],
            "CAT": [{}],
            "XGB": [{}],
        },
    },
    {
        "experiment": "search_default_bag5_stack1_300_seed42",
        "kind": "search_design",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 300,
        "random_seed": 42,
        "num_bag_folds": 5,
        "num_stack_levels": 1,
    },
]

FEATURE_ABLATION_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": "feature_baseline_seed202",
        "kind": "feature_ablation",
        "feature_set": "baseline",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
    {
        "experiment": "feature_noisy_removed_seed202",
        "kind": "feature_ablation",
        "feature_set": "noisy_removed",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
    {
        "experiment": "feature_structured_only_seed202",
        "kind": "feature_ablation",
        "feature_set": "structured_only",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
]

FEATURE_ADDITION_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": "feature_add_baseline_seed202",
        "kind": "feature_addition",
        "feature_variant": "baseline",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
    {
        "experiment": "feature_add_interactions_seed202",
        "kind": "feature_addition",
        "feature_variant": "interaction_features",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
    {
        "experiment": "feature_add_interactions_textlite_seed202",
        "kind": "feature_addition",
        "feature_variant": "interaction_plus_textlite",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
]

FEATURE_ADDITION_SEED_SWEEP_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": f"feature_add_interactions_seed_{seed}",
        "kind": "feature_addition_seed_sweep",
        "feature_variant": "interaction_features",
        "predictor_presets": "medium_quality",
        "calibration_mode": "grouped_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": seed,
    }
    for seed in [21, 42, 77, 101, 202, 314]
]

CALIBRATION_EXPANSION_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": f"calibration_{mode}_seed21",
        "kind": "calibration_expansion",
        "predictor_presets": "medium_quality",
        "calibration_mode": mode,
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 21,
    }
    for mode in [
        "none",
        "global_isotonic",
        "grouped_isotonic",
        "global_sigmoid",
        "grouped_sigmoid",
        "beta_calibration",
        "blend_raw_global_isotonic_15",
        "blend_raw_global_isotonic_25",
        "blend_raw_beta_15",
        "blend_raw_beta_25",
    ]
]

FEATURE_OPTIMIZATION_EXPERIMENTS: list[dict[str, Any]] = [
    {
        "experiment": "feature_opt_baseline_seed202",
        "kind": "feature_optimization",
        "feature_variant": "baseline",
        "predictor_presets": "medium_quality",
        "calibration_mode": "global_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
    {
        "experiment": "feature_opt_market_structure_v2_seed202",
        "kind": "feature_optimization",
        "feature_variant": "market_structure_v2",
        "predictor_presets": "medium_quality",
        "calibration_mode": "global_isotonic",
        "grouped_calibration_column": "horizon_hours",
        "grouped_calibration_min_rows": 20,
        "predictor_time_limit": 120,
        "random_seed": 202,
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run round-3 AutoGluon optimization experiments.")
    parser.add_argument("--suite", choices=["seed_sweep", "search_design", "feature_ablation", "feature_addition", "feature_addition_seed_sweep", "calibration_expansion", "feature_optimization", "all"], default="all")
    parser.add_argument("--artifact-mode", choices=["offline"], default="offline")
    return parser.parse_args()


def _prepare_feature_frame(artifact_mode: str) -> tuple[pd.DataFrame, list[str], dict[str, Any]]:
    rebuild_canonical_merged()
    artifact_paths = build_artifact_paths(artifact_mode)
    snapshots = load_research_snapshots()
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_artifact_split(snapshots, artifact_mode=artifact_mode)
    snapshots = assign_dataset_split(snapshots, split)
    snapshots = snapshots[snapshots["dataset_split"].isin(["train", "valid", "test"])].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)

    df_feat = build_feature_table(snapshots, market_feature_cache, market_annotations, rules)
    if df_feat.empty:
        raise RuntimeError("No feature rows available for AutoGluon experiments.")
    df_feat = add_training_targets(df_feat)
    feature_columns = [column for column in df_feat.columns if column not in DROP_COLS]
    return df_feat, feature_columns, split.to_dict()


def _select_feature_columns(feature_columns: list[str], feature_set: str) -> list[str]:
    if feature_set == "baseline":
        return list(feature_columns)

    noisy_columns = {
        "question_market",
        "description_market",
        "groupItemTitle_market",
        "gameId_market",
        "marketMakerAddress_market",
        "source_url_market",
        "source_host_market",
        "domain_parsed_market",
        "sub_domain_market",
        "startDate_market",
        "endDate_market",
        "closedTime_market",
    }
    if feature_set == "noisy_removed":
        return [column for column in feature_columns if column not in noisy_columns]

    if feature_set == "structured_only":
        structured_explicit = {
            "price",
            "horizon_hours",
            "log_horizon",
            "q_smooth",
            "rule_score",
            "q_full",
            "price_min",
            "price_max",
            "h_min",
            "h_max",
            "direction",
            "leaf_id",
            "group_key",
            "domain",
            "category",
            "market_type",
            "domain_parsed",
            "category_raw",
            "category_parsed",
            "sub_domain",
            "source_host",
            "outcome_pattern",
            "negRisk",
            "volume",
            "liquidity",
            "volume24hr",
            "volume1wk",
            "volume24hrClob",
            "volume1wkClob",
            "bestBid",
            "bestAsk",
            "spread",
            "lastTradePrice",
            "line",
            "oneHourPriceChange",
            "oneDayPriceChange",
            "oneWeekPriceChange",
            "liquidityAmm",
            "liquidityClob",
            "selected_quote_offset_sec",
            "selected_quote_points_in_window",
            "selected_quote_left_gap_sec",
            "selected_quote_right_gap_sec",
            "selected_quote_local_gap_sec",
            "stale_quote_flag",
            "snapshot_quality_score",
            "delta_hours_bucket",
            "market_duration_hours",
            "orderPriceMinTickSize",
            "rewardsMinSize",
            "rewardsMaxSpread",
            "category_override_flag",
            "primary_outcome",
            "secondary_outcome",
            "selected_quote_side",
        }
        structured_prefixes = (
            "selected_quote_",
            "price_change_",
            "quoted_",
            "log_",
            "clob_share_",
            "vol_",
            "activity",
            "engagement",
            "momentum",
            "book_imbalance",
            "cap_ratio",
        )
        selected = [
            column
            for column in feature_columns
            if column in structured_explicit or column.startswith(structured_prefixes)
        ]
        return selected

    raise ValueError(f"Unsupported feature_set: {feature_set}")


def _run_single_experiment(
    *,
    experiment: dict[str, Any],
    df_feat: pd.DataFrame,
    feature_columns: list[str],
    split_boundaries: dict[str, Any],
) -> list[dict[str, Any]]:
    df_train = df_feat[df_feat["dataset_split"] == "train"].copy()
    df_valid = df_feat[df_feat["dataset_split"] == "valid"].copy()
    rows: list[dict[str, Any]] = []
    started = time.perf_counter()
    with TemporaryDirectory() as tmpdir:
        result = fit_autogluon_q_model(
            df_train=df_train,
            df_valid=df_valid,
            feature_columns=feature_columns,
            bundle_dir=Path(tmpdir) / experiment["experiment"],
            artifact_mode="offline",
            split_boundaries=split_boundaries,
            calibration_mode=str(experiment["calibration_mode"]),
            predictor_presets=str(experiment["predictor_presets"]),
            time_limit=int(experiment["predictor_time_limit"]),
            random_seed=int(experiment["random_seed"]),
            grouped_calibration_column=str(experiment["grouped_calibration_column"]),
            grouped_calibration_min_rows=int(experiment["grouped_calibration_min_rows"]),
            predictor_hyperparameters=experiment.get("predictor_hyperparameters"),
            num_bag_folds=experiment.get("num_bag_folds"),
            num_bag_sets=experiment.get("num_bag_sets"),
            num_stack_levels=experiment.get("num_stack_levels"),
            auto_stack=experiment.get("auto_stack"),
        )
        fit_seconds = time.perf_counter() - started
        for split_name in ["train", "valid", "test"]:
            split_df = df_feat[df_feat["dataset_split"] == split_name].copy()
            q_pred = result.predict(split_df)
            metric_frame = split_df[["y", "price"]].copy()
            metric_frame["q_pred"] = q_pred
            metrics = probability_metrics(metric_frame)
            rows.append(
                {
                    "experiment": experiment["experiment"],
                    "kind": experiment["kind"],
                    "split": split_name,
                    "fit_seconds": fit_seconds,
                    "predictor_name": result.predictor_name,
                    "predictor_presets": experiment["predictor_presets"],
                    "predictor_time_limit": experiment["predictor_time_limit"],
                    "random_seed": experiment["random_seed"],
                    "calibration_mode": experiment["calibration_mode"],
                    "grouped_calibration_column": experiment["grouped_calibration_column"],
                    "grouped_calibration_min_rows": experiment["grouped_calibration_min_rows"],
                    "num_bag_folds": experiment.get("num_bag_folds"),
                    "num_bag_sets": experiment.get("num_bag_sets"),
                    "num_stack_levels": experiment.get("num_stack_levels"),
                    "auto_stack": experiment.get("auto_stack"),
                    "predictor_hyperparameters": json.dumps(
                        experiment.get("predictor_hyperparameters"),
                        ensure_ascii=False,
                        sort_keys=True,
                    )
                    if experiment.get("predictor_hyperparameters") is not None
                    else "None",
                    "feature_set": experiment.get("feature_set", "baseline"),
                    "feature_variant": experiment.get("feature_variant", "baseline"),
                    "feature_count": len(feature_columns),
                    "logloss_model": metrics["logloss_model"],
                    "brier_model": metrics["brier_model"],
                    "auc_model": metrics["auc_model"],
                }
            )
    return rows


def _write_outputs(
    *,
    result_rows: list[dict[str, Any]],
    output_csv: Path,
    output_json: Path,
    feature_rows: int,
    rows_by_split: dict[str, int],
    feature_count: int,
) -> None:
    result_df = pd.DataFrame(result_rows)
    result_df.to_csv(output_csv, index=False)
    test_df = (
        result_df[result_df["split"] == "test"]
        .sort_values(["logloss_model", "brier_model"], ascending=[True, True])
        .reset_index(drop=True)
    )
    summary = {
        "feature_rows": int(feature_rows),
        "rows_by_split": {key: int(value) for key, value in rows_by_split.items()},
        "feature_count": int(feature_count),
        "best_test_experiment": test_df.iloc[0].to_dict() if not test_df.empty else None,
        "test_ranking": test_df.to_dict(orient="records"),
    }
    write_json(output_json, summary)


def main() -> None:
    args = parse_args()
    df_feat, feature_columns, split_boundaries = _prepare_feature_frame(args.artifact_mode)

    experiments: list[dict[str, Any]] = []
    if args.suite in {"seed_sweep", "all"}:
        experiments.extend(SEED_SWEEP_EXPERIMENTS)
    if args.suite in {"search_design", "all"}:
        experiments.extend(SEARCH_DESIGN_EXPERIMENTS)
    if args.suite in {"feature_ablation", "all"}:
        experiments.extend(FEATURE_ABLATION_EXPERIMENTS)
    if args.suite in {"feature_addition", "all"}:
        experiments.extend(FEATURE_ADDITION_EXPERIMENTS)
    if args.suite in {"feature_addition_seed_sweep", "all"}:
        experiments.extend(FEATURE_ADDITION_SEED_SWEEP_EXPERIMENTS)
    if args.suite in {"calibration_expansion", "all"}:
        experiments.extend(CALIBRATION_EXPANSION_EXPERIMENTS)
    if args.suite in {"feature_optimization", "all"}:
        experiments.extend(FEATURE_OPTIMIZATION_EXPERIMENTS)

    result_rows: list[dict[str, Any]] = []
    for experiment in experiments:
        print(
            f"[INFO] Running {experiment['kind']} experiment={experiment['experiment']} "
            f"seed={experiment['random_seed']} time_limit={experiment['predictor_time_limit']}"
        )
        experiment_df_feat = apply_feature_variant(
            df_feat,
            feature_variant=str(experiment.get("feature_variant", "baseline")),
        )
        experiment_feature_columns = [column for column in experiment_df_feat.columns if column not in DROP_COLS]
        selected_feature_columns = _select_feature_columns(
            experiment_feature_columns,
            str(experiment.get("feature_set", "baseline")),
        )
        rows = _run_single_experiment(
            experiment=experiment,
            df_feat=experiment_df_feat,
            feature_columns=selected_feature_columns,
            split_boundaries=split_boundaries,
        )
        result_rows.extend(rows)

    artifact_paths = build_artifact_paths(args.artifact_mode)
    if args.suite == "seed_sweep":
        output_csv = artifact_paths.analysis_dir / "autogluon_seed_sweep_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_seed_sweep_summary.json"
    elif args.suite == "search_design":
        output_csv = artifact_paths.analysis_dir / "autogluon_search_design_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_search_design_summary.json"
    elif args.suite == "feature_ablation":
        output_csv = artifact_paths.analysis_dir / "autogluon_feature_ablation_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_feature_ablation_summary.json"
    elif args.suite == "feature_addition":
        output_csv = artifact_paths.analysis_dir / "autogluon_feature_addition_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_feature_addition_summary.json"
    elif args.suite == "feature_addition_seed_sweep":
        output_csv = artifact_paths.analysis_dir / "autogluon_feature_addition_seed_sweep_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_feature_addition_seed_sweep_summary.json"
    elif args.suite == "calibration_expansion":
        output_csv = artifact_paths.analysis_dir / "autogluon_calibration_expansion_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_calibration_expansion_summary.json"
    elif args.suite == "feature_optimization":
        output_csv = artifact_paths.analysis_dir / "autogluon_feature_optimization_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_feature_optimization_summary.json"
    else:
        output_csv = artifact_paths.analysis_dir / "autogluon_round3_results.csv"
        output_json = artifact_paths.analysis_dir / "autogluon_round3_summary.json"

    _write_outputs(
        result_rows=result_rows,
        output_csv=output_csv,
        output_json=output_json,
        feature_rows=len(df_feat),
        rows_by_split=df_feat["dataset_split"].value_counts().to_dict(),
        feature_count=len(feature_columns),
    )
    print(f"[INFO] Saved experiment rows to {output_csv}")
    print(f"[INFO] Saved summary to {output_json}")


if __name__ == "__main__":
    main()
