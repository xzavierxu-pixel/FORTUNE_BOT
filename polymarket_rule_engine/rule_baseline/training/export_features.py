from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.audits.snapshot_training_audit import (
    build_snapshot_training_audit_payload,
    write_snapshot_training_audit,
)
from rule_baseline.contracts import apply_model_feature_gates, classify_feature_columns
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged
from rule_baseline.datasets.snapshots import load_online_parity_snapshots, load_raw_markets
from rule_baseline.datasets.splits import assign_configured_dataset_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.features.annotation_normalization import build_normalization_manifest, normalize_market_annotations
from rule_baseline.features.snapshot_semantics import (
    FEATURE_SEMANTICS_VERSION,
    online_feature_columns,
    split_feature_contract_columns,
)
from rule_baseline.models.autogluon_qmodel import _coerce_feature_frame
from rule_baseline.models.tree_ensembles import infer_feature_types
from rule_baseline.training.train_snapshot_model import (
    DROP_COLS,
    TRAIN_PRICE_MAX,
    TRAIN_PRICE_MIN,
    add_training_targets,
    build_feature_table,
    load_rules,
    load_serving_feature_bundle,
    normalize_predictor_presets,
)
from rule_baseline.utils import config
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config, required_export_splits

TRAIN_PARQUET_PATH = config.PROCESSED_DIR / "train.parquet"
VALID_PARQUET_PATH = config.PROCESSED_DIR / "valid.parquet"
TEST_PARQUET_PATH = config.PROCESSED_DIR / "test.parquet"
FEATURE_EXPORT_MANIFEST_PATH = config.PROCESSED_DIR / "feature_export_manifest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export resolved snapshot features for model training.")
    parser.add_argument("--pipeline-config", type=str, default=None)
    parser.add_argument("--calibration-mode", type=str, default="none")
    parser.add_argument("--target-mode", choices=["q", "residual_q", "expected_pnl", "expected_roi"], default="q")
    parser.add_argument("--predictor-presets", type=str, default="medium_quality")
    parser.add_argument("--predictor-time-limit", type=int, default=300)
    parser.add_argument("--predictor-hyperparameters-profile", type=str, default="gbm_cat")
    parser.add_argument("--random-seed", type=int, default=21)
    parser.add_argument("--grouped-calibration-column", type=str, default="horizon_hours")
    parser.add_argument("--grouped-calibration-min-rows", type=int, default=20)
    return parser.parse_args()


def _require_non_empty_split(df_export: pd.DataFrame, split_name: str) -> None:
    if df_export[df_export["dataset_split"] == split_name].empty:
        raise RuntimeError(f"Required split '{split_name}' is empty.")


def build_feature_exports(args: argparse.Namespace) -> dict:
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)

    rebuild_canonical_merged()
    snapshots_loaded = load_online_parity_snapshots(
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        max_rows=pipeline_config.max_rows,
        recent_days=pipeline_config.recent_days,
    )
    snapshots_quality = snapshots_loaded[snapshots_loaded["quality_pass"]].copy()
    snapshots = assign_configured_dataset_split(snapshots_quality, pipeline_config.split)
    snapshots = snapshots[snapshots["dataset_split"].isin(pipeline_config.split.allowed_splits)].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations_raw = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    normalization_manifest = build_normalization_manifest(market_annotations_raw)
    market_annotations = normalize_market_annotations(
        market_annotations_raw,
        vocabulary_manifest=normalization_manifest,
    )
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)
    serving_feature_bundle = load_serving_feature_bundle(artifact_paths)

    df_feat = build_feature_table(
        snapshots,
        market_feature_cache,
        market_annotations,
        rules,
        serving_feature_bundle=serving_feature_bundle,
    )
    if df_feat.empty:
        raise RuntimeError("No feature rows available after rule matching.")
    df_feat = add_training_targets(df_feat)

    contract_candidate_columns = online_feature_columns([column for column in df_feat.columns if column not in DROP_COLS])
    classified_columns = classify_feature_columns(contract_candidate_columns)
    train_mask = df_feat["dataset_split"].astype(str).eq("train")
    feature_columns, gate_diagnostics = apply_model_feature_gates(
        df_feat,
        feature_columns=classified_columns.model_feature_columns,
        train_mask=train_mask,
        min_binary_prevalence=0.01,
        max_binary_prevalence=0.99,
    )
    required_critical_columns, required_noncritical_columns = split_feature_contract_columns(feature_columns)
    numeric_columns, categorical_columns = infer_feature_types(df_feat, feature_columns)

    coerced_features = _coerce_feature_frame(df_feat, feature_columns, numeric_columns, categorical_columns)
    extra_columns = [
        column
        for column in ["y", "dataset_split", "price", "trade_value_true"]
        if column not in coerced_features.columns and column in df_feat.columns
    ]
    df_export = pd.concat([coerced_features, df_feat.loc[:, extra_columns].copy()], axis=1)

    for split_name in required_export_splits(pipeline_config):
        _require_non_empty_split(df_export, split_name)

    df_train = df_export[df_export["dataset_split"] == "train"].copy()
    df_valid = df_export[df_export["dataset_split"] == "valid"].copy()
    df_test = df_export[df_export["dataset_split"] == "test"].copy()

    sample_rows = pipeline_config.sampling.train_sample_rows
    if sample_rows is not None and sample_rows > 0 and len(df_train) > sample_rows:
        df_train = df_train.sample(sample_rows, random_state=pipeline_config.sampling.train_sample_seed).copy()

    audit_payload = build_snapshot_training_audit_payload(
        artifact_paths=artifact_paths,
        snapshots_loaded=snapshots_loaded,
        snapshots_quality=snapshots_quality,
        snapshots_assigned=snapshots,
        df_feat=df_feat,
        feature_columns=feature_columns,
        rules_df=rules,
        sample_config={
            "max_rows": pipeline_config.max_rows,
            "recent_days": pipeline_config.recent_days,
            "random_sample_rows": sample_rows,
            "random_sample_seed": pipeline_config.sampling.train_sample_seed,
            "random_sample_scope": pipeline_config.sampling.train_sample_scope,
            "split_reference_end": pipeline_config.split.split_reference_end,
            "history_start": pipeline_config.split.history_start,
            "target_mode": args.target_mode,
            "predictor_hyperparameters_profile": args.predictor_hyperparameters_profile,
            "allowed_splits": list(pipeline_config.split.allowed_splits),
        },
    )
    write_snapshot_training_audit(artifact_paths=artifact_paths, payload=audit_payload)

    manifest = {
        "artifact_mode": pipeline_config.artifact_mode,
        "feature_columns": feature_columns,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "metadata_columns": classified_columns.metadata_columns,
        "monitoring_columns": classified_columns.monitoring_columns,
        "control_columns": classified_columns.control_columns,
        "required_critical_columns": required_critical_columns,
        "required_noncritical_columns": required_noncritical_columns,
        "feature_gate_diagnostics": gate_diagnostics,
        "split_boundaries": {
            "train_start": pipeline_config.split.train_start,
            "train_end": pipeline_config.split.train_end,
            "valid_start": pipeline_config.split.valid_start,
            "valid_end": pipeline_config.split.valid_end,
            "test_start": pipeline_config.split.test_start,
            "test_end": pipeline_config.split.test_end,
            "allowed_splits": list(pipeline_config.split.allowed_splits),
        },
        "feature_semantics_version": FEATURE_SEMANTICS_VERSION,
        "normalization_manifest": normalization_manifest,
        "sample_config": audit_payload["sample_config"],
        "predictor_presets": normalize_predictor_presets(args.predictor_presets),
        "predictor_time_limit": args.predictor_time_limit,
        "predictor_hyperparameters_profile": args.predictor_hyperparameters_profile,
        "publish": {
            "prediction_publish_split": pipeline_config.publish.prediction_publish_split,
            "fail_if_publish_split_empty": pipeline_config.publish.fail_if_publish_split_empty,
        },
        "rows": {
            "train_exported": int(len(df_train)),
            "valid_exported": int(len(df_valid)),
            "test_exported": int(len(df_test)),
            "feature_frame_total": int(len(df_feat)),
        },
    }
    return {
        "train": df_train,
        "valid": df_valid,
        "test": df_test,
        "manifest": manifest,
    }


def main() -> None:
    args = parse_args()
    exports = build_feature_exports(args)
    TRAIN_PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    exports["train"].to_parquet(TRAIN_PARQUET_PATH, index=False)
    exports["valid"].to_parquet(VALID_PARQUET_PATH, index=False)
    exports["test"].to_parquet(TEST_PARQUET_PATH, index=False)
    write_json(FEATURE_EXPORT_MANIFEST_PATH, exports["manifest"])
    print(f"[INFO] Exported train features to {TRAIN_PARQUET_PATH}")
    print(f"[INFO] Exported valid features to {VALID_PARQUET_PATH}")
    print(f"[INFO] Exported test features to {TEST_PARQUET_PATH}")
    print(f"[INFO] Wrote feature export manifest to {FEATURE_EXPORT_MANIFEST_PATH}")


if __name__ == "__main__":
    main()
