from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from rule_baseline.history.history_features import HISTORY_ARTIFACT_FILENAMES
from rule_baseline.models.runtime_bundle import (
    FULL_TRAINING_BUNDLE_DIRNAME,
    RUNTIME_BUNDLE_DIRNAME,
    build_runtime_bundle_paths,
)
from rule_baseline.utils import config

ARTIFACT_MODES = {"offline", "online"}


@dataclass(frozen=True)
class ArtifactPaths:
    mode: str
    root_dir: Path
    edge_dir: Path
    models_dir: Path
    predictions_dir: Path
    backtest_dir: Path
    analysis_dir: Path
    metadata_dir: Path
    audit_dir: Path
    docs_dir: Path
    docs_audit_dir: Path
    docs_groupkey_reports_dir: Path
    rules_path: Path
    rule_report_path: Path
    history_feature_paths: dict[str, Path]
    group_serving_features_path: Path
    fine_serving_features_path: Path
    serving_feature_defaults_path: Path
    model_path: Path
    model_bundle_dir: Path
    full_model_bundle_dir: Path
    legacy_model_path: Path
    predictions_path: Path
    predictions_full_path: Path
    split_summary_path: Path
    rule_training_summary_path: Path
    model_training_summary_path: Path
    rule_funnel_summary_path: Path
    rule_generation_audit_json_path: Path
    rule_generation_audit_markdown_path: Path
    artifact_inventory_json_path: Path
    artifact_inventory_markdown_path: Path
    snapshot_training_audit_json_path: Path
    snapshot_training_audit_markdown_path: Path
    docs_model_training_summary_path: Path
    groupkey_migration_validation_path: Path
    groupkey_consistency_report_path: Path
    groupkey_serving_schema_reference_path: Path
    groupkey_runtime_report_json_path: Path
    groupkey_runtime_report_markdown_path: Path
    pipeline_runtime_config_path: Path

    def ensure_dirs(self) -> None:
        bundle_paths = build_runtime_bundle_paths(self.model_bundle_dir)
        full_bundle_paths = build_runtime_bundle_paths(self.full_model_bundle_dir)
        for path in [
            self.root_dir,
            self.edge_dir,
            self.models_dir,
            self.predictions_dir,
            self.backtest_dir,
            self.analysis_dir,
            self.metadata_dir,
            self.audit_dir,
            self.docs_dir,
            self.docs_audit_dir,
            self.docs_groupkey_reports_dir,
        ]:
            path.mkdir(parents=True, exist_ok=True)
        bundle_paths.ensure_dirs()
        full_bundle_paths.ensure_dirs()


def build_artifact_paths(mode: str = "offline") -> ArtifactPaths:
    normalized = mode.lower().strip()
    if normalized not in ARTIFACT_MODES:
        raise ValueError(f"Unsupported artifact mode: {mode}")

    root = config.OFFLINE_DIR if normalized == "offline" else config.ONLINE_DIR
    paths = ArtifactPaths(
        mode=normalized,
        root_dir=root,
        edge_dir=root / "edge",
        models_dir=root / "models",
        predictions_dir=root / "predictions",
        backtest_dir=root / "backtesting",
        analysis_dir=root / "analysis",
        metadata_dir=root / "metadata",
        audit_dir=root / "audit",
        docs_dir=config.BASE_DIR / "docs",
        docs_audit_dir=config.BASE_DIR / "docs" / "audit",
        docs_groupkey_reports_dir=config.BASE_DIR / "docs" / "audit" / "groupkey_reports",
        rules_path=root / "edge" / "trading_rules.csv",
        rule_report_path=root / "audit" / "all_trading_rule_audit_report.csv",
        history_feature_paths={
            level_name: root / "edge" / filename
            for level_name, filename in HISTORY_ARTIFACT_FILENAMES.items()
        },
        group_serving_features_path=root / "edge" / "group_serving_features.parquet",
        fine_serving_features_path=root / "edge" / "fine_serving_features.parquet",
        serving_feature_defaults_path=root / "edge" / "serving_feature_defaults.json",
        model_path=root / "models" / RUNTIME_BUNDLE_DIRNAME,
        model_bundle_dir=root / "models" / RUNTIME_BUNDLE_DIRNAME,
        full_model_bundle_dir=root / "models" / FULL_TRAINING_BUNDLE_DIRNAME,
        legacy_model_path=root / "models" / "ensemble_snapshot_q.pkl",
        predictions_path=root / "predictions" / "snapshots_with_predictions.csv",
        predictions_full_path=root / "predictions" / "snapshots_with_predictions_all.csv",
        split_summary_path=root / "metadata" / "split_summary.json",
        rule_training_summary_path=root / "metadata" / "rule_training_summary.json",
        model_training_summary_path=root / "metadata" / "model_training_summary.json",
        rule_funnel_summary_path=root / "audit" / "rule_funnel_summary.json",
        rule_generation_audit_json_path=root / "audit" / "rule_generation_audit.json",
        rule_generation_audit_markdown_path=root / "audit" / "rule_generation_audit.md",
        artifact_inventory_json_path=root / "audit" / "artifact_inventory.json",
        artifact_inventory_markdown_path=root / "audit" / "artifact_inventory.md",
        snapshot_training_audit_json_path=root / "audit" / "snapshot_training_funnel.json",
        snapshot_training_audit_markdown_path=root / "audit" / "snapshot_training_funnel.md",
        docs_model_training_summary_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "model_training_summary.json",
        groupkey_migration_validation_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "groupkey_migration_validation.md",
        groupkey_consistency_report_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "groupkey_consistency_report.md",
        groupkey_serving_schema_reference_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "groupkey_serving_schema_reference.md",
        groupkey_runtime_report_json_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "groupkey_runtime_report.json",
        groupkey_runtime_report_markdown_path=config.BASE_DIR / "docs" / "audit" / "groupkey_reports" / "groupkey_runtime_report.md",
        pipeline_runtime_config_path=root / "audit" / "pipeline_runtime_config.json",
    )
    paths.ensure_dirs()
    return paths


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)
