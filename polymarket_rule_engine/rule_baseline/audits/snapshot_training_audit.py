from __future__ import annotations

from pathlib import Path

import pandas as pd

from rule_baseline.datasets.artifacts import write_json


def _shape_for_path(path: Path) -> list[int] | None:
    if not path.exists():
        return None
    if path.suffix == ".csv":
        return [int(value) for value in pd.read_csv(path).shape]
    if path.suffix == ".parquet":
        return [int(value) for value in pd.read_parquet(path).shape]
    return None


def _selection_impact_rows(
    snapshots_assigned: pd.DataFrame,
    report_df: pd.DataFrame,
) -> list[dict]:
    if snapshots_assigned.empty:
        return []

    grouped = snapshots_assigned.copy()
    grouped["group_key"] = (
        grouped["domain"].astype(str)
        + "|"
        + grouped["category"].astype(str)
        + "|"
        + grouped["market_type"].astype(str)
    )
    if report_df.empty or "group_key" not in report_df.columns:
        grouped["selection_status"] = "missing_rule_status"
    else:
        grouped = grouped.merge(
            report_df[["group_key", "selection_status"]],
            on="group_key",
            how="left",
        )
        grouped["selection_status"] = grouped["selection_status"].fillna("missing_rule_status")

    summary = (
        grouped.groupby("selection_status", observed=False)
        .agg(
            snapshot_rows=("market_id", "size"),
            unique_markets=("market_id", "nunique"),
            group_keys=("group_key", "nunique"),
        )
        .reset_index()
        .sort_values("snapshot_rows", ascending=False)
    )
    return summary.to_dict("records")


def _artifact_inventory(artifact_paths) -> list[dict]:
    items = [
        ("rules_csv", artifact_paths.rules_path),
        ("rule_report_csv", artifact_paths.rule_report_path),
        ("group_serving_features", artifact_paths.group_serving_features_path),
        ("fine_serving_features", artifact_paths.fine_serving_features_path),
        ("serving_feature_defaults", artifact_paths.serving_feature_defaults_path),
        ("predictions_full_csv", artifact_paths.predictions_full_path),
        ("predictions_csv", artifact_paths.predictions_path),
    ]
    items.extend((f"history_{level_name}", path) for level_name, path in artifact_paths.history_feature_paths.items())

    inventory: list[dict] = []
    for label, path in items:
        inventory.append(
            {
                "artifact": label,
                "path": str(path),
                "exists": bool(path.exists()),
                "shape": _shape_for_path(path),
                "size_bytes": int(path.stat().st_size) if path.exists() else None,
            }
        )
    return inventory


def build_snapshot_training_audit_payload(
    *,
    artifact_paths,
    snapshots_loaded: pd.DataFrame,
    snapshots_quality: pd.DataFrame,
    snapshots_assigned: pd.DataFrame,
    df_feat: pd.DataFrame,
    feature_columns: list[str],
    rules_df: pd.DataFrame,
    sample_config: dict,
) -> dict:
    report_df = pd.read_csv(artifact_paths.rule_report_path) if artifact_paths.rule_report_path.exists() else pd.DataFrame()
    selection_impact_rows = _selection_impact_rows(snapshots_assigned, report_df)
    kept_rows = next((row["snapshot_rows"] for row in selection_impact_rows if row["selection_status"] == "keep"), 0)
    kept_group_keys = next((row["group_keys"] for row in selection_impact_rows if row["selection_status"] == "keep"), 0)

    matched_keys = df_feat[["market_id", "snapshot_time"]].drop_duplicates() if not df_feat.empty else pd.DataFrame(columns=["market_id", "snapshot_time"])
    matched_rows = int(len(matched_keys))

    stage_rows = [
        {
            "stage": "snapshots_loaded",
            "snapshot_rows": int(len(snapshots_loaded)),
            "unique_markets": int(snapshots_loaded["market_id"].astype(str).nunique()) if not snapshots_loaded.empty else 0,
        },
        {
            "stage": "after_quality_pass",
            "snapshot_rows": int(len(snapshots_quality)),
            "unique_markets": int(snapshots_quality["market_id"].astype(str).nunique()) if not snapshots_quality.empty else 0,
        },
        {
            "stage": "after_dataset_split",
            "snapshot_rows": int(len(snapshots_assigned)),
            "unique_markets": int(snapshots_assigned["market_id"].astype(str).nunique()) if not snapshots_assigned.empty else 0,
        },
        {
            "stage": "after_group_selection_keep",
            "snapshot_rows": int(kept_rows),
            "unique_markets": int(
                next((row["unique_markets"] for row in selection_impact_rows if row["selection_status"] == "keep"), 0)
            ),
        },
        {
            "stage": "after_rule_bucket_match",
            "snapshot_rows": matched_rows,
            "unique_markets": int(df_feat["market_id"].astype(str).nunique()) if not df_feat.empty else 0,
        },
        {
            "stage": "model_feature_frame",
            "snapshot_rows": int(len(df_feat)),
            "unique_markets": int(df_feat["market_id"].astype(str).nunique()) if not df_feat.empty else 0,
        },
    ]

    return {
        "artifact_mode": artifact_paths.mode,
        "sample_config": sample_config,
        "stage_rows": stage_rows,
        "selection_status_impact": selection_impact_rows,
        "rules_summary": {
            "rule_rows": int(len(rules_df)),
            "rule_group_keys": int(rules_df["group_key"].nunique()) if not rules_df.empty and "group_key" in rules_df.columns else 0,
            "report_rows": int(len(report_df)),
            "report_group_keys": int(report_df["group_key"].nunique()) if not report_df.empty and "group_key" in report_df.columns else 0,
            "kept_group_keys": int(kept_group_keys),
        },
        "matching_summary": {
            "assigned_snapshot_rows": int(len(snapshots_assigned)),
            "kept_snapshot_rows": int(kept_rows),
            "matched_training_rows": matched_rows,
            "kept_but_unmatched_rows": int(max(kept_rows - matched_rows, 0)),
            "non_keep_rows": int(max(len(snapshots_assigned) - kept_rows, 0)),
        },
        "training_frame": {
            "full_dataframe_shape": [int(value) for value in df_feat.shape],
            "model_feature_shape": [int(len(df_feat)), int(len(feature_columns))],
            "rows_by_split": {
                str(key): int(value)
                for key, value in df_feat["dataset_split"].value_counts().to_dict().items()
            }
            if "dataset_split" in df_feat.columns
            else {},
        },
        "artifact_inventory": _artifact_inventory(artifact_paths),
    }


def build_snapshot_training_audit_markdown(payload: dict) -> str:
    lines = [
        "# Snapshot Training Funnel Audit",
        "",
        "## Sample Config",
        "",
    ]
    for key, value in payload.get("sample_config", {}).items():
        lines.append(f"- {key}={value}")
    lines.extend(["", "## Stage Rows", ""])
    for row in payload.get("stage_rows", []):
        lines.append(
            f"- stage={row['stage']}, snapshot_rows={row['snapshot_rows']}, unique_markets={row['unique_markets']}"
        )
    lines.extend(["", "## Selection Status Impact", ""])
    selection_rows = payload.get("selection_status_impact", [])
    if selection_rows:
        for row in selection_rows:
            lines.append(
                f"- selection_status={row['selection_status']}, snapshot_rows={row['snapshot_rows']}, "
                f"unique_markets={row['unique_markets']}, group_keys={row['group_keys']}"
            )
    else:
        lines.append("- none")
    lines.extend(["", "## Matching Summary", ""])
    for key, value in payload.get("matching_summary", {}).items():
        lines.append(f"- {key}={value}")
    lines.extend(["", "## Training Frame", ""])
    training_frame = payload.get("training_frame", {})
    lines.append(f"- full_dataframe_shape={training_frame.get('full_dataframe_shape')}")
    lines.append(f"- model_feature_shape={training_frame.get('model_feature_shape')}")
    lines.append(f"- rows_by_split={training_frame.get('rows_by_split')}")
    lines.extend(["", "## Artifact Inventory", ""])
    for row in payload.get("artifact_inventory", []):
        lines.append(
            f"- artifact={row['artifact']}, exists={row['exists']}, shape={row['shape']}, "
            f"size_bytes={row['size_bytes']}, path={row['path']}"
        )
    return "\n".join(lines).strip() + "\n"


def write_snapshot_training_audit(
    *,
    artifact_paths,
    payload: dict,
) -> None:
    write_json(artifact_paths.snapshot_training_audit_json_path, payload)
    artifact_paths.snapshot_training_audit_markdown_path.write_text(
        build_snapshot_training_audit_markdown(payload),
        encoding="utf-8",
    )
