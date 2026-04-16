from __future__ import annotations

import json
from datetime import UTC, datetime
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
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return [int(len(payload)), 2]
        if isinstance(payload, list):
            return [int(len(payload)), 1]
    return None


def _schema_preview(path: Path) -> list[str]:
    if not path.exists():
        return []
    if path.suffix == ".csv":
        return [str(column) for column in pd.read_csv(path, nrows=1).columns[:12]]
    if path.suffix == ".parquet":
        return [str(column) for column in pd.read_parquet(path).columns[:12]]
    if path.suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return [str(key) for key in list(payload.keys())[:12]]
        if isinstance(payload, list) and payload:
            first = payload[0]
            if isinstance(first, dict):
                return [str(key) for key in list(first.keys())[:12]]
            return [type(first).__name__]
    return []


def _json_summary(path: Path) -> dict | None:
    if not path.exists() or path.suffix != ".json":
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return {
            "root_type": "dict",
            "top_level_keys": int(len(payload)),
        }
    if isinstance(payload, list):
        return {
            "root_type": "list",
            "items": int(len(payload)),
        }
    return {"root_type": type(payload).__name__}


def build_artifact_inventory(artifact_paths) -> list[dict]:
    items = [
        ("rules_csv", artifact_paths.rules_path),
        ("rule_report_csv", artifact_paths.rule_report_path),
        ("group_serving_features", artifact_paths.group_serving_features_path),
        ("fine_serving_features", artifact_paths.fine_serving_features_path),
        ("serving_feature_defaults", artifact_paths.serving_feature_defaults_path),
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
                "modified_time_utc": datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat() if path.exists() else None,
                "schema_preview": _schema_preview(path),
                "json_summary": _json_summary(path),
            }
        )
    return inventory


def build_rule_generation_audit_payload(
    *,
    artifact_paths,
    rules_df: pd.DataFrame,
    report_df: pd.DataFrame,
    group_serving_features: pd.DataFrame,
    fine_serving_features: pd.DataFrame,
    rule_funnel_summary: dict,
    split_summary: dict,
    rule_training_summary: dict,
    debug_filters: dict,
) -> dict:
    artifact_inventory = build_artifact_inventory(artifact_paths)
    missing_history_artifacts = [
        row["artifact"]
        for row in artifact_inventory
        if row["artifact"].startswith("history_") and not row["exists"]
    ]

    kept_group_keys = 0
    dropped_group_keys = 0
    insufficient_group_keys = 0
    if not report_df.empty and "selection_status" in report_df.columns:
        status_counts = report_df["selection_status"].value_counts().to_dict()
        kept_group_keys = int(status_counts.get("keep", 0))
        dropped_group_keys = int(status_counts.get("drop", 0))
        insufficient_group_keys = int(status_counts.get("insufficient_data", 0))

    return {
        "artifact_mode": artifact_paths.mode,
        "debug_filters": debug_filters,
        "split_summary": split_summary,
        "rule_training_summary": rule_training_summary,
        "rule_funnel_summary": rule_funnel_summary,
        "rules_summary": {
            "rule_rows": int(len(rules_df)),
            "rule_group_keys": int(rules_df["group_key"].nunique()) if not rules_df.empty and "group_key" in rules_df.columns else 0,
            "report_rows": int(len(report_df)),
            "kept_group_keys": kept_group_keys,
            "dropped_group_keys": dropped_group_keys,
            "insufficient_group_keys": insufficient_group_keys,
            "group_serving_rows": int(len(group_serving_features)),
            "fine_serving_rows": int(len(fine_serving_features)),
        },
        "artifact_summary": {
            "history_artifact_count_expected": int(len(artifact_paths.history_feature_paths)),
            "history_artifact_count_present": int(
                sum(1 for row in artifact_inventory if row["artifact"].startswith("history_") and row["exists"])
            ),
            "missing_history_artifacts": missing_history_artifacts,
        },
        "artifact_inventory": artifact_inventory,
    }


def build_rule_generation_audit_markdown(payload: dict) -> str:
    lines = [
        "# Rule Generation Audit",
        "",
        "## Debug Filters",
        "",
    ]
    for key, value in payload.get("debug_filters", {}).items():
        lines.append(f"- {key}={value}")

    lines.extend(["", "## Rules Summary", ""])
    for key, value in payload.get("rules_summary", {}).items():
        lines.append(f"- {key}={value}")

    lines.extend(["", "## Artifact Summary", ""])
    for key, value in payload.get("artifact_summary", {}).items():
        lines.append(f"- {key}={value}")

    lines.extend(["", "## Snapshot Funnel", ""])
    snapshot_funnel = payload.get("rule_funnel_summary", {}).get("snapshot_funnel", [])
    if snapshot_funnel:
        for row in snapshot_funnel:
            lines.append(
                f"- stage={row.get('stage')}, snapshot_rows={row.get('snapshot_rows')}, unique_markets={row.get('unique_markets')}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Selection Status Market Impact", ""])
    impact_rows = payload.get("rule_funnel_summary", {}).get("rule_selection", {}).get("selection_status_market_impact", [])
    if impact_rows:
        for row in impact_rows:
            lines.append(
                f"- selection_status={row.get('selection_status')}, snapshot_rows={row.get('snapshot_rows')}, unique_markets={row.get('unique_markets')}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Artifact Inventory", ""])
    for row in payload.get("artifact_inventory", []):
        lines.append(
            f"- artifact={row['artifact']}, exists={row['exists']}, shape={row['shape']}, "
            f"size_bytes={row['size_bytes']}, modified_time_utc={row['modified_time_utc']}, "
            f"schema_preview={row['schema_preview']}, path={row['path']}"
        )
    return "\n".join(lines).strip() + "\n"


def build_artifact_inventory_markdown(payload: list[dict], artifact_mode: str) -> str:
    lines = [
        f"# {artifact_mode.title()} Artifact Inventory",
        "",
        f"- artifact_count={len(payload)}",
        "",
        "## Artifacts",
        "",
    ]
    for row in payload:
        lines.append(
            f"- artifact={row['artifact']}, exists={row['exists']}, shape={row['shape']}, "
            f"size_bytes={row['size_bytes']}, modified_time_utc={row['modified_time_utc']}, "
            f"schema_preview={row['schema_preview']}, path={row['path']}"
        )
    return "\n".join(lines).strip() + "\n"


def write_rule_generation_audit(
    *,
    artifact_paths,
    payload: dict,
) -> None:
    write_json(artifact_paths.rule_generation_audit_json_path, payload)
    artifact_paths.rule_generation_audit_markdown_path.write_text(
        build_rule_generation_audit_markdown(payload),
        encoding="utf-8",
    )
    write_json(
        artifact_paths.artifact_inventory_json_path,
        {
            "artifact_mode": artifact_paths.mode,
            "artifact_inventory": payload.get("artifact_inventory", []),
        },
    )
    artifact_paths.artifact_inventory_markdown_path.write_text(
        build_artifact_inventory_markdown(payload.get("artifact_inventory", []), artifact_paths.mode),
        encoding="utf-8",
    )
