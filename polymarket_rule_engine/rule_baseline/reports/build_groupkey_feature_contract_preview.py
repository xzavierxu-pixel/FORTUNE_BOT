from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged
from rule_baseline.datasets.splits import assign_configured_dataset_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.features.annotation_normalization import build_normalization_manifest, normalize_market_annotations
from rule_baseline.features.snapshot_semantics import online_feature_columns, split_feature_contract_columns
from rule_baseline.models.runtime_bundle import load_feature_contract
from rule_baseline.training.train_snapshot_model import (
    DROP_COLS,
    TRAIN_PRICE_MAX,
    TRAIN_PRICE_MIN,
    add_training_targets,
    build_feature_table,
    load_rules,
    load_serving_feature_bundle,
)
from rule_baseline.datasets.snapshots import load_online_parity_snapshots, load_raw_markets
from rule_baseline.utils import config
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview current GroupKey feature contract without training AutoGluon.")
    parser.add_argument("--markdown-preview-limit", type=int, default=100)
    parser.add_argument("--pipeline-config", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)
    max_rows = pipeline_config.max_rows
    recent_days = pipeline_config.recent_days

    rebuild_canonical_merged()
    snapshots = load_online_parity_snapshots(
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        max_rows=max_rows,
        recent_days=recent_days,
    )
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    snapshots = assign_configured_dataset_split(snapshots, pipeline_config.split)
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

    current_feature_columns = online_feature_columns([column for column in df_feat.columns if column not in DROP_COLS])
    current_serving_columns = sorted(
        column for column in current_feature_columns if column.startswith("group_feature_") or column.startswith("fine_feature_")
    )
    critical_columns, noncritical_columns = split_feature_contract_columns(current_feature_columns)

    bundle_contract_path = artifact_paths.model_bundle_dir / "feature_contract.json"
    existing_contract = load_feature_contract(bundle_contract_path) if bundle_contract_path.exists() else None
    existing_feature_columns = list(existing_contract.feature_columns) if existing_contract is not None else []
    existing_serving_columns = sorted(
        column for column in existing_feature_columns if column.startswith("group_feature_") or column.startswith("fine_feature_")
    )

    new_serving_columns = sorted(column for column in current_serving_columns if column not in existing_serving_columns)
    missing_from_current_code = sorted(column for column in existing_serving_columns if column not in current_serving_columns)

    docs_dir = Path("polymarket_rule_engine/docs")
    docs_dir.mkdir(parents=True, exist_ok=True)
    json_path = docs_dir / "groupkey_feature_contract_preview.json"
    markdown_path = docs_dir / "groupkey_feature_contract_preview.md"

    payload = {
        "artifact_mode": pipeline_config.artifact_mode,
        "sample_filters": {
            "max_rows": max_rows,
            "recent_days": recent_days,
            "split_reference_end": pipeline_config.split.split_reference_end,
            "history_start": pipeline_config.split.history_start,
        },
        "rows": int(len(df_feat)),
        "current_feature_columns": len(current_feature_columns),
        "current_serving_columns": len(current_serving_columns),
        "current_required_critical_columns": critical_columns,
        "current_required_noncritical_count": len(noncritical_columns),
        "existing_bundle_feature_columns": len(existing_feature_columns),
        "existing_bundle_serving_columns": len(existing_serving_columns),
        "new_serving_columns_vs_existing_bundle": new_serving_columns,
        "existing_bundle_serving_columns_missing_from_current_code": missing_from_current_code,
    }
    write_json(json_path, payload)

    markdown_lines = [
        "# GroupKey Feature Contract Preview",
        "",
        "## Summary",
        "",
        f"- rows={payload['rows']}",
        f"- max_rows={payload['sample_filters']['max_rows']}",
        f"- recent_days={payload['sample_filters']['recent_days']}",
        f"- current_feature_columns={payload['current_feature_columns']}",
        f"- current_serving_columns={payload['current_serving_columns']}",
        f"- existing_bundle_feature_columns={payload['existing_bundle_feature_columns']}",
        f"- existing_bundle_serving_columns={payload['existing_bundle_serving_columns']}",
        f"- new_serving_columns_vs_existing_bundle={len(new_serving_columns)}",
        f"- existing_bundle_serving_columns_missing_from_current_code={len(missing_from_current_code)}",
        "",
        "## New Serving Columns Vs Existing Bundle",
        "",
    ]
    if new_serving_columns:
        preview_columns = new_serving_columns[: args.markdown_preview_limit]
        markdown_lines.extend(f"- {column}" for column in preview_columns)
        if len(new_serving_columns) > len(preview_columns):
            markdown_lines.append(
                f"- ... truncated {len(new_serving_columns) - len(preview_columns)} additional columns; see JSON for full list"
            )
    else:
        markdown_lines.append("- none")
    markdown_lines.extend(["", "## Existing Bundle Serving Columns Missing From Current Code", ""])
    if missing_from_current_code:
        markdown_lines.extend(f"- {column}" for column in missing_from_current_code)
    else:
        markdown_lines.append("- none")
    markdown_path.write_text("\n".join(markdown_lines).strip() + "\n", encoding="utf-8")

    print(f"[INFO] Wrote contract preview json to {json_path}")
    print(f"[INFO] Wrote contract preview markdown to {markdown_path}")


if __name__ == "__main__":
    main()
