from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.datasets.snapshots import load_online_parity_snapshots
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features.annotation_normalization import build_normalization_manifest, normalize_market_annotations
from rule_baseline.features.serving import attach_serving_features
from rule_baseline.models.runtime_bundle import FeatureContract, load_feature_contract
from rule_baseline.training.train_snapshot_model import (
    TRAIN_PRICE_MAX,
    TRAIN_PRICE_MIN,
    load_rules,
    load_serving_feature_bundle,
    match_snapshots_to_rules,
)
from rule_baseline.utils import config


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _resolve_feature_contract_path(artifact_paths) -> Path:
    candidates = [
        artifact_paths.model_bundle_dir / "feature_contract.json",
        artifact_paths.full_model_bundle_dir / "feature_contract.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("No feature contract found in deploy or full training bundle.")


def _render_rows(title: str, rows: list[dict], keys: list[str]) -> list[str]:
    lines = [f"### {title}", ""]
    if not rows:
        lines.append("- none")
        lines.append("")
        return lines
    for row in rows:
        rendered = ", ".join(f"{key}={row.get(key)}" for key in keys)
        lines.append(f"- {rendered}")
    lines.append("")
    return lines


def _selection_distribution_rows(report_df: pd.DataFrame, dimension: str) -> list[dict]:
    if report_df.empty or dimension not in report_df.columns:
        return []
    grouped = (
        report_df.groupby([dimension, "selection_status"], observed=True)
        .agg(
            group_keys=("group_key", "nunique"),
            snapshot_rows=("group_snapshot_rows", "sum"),
            unique_markets=("group_unique_markets", "sum"),
        )
        .reset_index()
        .sort_values(["snapshot_rows", "group_keys"], ascending=[False, False])
    )
    return grouped.to_dict("records")


def build_migration_validation_markdown(
    *,
    rule_funnel_summary: dict,
    report_df: pd.DataFrame,
    rules_df: pd.DataFrame,
) -> str:
    snapshot_funnel = rule_funnel_summary.get("snapshot_funnel", [])
    before = snapshot_funnel[0] if snapshot_funnel else {"snapshot_rows": 0, "unique_markets": 0}
    after = next(
        (row for row in snapshot_funnel if row.get("stage") == "after_rule_selection"),
        {"snapshot_rows": 0, "unique_markets": 0},
    )
    selected_groups = report_df[report_df["selection_status"] == "keep"]["group_key"].nunique() if not report_df.empty else 0
    selected_rule_rows = len(rules_df)

    lines = [
        "# GroupKey Migration Validation",
        "",
        "## Summary",
        "",
        f"- snapshot_rows_before={int(before.get('snapshot_rows', 0))}",
        f"- snapshot_rows_after={int(after.get('snapshot_rows', 0))}",
        f"- unique_markets_before={int(before.get('unique_markets', 0))}",
        f"- unique_markets_after={int(after.get('unique_markets', 0))}",
        f"- retained_group_key_count={int(selected_groups)}",
        f"- retained_rule_row_count={int(selected_rule_rows)}",
        "",
        "## Snapshot Funnel",
        "",
    ]
    for stage in snapshot_funnel:
        lines.append(
            f"- stage={stage.get('stage')}, snapshot_rows={stage.get('snapshot_rows')}, "
            f"unique_markets={stage.get('unique_markets')}, snapshot_rows_delta={stage.get('snapshot_rows_delta')}, "
            f"unique_markets_delta={stage.get('unique_markets_delta')}"
        )
    lines.append("")

    selection_impact = rule_funnel_summary.get("rule_selection", {}).get("selection_status_market_impact", [])
    lines.extend(_render_rows("Selection Status Market Impact", selection_impact, ["selection_status", "snapshot_rows", "unique_markets"]))
    lines.extend(_render_rows("Domain Distribution", _selection_distribution_rows(report_df, "domain"), ["domain", "selection_status", "group_keys", "snapshot_rows", "unique_markets"]))
    lines.extend(_render_rows("Category Distribution", _selection_distribution_rows(report_df, "category"), ["category", "selection_status", "group_keys", "snapshot_rows", "unique_markets"]))
    lines.extend(_render_rows("Market Type Distribution", _selection_distribution_rows(report_df, "market_type"), ["market_type", "selection_status", "group_keys", "snapshot_rows", "unique_markets"]))
    return "\n".join(lines).strip() + "\n"


def build_consistency_report_markdown(
    *,
    feature_contract: FeatureContract,
    group_features: pd.DataFrame,
    fine_features: pd.DataFrame,
    defaults_manifest: dict,
) -> str:
    feature_columns = set(feature_contract.feature_columns)
    serving_contract_columns = sorted(
        column for column in feature_contract.feature_columns if column.startswith("group_feature_") or column.startswith("fine_feature_")
    )
    actual_group_columns = sorted(
        f"group_feature_{column}" for column in group_features.columns if column != "group_key"
    )
    actual_fine_columns = sorted(
        f"fine_feature_{column}" for column in fine_features.columns if column not in {"group_key", "price_bin", "horizon_hours"}
    )
    actual_serving_columns = sorted(actual_group_columns + actual_fine_columns)
    bundle_missing_asset_backed_columns = sorted(column for column in actual_serving_columns if column not in serving_contract_columns)
    bundle_registered_asset_backed_columns = sorted(column for column in actual_serving_columns if column in serving_contract_columns)
    missing_in_assets = sorted(column for column in serving_contract_columns if column not in actual_serving_columns)
    unused_asset_columns = sorted(column for column in actual_serving_columns if column not in feature_columns)

    fine_feature_defaults = defaults_manifest.get("fine_feature_defaults", {})
    fine_only_columns = sorted(column for column in fine_features.columns if column not in {"group_key", "price_bin", "horizon_hours"})
    missing_fallback_entries = sorted(column for column in fine_only_columns if column not in fine_feature_defaults)

    lines = [
        "# GroupKey Consistency Report",
        "",
        "## Summary",
        "",
        f"- feature_contract_columns={len(feature_contract.feature_columns)}",
        f"- critical_feature_columns={len(feature_contract.required_critical_columns)}",
        f"- noncritical_feature_columns={len(feature_contract.required_noncritical_columns)}",
        f"- serving_contract_columns={len(serving_contract_columns)}",
        f"- actual_group_serving_columns={len(actual_group_columns)}",
        f"- actual_fine_serving_columns={len(actual_fine_columns)}",
        f"- bundle_registered_asset_backed_columns={len(bundle_registered_asset_backed_columns)}",
        f"- bundle_missing_asset_backed_columns={len(bundle_missing_asset_backed_columns)}",
        f"- missing_contract_columns_in_assets={len(missing_in_assets)}",
        f"- unused_asset_columns_not_in_contract={len(unused_asset_columns)}",
        f"- fine_only_columns={len(fine_only_columns)}",
        f"- fine_only_columns_missing_fallback={len(missing_fallback_entries)}",
        "",
    ]
    lines.extend(_render_rows("Bundle Missing Asset-Backed Serving Columns", [{"column": value} for value in bundle_missing_asset_backed_columns], ["column"]))
    lines.extend(_render_rows("Missing Contract Columns In Assets", [{"column": value} for value in missing_in_assets], ["column"]))
    lines.extend(_render_rows("Unused Asset Columns Not In Contract", [{"column": value} for value in unused_asset_columns], ["column"]))
    lines.extend(_render_rows("Fine Columns Missing Fallback Defaults", [{"column": value} for value in missing_fallback_entries], ["column"]))
    return "\n".join(lines).strip() + "\n"


def _frame_memory_mb(frame: pd.DataFrame) -> float:
    if frame.empty:
        return 0.0
    return float(frame.memory_usage(deep=True).sum() / (1024 ** 2))


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def build_runtime_report_payload(
    *,
    artifact_mode: str = "offline",
    max_rows: int | None = 2000,
    recent_days: int | None = 14,
    split_reference_end: str | None = None,
    history_start: str | None = None,
    unknown_group_preview_limit: int = 20,
) -> dict:
    artifact_paths = build_artifact_paths(artifact_mode)

    snapshots = load_online_parity_snapshots(
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        max_rows=max_rows,
        recent_days=recent_days,
    )
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_artifact_split(
        snapshots,
        artifact_mode=artifact_mode,
        reference_end=split_reference_end,
        history_start_override=history_start,
    )
    snapshots = assign_dataset_split(snapshots, split)
    allowed_splits = ["train", "valid", "test"] if artifact_mode == "offline" else ["train", "valid"]
    snapshots = snapshots[snapshots["dataset_split"].isin(allowed_splits)].copy()

    market_annotations_raw = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    normalization_manifest = build_normalization_manifest(market_annotations_raw)
    market_annotations = normalize_market_annotations(
        market_annotations_raw,
        vocabulary_manifest=normalization_manifest,
    )
    rules = load_rules(artifact_paths.rules_path)

    bundle_load_started = time.perf_counter()
    serving_feature_bundle = load_serving_feature_bundle(artifact_paths)
    bundle_load_seconds = time.perf_counter() - bundle_load_started
    if serving_feature_bundle is None:
        raise FileNotFoundError("Serving feature bundle is incomplete; required parquet/default files are missing.")

    matched = match_snapshots_to_rules(snapshots, market_annotations, rules)
    attach_started = time.perf_counter()
    attached = attach_serving_features(
        matched,
        serving_feature_bundle,
        price_column="price",
        horizon_column="horizon_hours",
    )
    attach_seconds = time.perf_counter() - attach_started

    rows = int(len(attached))
    unknown_group_rows = int((~attached["group_match_found"]).sum()) if "group_match_found" in attached.columns else rows
    group_fallback_rows = int(attached["used_group_fallback_only"].sum()) if "used_group_fallback_only" in attached.columns else 0
    fine_match_rows = int(attached["fine_match_found"].sum()) if "fine_match_found" in attached.columns else 0

    unknown_group_preview: list[dict] = []
    if rows and "group_key" in attached.columns and "group_match_found" in attached.columns:
        unknown_group_preview = (
            attached.loc[~attached["group_match_found"], ["group_key"]]
            .value_counts()
            .rename("rows")
            .reset_index()
            .head(unknown_group_preview_limit)
            .to_dict("records")
        )

    split_rows: list[dict] = []
    if rows and "dataset_split" in attached.columns:
        grouped = (
            attached.groupby("dataset_split", observed=True)
            .agg(
                rows=("market_id", "size"),
                fine_match_rows=("fine_match_found", "sum"),
                group_match_rows=("group_match_found", "sum"),
                group_fallback_rows=("used_group_fallback_only", "sum"),
            )
            .reset_index()
        )
        for row in grouped.to_dict("records"):
            split_rows.append(
                {
                    "dataset_split": row["dataset_split"],
                    "rows": int(row["rows"]),
                    "fine_match_rate": _rate(int(row["fine_match_rows"]), int(row["rows"])),
                    "group_match_rate": _rate(int(row["group_match_rows"]), int(row["rows"])),
                    "group_fallback_only_rate": _rate(int(row["group_fallback_rows"]), int(row["rows"])),
                }
            )

    return {
        "artifact_mode": artifact_mode,
        "sample_filters": {
            "max_rows": max_rows,
            "recent_days": recent_days,
            "split_reference_end": split_reference_end,
            "history_start": history_start,
        },
        "snapshot_rows_after_quality_and_split": int(len(snapshots)),
        "matched_rule_rows": int(len(matched)),
        "matched_group_keys": int(matched["group_key"].nunique()) if not matched.empty and "group_key" in matched.columns else 0,
        "bundle_load_seconds": float(bundle_load_seconds),
        "attach_seconds": float(attach_seconds),
        "group_features_rows": int(len(serving_feature_bundle.group_features)),
        "fine_features_rows": int(len(serving_feature_bundle.fine_features)),
        "group_features_memory_mb": _frame_memory_mb(serving_feature_bundle.group_features),
        "fine_features_memory_mb": _frame_memory_mb(serving_feature_bundle.fine_features),
        "defaults_manifest_entries": int(len(serving_feature_bundle.defaults_manifest.get("fine_feature_defaults", {}))),
        "group_match_rate": _rate(rows - unknown_group_rows, rows),
        "fine_match_rate": _rate(fine_match_rows, rows),
        "group_fallback_only_rate": _rate(group_fallback_rows, rows),
        "unknown_group_rate": _rate(unknown_group_rows, rows),
        "split_rows": split_rows,
        "unknown_group_preview": unknown_group_preview,
    }


def build_runtime_report_markdown(payload: dict) -> str:
    filters = payload.get("sample_filters", {})
    lines = [
        "# GroupKey Runtime Report",
        "",
        "## Summary",
        "",
        f"- artifact_mode={payload.get('artifact_mode')}",
        f"- snapshot_rows_after_quality_and_split={payload.get('snapshot_rows_after_quality_and_split', 0)}",
        f"- matched_rule_rows={payload.get('matched_rule_rows', 0)}",
        f"- matched_group_keys={payload.get('matched_group_keys', 0)}",
        f"- group_features_rows={payload.get('group_features_rows', 0)}",
        f"- fine_features_rows={payload.get('fine_features_rows', 0)}",
        f"- defaults_manifest_entries={payload.get('defaults_manifest_entries', 0)}",
        f"- bundle_load_seconds={payload.get('bundle_load_seconds', 0.0):.4f}",
        f"- attach_seconds={payload.get('attach_seconds', 0.0):.4f}",
        f"- group_features_memory_mb={payload.get('group_features_memory_mb', 0.0):.3f}",
        f"- fine_features_memory_mb={payload.get('fine_features_memory_mb', 0.0):.3f}",
        f"- group_match_rate={payload.get('group_match_rate', 0.0):.4f}",
        f"- fine_match_rate={payload.get('fine_match_rate', 0.0):.4f}",
        f"- group_fallback_only_rate={payload.get('group_fallback_only_rate', 0.0):.4f}",
        f"- unknown_group_rate={payload.get('unknown_group_rate', 0.0):.4f}",
        "",
        "## Sample Filters",
        "",
        f"- max_rows={filters.get('max_rows')}",
        f"- recent_days={filters.get('recent_days')}",
        f"- split_reference_end={filters.get('split_reference_end')}",
        f"- history_start={filters.get('history_start')}",
        "",
    ]
    lines.extend(
        _render_rows(
            "By Split Coverage",
            payload.get("split_rows", []),
            ["dataset_split", "rows", "group_match_rate", "fine_match_rate", "group_fallback_only_rate"],
        )
    )
    lines.extend(
        _render_rows(
            "Unknown Group Preview",
            payload.get("unknown_group_preview", []),
            ["group_key", "rows"],
        )
    )
    return "\n".join(lines).strip() + "\n"


def write_groupkey_reports(artifact_mode: str = "offline") -> dict[str, Path]:
    artifact_paths = build_artifact_paths(artifact_mode)
    docs_dir = Path("polymarket_rule_engine/docs")
    docs_dir.mkdir(parents=True, exist_ok=True)

    rule_funnel_summary = _read_json(artifact_paths.rule_funnel_summary_path)
    defaults_manifest = _read_json(artifact_paths.serving_feature_defaults_path)
    report_df = pd.read_csv(artifact_paths.rule_report_path) if artifact_paths.rule_report_path.exists() else pd.DataFrame()
    rules_df = pd.read_csv(artifact_paths.rules_path) if artifact_paths.rules_path.exists() else pd.DataFrame()
    group_features = pd.read_parquet(artifact_paths.group_serving_features_path)
    fine_features = pd.read_parquet(artifact_paths.fine_serving_features_path)
    feature_contract = load_feature_contract(_resolve_feature_contract_path(artifact_paths))

    migration_path = docs_dir / "groupkey_migration_validation.md"
    consistency_path = docs_dir / "groupkey_consistency_report.md"

    migration_path.write_text(
        build_migration_validation_markdown(
            rule_funnel_summary=rule_funnel_summary,
            report_df=report_df,
            rules_df=rules_df,
        ),
        encoding="utf-8",
    )
    consistency_path.write_text(
        build_consistency_report_markdown(
            feature_contract=feature_contract,
            group_features=group_features,
            fine_features=fine_features,
            defaults_manifest=defaults_manifest,
        ),
        encoding="utf-8",
    )
    return {
        "migration": migration_path,
        "consistency": consistency_path,
    }
