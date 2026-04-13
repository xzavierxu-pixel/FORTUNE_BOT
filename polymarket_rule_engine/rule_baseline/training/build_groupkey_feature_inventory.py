from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths


INVENTORY_COLUMNS = [
    "feature_name",
    "feature_family",
    "grain",
    "window",
    "source_table",
    "status",
    "audit_class",
    "implemented_in",
    "match_quality",
    "matched_feature_name",
    "serving_asset",
    "notes",
]


def _infer_window(feature_name: str) -> str:
    if "recent_50" in feature_name:
        return "recent_50"
    if "recent_200" in feature_name:
        return "recent_200"
    if "expanding" in feature_name:
        return "expanding"
    return "static"


def _infer_grain(feature_name: str, source_table: str) -> str:
    if source_table == "trading_rules.csv":
        return "rule_or_group_static"
    if source_table == "fine_serving_features.parquet":
        return "group_key_price_bin_horizon"
    if source_table == "group_serving_features.parquet":
        if feature_name.startswith("global_"):
            return "global"
        if feature_name.startswith("domain_x_category_"):
            return "domain_x_category"
        if feature_name.startswith("domain_x_market_type_"):
            return "domain_x_market_type"
        if feature_name.startswith("category_x_market_type_"):
            return "category_x_market_type"
        if feature_name.startswith("domain_"):
            return "domain"
        if feature_name.startswith("category_"):
            return "category"
        if feature_name.startswith("market_type_"):
            return "market_type"
        if feature_name.startswith("full_group_"):
            return "full_group_key"
        return "group_key"
    return "unknown"


def _infer_family(feature_name: str, source_table: str) -> str:
    if feature_name in {"group_key", "domain", "category", "market_type", "price_bin", "horizon_hours"}:
        return "keys"
    if feature_name.startswith("group_default_"):
        return "fallback_default"
    if feature_name.endswith("_match_found") or "fallback" in feature_name:
        return "fallback_indicator"
    if feature_name.startswith("hist_price_x_") or feature_name.startswith("price_x_"):
        return "history_price_interaction"
    if feature_name.startswith("rule_edge_minus_") or feature_name.startswith("rule_score_minus_"):
        return "rule_gap_interaction"
    if feature_name.endswith("_tail_instability_ratio"):
        return "tail_risk"
    if feature_name.endswith("_tail_spread") or "_tail_" in feature_name:
        return "tail_risk"
    if feature_name.endswith("_zscore"):
        return "drift_gap"
    if "_vs_" in feature_name and "_gap" in feature_name:
        return "drift_gap"
    if feature_name.startswith("rule_") or feature_name in {
        "leaf_id",
        "direction",
        "q_full",
        "p_full",
        "edge_full",
        "edge_std_full",
        "edge_lower_bound_full",
        "rule_score",
        "n_full",
    }:
        return "rule_prior"
    if any(token in feature_name for token in ["logloss", "brier", "bias", "abs_bias", "snapshot_count", "market_count"]):
        return "history_metric"
    if source_table == "trading_rules.csv":
        return "rule_schema"
    return "other"


def _implemented_row(feature_name: str, source_table: str) -> dict[str, str]:
    return {
        "feature_name": feature_name,
        "feature_family": _infer_family(feature_name, source_table),
        "grain": _infer_grain(feature_name, source_table),
        "window": _infer_window(feature_name),
        "source_table": source_table,
        "status": "implemented",
        "audit_class": "C_already_implemented",
        "implemented_in": "serving_assets",
        "match_quality": "direct_asset_column",
        "matched_feature_name": feature_name,
        "serving_asset": source_table,
        "notes": "",
    }


def _pending_rows() -> list[dict[str, str]]:
    rows = [
        {
            "feature_name": "full_group_recent_50_vs_expanding_bias_zscore",
            "feature_family": "drift_gap",
            "grain": "full_group_key",
            "window": "recent_50_vs_expanding",
            "source_table": "planned_group_serving_features",
            "status": "pending",
            "audit_class": "B_keep_but_later",
            "implemented_in": "",
            "match_quality": "",
            "matched_feature_name": "",
            "serving_asset": "group_serving_features.parquet",
            "notes": "Need normalized drift intensity rather than raw gap only.",
        },
        {
            "feature_name": "full_group_recent_200_vs_expanding_logloss_zscore",
            "feature_family": "drift_gap",
            "grain": "full_group_key",
            "window": "recent_200_vs_expanding",
            "source_table": "planned_group_serving_features",
            "status": "pending",
            "audit_class": "B_keep_but_later",
            "implemented_in": "",
            "match_quality": "",
            "matched_feature_name": "",
            "serving_asset": "group_serving_features.parquet",
            "notes": "Need standardized long-window drift metric.",
        },
        {
            "feature_name": "full_group_recent_50_tail_instability_ratio",
            "feature_family": "tail_risk",
            "grain": "full_group_key",
            "window": "recent_50",
            "source_table": "planned_group_serving_features",
            "status": "pending",
            "audit_class": "B_keep_but_later",
            "implemented_in": "",
            "match_quality": "",
            "matched_feature_name": "",
            "serving_asset": "group_serving_features.parquet",
            "notes": "Need tail spread normalized by median or std.",
        },
        {
            "feature_name": "full_group_recent_200_tail_instability_ratio",
            "feature_family": "tail_risk",
            "grain": "full_group_key",
            "window": "recent_200",
            "source_table": "planned_group_serving_features",
            "status": "pending",
            "audit_class": "B_keep_but_later",
            "implemented_in": "",
            "match_quality": "",
            "matched_feature_name": "",
            "serving_asset": "group_serving_features.parquet",
            "notes": "Need long-window tail instability ratio.",
        },
        {
            "feature_name": "feature_blueprint_exhaustive_500_review",
            "feature_family": "inventory_governance",
            "grain": "meta",
            "window": "static",
            "source_table": "inventory",
            "status": "implemented",
            "audit_class": "C_already_implemented",
            "implemented_in": "inventory_generator",
            "match_quality": "exact",
            "matched_feature_name": "feature_blueprint_exhaustive_500_review",
            "serving_asset": "groupkey_feature_inventory.csv",
            "notes": "Blueprint rows are now explicitly parsed into the inventory.",
        },
    ]
    return rows


def _parse_blueprint_rows() -> list[dict[str, str]]:
    blueprint_path = Path("polymarket_rule_engine/docs/polymarket_groupkey_500_feature_blueprint.md")
    if not blueprint_path.exists():
        return []
    lines = blueprint_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    parsed_rows: list[dict[str, str]] = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        parts = [part.strip() for part in stripped.strip("|").split("|")]
        if len(parts) < 8 or not parts[0].isdigit():
            continue
        parsed_rows.append(
            {
                "feature_name": parts[1],
                "feature_family": parts[2],
                "grain": parts[3],
                "window": parts[4],
                "source_table": "blueprint",
                "status": "pending",
                "audit_class": "B_keep_but_later",
                "implemented_in": "",
                "match_quality": "",
                "matched_feature_name": "",
                "serving_asset": "",
                "notes": parts[7],
            }
        )
    return parsed_rows


def _alias_candidates(feature_name: str) -> list[str]:
    candidates = [feature_name]
    if feature_name.startswith("group_key_"):
        candidates.append(feature_name.replace("group_key_", "full_group_", 1))
    if feature_name.startswith("full_group_key_"):
        candidates.append(feature_name.replace("full_group_key_", "full_group_", 1))
    replacements = [
        ("_median", "_p50"),
        ("_q25", "_p25"),
        ("_q75", "_p75"),
        ("_q90", "_p90"),
    ]
    expanded = list(candidates)
    for candidate in list(candidates):
        for old, new in replacements:
            if old in candidate:
                expanded.append(candidate.replace(old, new))
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in expanded:
        if candidate not in seen:
            ordered.append(candidate)
            seen.add(candidate)
    return ordered


def build_inventory(artifact_mode: str = "offline") -> pd.DataFrame:
    artifact_paths = build_artifact_paths(artifact_mode)
    implemented_rows: list[dict[str, str]] = []

    sources = [
        ("trading_rules.csv", artifact_paths.rules_path, "csv"),
        ("group_serving_features.parquet", artifact_paths.group_serving_features_path, "parquet"),
        ("fine_serving_features.parquet", artifact_paths.fine_serving_features_path, "parquet"),
    ]
    for source_name, path, kind in sources:
        if not path.exists():
            continue
        frame = pd.read_csv(path, nrows=3) if kind == "csv" else pd.read_parquet(path)
        for column in frame.columns:
            implemented_rows.append(_implemented_row(str(column), source_name))

    inventory = pd.DataFrame(implemented_rows, columns=INVENTORY_COLUMNS)
    implemented_names = set(inventory["feature_name"].astype(str))

    pending_rows = [row for row in _pending_rows() if row["feature_name"] not in implemented_names]
    inventory = pd.concat([inventory, pd.DataFrame(pending_rows, columns=INVENTORY_COLUMNS)], ignore_index=True)

    blueprint_rows = _parse_blueprint_rows()
    normalized_blueprint_rows: list[dict[str, str]] = []
    for row in blueprint_rows:
        feature_name = str(row["feature_name"])
        if feature_name in implemented_names:
            row["status"] = "implemented"
            row["audit_class"] = "C_already_implemented"
            row["implemented_in"] = "blueprint_exact_name_match"
            row["match_quality"] = "exact"
            row["matched_feature_name"] = feature_name
            row["serving_asset"] = "matched_existing_asset"
        else:
            alias_hit = next((candidate for candidate in _alias_candidates(feature_name) if candidate in implemented_names), None)
            if alias_hit is not None:
                row["status"] = "implemented"
                row["audit_class"] = "D_duplicate_or_merge"
                row["implemented_in"] = "blueprint_alias_match"
                row["match_quality"] = "approximate"
                row["matched_feature_name"] = alias_hit
                row["serving_asset"] = "matched_existing_asset"
        normalized_blueprint_rows.append(row)

    inventory = pd.concat([inventory, pd.DataFrame(normalized_blueprint_rows, columns=INVENTORY_COLUMNS)], ignore_index=True)
    inventory = inventory.drop_duplicates(subset=["feature_name", "source_table"], keep="first")
    inventory = inventory.sort_values(
        ["status", "audit_class", "source_table", "feature_family", "feature_name"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)
    return inventory


def main() -> None:
    inventory = build_inventory("offline")
    output_path = Path("polymarket_rule_engine/docs/groupkey_feature_inventory.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    inventory.to_csv(output_path, index=False)
    print(f"[INFO] Wrote {len(inventory)} inventory rows to {output_path.resolve()}")


if __name__ == "__main__":
    main()
