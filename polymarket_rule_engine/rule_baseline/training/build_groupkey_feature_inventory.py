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

def _docs_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "docs"


INVENTORY_OUTPUT_PATH = _docs_dir() / "groupkey_feature_inventory.csv"
INVENTORY_SUMMARY_PATH = _docs_dir() / "groupkey_feature_inventory_summary.md"
OVERRIDES_PATH = _docs_dir() / "groupkey_feature_inventory_overrides.csv"

FINAL_STATUS_SORT_ORDER = {
    "implemented_exact": 0,
    "implemented_approximate": 1,
    "duplicate_or_merge": 2,
    "intentionally_excluded": 3,
    "unsupported_now": 4,
    "pending_implementation": 5,
}


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
        "status": "implemented_exact",
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
            "status": "pending_implementation",
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
            "status": "pending_implementation",
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
            "status": "pending_implementation",
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
            "status": "pending_implementation",
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
            "status": "implemented_exact",
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
    blueprint_path = _docs_dir() / "polymarket_groupkey_500_feature_blueprint.md"
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
                "status": "pending_implementation",
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
    structural_prefix_replacements = [
        ("domain_category_", "domain_x_category_"),
        ("domain_market_type_", "domain_x_market_type_"),
        ("category_market_type_", "category_x_market_type_"),
    ]
    expanded_prefixes = list(candidates)
    for candidate in list(candidates):
        for old, new in structural_prefix_replacements:
            if candidate.startswith(old):
                expanded_prefixes.append(candidate.replace(old, new, 1))
    replacements = [
        ("_median", "_p50"),
        ("_q25", "_p25"),
        ("_q75", "_p75"),
        ("_q90", "_p90"),
    ]
    expanded = list(expanded_prefixes)
    for candidate in list(expanded_prefixes):
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


def _load_overrides() -> dict[tuple[str, str], dict[str, str]]:
    if not OVERRIDES_PATH.exists():
        return {}
    overrides_df = pd.read_csv(OVERRIDES_PATH).fillna("")
    overrides: dict[tuple[str, str], dict[str, str]] = {}
    for row in overrides_df.to_dict("records"):
        key = (str(row.get("feature_name", "")), str(row.get("source_table", "")))
        overrides[key] = {str(k): str(v) for k, v in row.items()}
    return overrides


def classify_blueprint_pending_row(row: dict[str, str]) -> dict[str, str]:
    notes = str(row.get("notes", ""))
    feature_name = str(row.get("feature_name", ""))
    unsupported_prefixes = (
        "p_",
        "logit_p_",
        "delta_p_",
        "delta_logit_",
        "dist_to_0p5_",
        "dist_to_extreme_",
        "local_accel_p_",
        "local_slope_p_",
    )
    if "建议交给树模型自动学习" in notes:
        row["status"] = "intentionally_excluded"
        row["audit_class"] = "E_intentionally_excluded"
        return row
    if feature_name == "group_key_history_share_expanding":
        row["status"] = "implemented_approximate"
        row["audit_class"] = "D_duplicate_or_merge"
        row["implemented_in"] = "derived_group_share"
        row["match_quality"] = "approximate"
        row["matched_feature_name"] = "group_snapshot_share_global"
        row["serving_asset"] = "group_serving_features.parquet"
        return row
    if feature_name in {
        "domain_history_share_expanding",
        "category_history_share_expanding",
        "market_type_history_share_expanding",
    }:
        prefix = feature_name.replace("_history_share_expanding", "")
        row["status"] = "implemented_approximate"
        row["audit_class"] = "D_duplicate_or_merge"
        row["implemented_in"] = "derived_history_share"
        row["match_quality"] = "approximate"
        row["matched_feature_name"] = f"{prefix}_expanding_snapshot_count / global_expanding_snapshot_count"
        row["serving_asset"] = "group_serving_features.parquet"
        return row
    if feature_name.startswith(unsupported_prefixes):
        row["status"] = "unsupported_now"
        row["audit_class"] = "F_unsupported_now"
        return row
    if "建议与 count/shrinkage 一起使用" in notes or "越宽说明方向性越不稳" in notes:
        row["status"] = "unsupported_now"
        row["audit_class"] = "F_unsupported_now"
        return row
    if "建议直接 categorical" in notes or "target encoding" in notes or feature_name.endswith("_id_hash") or feature_name.endswith("_id"):
        row["status"] = "intentionally_excluded"
        row["audit_class"] = "E_intentionally_excluded"
        return row
    if "h=12 时该值恒为 0" in notes or "若建多 outcome 模型" in notes:
        row["status"] = "intentionally_excluded"
        row["audit_class"] = "E_intentionally_excluded"
        return row
    if feature_name.startswith(("category_is_", "market_type_is_")):
        row["status"] = "intentionally_excluded"
        row["audit_class"] = "E_intentionally_excluded"
        return row
    row["status"] = "pending_implementation"
    row["audit_class"] = "B_keep_but_later"
    return row


def apply_inventory_override(row: dict[str, str], overrides: dict[tuple[str, str], dict[str, str]]) -> dict[str, str]:
    override = overrides.get((str(row.get("feature_name", "")), str(row.get("source_table", ""))))
    if override is None:
        return row
    if override.get("status"):
        row["status"] = override["status"]
    if override.get("audit_class"):
        row["audit_class"] = override["audit_class"]
    notes_append = override.get("notes_append", "")
    if notes_append:
        row["notes"] = f"{row.get('notes', '')} | {notes_append}".strip(" |")
    return row


def build_inventory_summary_markdown(inventory: pd.DataFrame) -> str:
    status_counts = inventory["status"].value_counts().to_dict()
    audit_counts = inventory["audit_class"].value_counts().to_dict()
    lines = [
        "# GroupKey Feature Inventory Summary",
        "",
        f"- total_rows={len(inventory)}",
        "",
        "## Status Counts",
        "",
    ]
    for status, count in sorted(status_counts.items(), key=lambda item: FINAL_STATUS_SORT_ORDER.get(item[0], 999)):
        lines.append(f"- {status}={count}")
    lines.extend(["", "## Audit Class Counts", ""])
    for audit_class, count in sorted(audit_counts.items()):
        lines.append(f"- {audit_class}={count}")
    return "\n".join(lines).strip() + "\n"


def build_inventory(artifact_mode: str = "offline") -> pd.DataFrame:
    artifact_paths = build_artifact_paths(artifact_mode)
    overrides = _load_overrides()
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
            row["status"] = "implemented_exact"
            row["audit_class"] = "C_already_implemented"
            row["implemented_in"] = "blueprint_exact_name_match"
            row["match_quality"] = "exact"
            row["matched_feature_name"] = feature_name
            row["serving_asset"] = "matched_existing_asset"
        else:
            alias_hit = next((candidate for candidate in _alias_candidates(feature_name) if candidate in implemented_names), None)
            if alias_hit is not None:
                row["status"] = "implemented_approximate"
                row["audit_class"] = "D_duplicate_or_merge"
                row["implemented_in"] = "blueprint_alias_match"
                row["match_quality"] = "approximate"
                row["matched_feature_name"] = alias_hit
                row["serving_asset"] = "matched_existing_asset"
            else:
                row = classify_blueprint_pending_row(row)
        row = apply_inventory_override(row, overrides)
        normalized_blueprint_rows.append(row)

    inventory = pd.concat([inventory, pd.DataFrame(normalized_blueprint_rows, columns=INVENTORY_COLUMNS)], ignore_index=True)
    inventory = inventory.drop_duplicates(subset=["feature_name", "source_table"], keep="first")
    inventory["_status_sort"] = inventory["status"].map(FINAL_STATUS_SORT_ORDER).fillna(999)
    inventory = inventory.sort_values(
        ["_status_sort", "audit_class", "source_table", "feature_family", "feature_name"],
        ascending=[True, True, True, True, True],
    ).drop(columns=["_status_sort"]).reset_index(drop=True)
    return inventory


def main() -> None:
    inventory = build_inventory("offline")
    INVENTORY_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    inventory.to_csv(INVENTORY_OUTPUT_PATH, index=False)
    INVENTORY_SUMMARY_PATH.write_text(build_inventory_summary_markdown(inventory), encoding="utf-8")
    print(f"[INFO] Wrote {len(inventory)} inventory rows to {INVENTORY_OUTPUT_PATH.resolve()}")
    print(f"[INFO] Wrote inventory summary to {INVENTORY_SUMMARY_PATH.resolve()}")


if __name__ == "__main__":
    main()
