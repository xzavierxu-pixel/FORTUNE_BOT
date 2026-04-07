from __future__ import annotations

from typing import Any

import pandas as pd

NORMALIZATION_MANIFEST_VERSION = 1
ANNOTATION_PIPELINE_VERSION = "shared_v1"
UNKNOWN = "UNKNOWN"
OTHER = "OTHER"
ANNOTATION_CATEGORY_SOURCE = "annotation"
SNAPSHOT_CATEGORY_SOURCE = "snapshot"
CACHE_CATEGORY_SOURCE = "cache"


def build_normalization_manifest(offline_annotations: pd.DataFrame) -> dict[str, Any]:
    allowed_domains: list[str] = []
    if not offline_annotations.empty and "domain" in offline_annotations.columns:
        domain_values = (
            offline_annotations["domain"]
            .fillna("")
            .astype(str)
            .map(lambda value: value.strip())
        )
        allowed_domains = sorted(
            {
                value
                for value in domain_values
                if value and value not in {UNKNOWN, OTHER}
            }
        )
    return {
        "manifest_version": NORMALIZATION_MANIFEST_VERSION,
        "annotation_pipeline_version": ANNOTATION_PIPELINE_VERSION,
        "domain_policy": {
            "allowed_domains": allowed_domains,
            "unknown_fallback": UNKNOWN,
            "other_fallback": OTHER,
        },
    }


def normalize_domain_value(candidate: Any, manifest: dict[str, Any]) -> str:
    policy = manifest.get("domain_policy") or {}
    allowed_domains = {str(value) for value in policy.get("allowed_domains", [])}
    unknown_fallback = str(policy.get("unknown_fallback") or UNKNOWN)
    other_fallback = str(policy.get("other_fallback") or OTHER)
    text = str(candidate or "").strip()
    if not text or text == unknown_fallback:
        return unknown_fallback
    if text in allowed_domains:
        return text
    if text == other_fallback:
        return other_fallback
    return other_fallback


def normalize_market_annotations(
    annotations: pd.DataFrame,
    *,
    vocabulary_manifest: dict[str, Any],
) -> pd.DataFrame:
    out = annotations.copy()
    domain_source = "domain" if "domain" in out.columns else "domain_candidate"
    if domain_source in out.columns:
        out["domain"] = (
            out[domain_source]
            .fillna(UNKNOWN)
            .astype(str)
            .map(lambda value: normalize_domain_value(value, vocabulary_manifest))
        )
    else:
        out["domain"] = UNKNOWN

    for column in ["category", "category_raw", "category_parsed", "market_type", "domain_parsed", "source_url", "outcome_pattern"]:
        if column in out.columns:
            out[column] = out[column].fillna(UNKNOWN).replace("", UNKNOWN).astype(str)
    if "sub_domain" in out.columns:
        out["sub_domain"] = out["sub_domain"].fillna("").astype(str)
    if "category_override_flag" in out.columns:
        out["category_override_flag"] = out["category_override_flag"].fillna(False).astype(bool)
    return out


def _annotation_value_present(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text != ""


def merge_market_annotation_projection(
    base: pd.DataFrame,
    annotations: pd.DataFrame,
    *,
    category_source_column: str = "category_source",
) -> pd.DataFrame:
    out = base.copy()
    existing_category_source = (
        out.get(category_source_column, pd.Series(SNAPSHOT_CATEGORY_SOURCE, index=out.index))
        .astype("string")
        .fillna(SNAPSHOT_CATEGORY_SOURCE)
        .replace("", SNAPSHOT_CATEGORY_SOURCE)
    )

    annotation_columns = [
        "market_id",
        "domain",
        "domain_parsed",
        "sub_domain",
        "source_url",
        "category",
        "category_raw",
        "category_parsed",
        "category_override_flag",
        "market_type",
        "outcome_pattern",
    ]
    available_columns = [column for column in annotation_columns if column in annotations.columns]
    if available_columns:
        merged = annotations[available_columns].copy()
        merged["market_id"] = merged["market_id"].astype(str)
        out = out.merge(
            merged,
            on="market_id",
            how="left",
            suffixes=("", "_annotation"),
        )

        for column in [name for name in annotation_columns if name != "market_id"]:
            annotation_column = f"{column}_annotation"
            if annotation_column not in out.columns:
                continue
            current = out[column] if column in out.columns else pd.Series(pd.NA, index=out.index)
            annotation_values = out[annotation_column]
            present = (
                annotation_values.notna()
                if annotation_values.dtype == bool
                else _annotation_value_present(annotation_values)
            )
            out[column] = annotation_values.where(present, current)
            if column == "category":
                out[category_source_column] = existing_category_source.where(
                    ~present,
                    ANNOTATION_CATEGORY_SOURCE,
                )
            out = out.drop(columns=[annotation_column])

    for column in ["domain", "category", "market_type"]:
        if column not in out.columns:
            out[column] = UNKNOWN
        out[column] = out[column].fillna(UNKNOWN).astype(str)

    out[category_source_column] = (
        out.get(category_source_column, existing_category_source)
        .astype("string")
        .fillna(SNAPSHOT_CATEGORY_SOURCE)
        .replace("", SNAPSHOT_CATEGORY_SOURCE)
        .astype(str)
    )
    return out
