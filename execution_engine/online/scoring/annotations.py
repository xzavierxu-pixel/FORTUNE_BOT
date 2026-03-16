"""Shared online market annotation helpers."""

from __future__ import annotations

from collections import Counter
from typing import Any
import json
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULE_IMPORTS_READY = False
_ANNOTATION_COLUMNS = [
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


def _ensure_rule_engine_import_path(cfg: PegConfig) -> None:
    global _RULE_IMPORTS_READY
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)
    _RULE_IMPORTS_READY = True


def _normalize_category_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if text else "UNKNOWN"


def _normalize_game_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.upper() == "UNKNOWN":
        return ""
    return text


def _build_annotation_input_frame(markets: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for row in markets.to_dict(orient="records"):
        outcome_labels = [
            str(row.get("outcome_0_label") or ""),
            str(row.get("outcome_1_label") or ""),
        ]
        rows.append(
            {
                "id": str(row.get("market_id") or ""),
                "resolutionSource": str(row.get("resolution_source") or ""),
                "description": str(row.get("description") or ""),
                "outcomes": json.dumps(outcome_labels, ensure_ascii=True),
                "gameId": _normalize_game_id(row.get("game_id")),
                "category": _normalize_category_text(row.get("category_raw") or row.get("category")),
            }
        )
    return pd.DataFrame(rows)


def _normalize_domains_against_offline_reference(
    annotations: pd.DataFrame,
    load_market_annotations,
    rule_config,
) -> pd.DataFrame:
    offline_annotations = load_market_annotations(rebuild_if_missing=False)
    if offline_annotations.empty or "domain_parsed" not in offline_annotations.columns:
        return annotations

    domain_counts = Counter(
        str(domain)
        for domain in offline_annotations["domain_parsed"].fillna("").astype(str)
        if str(domain) not in {"", "UNKNOWN"}
    )
    if not domain_counts:
        return annotations

    out = annotations.copy()

    def normalize_domain(domain: str) -> str:
        if domain in {"", "UNKNOWN"}:
            return "UNKNOWN"
        if domain_counts.get(domain, 0) < rule_config.LOW_FREQUENCY_DOMAIN_COUNT:
            return "OTHER"
        return domain

    out["domain"] = out["domain_parsed"].fillna("UNKNOWN").astype(str).apply(normalize_domain)
    return out


def build_online_annotations(cfg: PegConfig, markets: pd.DataFrame) -> pd.DataFrame:
    if markets.empty:
        return pd.DataFrame(columns=_ANNOTATION_COLUMNS)

    _ensure_rule_engine_import_path(cfg)
    from rule_baseline.domain_extractor.market_annotations import (  # type: ignore
        build_market_annotations,
        load_market_annotations,
    )
    from rule_baseline.utils import config as rule_config  # type: ignore

    annotation_inputs = _build_annotation_input_frame(markets)
    if annotation_inputs.empty:
        return pd.DataFrame(columns=_ANNOTATION_COLUMNS)

    annotations = build_market_annotations(annotation_inputs.fillna(""))
    if annotations.empty:
        return pd.DataFrame(columns=_ANNOTATION_COLUMNS)

    annotations["market_id"] = annotations["market_id"].astype(str)
    annotations = _normalize_domains_against_offline_reference(
        annotations,
        load_market_annotations=load_market_annotations,
        rule_config=rule_config,
    )
    return annotations.drop_duplicates(subset=["market_id"]).reset_index(drop=True)


def apply_online_market_annotations(cfg: PegConfig, markets: pd.DataFrame) -> pd.DataFrame:
    if markets.empty:
        return markets

    annotations = build_online_annotations(cfg, markets)
    if annotations.empty:
        return markets

    merge_columns = [column for column in _ANNOTATION_COLUMNS if column in annotations.columns]
    out = markets.merge(
        annotations[merge_columns],
        on="market_id",
        how="left",
        suffixes=("", "_annotation"),
    )

    for column in merge_columns:
        if column == "market_id":
            continue
        annotation_column = f"{column}_annotation"
        if annotation_column not in out.columns:
            continue
        current = out[column] if column in out.columns else pd.Series(pd.NA, index=out.index)
        out[column] = out[annotation_column].where(out[annotation_column].notna(), current)
        out = out.drop(columns=[annotation_column])

    for column in [
        "domain",
        "category",
        "market_type",
        "domain_parsed",
        "source_url",
        "category_raw",
        "category_parsed",
        "outcome_pattern",
    ]:
        if column not in out.columns:
            out[column] = "UNKNOWN"
        out[column] = out[column].fillna("UNKNOWN").replace("", "UNKNOWN").astype(str)

    if "sub_domain" not in out.columns:
        out["sub_domain"] = ""
    out["sub_domain"] = out["sub_domain"].fillna("").astype(str)

    if "category_override_flag" not in out.columns:
        out["category_override_flag"] = False
    out["category_override_flag"] = out["category_override_flag"].fillna(False).astype(bool)
    return out
