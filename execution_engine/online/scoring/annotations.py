"""Shared online market annotation helpers."""

from __future__ import annotations

from typing import Any, Dict, List
import json
import sys

import pandas as pd

from execution_engine.runtime.config import PegConfig

_RULE_IMPORTS_READY = False


def _ensure_rule_engine_import_path(cfg: PegConfig) -> None:
    global _RULE_IMPORTS_READY
    rule_engine_dir = str(cfg.rule_engine_dir)
    if rule_engine_dir not in sys.path:
        sys.path.insert(0, rule_engine_dir)
    _RULE_IMPORTS_READY = True


def _normalize_category_text(value: Any) -> str:
    text = str(value or "").strip().upper()
    return text if text else "UNKNOWN"


def build_online_annotations(cfg: PegConfig, markets: pd.DataFrame) -> pd.DataFrame:
    if markets.empty:
        return pd.DataFrame(columns=["market_id"])

    _ensure_rule_engine_import_path(cfg)
    from rule_baseline.domain_extractor.market_annotations import (  # type: ignore
        MarketSourceParser,
        infer_category_from_source,
        normalize_outcomes,
    )

    rows: List[Dict[str, Any]] = []
    for row in markets.to_dict(orient="records"):
        resolution_source = str(row.get("resolution_source") or "")
        description = str(row.get("description") or "")
        source_url = resolution_source or (MarketSourceParser.extract_url_from_text(description) or "UNKNOWN")
        domain_parsed, sub_domain, source_url = MarketSourceParser.parse_domain_parts(source_url)
        domain = domain_parsed if domain_parsed not in {"", "UNKNOWN"} else str(row.get("domain") or "UNKNOWN")

        outcome_labels = [
            str(row.get("outcome_0_label") or ""),
            str(row.get("outcome_1_label") or ""),
        ]
        market_type, outcome_pattern = normalize_outcomes(json.dumps(outcome_labels, ensure_ascii=True))
        raw_category = _normalize_category_text(row.get("category"))
        category_parsed = str(
            infer_category_from_source(
                pd.Series([domain_parsed]),
                pd.Series([str(row.get("game_id") or "")]),
            ).iloc[0]
        )
        category = category_parsed if category_parsed != "UNKNOWN" else raw_category
        if not market_type:
            market_type = str(row.get("market_type") or "UNKNOWN")

        rows.append(
            {
                "market_id": str(row.get("market_id") or ""),
                "domain": domain or "UNKNOWN",
                "domain_parsed": domain_parsed or domain or "UNKNOWN",
                "sub_domain": sub_domain,
                "source_url": source_url or "UNKNOWN",
                "category": category or "UNKNOWN",
                "category_raw": raw_category,
                "category_parsed": category_parsed,
                "category_override_flag": bool(
                    category_parsed != "UNKNOWN" and raw_category != "UNKNOWN" and category_parsed != raw_category
                ),
                "market_type": market_type or "UNKNOWN",
                "outcome_pattern": outcome_pattern or "UNKNOWN",
            }
        )
    return pd.DataFrame(rows).drop_duplicates(subset=["market_id"]).reset_index(drop=True)


def apply_online_market_annotations(cfg: PegConfig, markets: pd.DataFrame) -> pd.DataFrame:
    if markets.empty:
        return markets
    annotations = build_online_annotations(cfg, markets)
    if annotations.empty:
        return markets
    out = markets.merge(
        annotations[["market_id", "domain", "category", "market_type"]],
        on="market_id",
        how="left",
        suffixes=("", "_annotation"),
    )
    for column in ["domain", "category", "market_type"]:
        annotation_column = f"{column}_annotation"
        if annotation_column in out.columns:
            current = out[column] if column in out.columns else pd.Series("UNKNOWN", index=out.index)
            out[column] = out[annotation_column].fillna(current).replace("", "UNKNOWN")
            out = out.drop(columns=[annotation_column])
        if column not in out.columns:
            out[column] = "UNKNOWN"
        out[column] = out[column].fillna("UNKNOWN").astype(str)
    return out

