from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ServingFeatureBundle:
    fine_features: pd.DataFrame
    group_features: pd.DataFrame
    defaults_manifest: dict


def build_group_key(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["domain"].astype(str)
        + "|"
        + frame["category"].astype(str)
        + "|"
        + frame["market_type"].astype(str)
    )


def round_horizon_hours(values: pd.Series) -> pd.Series:
    rounded = pd.to_numeric(values, errors="coerce").round()
    return rounded.astype("Int64")


def build_price_bin(values: pd.Series) -> pd.Series:
    price = pd.to_numeric(values, errors="coerce")
    lower = (np.floor(price * 10.0) / 10.0).clip(lower=0.0, upper=0.9)
    upper = (lower + 0.1).clip(lower=0.1, upper=1.0)
    labels = lower.map(lambda value: f"{float(value):.2f}") + "-" + upper.map(lambda value: f"{float(value):.2f}")
    return labels.where(price.notna(), None)


def attach_serving_features(
    frame: pd.DataFrame,
    bundle: ServingFeatureBundle,
    *,
    price_column: str,
    horizon_column: str,
) -> pd.DataFrame:
    out = frame.copy().reset_index(drop=True)
    if out.empty:
        return out

    out["group_key"] = build_group_key(out)
    out["price_bin"] = build_price_bin(out[price_column]) if price_column in out.columns else None
    out["rounded_horizon_hours"] = (
        round_horizon_hours(out[horizon_column])
        if horizon_column in out.columns
        else pd.Series(pd.NA, index=out.index, dtype="Int64")
    )
    out["group_match_found"] = False
    out["fine_match_found"] = False
    out["used_group_fallback_only"] = False

    group_features = bundle.group_features.copy()
    if not group_features.empty:
        group_prefixed = group_features.add_prefix("group_feature_")
        group_prefixed = group_prefixed.rename(columns={"group_feature_group_key": "group_key"})
        out = out.merge(group_prefixed, on="group_key", how="left")
        out["group_match_found"] = (
            out["group_feature_group_decision"].notna()
            if "group_feature_group_decision" in out.columns
            else out["group_key"].notna()
        )

    fine_features = bundle.fine_features.copy()
    if not fine_features.empty:
        fine_features["horizon_hours"] = round_horizon_hours(fine_features["horizon_hours"])
        fine_prefixed = fine_features.add_prefix("fine_feature_")
        fine_prefixed = fine_prefixed.rename(
            columns={
                "fine_feature_group_key": "group_key",
                "fine_feature_price_bin": "price_bin",
                "fine_feature_horizon_hours": "rounded_horizon_hours",
            }
        )
        out = out.merge(
            fine_prefixed,
            on=["group_key", "price_bin", "rounded_horizon_hours"],
            how="left",
        )
        if "fine_feature_leaf_id" in out.columns:
            out["fine_match_found"] = out["fine_feature_leaf_id"].notna()

    defaults = bundle.defaults_manifest.get("fine_feature_defaults", {})
    for feature_name, metadata in defaults.items():
        fine_column = f"fine_feature_{feature_name}"
        group_column = f"group_feature_{metadata.get('group_column')}"
        if fine_column not in out.columns:
            out[fine_column] = pd.NA
        if group_column not in out.columns:
            out[group_column] = pd.NA
        out[fine_column] = out[fine_column].where(out["fine_match_found"], out[group_column])

    out["used_group_fallback_only"] = out["group_match_found"] & ~out["fine_match_found"]
    return out
