from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.research_context import assign_dataset_split, build_artifact_paths, compute_temporal_split
from rule_baseline.utils.research_data import load_research_snapshots

PRICE_MIN = 0.05
PRICE_MAX = 0.95
MIN_RULE_N = 50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze rule alpha on the strict test split.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    return parser.parse_args()


def load_inputs(artifact_paths) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshots = load_research_snapshots()
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_temporal_split(snapshots)
    snapshots = assign_dataset_split(snapshots, split)
    snapshots = snapshots[snapshots["dataset_split"] == "test"].copy()
    snapshots = snapshots[(snapshots["price"] >= PRICE_MIN) & (snapshots["price"] <= PRICE_MAX)].copy()

    rules = pd.read_csv(artifact_paths.rules_path)
    for column in ["domain", "category", "market_type"]:
        rules[column] = rules[column].fillna("UNKNOWN").astype(str)
    return snapshots, rules


def match_rules_to_snapshots(snapshots: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    merged = snapshots.merge(
        rules[
            [
                "group_key",
                "domain",
                "category",
                "market_type",
                "leaf_id",
                "price_min",
                "price_max",
                "h_min",
                "h_max",
                "edge_sample_trade",
                "direction",
            ]
        ],
        on=["domain", "category", "market_type"],
        how="inner",
    )

    mask = (
        (merged["price"] >= merged["price_min"])
        & (merged["price"] <= merged["price_max"])
        & (merged["horizon_hours"] >= merged["h_min"])
        & (merged["horizon_hours"] <= merged["h_max"])
    )
    matched = merged[mask].copy()
    if matched.empty:
        raise RuntimeError("No rules matched test snapshots under current schema.")
    return matched


def classify_rule_quadrant(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rule_says_buy = out["direction"] > 0
    market_leans_yes = out["price"] > 0.5
    is_contrarian = (rule_says_buy & ~market_leans_yes) | (~rule_says_buy & market_leans_yes)
    actual_edge = out["y"] - out["price"]
    rule_correct = ((out["direction"] > 0) & (actual_edge > 0)) | ((out["direction"] < 0) & (actual_edge < 0))

    out["quadrant"] = np.where(
        is_contrarian & rule_correct,
        "contrarian_correct",
        np.where(
            ~is_contrarian & rule_correct,
            "consensus_correct",
            np.where(~is_contrarian, "consensus_wrong", "contrarian_wrong"),
        ),
    )
    out["is_contrarian"] = is_contrarian
    out["rule_correct"] = rule_correct
    return out


def compute_rule_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (group_key, leaf_id), subset in df.groupby(["group_key", "leaf_id"]):
        if len(subset) < MIN_RULE_N:
            continue

        counts = subset["quadrant"].value_counts()
        cc = counts.get("contrarian_correct", 0)
        cw = counts.get("contrarian_wrong", 0)
        sc = counts.get("consensus_correct", 0)
        sw = counts.get("consensus_wrong", 0)
        contrarian_n = cc + cw
        alpha_ratio = cc / max(contrarian_n, 1) if contrarian_n else np.nan
        weighted_score = (2 * cc + sc - sw - 2 * cw) / len(subset)
        edge = subset["edge_sample_trade"].iloc[0]
        actual_edge = (subset["y"] - subset["price"]).mean()
        fee = config.FEE_RATE
        pnl = (subset["y"] - subset["price"] - fee).mean() if edge > 0 else (subset["price"] - subset["y"] - fee).mean()

        rows.append(
            {
                "group_key": group_key,
                "leaf_id": int(leaf_id),
                "domain": subset["domain"].iloc[0],
                "category": subset["category"].iloc[0],
                "market_type": subset["market_type"].iloc[0],
                "n": len(subset),
                "rule_edge": round(edge, 4),
                "actual_edge": round(actual_edge, 4),
                "contrarian_pct": round(contrarian_n / len(subset) * 100.0, 1),
                "alpha_ratio": round(alpha_ratio, 3) if not np.isnan(alpha_ratio) else np.nan,
                "weighted_score": round(weighted_score, 4),
                "mean_pnl": round(pnl, 4),
            }
        )

    result = pd.DataFrame(rows)
    return result.sort_values("weighted_score", ascending=False) if not result.empty else result


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    snapshots, rules = load_inputs(artifact_paths)
    matched = match_rules_to_snapshots(snapshots, rules)
    classified = classify_rule_quadrant(matched)
    metrics = compute_rule_metrics(classified)

    artifact_paths.analysis_dir.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(artifact_paths.analysis_dir / "rules_alpha_metrics.csv", index=False)
    classified.to_csv(artifact_paths.analysis_dir / "rules_predictions_with_quadrant.csv", index=False)

    print(metrics.to_string(index=False) if not metrics.empty else "<empty>")


if __name__ == "__main__":
    main()
