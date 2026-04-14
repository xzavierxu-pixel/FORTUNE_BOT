from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.snapshots import load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_temporal_split
from rule_baseline.utils import config

PRICE_MIN = 0.05
PRICE_MAX = 0.95
MIN_RULE_N = 50


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze rule alpha on the strict test split.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    return parser.parse_args()


def load_inputs(
    artifact_paths,
    split_reference_end: str | None,
    history_start: str | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    snapshots = load_research_snapshots()
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_temporal_split(
        snapshots,
        reference_end=split_reference_end,
        history_start_override=history_start,
    )
    snapshots = assign_dataset_split(snapshots, split)
    preferred_splits = ["test", "valid", "train"]
    selected_split = "empty"
    selected = pd.DataFrame(columns=snapshots.columns)
    for split_name in preferred_splits:
        candidate = snapshots[snapshots["dataset_split"] == split_name].copy()
        if not candidate.empty:
            selected = candidate
            selected_split = split_name
            break
    snapshots = selected.copy()
    snapshots = snapshots[(snapshots["price"] >= PRICE_MIN) & (snapshots["price"] <= PRICE_MAX)].copy()
    snapshots.attrs["evaluation_scope"] = selected_split

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
                "edge_full",
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
        direction = int(subset["direction"].iloc[0])
        edge = subset["edge_full"].iloc[0]
        actual_edge = (subset["direction"].astype(float) * (subset["y"] - subset["price"])).mean()
        fee = config.FEE_RATE
        pnl = (
            (subset["y"] - subset["price"] - fee).mean()
            if direction > 0
            else (subset["price"] - subset["y"] - fee).mean()
        )

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
    snapshots, rules = load_inputs(artifact_paths, args.split_reference_end, args.history_start)
    artifact_paths.analysis_dir.mkdir(parents=True, exist_ok=True)
    evaluation_scope = str(snapshots.attrs.get("evaluation_scope") or "unknown")
    if snapshots.empty:
        pd.DataFrame(columns=["group_key", "leaf_id", "domain", "category", "market_type", "n", "rule_edge", "actual_edge", "contrarian_pct", "alpha_ratio", "weighted_score", "mean_pnl", "evaluation_scope"]).to_csv(
            artifact_paths.analysis_dir / "rules_alpha_metrics.csv",
            index=False,
        )
        pd.DataFrame().to_csv(artifact_paths.analysis_dir / "rules_predictions_with_quadrant.csv", index=False)
        print("[INFO] No snapshots available for rules alpha analysis. Wrote empty artifacts.")
        return

    try:
        matched = match_rules_to_snapshots(snapshots, rules)
    except RuntimeError:
        pd.DataFrame(columns=["group_key", "leaf_id", "domain", "category", "market_type", "n", "rule_edge", "actual_edge", "contrarian_pct", "alpha_ratio", "weighted_score", "mean_pnl", "evaluation_scope"]).to_csv(
            artifact_paths.analysis_dir / "rules_alpha_metrics.csv",
            index=False,
        )
        pd.DataFrame().to_csv(artifact_paths.analysis_dir / "rules_predictions_with_quadrant.csv", index=False)
        print(f"[INFO] No rules matched snapshots for rules alpha analysis under scope={evaluation_scope}. Wrote empty artifacts.")
        return

    classified = classify_rule_quadrant(matched)
    classified["evaluation_scope"] = evaluation_scope
    metrics = compute_rule_metrics(classified)
    if not metrics.empty:
        metrics["evaluation_scope"] = evaluation_scope

    metrics.to_csv(artifact_paths.analysis_dir / "rules_alpha_metrics.csv", index=False)
    classified.to_csv(artifact_paths.analysis_dir / "rules_predictions_with_quadrant.csv", index=False)

    print(metrics.to_string(index=False) if not metrics.empty else "<empty>")


if __name__ == "__main__":
    main()
