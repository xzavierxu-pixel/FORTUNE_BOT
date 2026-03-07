from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils.research_context import build_artifact_paths

CONTRARIAN_THRESHOLD = 0.05
PRICE_MIN = 0.05
PRICE_MAX = 0.95


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze alpha quadrants on strict OOS predictions.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    return parser.parse_args()


def classify_quadrant(p: np.ndarray, q: np.ndarray, y: np.ndarray, threshold: float = CONTRARIAN_THRESHOLD) -> np.ndarray:
    is_contrarian = np.abs(q - p) > threshold
    model_lean_yes = q > 0.5
    model_correct = (model_lean_yes & (y == 1)) | (~model_lean_yes & (y == 0))
    return np.where(
        is_contrarian & model_correct,
        "contrarian_correct",
        np.where(
            ~is_contrarian & model_correct,
            "consensus_correct",
            np.where(~is_contrarian, "consensus_wrong", "contrarian_wrong"),
        ),
    )


def compute_quadrant_metrics(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for quadrant in ["contrarian_correct", "consensus_correct", "consensus_wrong", "contrarian_wrong"]:
        subset = df[df["quadrant"] == quadrant]
        if subset.empty:
            continue

        yes_bets = subset["q_pred"] > 0.5
        signed_edge = np.where(yes_bets, subset["y"] - subset["price"], subset["price"] - subset["y"])
        rows.append(
            {
                "quadrant": quadrant,
                "n": len(subset),
                "pct": round(len(subset) / len(df) * 100.0, 2),
                "mean_edge": round(float(np.mean(signed_edge)), 4),
                "std_edge": round(float(np.std(signed_edge)), 4),
                "mean_p": round(float(subset["price"].mean()), 4),
                "mean_q": round(float(subset["q_pred"].mean()), 4),
                "mean_y": round(float(subset["y"].mean()), 4),
                "brier_market": round(float(((subset["price"] - subset["y"]) ** 2).mean()), 4),
                "brier_model": round(float(((subset["q_pred"] - subset["y"]) ** 2).mean()), 4),
                "mean_deviation": round(float(np.abs(subset["q_pred"] - subset["price"]).mean()), 4),
            }
        )
    return pd.DataFrame(rows)


def compute_alpha_score(df: pd.DataFrame) -> dict[str, float]:
    counts = df["quadrant"].value_counts()
    total = len(df)
    cc = counts.get("contrarian_correct", 0)
    cw = counts.get("contrarian_wrong", 0)
    sc = counts.get("consensus_correct", 0)
    sw = counts.get("consensus_wrong", 0)
    contrarian_total = cc + cw
    consensus_total = sc + sw
    alpha_ratio = cc / max(contrarian_total, 1) if contrarian_total else 0.0
    market_accuracy = sc / max(consensus_total, 1) if consensus_total else 0.0
    net_alpha = alpha_ratio - (1 - market_accuracy)
    weighted_score = (2 * cc + sc - sw - 2 * cw) / total
    return {
        "alpha_ratio": round(alpha_ratio, 4),
        "market_accuracy": round(market_accuracy, 4),
        "net_alpha": round(net_alpha, 4),
        "weighted_score": round(weighted_score, 4),
        "contrarian_pct": round(contrarian_total / total * 100.0, 2),
    }


def slice_alpha(df: pd.DataFrame, column: str, min_count: int = 100) -> pd.DataFrame:
    rows = []
    for value, subset in df.groupby(column):
        if len(subset) < min_count:
            continue
        metrics = compute_alpha_score(subset)
        metrics[column] = value
        metrics["n"] = len(subset)
        rows.append(metrics)
    result = pd.DataFrame(rows)
    return result.sort_values("weighted_score", ascending=False) if not result.empty else result


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    predictions_path = artifact_paths.predictions_path
    if not predictions_path.exists():
        raise FileNotFoundError(f"Predictions file not found: {predictions_path}")

    df = pd.read_csv(predictions_path)
    df["price"] = df["price"].astype(float)
    df["q_pred"] = df["q_pred"].astype(float)
    df["y"] = df["y"].astype(int)
    df = df[(df["price"] >= PRICE_MIN) & (df["price"] <= PRICE_MAX)].copy()
    if df.empty:
        raise RuntimeError("No predictions available for alpha analysis.")

    for column in ["category", "domain", "market_type"]:
        if column not in df.columns:
            df[column] = "UNKNOWN"

    df["quadrant"] = classify_quadrant(df["price"].values, df["q_pred"].values, df["y"].values)
    quadrant_metrics = compute_quadrant_metrics(df)
    overall_alpha = pd.DataFrame([compute_alpha_score(df)])
    by_category = slice_alpha(df, "category")
    by_domain = slice_alpha(df, "domain")
    by_horizon = slice_alpha(df, "horizon_hours")

    artifact_paths.analysis_dir.mkdir(parents=True, exist_ok=True)
    quadrant_metrics.to_csv(artifact_paths.analysis_dir / "alpha_quadrant_metrics.csv", index=False)
    overall_alpha.to_csv(artifact_paths.analysis_dir / "alpha_summary.csv", index=False)
    by_category.to_csv(artifact_paths.analysis_dir / "alpha_by_category.csv", index=False)
    by_domain.to_csv(artifact_paths.analysis_dir / "alpha_by_domain.csv", index=False)
    by_horizon.to_csv(artifact_paths.analysis_dir / "alpha_by_horizon.csv", index=False)
    df.to_csv(artifact_paths.analysis_dir / "predictions_with_quadrant.csv", index=False)

    print(quadrant_metrics.to_string(index=False))
    print("\n[INFO] Overall alpha:")
    print(overall_alpha.to_string(index=False))


if __name__ == "__main__":
    main()
