import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
PREDICTIONS_PATH = DATA_DIR / "predictions" / "snapshots_with_predictions.csv"
OUTPUT_DIR = DATA_DIR / "analysis"

CONTRARIAN_THRESHOLD = 0.05
PRICE_MIN = 0.05
PRICE_MAX = 0.95


def classify_quadrant(p: np.ndarray, q: np.ndarray, y: np.ndarray, threshold: float = CONTRARIAN_THRESHOLD) -> np.ndarray:
    is_contrarian = np.abs(q - p) > threshold
    model_lean_yes = q > 0.5
    model_correct = (model_lean_yes & (y == 1)) | (~model_lean_yes & (y == 0))
    return np.where(
        is_contrarian & model_correct,
        "contrarian_correct",
        np.where(~is_contrarian & model_correct, "consensus_correct", np.where(~is_contrarian, "consensus_wrong", "contrarian_wrong")),
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
                "mean_edge": round(signed_edge.mean(), 4),
                "std_edge": round(signed_edge.std(), 4),
                "mean_p": round(subset["price"].mean(), 4),
                "mean_q": round(subset["q_pred"].mean(), 4),
                "mean_y": round(subset["y"].mean(), 4),
                "brier_market": round(((subset["price"] - subset["y"]) ** 2).mean(), 4),
                "brier_model": round(((subset["q_pred"] - subset["y"]) ** 2).mean(), 4),
                "mean_deviation": round(np.abs(subset["q_pred"] - subset["price"]).mean(), 4),
            }
        )
    return pd.DataFrame(rows)


def compute_alpha_score(df: pd.DataFrame) -> dict:
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


def load_predictions() -> pd.DataFrame:
    df = pd.read_csv(PREDICTIONS_PATH)
    df["price"] = df["price"].astype(float)
    df["q_pred"] = df["q_pred"].astype(float)
    df["y"] = df["y"].astype(int)
    df = df[(df["price"] >= PRICE_MIN) & (df["price"] <= PRICE_MAX)].copy()
    for column in ["category", "domain", "market_type"]:
        if column not in df.columns:
            df[column] = "UNKNOWN"
    return df


def main():
    print("=" * 60)
    print("Alpha Quadrant Analysis")
    print("=" * 60)

    df = load_predictions()
    if df.empty:
        print("[WARN] No predictions available for alpha analysis.")
        return

    df["quadrant"] = classify_quadrant(df["price"].values, df["q_pred"].values, df["y"].values)
    quadrant_metrics = compute_quadrant_metrics(df)
    alpha_scores = compute_alpha_score(df)
    by_category = slice_alpha(df, "category")
    by_domain = slice_alpha(df, "domain")
    by_horizon = slice_alpha(df, "horizon_hours")

    print("\n[INFO] Quadrant metrics:")
    print(quadrant_metrics.to_string(index=False))
    print("\n[INFO] Overall alpha:")
    for key, value in alpha_scores.items():
        print(f"  {key}: {value}")
    print("\n[INFO] Alpha by category:")
    print(by_category.to_string(index=False) if not by_category.empty else "  <empty>")
    print("\n[INFO] Alpha by domain:")
    print(by_domain.to_string(index=False) if not by_domain.empty else "  <empty>")
    print("\n[INFO] Alpha by horizon:")
    print(by_horizon.to_string(index=False) if not by_horizon.empty else "  <empty>")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    quadrant_metrics.to_csv(OUTPUT_DIR / "alpha_quadrant_metrics.csv", index=False)
    by_category.to_csv(OUTPUT_DIR / "alpha_by_category.csv", index=False)
    by_domain.to_csv(OUTPUT_DIR / "alpha_by_domain.csv", index=False)
    by_horizon.to_csv(OUTPUT_DIR / "alpha_by_horizon.csv", index=False)
    df.to_csv(OUTPUT_DIR / "predictions_with_quadrant.csv", index=False)

    print(f"\n[INFO] Results saved to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
