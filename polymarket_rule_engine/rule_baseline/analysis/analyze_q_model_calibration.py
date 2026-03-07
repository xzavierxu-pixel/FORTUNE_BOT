from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
PREDICTIONS_PATH = DATA_DIR / "predictions" / "snapshots_with_predictions.csv"


def main():
    if not PREDICTIONS_PATH.exists():
        print(f"[ERROR] File not found: {PREDICTIONS_PATH}")
        return

    print(f"[INFO] Loading predictions from {PREDICTIONS_PATH} ...")
    df = pd.read_csv(PREDICTIONS_PATH)
    df["y"] = df["y"].astype(int)
    df["price"] = df["price"].astype(float)
    df["q_pred"] = df["q_pred"].astype(float)

    df = df[(df["price"] > 0.0) & (df["price"] < 1.0)]
    df = df[(df["q_pred"] > 0.0) & (df["q_pred"] < 1.0)]
    print(f"[INFO] Valid rows: {len(df)}")
    if df.empty:
        print("[WARN] No valid rows available.")
        return

    y = df["y"].values
    p = df["price"].values
    q = df["q_pred"].values

    print("\n=== Global metrics ===")
    logloss_p = log_loss(y, p)
    brier_p = brier_score_loss(y, p)
    auc_p = roc_auc_score(y, p)

    logloss_q = log_loss(y, q)
    brier_q = brier_score_loss(y, q)
    auc_q = roc_auc_score(y, q)

    print(f"Baseline (price)   - logloss={logloss_p:.4f}, brier={brier_p:.4f}, AUC={auc_p:.4f}")
    print(f"Model (q_pred)     - logloss={logloss_q:.4f}, brier={brier_q:.4f}, AUC={auc_q:.4f}")
    print(
        f"Delta (pred-price) - logloss={logloss_q - logloss_p:+.4f}, "
        f"brier={brier_q - brier_p:+.4f}, AUC={auc_q - auc_p:+.4f}"
    )

    print("\n=== Reliability by q_pred deciles ===")
    df["q_bucket"] = pd.qcut(df["q_pred"], 10, duplicates="drop")
    rel = (
        df.groupby("q_bucket", observed=False)
        .agg(
            n=("y", "size"),
            q_mean=("q_pred", "mean"),
            y_rate=("y", "mean"),
            p_mean=("price", "mean"),
        )
        .reset_index()
    )
    rel["edge_true"] = rel["y_rate"] - rel["p_mean"]
    rel["edge_model"] = rel["q_mean"] - rel["p_mean"]
    print(rel.to_string(index=False, float_format=lambda value: f"{value:.4f}"))

    df["edge_true"] = df["y"] - df["price"]
    df["edge_model"] = df["q_pred"] - df["price"]
    corr = np.corrcoef(df["edge_true"], df["edge_model"])[0, 1]
    print(f"\nCorrelation(edge_true, edge_model) = {corr:.4f}")

    df["abs_edge_bucket"] = pd.qcut(df["edge_model"].abs(), 5, duplicates="drop")
    edge_table = (
        df.groupby("abs_edge_bucket", observed=False)
        .agg(
            n=("y", "size"),
            edge_model_mean=("edge_model", "mean"),
            edge_true_mean=("edge_true", "mean"),
        )
        .reset_index()
    )
    print("\n=== True edge vs model edge magnitude buckets ===")
    print(edge_table.to_string(index=False, float_format=lambda value: f"{value:.4f}"))


if __name__ == "__main__":
    main()
