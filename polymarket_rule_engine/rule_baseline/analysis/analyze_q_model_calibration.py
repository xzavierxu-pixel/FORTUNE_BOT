from __future__ import annotations

import argparse
import os
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze q-model calibration on strict OOS predictions.")
    parser.add_argument("--pipeline-config", type=str, default=None)
    return parser.parse_args()


def compute_metrics(df: pd.DataFrame) -> dict[str, float]:
    y = df["y"].astype(int).values
    p = df["price"].astype(float).clip(1e-6, 1 - 1e-6).values
    q = df["q_pred"].astype(float).clip(1e-6, 1 - 1e-6).values

    metrics = {
        "rows": float(len(df)),
        "logloss_price": float(log_loss(y, p, labels=[0, 1])),
        "logloss_model": float(log_loss(y, q, labels=[0, 1])),
        "brier_price": float(brier_score_loss(y, p)),
        "brier_model": float(brier_score_loss(y, q)),
        "auc_price": float(roc_auc_score(y, p)) if df["y"].nunique() > 1 else np.nan,
        "auc_model": float(roc_auc_score(y, q)) if df["y"].nunique() > 1 else np.nan,
    }
    metrics["logloss_delta"] = metrics["logloss_model"] - metrics["logloss_price"]
    metrics["brier_delta"] = metrics["brier_model"] - metrics["brier_price"]
    metrics["auc_delta"] = metrics["auc_model"] - metrics["auc_price"] if np.isfinite(metrics["auc_model"]) else np.nan
    return metrics


def load_analysis_predictions(artifact_paths) -> tuple[pd.DataFrame, str]:
    if artifact_paths.predictions_path.exists():
        published = pd.read_csv(artifact_paths.predictions_path)
        if not published.empty:
            return published, "published"

    if not artifact_paths.predictions_full_path.exists():
        raise FileNotFoundError(
            f"Predictions files not found: {artifact_paths.predictions_path} / {artifact_paths.predictions_full_path}"
        )

    return pd.read_csv(artifact_paths.predictions_full_path), "full_diagnostic"


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)
    artifact_paths.analysis_dir.mkdir(parents=True, exist_ok=True)
    df, evaluation_scope = load_analysis_predictions(artifact_paths)
    df = df[(df["price"] > 0.0) & (df["price"] < 1.0)].copy()
    df = df[(df["q_pred"] > 0.0) & (df["q_pred"] < 1.0)].copy()
    if df.empty:
        pd.DataFrame(
            [
                {
                    "evaluation_scope": evaluation_scope,
                    "rows": 0.0,
                    "logloss_price": np.nan,
                    "logloss_model": np.nan,
                    "brier_price": np.nan,
                    "brier_model": np.nan,
                    "auc_price": np.nan,
                    "auc_model": np.nan,
                    "logloss_delta": np.nan,
                    "brier_delta": np.nan,
                    "auc_delta": np.nan,
                }
            ]
        ).to_csv(artifact_paths.analysis_dir / "calibration_metrics.csv", index=False)
        pd.DataFrame(columns=["q_bucket", "n", "q_mean", "y_rate", "p_mean", "edge_true", "edge_model"]).to_csv(
            artifact_paths.analysis_dir / "calibration_reliability.csv",
            index=False,
        )
        pd.DataFrame(columns=["abs_edge_bucket", "n", "edge_model_mean", "edge_true_mean"]).to_csv(
            artifact_paths.analysis_dir / "calibration_edge_buckets.csv",
            index=False,
        )
        print("[INFO] No valid prediction rows available for calibration analysis. Wrote empty artifacts.")
        return

    metrics = compute_metrics(df)
    metrics["evaluation_scope"] = evaluation_scope
    metrics_df = pd.DataFrame([metrics])

    df["q_bucket"] = pd.qcut(df["q_pred"], 10, duplicates="drop")
    reliability = (
        df.groupby("q_bucket", observed=False)
        .agg(
            n=("y", "size"),
            q_mean=("q_pred", "mean"),
            y_rate=("y", "mean"),
            p_mean=("price", "mean"),
        )
        .reset_index()
    )
    reliability["edge_true"] = reliability["y_rate"] - reliability["p_mean"]
    reliability["edge_model"] = reliability["q_mean"] - reliability["p_mean"]

    df["edge_true"] = df["y"] - df["price"]
    df["edge_model"] = df["q_pred"] - df["price"]
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

    metrics_df.to_csv(artifact_paths.analysis_dir / "calibration_metrics.csv", index=False)
    reliability.to_csv(artifact_paths.analysis_dir / "calibration_reliability.csv", index=False)
    edge_table.to_csv(artifact_paths.analysis_dir / "calibration_edge_buckets.csv", index=False)

    print(metrics_df.to_string(index=False))
    print("\n[INFO] Reliability table:")
    print(reliability.to_string(index=False, float_format=lambda value: f"{value:.4f}"))
    print("\n[INFO] Edge bucket table:")
    print(edge_table.to_string(index=False, float_format=lambda value: f"{value:.4f}"))


if __name__ == "__main__":
    main()
