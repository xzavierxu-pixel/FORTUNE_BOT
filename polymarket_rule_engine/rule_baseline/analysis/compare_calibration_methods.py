import os
import sys
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.snapshots import load_raw_markets, load_snapshots
from rule_baseline.datasets.splits import compute_train_valid_boundary
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.models import fit_autogluon_q_model
from rule_baseline.training.train_snapshot_model import DROP_COLS, build_feature_table, load_rules
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged

COMPARISON_PATH = config.ANALYSIS_DIR / "autogluon_calibration_expansion_results.csv"
PREDICTION_PATH = config.ANALYSIS_DIR / "autogluon_calibration_expansion_predictions.csv"
SUMMARY_PATH = config.ANALYSIS_DIR / "autogluon_calibration_expansion_summary.json"
CALIBRATION_MODES = [
    "none",
    "global_isotonic",
    "grouped_isotonic",
    "global_sigmoid",
    "grouped_sigmoid",
    "beta_calibration",
    "blend_raw_global_isotonic_15",
    "blend_raw_global_isotonic_25",
    "blend_raw_global_isotonic_35",
    "blend_raw_beta_15",
    "blend_raw_beta_25",
    "blend_raw_beta_35",
]


def build_dataset() -> pd.DataFrame:
    rebuild_canonical_merged()
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(config.RULES_OUTPUT_PATH)
    return build_feature_table(snapshots, market_feature_cache, market_annotations, rules)


def split_train_calibration_test(df_feat: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train_end, valid_start = compute_train_valid_boundary(df_feat)
    df_train = df_feat[df_feat["closedTime"] <= train_end].copy()
    df_future = df_feat[df_feat["closedTime"] >= valid_start].copy()
    if df_future.empty:
        raise ValueError("No future-period rows available for calibration/test comparison.")

    reference_end = pd.to_datetime(df_future["closedTime"], utc=True).max()
    test_days = max(7, config.VALIDATION_DAYS // 2)
    test_start = reference_end - pd.Timedelta(days=test_days)

    df_calib = df_future[df_future["closedTime"] < test_start].copy()
    df_test = df_future[df_future["closedTime"] >= test_start].copy()

    if df_calib.empty or df_test.empty:
        midpoint = df_future["closedTime"].sort_values().iloc[len(df_future) // 2]
        df_calib = df_future[df_future["closedTime"] < midpoint].copy()
        df_test = df_future[df_future["closedTime"] >= midpoint].copy()

    if df_calib.empty or df_test.empty:
        raise ValueError("Unable to create non-empty calibration and test splits.")

    print(f"[INFO] Train rows: {len(df_train)}")
    print(f"[INFO] Calibration rows: {len(df_calib)}")
    print(f"[INFO] Test rows: {len(df_test)}")
    print(f"[INFO] Train <= {train_end}")
    print(f"[INFO] Calibration >= {valid_start} and < {df_test['closedTime'].min()}")
    print(f"[INFO] Test >= {df_test['closedTime'].min()}")
    return df_train, df_calib, df_test


def compute_metrics(y_true, probs) -> dict[str, float]:
    return {
        "logloss": log_loss(y_true, probs, labels=[0, 1]),
        "brier": brier_score_loss(y_true, probs),
        "auc": roc_auc_score(y_true, probs),
    }


def main():
    df_feat = build_dataset()
    if df_feat.empty:
        print("[ERROR] No feature rows available for calibration comparison.")
        return

    df_train, df_calib, df_test = split_train_calibration_test(df_feat)
    feature_columns = [column for column in df_feat.columns if column not in DROP_COLS]

    rows = []
    prediction_frame = df_test[
        [
            "market_id",
            "snapshot_time",
            "closedTime",
            "price",
            "y",
            "domain",
            "category",
            "market_type",
        ]
    ].copy()

    for mode in CALIBRATION_MODES:
        print(f"[INFO] Fitting calibration mode: {mode}")
        with TemporaryDirectory() as tmpdir:
            result = fit_autogluon_q_model(
                df_train=df_train,
                df_valid=df_calib,
                feature_columns=feature_columns,
                calibration_mode=mode,
                bundle_dir=Path(tmpdir) / f"bundle_{mode}",
                artifact_mode="offline",
                split_boundaries={},
                predictor_presets="medium_quality",
            )
        probs = result.predict(df_test)
        metrics = compute_metrics(df_test["y"].astype(int).values, probs)
        metrics["mode"] = mode
        rows.append(metrics)
        prediction_frame[f"q_pred_{mode}"] = probs

    baseline_metrics = compute_metrics(df_test["y"].astype(int).values, df_test["price"].astype(float).values)
    baseline_metrics["mode"] = "market_price"
    rows.append(baseline_metrics)

    result_df = pd.DataFrame(rows)[["mode", "logloss", "brier", "auc"]].sort_values("logloss").reset_index(drop=True)
    result_df.to_csv(COMPARISON_PATH, index=False)
    prediction_frame.to_csv(PREDICTION_PATH, index=False)

    non_baseline_df = result_df.loc[result_df["mode"] != "market_price"].copy()
    best_logloss = non_baseline_df.sort_values("logloss").iloc[0]
    best_brier = non_baseline_df.sort_values("brier").iloc[0]
    best_auc = non_baseline_df.sort_values("auc", ascending=False).iloc[0]
    SUMMARY_PATH.write_text(
        json.dumps(
            {
                "train_rows": int(len(df_train)),
                "calibration_rows": int(len(df_calib)),
                "test_rows": int(len(df_test)),
                "modes_evaluated": CALIBRATION_MODES,
                "best_logloss": {"mode": str(best_logloss["mode"]), "value": float(best_logloss["logloss"])},
                "best_brier": {"mode": str(best_brier["mode"]), "value": float(best_brier["brier"])},
                "best_auc": {"mode": str(best_auc["mode"]), "value": float(best_auc["auc"])},
                "ranking": result_df.to_dict(orient="records"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("\n=== Calibration Comparison ===")
    print(result_df.to_string(index=False))
    print(f"\n[INFO] Saved comparison to {COMPARISON_PATH}")
    print(f"[INFO] Saved per-row predictions to {PREDICTION_PATH}")
    print(f"[INFO] Saved summary to {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
