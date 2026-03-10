from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.backtesting.backtest_portfolio_qmodel import BacktestConfig, match_rules, select_top_rules, trade_pnl
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import apply_earliest_market_dedup, build_rule_bins, load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import TemporalSplit, assign_dataset_split, build_walk_forward_splits, compute_temporal_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache, preprocess_features
from rule_baseline.models import fit_model_payload, fit_regression_payload, predict_probabilities, predict_regression
from rule_baseline.training.train_rules_naive_output_rule import build_rules
from rule_baseline.training.train_snapshot_model import DROP_COLS
from rule_baseline.utils import config

TOP_K = 50
DEFAULT_STAKE_FRACTION = 0.01
DEFAULT_MAX_DAILY_TRADES = 20
DEFAULT_MAX_DAILY_EXPOSURE_F = 0.20


@dataclass(frozen=True)
class BaselinePredictionSpec:
    name: str
    ranking_column: str
    decision_column: str


BASELINE_SPECS = [
    BaselinePredictionSpec("q_only", "score_q_only", "trade_q_only"),
    BaselinePredictionSpec("residual_q", "score_residual_q", "trade_residual_q"),
    BaselinePredictionSpec("tradeable_only", "score_tradeable_only", "trade_tradeable_only"),
    BaselinePredictionSpec("two_stage", "score_two_stage", "trade_two_stage"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare baseline families under strict offline and walk-forward validation.")
    parser.add_argument("--artifact-mode", choices=["offline"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--top-k", type=int, default=TOP_K)
    parser.add_argument("--walk-forward-windows", type=int, default=3)
    parser.add_argument("--walk-forward-step-days", type=int, default=config.TEST_DAYS)
    return parser.parse_args()


def get_feature_columns(df_feat: pd.DataFrame) -> list[str]:
    extra_drop = {
        "signed_edge_true",
        "tradeable_label",
        "rule_direction",
        "rule_group_key",
        "rule_leaf_id",
        "q_pred_q_only",
        "signed_edge_pred_q_only",
        "q_pred_residual_q",
        "residual_pred_residual_q",
        "signed_edge_pred_residual_q",
        "profit_prob_tradeable_only",
        "edge_prob_tradeable_only",
        "profit_prob_two_stage",
        "edge_pred_two_stage",
        "score_q_only",
        "score_residual_q",
        "score_tradeable_only",
        "score_two_stage",
        "trade_q_only",
        "trade_residual_q",
        "trade_tradeable_only",
        "trade_two_stage",
    }
    return [column for column in df_feat.columns if column not in DROP_COLS and column not in extra_drop]


def split_frame(df_feat: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        df_feat[df_feat["dataset_split"] == "train"].copy(),
        df_feat[df_feat["dataset_split"] == "valid"].copy(),
        df_feat[df_feat["dataset_split"] == "test"].copy(),
    )


def build_window_feature_frame(
    raw_snapshots: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    split: TemporalSplit,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    split_snapshots = assign_dataset_split(raw_snapshots, split)
    split_snapshots = split_snapshots[split_snapshots["dataset_split"].isin(["train", "valid", "test"])].copy()

    rule_frame = build_rule_bins(split_snapshots)
    rules_df, _ = build_rules(rule_frame, artifact_mode="offline")
    if rules_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    try:
        top_rules = select_top_rules(rules_df, BacktestConfig())
    except ValueError:
        return pd.DataFrame(), pd.DataFrame()

    matched = match_rules(split_snapshots, top_rules)
    if matched.empty:
        return pd.DataFrame(), top_rules

    matched = matched.sort_values(
        ["market_id", "snapshot_time", "rule_score"],
        ascending=[True, True, False],
    ).drop_duplicates(subset=["market_id", "snapshot_time"], keep="first")
    matched = apply_earliest_market_dedup(matched, score_column="rule_score")
    if matched.empty:
        return pd.DataFrame(), top_rules

    model_input = matched.copy()
    model_input["leaf_id"] = model_input["rule_leaf_id"]
    model_input["direction"] = model_input["rule_direction"]
    model_input["group_key"] = model_input["rule_group_key"]
    features = preprocess_features(model_input, market_feature_cache)

    features["signed_edge_true"] = np.where(
        features["rule_direction"].astype(int) > 0,
        features["y"] - features["price"],
        features["price"] - features["y"],
    )
    features["tradeable_label"] = (features["signed_edge_true"] > 0).astype(int)
    return features, top_rules


def fit_baselines(df_feat: pd.DataFrame) -> pd.DataFrame:
    feature_columns = get_feature_columns(df_feat)
    df_train, df_valid, _ = split_frame(df_feat)
    if df_train.empty or df_valid.empty:
        raise RuntimeError("Baseline fitting requires non-empty train and valid splits.")

    q_payload = fit_model_payload(
        df_train,
        df_valid,
        feature_columns=feature_columns,
        target_column="y",
        calibration_mode="valid_isotonic",
    )
    q_pred = predict_probabilities(q_payload, df_feat)
    signed_edge_pred_q = np.where(df_feat["rule_direction"].astype(int) > 0, q_pred - df_feat["price"], df_feat["price"] - q_pred)

    residual_train = df_train.copy()
    residual_train["residual_target"] = residual_train["y"] - residual_train["price"]
    residual_payload = fit_regression_payload(
        residual_train,
        feature_columns=feature_columns,
        target_column="residual_target",
    )
    residual_pred = predict_regression(residual_payload, df_feat)
    q_pred_residual = np.clip(df_feat["price"] + residual_pred, 0.0, 1.0)
    signed_edge_pred_residual = np.where(
        df_feat["rule_direction"].astype(int) > 0,
        q_pred_residual - df_feat["price"],
        df_feat["price"] - q_pred_residual,
    )

    trade_payload = fit_model_payload(
        df_train,
        df_valid,
        feature_columns=feature_columns,
        target_column="tradeable_label",
        calibration_mode="valid_isotonic",
    )
    profit_prob_trade = predict_probabilities(trade_payload, df_feat)

    stage2_train = df_train[df_train["tradeable_label"] == 1].copy()
    if len(stage2_train) < 50:
        stage2_train = df_train.copy()
    edge_payload = fit_regression_payload(
        stage2_train,
        feature_columns=feature_columns,
        target_column="signed_edge_true",
    )
    edge_pred_two_stage = predict_regression(edge_payload, df_feat)

    out = df_feat.copy()
    out["q_pred_q_only"] = q_pred
    out["signed_edge_pred_q_only"] = signed_edge_pred_q
    out["residual_pred_residual_q"] = residual_pred
    out["q_pred_residual_q"] = q_pred_residual
    out["signed_edge_pred_residual_q"] = signed_edge_pred_residual
    out["profit_prob_tradeable_only"] = profit_prob_trade
    out["edge_prob_tradeable_only"] = profit_prob_trade - 0.5
    out["profit_prob_two_stage"] = profit_prob_trade
    out["edge_pred_two_stage"] = edge_pred_two_stage

    out["score_q_only"] = out["signed_edge_pred_q_only"]
    out["score_residual_q"] = out["signed_edge_pred_residual_q"]
    out["score_tradeable_only"] = out["edge_prob_tradeable_only"]
    out["score_two_stage"] = np.clip(out["profit_prob_two_stage"] - 0.5, 0.0, None) * np.clip(out["edge_pred_two_stage"], 0.0, None)

    out["trade_q_only"] = out["score_q_only"] > 0
    out["trade_residual_q"] = out["score_residual_q"] > 0
    out["trade_tradeable_only"] = out["profit_prob_tradeable_only"] > 0.5
    out["trade_two_stage"] = (out["profit_prob_two_stage"] > 0.5) & (out["edge_pred_two_stage"] > 0)
    return out


def compute_slice_metrics(df: pd.DataFrame, ranking_column: str, decision_column: str, top_k: int) -> dict[str, float | int | None]:
    result: dict[str, float | int | None] = {
        "rows": int(len(df)),
        "signals": int(df[decision_column].sum()),
        "signal_rate": float(df[decision_column].mean()) if len(df) else 0.0,
    }
    signaled = df[df[decision_column]].copy()
    result["mean_signed_edge"] = float(signaled["signed_edge_true"].mean()) if not signaled.empty else np.nan

    ranked = df.sort_values(ranking_column, ascending=False).head(min(top_k, len(df))).copy()
    positive_total = int((df["signed_edge_true"] > 0).sum())
    positive_topk = int((ranked["signed_edge_true"] > 0).sum()) if not ranked.empty else 0
    result["top_k"] = int(min(top_k, len(df)))
    result["top_k_mean_signed_edge"] = float(ranked["signed_edge_true"].mean()) if not ranked.empty else np.nan
    result["precision_at_k"] = float(positive_topk / len(ranked)) if len(ranked) else np.nan
    result["recall_at_k"] = float(positive_topk / positive_total) if positive_total else np.nan
    if df["tradeable_label"].nunique() > 1:
        result["auc_tradeable"] = float(roc_auc_score(df["tradeable_label"], df[ranking_column]))
    else:
        result["auc_tradeable"] = np.nan
    return result


def summarize_baselines(df_pred: pd.DataFrame, top_k: int, window_label: str) -> pd.DataFrame:
    rows = []
    for spec in BASELINE_SPECS:
        for split_name in ["train", "valid", "test"]:
            split_df = df_pred[df_pred["dataset_split"] == split_name].copy()
            metrics = compute_slice_metrics(split_df, spec.ranking_column, spec.decision_column, top_k)
            metrics["baseline"] = spec.name
            metrics["dataset_split"] = split_name
            metrics["window_label"] = window_label
            rows.append(metrics)
    return pd.DataFrame(rows)


def slice_stability(
    df_pred: pd.DataFrame,
    baseline: str,
    ranking_column: str,
    decision_column: str,
    group_column: str,
    window_label: str,
    min_count: int = 20,
) -> pd.DataFrame:
    test_df = df_pred[df_pred["dataset_split"] == "test"].copy()
    rows = []
    for value, subset in test_df.groupby(group_column, observed=False):
        if len(subset) < min_count:
            continue
        signaled = subset[subset[decision_column]].copy()
        rows.append(
            {
                "window_label": window_label,
                "baseline": baseline,
                "group_column": group_column,
                "group_value": value,
                "rows": int(len(subset)),
                "signals": int(signaled.shape[0]),
                "mean_signed_edge": float(signaled["signed_edge_true"].mean()) if not signaled.empty else np.nan,
                "precision_signal": float((signaled["signed_edge_true"] > 0).mean()) if not signaled.empty else np.nan,
                "mean_score": float(subset[ranking_column].mean()),
            }
        )
    return pd.DataFrame(rows)


def run_flat_backtest(
    df_pred: pd.DataFrame,
    ranking_column: str,
    decision_column: str,
    stake_fraction: float = DEFAULT_STAKE_FRACTION,
    max_daily_trades: int = DEFAULT_MAX_DAILY_TRADES,
    max_daily_exposure_f: float = DEFAULT_MAX_DAILY_EXPOSURE_F,
) -> dict[str, float | int | None]:
    bankroll = 10_000.0
    test_df = df_pred[(df_pred["dataset_split"] == "test") & (df_pred[decision_column])].copy()
    if test_df.empty:
        return {"trades": 0, "final_bankroll": bankroll, "roi": 0.0, "win_rate": np.nan, "max_drawdown_pct": 0.0}

    trade_pnls: list[float] = []
    equity_curve: list[float] = [bankroll]
    for current_date in sorted(test_df["snapshot_date"].unique()):
        day_candidates = (
            test_df[test_df["snapshot_date"] == current_date]
            .sort_values(ranking_column, ascending=False)
            .head(max_daily_trades)
        )
        bankroll_start = bankroll
        remaining_exposure = bankroll_start * max_daily_exposure_f
        stake = bankroll_start * stake_fraction
        daily_pnl = 0.0
        for _, row in day_candidates.iterrows():
            if remaining_exposure < stake:
                break
            pnl = trade_pnl(int(row["rule_direction"]), stake, float(row["price"]), int(row["y"]), config.FEE_RATE)
            daily_pnl += pnl
            remaining_exposure -= stake
            trade_pnls.append(pnl)
        bankroll += daily_pnl
        equity_curve.append(bankroll)

    win_rate = float(np.mean(np.array(trade_pnls) > 0)) if trade_pnls else np.nan
    curve = np.array(equity_curve, dtype=float)
    peaks = np.maximum.accumulate(curve)
    max_drawdown_pct = float(np.min(curve / peaks - 1.0)) if len(curve) else 0.0
    return {
        "trades": int(len(trade_pnls)),
        "final_bankroll": float(bankroll),
        "roi": float(bankroll / 10_000.0 - 1.0),
        "win_rate": win_rate,
        "max_drawdown_pct": max_drawdown_pct,
    }


def compare_backtests(df_pred: pd.DataFrame, window_label: str) -> pd.DataFrame:
    rows = []
    for spec in BASELINE_SPECS:
        summary = run_flat_backtest(df_pred, spec.ranking_column, spec.decision_column)
        summary["baseline"] = spec.name
        summary["window_label"] = window_label
        rows.append(summary)
    return pd.DataFrame(rows)


def evaluate_window(
    raw_snapshots: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    split: TemporalSplit,
    window_label: str,
    top_k: int,
) -> dict[str, pd.DataFrame | dict]:
    df_feat, top_rules = build_window_feature_frame(raw_snapshots, market_feature_cache, split)
    if df_feat.empty or top_rules.empty:
        empty_summary = pd.DataFrame(
            [{"window_label": window_label, "baseline": spec.name, "dataset_split": "test", "rows": 0, "signals": 0} for spec in BASELINE_SPECS]
        )
        empty_backtest = pd.DataFrame(
            [{"window_label": window_label, "baseline": spec.name, "trades": 0, "final_bankroll": 10_000.0, "roi": 0.0} for spec in BASELINE_SPECS]
        )
        return {
            "summary": empty_summary,
            "backtest": empty_backtest,
            "stability": pd.DataFrame(),
            "test_predictions": pd.DataFrame(),
            "metadata": {
                "window_label": window_label,
                "rules_selected": int(len(top_rules)),
                "candidate_rows": 0,
                "rows_by_split": {},
                "boundaries": split.to_dict(),
            },
        }

    df_pred = fit_baselines(df_feat)
    summary_df = summarize_baselines(df_pred, top_k, window_label)
    backtest_df = compare_backtests(df_pred, window_label)

    stability_frames = []
    for spec in BASELINE_SPECS:
        stability_frames.append(slice_stability(df_pred, spec.name, spec.ranking_column, spec.decision_column, "domain", window_label))
        stability_frames.append(slice_stability(df_pred, spec.name, spec.ranking_column, spec.decision_column, "market_type", window_label))
        stability_frames.append(slice_stability(df_pred, spec.name, spec.ranking_column, spec.decision_column, "horizon_hours", window_label))
    stability_df = pd.concat(stability_frames, ignore_index=True) if stability_frames else pd.DataFrame()

    metadata = {
        "window_label": window_label,
        "rules_selected": int(len(top_rules)),
        "candidate_rows": int(len(df_pred)),
        "rows_by_split": df_pred["dataset_split"].value_counts().to_dict(),
        "boundaries": split.to_dict(),
    }
    return {
        "summary": summary_df,
        "backtest": backtest_df,
        "stability": stability_df,
        "test_predictions": df_pred[df_pred["dataset_split"] == "test"].copy(),
        "metadata": metadata,
    }


def aggregate_walk_forward(summary_df: pd.DataFrame, backtest_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    test_only = summary_df[summary_df["dataset_split"] == "test"].copy()
    aggregate_summary = (
        test_only.groupby("baseline", observed=False)
        .agg(
            windows=("window_label", "nunique"),
            mean_signal_rate=("signal_rate", "mean"),
            std_signal_rate=("signal_rate", "std"),
            mean_signed_edge=("mean_signed_edge", "mean"),
            std_mean_signed_edge=("mean_signed_edge", "std"),
            mean_top_k_signed_edge=("top_k_mean_signed_edge", "mean"),
            std_top_k_signed_edge=("top_k_mean_signed_edge", "std"),
            mean_precision_at_k=("precision_at_k", "mean"),
            std_precision_at_k=("precision_at_k", "std"),
            mean_recall_at_k=("recall_at_k", "mean"),
            mean_auc_tradeable=("auc_tradeable", "mean"),
        )
        .reset_index()
    )

    aggregate_backtest = (
        backtest_df.groupby("baseline", observed=False)
        .agg(
            windows=("window_label", "nunique"),
            mean_trades=("trades", "mean"),
            mean_roi=("roi", "mean"),
            std_roi=("roi", "std"),
            mean_win_rate=("win_rate", "mean"),
            mean_max_drawdown_pct=("max_drawdown_pct", "mean"),
        )
        .reset_index()
    )
    return aggregate_summary, aggregate_backtest


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)

    raw_snapshots = load_research_snapshots(max_rows=args.max_rows, recent_days=args.recent_days)
    raw_snapshots = raw_snapshots[raw_snapshots["quality_pass"]].copy()
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)

    latest_split = compute_temporal_split(raw_snapshots)
    walk_forward_splits = build_walk_forward_splits(
        raw_snapshots,
        n_windows=args.walk_forward_windows,
        step_days=args.walk_forward_step_days,
    )
    if not walk_forward_splits:
        walk_forward_splits = [latest_split]

    window_results = []
    for index, split in enumerate(walk_forward_splits, start=1):
        window_label = f"wf_{index}"
        window_results.append(evaluate_window(raw_snapshots, market_feature_cache, split, window_label, args.top_k))

    latest_result = evaluate_window(raw_snapshots, market_feature_cache, latest_split, "latest", args.top_k)

    walk_summary_df = pd.concat([result["summary"] for result in window_results], ignore_index=True)
    walk_backtest_df = pd.concat([result["backtest"] for result in window_results], ignore_index=True)
    walk_stability_df = pd.concat([result["stability"] for result in window_results], ignore_index=True) if any(
        not result["stability"].empty for result in window_results
    ) else pd.DataFrame()
    aggregate_summary_df, aggregate_backtest_df = aggregate_walk_forward(walk_summary_df, walk_backtest_df)

    analysis_dir = artifact_paths.analysis_dir
    analysis_dir.mkdir(parents=True, exist_ok=True)

    latest_summary_path = analysis_dir / "baseline_family_comparison.csv"
    latest_backtest_path = analysis_dir / "baseline_family_backtest.csv"
    latest_stability_path = analysis_dir / "baseline_family_stability.csv"
    latest_test_predictions_path = analysis_dir / "baseline_family_test_predictions.csv"

    walk_summary_path = analysis_dir / "baseline_family_walk_forward_summary.csv"
    walk_backtest_path = analysis_dir / "baseline_family_walk_forward_backtest.csv"
    walk_stability_path = analysis_dir / "baseline_family_walk_forward_stability.csv"
    walk_aggregate_path = analysis_dir / "baseline_family_walk_forward_aggregate.csv"
    walk_backtest_aggregate_path = analysis_dir / "baseline_family_walk_forward_backtest_aggregate.csv"

    latest_result["summary"].to_csv(latest_summary_path, index=False)
    latest_result["backtest"].to_csv(latest_backtest_path, index=False)
    latest_result["stability"].to_csv(latest_stability_path, index=False)
    latest_result["test_predictions"].to_csv(latest_test_predictions_path, index=False)

    walk_summary_df.to_csv(walk_summary_path, index=False)
    walk_backtest_df.to_csv(walk_backtest_path, index=False)
    walk_stability_df.to_csv(walk_stability_path, index=False)
    aggregate_summary_df.to_csv(walk_aggregate_path, index=False)
    aggregate_backtest_df.to_csv(walk_backtest_aggregate_path, index=False)

    write_json(
        analysis_dir / "baseline_family_summary.json",
        {
            "latest_window": latest_result["metadata"],
            "walk_forward_windows": [result["metadata"] for result in window_results],
            "top_k": args.top_k,
            "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
        },
    )

    print(latest_result["summary"].to_string(index=False))
    print("\n[INFO] Latest-window backtest comparison:")
    print(latest_result["backtest"].to_string(index=False))
    print("\n[INFO] Walk-forward aggregate:")
    print(aggregate_summary_df.to_string(index=False))
    print("\n[INFO] Walk-forward backtest aggregate:")
    print(aggregate_backtest_df.to_string(index=False))
    print(f"\n[INFO] Saved latest comparison to {latest_summary_path}")
    print(f"[INFO] Saved walk-forward summary to {walk_summary_path}")


if __name__ == "__main__":
    main()
