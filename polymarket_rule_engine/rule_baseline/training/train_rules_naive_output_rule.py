from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from math import sqrt

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import prepare_rule_training_frame

MIN_GROUP_ROWS = 20
MIN_TRAIN_ROWS = 15
MIN_VALID_N = 8

GROUP_COLUMNS = ["domain", "category", "market_type", "price_bin", "horizon_bin"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train naive rule buckets with a simple raw-frequency estimator."
    )
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None, help="Optional tail-sample row cap for fast debugging.")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=None,
        help="Optional rolling window for quick debugging without touching the full history.",
    )
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    return parser.parse_args()


def edge_sign(value: float, eps: float = 1e-6) -> int:
    if abs(value) < eps:
        return 0
    return 1 if value > 0 else -1


def parse_bounds(price_label: str, horizon_label: str) -> tuple[float, float, int, int]:
    price_min, price_max = (float(item) for item in price_label.split("-"))

    if horizon_label.startswith("<"):
        horizon_min = 0
        horizon_max = int(horizon_label.replace("<", "").replace("h", ""))
    elif horizon_label.startswith(">"):
        horizon_min = int(horizon_label.replace(">", "").replace("h", ""))
        horizon_max = 1000
    else:
        horizon_min, horizon_max = (int(item) for item in horizon_label.replace("h", "").split("-"))
    return price_min, price_max, horizon_min, horizon_max


def stable_leaf_id(group_key: str, price_label: str, horizon_label: str) -> int:
    digest = hashlib.sha1(f"{group_key}|{price_label}|{horizon_label}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16)


def aggregate_rule_stats(df: pd.DataFrame) -> pd.DataFrame:
    agg_spec = {
        "n": ("y", "size"),
        "wins": ("y", "sum"),
        "p_mean": ("price", "mean"),
        "edge_raw_mean": ("e_sample", "mean"),
        "edge_std_mean": ("r_std", "mean"),
    }
    return df.groupby(GROUP_COLUMNS, observed=True).agg(**agg_spec)


def rename_stats(stats: pd.DataFrame, suffix: str) -> pd.DataFrame:
    return stats.rename(columns={column: f"{column}_{suffix}" for column in stats.columns})


def get_metric(row: pd.Series, suffix: str, metric: str) -> float:
    return float(row.get(f"{metric}_{suffix}", np.nan))


def wilson_interval(successes: float, n_obs: float, z_value: float = 1.96) -> tuple[float, float]:
    if not np.isfinite(n_obs) or n_obs <= 0:
        return 0.0, 1.0
    n_obs = float(n_obs)
    p_hat = float(successes) / n_obs
    denom = 1.0 + z_value**2 / n_obs
    center = (p_hat + z_value**2 / (2.0 * n_obs)) / denom
    radius = z_value * sqrt((p_hat * (1.0 - p_hat) + z_value**2 / (4.0 * n_obs)) / n_obs) / denom
    return max(0.0, center - radius), min(1.0, center + radius)


def summarize_directional_metrics(q_value: float, p_mean: float, edge_raw: float, edge_std: float, direction: int) -> dict[str, float]:
    if direction >= 0:
        q_trade = q_value
        p_trade = p_mean
        edge_net_trade = q_value - p_mean
        edge_sample_trade = edge_raw
        edge_std_trade = edge_std
        roi_trade = edge_raw / max(p_mean, 1e-6)
    else:
        q_trade = 1.0 - q_value
        p_trade = 1.0 - p_mean
        edge_net_trade = p_mean - q_value
        edge_sample_trade = -edge_raw
        edge_std_trade = -edge_std
        roi_trade = -edge_raw / max(1.0 - p_mean, 1e-6)

    return {
        "q_trade": float(q_trade),
        "p_trade": float(p_trade),
        "edge_net_trade": float(edge_net_trade),
        "edge_sample_trade": float(edge_sample_trade),
        "edge_std_trade": float(edge_std_trade),
        "roi_trade": float(roi_trade),
    }


def lower_bound_sortino(edge_lower_bound: float, q_trade_lower: float, p_trade: float, eps: float = 1e-6) -> float:
    q_trade_lower = float(np.clip(q_trade_lower, 0.0, 1.0))
    p_trade = float(np.clip(p_trade, 0.0, 1.0))
    downside_std = max(p_trade, eps) * sqrt(max(1.0 - q_trade_lower, 0.0))
    return float(edge_lower_bound / max(downside_std, eps))


def build_rule_grid(df: pd.DataFrame) -> pd.DataFrame:
    split_frames = {
        "all": df,
        "train": df[df["dataset_split"] == "train"].copy(),
        "valid": df[df["dataset_split"] == "valid"].copy(),
        "test": df[df["dataset_split"] == "test"].copy(),
    }

    grid = rename_stats(aggregate_rule_stats(split_frames["all"]), "all")
    for suffix in ["train", "valid", "test"]:
        grid = grid.join(rename_stats(aggregate_rule_stats(split_frames[suffix]), suffix), how="left")
    return grid.reset_index()


def evaluate_rule_candidate(row: pd.Series, artifact_mode: str) -> tuple[dict, str]:
    n_train = get_metric(row, "train", "n")
    n_valid = get_metric(row, "valid", "n")
    n_all = get_metric(row, "all", "n")
    n_definition = n_all if artifact_mode == "online" else n_train + n_valid

    if not np.isfinite(n_definition) or n_definition < MIN_GROUP_ROWS:
        return {}, "insufficient_definition_rows"
    if not np.isfinite(n_train) or n_train < MIN_TRAIN_ROWS:
        return {}, "insufficient_train_rows"
    if not np.isfinite(n_valid) or n_valid < MIN_VALID_N:
        return {}, "insufficient_valid_rows"

    wins_train = get_metric(row, "train", "wins")
    wins_valid = get_metric(row, "valid", "wins")
    wins_all = get_metric(row, "all", "wins")

    q_train = wins_train / n_train
    p_train = get_metric(row, "train", "p_mean")
    edge_train = q_train - p_train

    q_valid = wins_valid / n_valid
    p_valid = get_metric(row, "valid", "p_mean")
    edge_valid = q_valid - p_valid
    edge_raw_valid = get_metric(row, "valid", "edge_raw_mean")
    edge_std_valid = get_metric(row, "valid", "edge_std_mean")

    sign_train = edge_sign(edge_train)
    sign_valid = edge_sign(edge_valid)
    if sign_train == 0 or sign_valid == 0:
        return {}, "ambiguous_direction"
    if sign_train != sign_valid:
        return {}, "train_valid_direction_mismatch"

    q_test = np.nan
    edge_test = np.nan
    n_test = get_metric(row, "test", "n")
    if np.isfinite(n_test) and n_test > 0:
        q_test = get_metric(row, "test", "wins") / n_test
        edge_test = q_test - get_metric(row, "test", "p_mean")

    direction = sign_train
    estimation_suffix = "all" if artifact_mode == "online" else "train"
    wins_est = wins_all if estimation_suffix == "all" else wins_train
    n_est = n_all if estimation_suffix == "all" else n_train
    q_est = wins_est / n_est
    p_est = get_metric(row, estimation_suffix, "p_mean")
    edge_est = q_est - p_est
    edge_raw_est = get_metric(row, estimation_suffix, "edge_raw_mean")
    edge_std_est = get_metric(row, estimation_suffix, "edge_std_mean")

    price_label = str(row["price_bin"])
    horizon_label = str(row["horizon_bin"])
    group_key = f"{row['domain']}|{row['category']}|{row['market_type']}"
    leaf_id = stable_leaf_id(group_key, price_label, horizon_label)
    price_min, price_max, horizon_min, horizon_max = parse_bounds(price_label, horizon_label)

    directional = summarize_directional_metrics(
        q_value=q_est,
        p_mean=p_est,
        edge_raw=edge_raw_est,
        edge_std=edge_std_est,
        direction=direction,
    )
    directional_valid = summarize_directional_metrics(
        q_value=q_valid,
        p_mean=p_valid,
        edge_raw=edge_raw_valid,
        edge_std=edge_std_valid,
        direction=direction,
    )

    q_valid_lower, q_valid_upper = wilson_interval(wins_valid, n_valid)
    if direction >= 0:
        edge_lower_bound_valid = q_valid_lower - p_valid
        q_trade_lower_valid = q_valid_lower
    else:
        edge_lower_bound_valid = p_valid - q_valid_upper
        q_trade_lower_valid = 1.0 - q_valid_upper

    if edge_lower_bound_valid <= 0:
        return {}, "nonpositive_edge_lower_bound_valid"

    # Score rules with the conservative Wilson lower-bound edge and binary-contract Sortino.
    rule_score = lower_bound_sortino(
        edge_lower_bound=edge_lower_bound_valid,
        q_trade_lower=q_trade_lower_valid,
        p_trade=directional_valid["p_trade"],
    )

    rule = {
        "group_key": group_key,
        "domain": row["domain"],
        "category": row["category"],
        "market_type": row["market_type"],
        "leaf_id": leaf_id,
        "price_bin": price_label,
        "horizon_bin": horizon_label,
        "price_min": price_min,
        "price_max": price_max,
        "h_min": horizon_min,
        "h_max": horizon_max,
        "rule_bounds": json.dumps(
            {
                "price_min": price_min,
                "price_max": price_max,
                "horizon_min": horizon_min,
                "horizon_max": horizon_max,
            }
        ),
        "direction": int(direction),
        "n_train": int(n_train),
        "n_valid": int(n_valid),
        "n_test": int(n_test) if np.isfinite(n_test) else 0,
        "n_full": int(n_all),
        "q_smooth": float(q_est),
        "q_raw_est": float(q_est),
        "prior_mean": np.nan,
        "p_mean": float(p_est),
        "edge_net": float(edge_est),
        "edge_sample": float(edge_raw_est),
        "edge_std": float(edge_std_est),
        "roi": float(directional["roi_trade"]),
        "q_train": float(q_train),
        "q_train_raw": float(q_train),
        "p_train": float(p_train),
        "edge_train": float(edge_train),
        "q_valid": float(q_valid),
        "p_valid": float(p_valid),
        "edge_valid": float(edge_valid),
        "edge_raw_valid": float(edge_raw_valid),
        "edge_std_valid": float(edge_std_valid),
        "edge_lower_bound_valid": float(edge_lower_bound_valid),
        "p_value_valid": np.nan,
        "p_value_valid_adj": np.nan,
        "q_test": float(q_test) if np.isfinite(q_test) else np.nan,
        "edge_test": float(edge_test) if np.isfinite(edge_test) else np.nan,
        "rule_score": float(rule_score),
        "estimation_source": estimation_suffix,
        "selection_status": "selected",
    }
    rule.update(directional)
    rule.update({f"{key}_valid": value for key, value in directional_valid.items()})
    return rule, "selected"


def build_rules(df: pd.DataFrame, artifact_mode: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    grid = build_rule_grid(df)
    selected_rules: list[dict] = []
    full_report: list[dict] = []

    for _, row in grid.iterrows():
        rule_candidate, status = evaluate_rule_candidate(row, artifact_mode)
        report_row = row.to_dict()
        report_row["selection_status"] = status
        report_row["prior_mean"] = np.nan
        report_row["p_value_valid"] = np.nan
        report_row["p_value_valid_adj"] = np.nan

        if rule_candidate:
            report_row.update(rule_candidate)
            selected_rules.append(rule_candidate)

        full_report.append(report_row)

    report_df = pd.DataFrame(full_report)
    rules_df = pd.DataFrame(selected_rules)
    if not rules_df.empty:
        rules_df = rules_df.sort_values("rule_score", ascending=False).reset_index(drop=True)

    if not report_df.empty:
        report_df = report_df.sort_values(
            ["selection_status", "n_all", "n_train"],
            ascending=[True, False, False],
        ).reset_index(drop=True)

    return rules_df, report_df


def empty_rules_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "group_key",
            "domain",
            "category",
            "market_type",
            "leaf_id",
            "price_min",
            "price_max",
            "h_min",
            "h_max",
            "n_train",
            "n_valid",
            "n_test",
            "n_full",
            "q_smooth",
            "prior_mean",
            "p_mean",
            "edge_net",
            "edge_sample_trade",
            "edge_std_trade",
            "edge_raw_valid",
            "edge_std_valid",
            "edge_lower_bound_valid",
            "p_value_valid",
            "p_value_valid_adj",
            "rule_score",
            "direction",
            "rule_bounds",
        ]
    )


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)

    df, split = prepare_rule_training_frame(
        artifact_mode=args.artifact_mode,
        max_rows=args.max_rows,
        recent_days=args.recent_days,
        split_reference_end=args.split_reference_end,
        history_start_override=args.history_start,
    )
    rules_df, report_df = build_rules(df, args.artifact_mode)

    if rules_df.empty:
        rules_df = empty_rules_frame()

    rules_df.to_csv(artifact_paths.rules_path, index=False)
    rules_df.to_csv(artifact_paths.naive_rules_dir / "naive_trading_rules.csv", index=False)
    report_df.to_csv(artifact_paths.rule_report_path, index=False)
    with artifact_paths.rule_json_path.open("w", encoding="utf-8") as file:
        json.dump(rules_df.to_dict("records"), file, ensure_ascii=False, indent=2)

    split_summary = {
        "artifact_mode": args.artifact_mode,
        "total_rows": int(len(df)),
        "rows_by_split": df["dataset_split"].value_counts().to_dict(),
        "boundaries": split.to_dict(),
        "quality_pass_rows": int(len(df)),
    }
    write_json(artifact_paths.split_summary_path, split_summary)
    write_json(
        artifact_paths.rule_training_summary_path,
        {
            "artifact_mode": args.artifact_mode,
            "selected_rules": int(len(rules_df)),
            "report_rows": int(len(report_df)),
            "selection_status_counts": report_df["selection_status"].value_counts().to_dict() if not report_df.empty else {},
            "boundaries": split.to_dict(),
            "debug_filters": {"max_rows": args.max_rows, "recent_days": args.recent_days},
            "method": {
                "estimator": "raw_frequency",
                "bayesian_smoothing": False,
                "prior_mean": False,
                "benjamini_hochberg": False,
            },
        },
    )

    print(f"[INFO] Saved {len(rules_df)} rules to {artifact_paths.rules_path}")
    print(f"[INFO] Saved full rule report to {artifact_paths.rule_report_path}")
    print(f"[INFO] Saved split summary to {artifact_paths.split_summary_path}")


if __name__ == "__main__":
    main()
