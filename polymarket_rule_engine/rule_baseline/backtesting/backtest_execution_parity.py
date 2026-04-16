from __future__ import annotations

import argparse
import heapq
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.backtesting.backtest_portfolio_qmodel import (
    FEE_RATE,
    compute_growth_and_direction,
    load_model_payload,
    load_rules,
    match_rules,
    predict_candidates,
    trade_pnl,
)
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.models import load_model_artifact
from rule_baseline.datasets.snapshots import apply_earliest_market_dedup, load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split, select_preferred_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.utils import config

INITIAL_BANKROLL = 10_000.0
KELLY_FRACTION = 0.25
MAX_POSITION_F = 0.02
MAX_TRADE_AMOUNT = 1000.0
MAX_TIME_TO_EXPIRY_HOURS = 24.0


@dataclass
class ExecutionParityConfig:
    initial_bankroll: float = INITIAL_BANKROLL
    fee_rate: float = FEE_RATE
    min_prob_edge: float = 0.0
    min_trade_confidence: float = 0.0
    kelly_fraction: float = KELLY_FRACTION
    max_position_f: float = MAX_POSITION_F
    max_trade_amount: float = MAX_TRADE_AMOUNT
    max_time_to_expiry_hours: float = MAX_TIME_TO_EXPIRY_HOURS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest parity with the simple online execution logic.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    parser.add_argument("--compare-model-path", type=str, default=None)
    return parser.parse_args()


def prepare_execution_candidates(
    snapshots: pd.DataFrame,
    rules: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    payload: dict,
    cfg: ExecutionParityConfig,
) -> pd.DataFrame:
    snapshots = snapshots.copy()
    snapshots["snapshot_time"] = pd.to_datetime(snapshots["snapshot_time"], utc=True, errors="coerce")
    snapshots["closedTime"] = pd.to_datetime(snapshots["closedTime"], utc=True, errors="coerce")
    snapshots["time_to_expiry_hours"] = (
        (snapshots["closedTime"] - snapshots["snapshot_time"]).dt.total_seconds() / 3600.0
    )
    snapshots = snapshots[
        snapshots["time_to_expiry_hours"].between(0.0, cfg.max_time_to_expiry_hours, inclusive="both")
    ].copy()
    if snapshots.empty:
        return snapshots

    matched = match_rules(snapshots, rules)
    if matched.empty:
        return matched

    matched = matched.sort_values(
        ["market_id", "snapshot_time", "rule_score"],
        ascending=[True, True, False],
    )
    matched = matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    scored = predict_candidates(matched, market_feature_cache, payload)
    scored = compute_growth_and_direction(scored, cfg)
    if scored.empty:
        return scored

    # Online parity: a market can only be acted on once, at the earliest tradable snapshot.
    scored = apply_earliest_market_dedup(scored, score_column="edge_final")
    return scored.sort_values(["snapshot_time", "market_id"]).reset_index(drop=True)


def compute_filter_breakdown(
    evaluation_snapshots: pd.DataFrame,
    rules: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    payload: dict,
    cfg: ExecutionParityConfig,
    evaluation_split: str,
) -> tuple[dict[str, int], pd.DataFrame]:
    matched = match_rules(evaluation_snapshots, rules)
    matched_unique = matched[["market_id", "snapshot_time"]].drop_duplicates() if not matched.empty else pd.DataFrame()
    if not matched.empty:
        matched = matched.sort_values(["market_id", "snapshot_time", "rule_score"], ascending=[True, True, False])
        dedup_snapshot = matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    else:
        dedup_snapshot = matched

    if dedup_snapshot.empty:
        breakdown = {
            "evaluation_split": evaluation_split,
            "evaluation_snapshot_rows": int(len(evaluation_snapshots)),
            "evaluation_market_ids": int(evaluation_snapshots["market_id"].nunique()),
            "test_snapshot_rows": int(len(evaluation_snapshots)),
            "test_market_ids": int(evaluation_snapshots["market_id"].nunique()),
            "matched_rule_rows": 0,
            "matched_market_ids": 0,
            "matched_market_snapshot_pairs": 0,
            "post_rule_snapshot_dedup_rows": 0,
            "positive_edge_rows": 0,
            "positive_edge_market_ids": 0,
            "earliest_only_rows": 0,
            "earliest_only_market_ids": 0,
        }
        return breakdown, pd.DataFrame()

    scored = predict_candidates(dedup_snapshot, market_feature_cache, payload)
    grown = compute_growth_and_direction(scored, cfg)
    earliest = apply_earliest_market_dedup(grown, score_column="edge_final") if not grown.empty else grown
    breakdown = {
        "evaluation_split": evaluation_split,
        "evaluation_snapshot_rows": int(len(evaluation_snapshots)),
        "evaluation_market_ids": int(evaluation_snapshots["market_id"].nunique()),
        "test_snapshot_rows": int(len(evaluation_snapshots)),
        "test_market_ids": int(evaluation_snapshots["market_id"].nunique()),
        "matched_rule_rows": int(len(matched)),
        "matched_market_ids": int(matched["market_id"].nunique()),
        "matched_market_snapshot_pairs": int(len(matched_unique)),
        "post_rule_snapshot_dedup_rows": int(len(dedup_snapshot)),
        "positive_edge_rows": int(len(grown)),
        "positive_edge_market_ids": int(grown["market_id"].nunique()) if not grown.empty else 0,
        "earliest_only_rows": int(len(earliest)),
        "earliest_only_market_ids": int(earliest["market_id"].nunique()) if not earliest.empty else 0,
    }
    return breakdown, earliest


def run_execution_parity_backtest(
    candidates: pd.DataFrame,
    cfg: ExecutionParityConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    cash = float(cfg.initial_bankroll)
    trade_records: list[dict] = []
    skip_records: list[dict] = []
    pending_heap: list[tuple[pd.Timestamp, int, dict]] = []
    daily_stats: dict = {}
    sequence = 0

    candidates = candidates.copy()
    candidates["snapshot_time"] = pd.to_datetime(candidates["snapshot_time"], utc=True, errors="coerce")
    candidates["closedTime"] = pd.to_datetime(candidates["closedTime"], utc=True, errors="coerce")
    candidates["snapshot_date"] = candidates["snapshot_time"].dt.date
    candidates["settlement_date"] = candidates["closedTime"].dt.date
    candidates = candidates.sort_values(["snapshot_time", "market_id"]).reset_index(drop=True)

    candidate_count_by_date = candidates.groupby("snapshot_date").size().to_dict()
    candidate_dates = set(candidate_count_by_date.keys())
    settlement_dates = set(candidates["settlement_date"].dropna().tolist())
    all_dates = sorted(candidate_dates | settlement_dates)
    candidates_by_date = {
        day: group.sort_values(["snapshot_time", "market_id"]).reset_index(drop=True)
        for day, group in candidates.groupby("snapshot_date", sort=True)
    }

    def ensure_day(day_value):
        if day_value not in daily_stats:
            daily_stats[day_value] = {
                "date": day_value,
                "candidate_count": int(candidate_count_by_date.get(day_value, 0)),
                "executed_count": 0,
                "skipped_count": 0,
                "cash_before_trades": np.nan,
                "cash_after_trades": np.nan,
                "start_equity": np.nan,
                "ending_equity": np.nan,
                "realized_pnl": 0.0,
                "open_positions_after": 0,
            }
        return daily_stats[day_value]

    def current_open_stake() -> float:
        return float(sum(item["stake"] for _, _, item in pending_heap))

    def release_settlements(up_to_ts: pd.Timestamp) -> None:
        nonlocal cash
        while pending_heap and pending_heap[0][0] <= up_to_ts:
            settlement_ts, _, settled = heapq.heappop(pending_heap)
            settlement_day = settlement_ts.date()
            day_stats = ensure_day(settlement_day)
            cash += float(settled["stake"]) + float(settled["pnl"])
            day_stats["realized_pnl"] += float(settled["pnl"])

    equity_records: list[dict] = []
    for current_date in all_dates:
        day_stats = ensure_day(current_date)
        opening_equity = cash + current_open_stake()
        if pd.isna(day_stats["start_equity"]):
            day_stats["start_equity"] = float(opening_equity)

        day_candidates = candidates_by_date.get(current_date)
        if day_candidates is not None and not day_candidates.empty:
            for _, row in day_candidates.iterrows():
                snapshot_ts = row["snapshot_time"]
                release_settlements(snapshot_ts)

                if pd.isna(day_stats["cash_before_trades"]):
                    day_stats["cash_before_trades"] = float(cash)

                current_equity = cash + current_open_stake()
                desired_stake = float(row["f_exec"]) * current_equity

                if cash <= 0:
                    day_stats["skipped_count"] += 1
                    skip_records.append(
                        {
                            "date": current_date,
                            "market_id": row["market_id"],
                            "snapshot_time": snapshot_ts,
                            "desired_stake": desired_stake,
                            "available_cash_before": 0.0,
                            "skip_reason": "cash_exhausted",
                        }
                    )
                    continue

                stake = min(
                    desired_stake,
                    cfg.max_position_f * current_equity,
                    cfg.max_trade_amount,
                    cash,
                )
                if stake <= 0:
                    day_stats["skipped_count"] += 1
                    skip_records.append(
                        {
                            "date": current_date,
                            "market_id": row["market_id"],
                            "snapshot_time": snapshot_ts,
                            "desired_stake": desired_stake,
                            "available_cash_before": float(cash),
                            "skip_reason": "stake_non_positive",
                        }
                    )
                    continue

                settlement_ts = row["closedTime"]
                settlement_date = settlement_ts.date() if pd.notna(settlement_ts) else current_date
                pnl = trade_pnl(int(row["direction_model"]), stake, float(row["price"]), int(row["y"]), cfg.fee_rate)

                cash -= stake
                heapq.heappush(
                    pending_heap,
                    (
                        settlement_ts,
                        sequence,
                        {
                            "stake": float(stake),
                            "pnl": float(pnl),
                        },
                    ),
                )
                sequence += 1
                day_stats["executed_count"] += 1

                trade_records.append(
                    {
                        "date": current_date,
                        "settlement_date": settlement_date,
                        "snapshot_time": snapshot_ts,
                        "settlement_time": settlement_ts,
                        "market_id": row["market_id"],
                        "domain": row["domain"],
                        "category": row["category"],
                        "market_type": row["market_type"],
                        "horizon_hours": row["horizon_hours"],
                        "price": float(row["price"]),
                        "y": int(row["y"]),
                        "q_pred": float(row["q_pred"]),
                        "trade_value_pred": float(row.get("trade_value_pred", np.nan)),
                        "edge_prob": float(row["edge_prob"]),
                        "edge_final": float(row["edge_final"]),
                        "direction": int(row["direction_model"]),
                        "rule_group_key": row["rule_group_key"],
                        "rule_leaf_id": int(row["rule_leaf_id"]),
                        "rule_score": float(row.get("rule_score", np.nan)),
                        "growth_score": float(row["growth_score"]),
                        "stake": float(stake),
                        "stake_fraction_of_start_equity": float(stake / current_equity) if current_equity else 0.0,
                        "pnl": float(pnl),
                        "pnl_pct_of_stake": float(pnl / stake) if stake else 0.0,
                    }
                )

        if pd.isna(day_stats["cash_before_trades"]):
            day_stats["cash_before_trades"] = float(cash)

        day_end_ts = pd.Timestamp(current_date) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        day_end_ts = day_end_ts.tz_localize("UTC")
        release_settlements(day_end_ts)

        day_stats["cash_after_trades"] = float(cash)
        day_stats["ending_equity"] = float(cash + current_open_stake())
        day_stats["open_positions_after"] = int(len(pending_heap))
        equity_records.append(
            {
                "date": current_date,
                "bankroll": day_stats["ending_equity"],
                "daily_pnl": float(day_stats["ending_equity"] - day_stats["start_equity"]),
                "realized_pnl": float(day_stats["realized_pnl"]),
                "num_trades": int(day_stats["executed_count"]),
                "cash": float(cash),
                "open_stake": float(current_open_stake()),
            }
        )

    daily_df = pd.DataFrame([daily_stats[day] for day in sorted(daily_stats.keys())])
    equity_df = pd.DataFrame(equity_records)
    return equity_df, pd.DataFrame(trade_records), pd.DataFrame(skip_records), daily_df


def compute_summary(equity_df: pd.DataFrame, trades_df: pd.DataFrame, cfg: ExecutionParityConfig) -> dict[str, float | int | None]:
    if equity_df.empty:
        return {"total_trades": 0}

    final_bankroll = float(equity_df["bankroll"].iloc[-1])
    total_pnl = final_bankroll - cfg.initial_bankroll
    roi = total_pnl / cfg.initial_bankroll if cfg.initial_bankroll else 0.0
    total_trades = int(len(trades_df))
    win_rate = float((trades_df["pnl"] > 0).mean()) if total_trades else None

    equity = equity_df["bankroll"].astype(float).values
    peak = np.maximum.accumulate(equity) if len(equity) else np.array([])
    max_dd = float((equity - peak).min()) if len(peak) else 0.0
    max_dd_pct = float((equity / peak - 1.0).min()) if len(peak) else 0.0
    previous_bankroll = equity_df["bankroll"].shift(1).astype(float)
    daily_returns = equity_df["daily_pnl"].astype(float) / previous_bankroll
    daily_returns = daily_returns.replace([np.inf, -np.inf], np.nan).dropna()
    downside = daily_returns[daily_returns < 0]
    sortino = None
    sharpe = None
    annualized_volatility = None
    calmar = None
    if not daily_returns.empty:
        annualized_volatility = float(daily_returns.std(ddof=0) * np.sqrt(252.0))
        returns_std = float(daily_returns.std(ddof=0))
        if returns_std > 0:
            sharpe = float(daily_returns.mean() / returns_std * np.sqrt(252.0))
        downside_std = float(np.sqrt(np.mean(np.square(downside)))) if len(downside) else 0.0
        if downside_std > 0:
            sortino = float(daily_returns.mean() / downside_std * np.sqrt(252.0))
    if max_dd_pct < 0:
        calmar = float(roi / abs(max_dd_pct))

    return {
        "initial_bankroll": float(cfg.initial_bankroll),
        "final_bankroll": final_bankroll,
        "total_pnl": float(total_pnl),
        "total_roi": float(roi),
        "max_drawdown": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "annualized_volatility": annualized_volatility,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "total_trades": total_trades,
        "win_rate": win_rate,
        "kelly_fraction": float(cfg.kelly_fraction),
        "max_position_f": float(cfg.max_position_f),
        "max_trade_amount": float(cfg.max_trade_amount),
        "max_time_to_expiry_hours": float(cfg.max_time_to_expiry_hours),
    }


def compute_capital_timing_audit(trades_df: pd.DataFrame) -> dict[str, float | int | None]:
    if trades_df.empty:
        return {}

    trades = trades_df.copy()
    trades["snapshot_time"] = pd.to_datetime(trades["snapshot_time"], utc=True, errors="coerce")
    trades["settlement_time"] = pd.to_datetime(trades["settlement_time"], utc=True, errors="coerce")
    trades = trades.sort_values(["snapshot_time", "market_id"]).reset_index(drop=True)

    hold_hours = (trades["settlement_time"] - trades["snapshot_time"]).dt.total_seconds() / 3600.0
    pending_settlements: list[tuple[pd.Timestamp, int]] = []
    sequence = 0
    trades_after_prior_release = 0
    released_positions_before_trade = 0
    same_day_reuse_trades = 0

    for _, row in trades.iterrows():
        released_now = 0
        released_same_day = 0
        while pending_settlements and pending_settlements[0][0] <= row["snapshot_time"]:
            settlement_ts, _ = heapq.heappop(pending_settlements)
            released_now += 1
            if settlement_ts.date() == row["snapshot_time"].date():
                released_same_day += 1
        if released_now > 0:
            trades_after_prior_release += 1
            released_positions_before_trade += released_now
        if released_same_day > 0:
            same_day_reuse_trades += 1
        heapq.heappush(pending_settlements, (row["settlement_time"], sequence))
        sequence += 1

    same_day_settlement_trades = int((trades["snapshot_time"].dt.date == trades["settlement_time"].dt.date).sum())
    overnight_settlement_trades = int(len(trades) - same_day_settlement_trades)

    return {
        "holding_hours_min": float(hold_hours.min()),
        "holding_hours_median": float(hold_hours.median()),
        "holding_hours_max": float(hold_hours.max()),
        "same_day_settlement_trades": same_day_settlement_trades,
        "overnight_settlement_trades": overnight_settlement_trades,
        "trades_after_prior_release": int(trades_after_prior_release),
        "released_positions_before_trade_total": int(released_positions_before_trade),
        "same_day_reuse_trades": int(same_day_reuse_trades),
    }


def compute_decision_parity_summary(reference_candidates: pd.DataFrame, comparison_candidates: pd.DataFrame) -> dict[str, object]:
    key_cols = ["market_id", "snapshot_time"]
    if reference_candidates.empty and comparison_candidates.empty:
        return {
            "reference_candidate_count": 0,
            "comparison_candidate_count": 0,
            "selected_overlap_count": 0,
            "selected_overlap_ratio_reference": 0.0,
            "selected_overlap_ratio_comparison": 0.0,
        }

    ref = reference_candidates.copy()
    cmp = comparison_candidates.copy()
    ref["snapshot_time"] = pd.to_datetime(ref["snapshot_time"], utc=True, errors="coerce")
    cmp["snapshot_time"] = pd.to_datetime(cmp["snapshot_time"], utc=True, errors="coerce")
    merged = ref.merge(cmp, on=key_cols, how="outer", suffixes=("_reference", "_comparison"), indicator=True)
    overlap = merged[merged["_merge"] == "both"].copy()
    q_gap = (
        overlap["q_pred_reference"].astype(float) - overlap["q_pred_comparison"].astype(float)
        if {"q_pred_reference", "q_pred_comparison"}.issubset(overlap.columns)
        else pd.Series(dtype=float)
    )
    edge_gap = (
        overlap["edge_prob_reference"].astype(float) - overlap["edge_prob_comparison"].astype(float)
        if {"edge_prob_reference", "edge_prob_comparison"}.issubset(overlap.columns)
        else pd.Series(dtype=float)
    )
    return {
        "reference_candidate_count": int(len(ref)),
        "comparison_candidate_count": int(len(cmp)),
        "selected_overlap_count": int(len(overlap)),
        "selected_overlap_ratio_reference": float(len(overlap) / len(ref)) if len(ref) else 0.0,
        "selected_overlap_ratio_comparison": float(len(overlap) / len(cmp)) if len(cmp) else 0.0,
        "reference_only_count": int((merged["_merge"] == "left_only").sum()),
        "comparison_only_count": int((merged["_merge"] == "right_only").sum()),
        "q_pred_abs_diff_mean": float(q_gap.abs().mean()) if not q_gap.empty else None,
        "q_pred_abs_diff_max": float(q_gap.abs().max()) if not q_gap.empty else None,
        "edge_prob_abs_diff_mean": float(edge_gap.abs().mean()) if not edge_gap.empty else None,
        "edge_prob_abs_diff_max": float(edge_gap.abs().max()) if not edge_gap.empty else None,
    }


def main() -> None:
    args = parse_args()
    if args.artifact_mode != "offline":
        raise ValueError("Execution parity backtest is only supported for offline artifacts.")

    cfg = ExecutionParityConfig()
    artifact_paths = build_artifact_paths(args.artifact_mode)

    snapshots = load_research_snapshots(max_rows=args.max_rows, recent_days=args.recent_days)
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_artifact_split(
        snapshots,
        artifact_mode="offline",
        reference_end=args.split_reference_end,
        history_start_override=args.history_start,
    )
    snapshots = assign_dataset_split(snapshots, split)
    evaluation_split, snapshots = select_preferred_split(snapshots)
    if snapshots.empty:
        raise RuntimeError("No evaluation-period snapshots available.")

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)
    payload = load_model_payload(artifact_paths.model_path)

    filter_breakdown, candidates = compute_filter_breakdown(
        snapshots,
        rules,
        market_feature_cache,
        payload,
        cfg,
        evaluation_split=evaluation_split,
    )
    artifact_paths.backtest_dir.mkdir(parents=True, exist_ok=True)
    equity_path = artifact_paths.backtest_dir / "backtest_equity_execution_parity.csv"
    trades_path = artifact_paths.backtest_dir / "backtest_trades_execution_parity.csv"
    skipped_path = artifact_paths.backtest_dir / "backtest_skipped_execution_parity.csv"
    daily_path = artifact_paths.backtest_dir / "backtest_daily_execution_parity.csv"
    breakdown_path = artifact_paths.backtest_dir / "backtest_filter_breakdown_execution_parity.csv"
    summary_path = artifact_paths.metadata_dir / "backtest_summary_execution_parity.json"

    if candidates.empty:
        equity_df = pd.DataFrame(columns=["date", "bankroll", "daily_pnl", "num_trades"])
        trades_df = pd.DataFrame(columns=["date", "market_id", "stake", "pnl"])
        skipped_df = pd.DataFrame(columns=["date", "market_id", "skip_reason"])
        daily_df = pd.DataFrame(columns=["date", "candidate_count", "executed_count", "skipped_count"])
        summary = {"candidate_markets": 0, "candidate_rows": 0, "total_trades": 0}
    else:
        equity_df, trades_df, skipped_df, daily_df = run_execution_parity_backtest(candidates, cfg)
        summary = compute_summary(equity_df, trades_df, cfg)
        summary.update(compute_capital_timing_audit(trades_df))
        summary["candidate_markets"] = int(candidates["market_id"].nunique())
        summary["candidate_rows"] = int(len(candidates))
        summary["skipped_candidates"] = int(len(skipped_df))
        summary["active_entry_days"] = int((daily_df["executed_count"] > 0).sum()) if not daily_df.empty else 0

    equity_df.to_csv(equity_path, index=False)
    trades_df.to_csv(trades_path, index=False)
    skipped_df.to_csv(skipped_path, index=False)
    daily_df.to_csv(daily_path, index=False)
    pd.DataFrame([filter_breakdown]).to_csv(breakdown_path, index=False)
    summary["split_boundaries"] = split.to_dict()
    summary["debug_filters"] = {"max_rows": args.max_rows, "recent_days": args.recent_days}
    write_json(summary_path, summary)

    if args.compare_model_path:
        comparator_payload = load_model_artifact(Path(args.compare_model_path))
        comparison_candidates = prepare_execution_candidates(
            snapshots,
            rules,
            market_feature_cache,
            comparator_payload,
            cfg,
        )
        parity_path = artifact_paths.backtest_dir / "backtest_execution_parity_decision_overlap.json"
        write_json(parity_path, compute_decision_parity_summary(candidates, comparison_candidates))
        print(f"[INFO] Saved execution parity decision overlap to {parity_path}")

    print(f"[INFO] Saved execution parity equity curve to {equity_path}")
    print(f"[INFO] Saved execution parity trade log to {trades_path}")
    print(f"[INFO] Saved execution parity skipped candidates to {skipped_path}")
    print(f"[INFO] Saved execution parity daily activity to {daily_path}")
    print(f"[INFO] Saved execution parity filter breakdown to {breakdown_path}")
    print(f"[INFO] Saved execution parity summary to {summary_path}")


if __name__ == "__main__":
    main()
