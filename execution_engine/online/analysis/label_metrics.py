"""Analysis and aggregation helpers for daily label reports."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from execution_engine.online.analysis.label_io import horizon_bucket, num_series, to_bool


def build_executed_analysis(labels: pd.DataFrame, order_lifecycle: pd.DataFrame) -> pd.DataFrame:
    if order_lifecycle.empty:
        return pd.DataFrame()

    executed = order_lifecycle[order_lifecycle["opened_position"].fillna(False).astype(bool)].copy()
    if executed.empty:
        return executed

    executed = executed.sort_values(
        by=["market_id", "first_fill_at_utc", "submitted_at_utc", "order_attempt_id"],
        ascending=[True, True, True, True],
    ).drop_duplicates(subset=["market_id"], keep="first").reset_index(drop=True)

    executed = executed.merge(labels, on="market_id", how="left")
    executed["resolved"] = executed["resolved_outcome_label"].fillna("").astype(str).str.strip() != ""
    executed["predicted_correct"] = (
        executed["selected_outcome_label"].fillna("").astype(str).str.strip()
        == executed["resolved_outcome_label"].fillna("").astype(str).str.strip()
    )
    executed["predicted_correct"] = executed["predicted_correct"].where(executed["resolved"], pd.NA)
    executed["analysis_amount_usdc"] = num_series(executed, "filled_amount_usdc").fillna(0.0)
    executed["analysis_payout_usdc"] = num_series(executed, "filled_shares").where(
        executed["predicted_correct"].fillna(False).astype(bool),
        0.0,
    )
    executed["analysis_pnl_usdc"] = executed["analysis_payout_usdc"] - executed["analysis_amount_usdc"]
    executed["analysis_return_pct"] = executed["analysis_pnl_usdc"] / executed["analysis_amount_usdc"].replace(0.0, pd.NA)
    return executed


def build_opportunity_analysis(
    labels: pd.DataFrame,
    selections: pd.DataFrame,
    order_lifecycle: pd.DataFrame,
) -> pd.DataFrame:
    if selections.empty:
        return pd.DataFrame()

    opportunity = selections.copy()
    opportunity["horizon_bucket"] = opportunity.get("horizon_hours", pd.Series(dtype=str)).apply(horizon_bucket)
    lifecycle_subset = pd.DataFrame()
    if not order_lifecycle.empty:
        lifecycle_subset = order_lifecycle[
            [
                "run_id",
                "market_id",
                "token_id",
                "order_attempt_id",
                "terminal_status",
                "submitted_at_utc",
                "submitted_amount_usdc",
                "opened_position",
                "full_fill",
                "partial_fill",
                "filled_amount_usdc",
                "filled_shares",
            ]
        ].rename(
            columns={
                "token_id": "selected_token_id",
                "terminal_status": "submission_status",
            }
        )
    if not lifecycle_subset.empty:
        opportunity = opportunity.merge(
            lifecycle_subset,
            on=["run_id", "market_id", "selected_token_id"],
            how="left",
        )
    else:
        opportunity["submission_status"] = ""
        opportunity["order_attempt_id"] = ""
        opportunity["submitted_at_utc"] = ""
        opportunity["opened_position"] = False
        opportunity["full_fill"] = False
        opportunity["partial_fill"] = False
        opportunity["filled_amount_usdc"] = 0.0
        opportunity["filled_shares"] = 0.0

    selected_mask = opportunity.get("selected_for_submission", pd.Series(dtype=bool)).map(to_bool)
    opportunity["submitted"] = opportunity["submission_status"].fillna("").astype(str).str.strip() != ""
    opportunity["opened_position"] = opportunity.get(
        "opened_position",
        pd.Series(False, index=opportunity.index),
    ).map(to_bool)
    opportunity["execution_outcome"] = "matched_not_selected"
    opportunity.loc[selected_mask & ~opportunity["submitted"], "execution_outcome"] = "selected_not_submitted"
    opportunity.loc[selected_mask & opportunity["submitted"], "execution_outcome"] = "submitted_not_filled"
    opportunity.loc[opportunity["opened_position"], "execution_outcome"] = "opened_position"
    opportunity["edge_prob"] = pd.to_numeric(opportunity.get("q_pred"), errors="coerce") - pd.to_numeric(
        opportunity.get("price"), errors="coerce"
    )
    opportunity = opportunity.merge(labels, on="market_id", how="left")
    opportunity["resolved"] = opportunity["resolved_outcome_label"].fillna("").astype(str).str.strip() != ""
    opportunity["predicted_correct"] = (
        opportunity["selected_outcome_label"].fillna("").astype(str).str.strip()
        == opportunity["resolved_outcome_label"].fillna("").astype(str).str.strip()
    )
    opportunity["predicted_correct"] = opportunity["predicted_correct"].where(opportunity["resolved"], pd.NA)
    opportunity = opportunity[opportunity["execution_outcome"] != "opened_position"].copy()
    opportunity["analysis_amount_usdc"] = (
        num_series(opportunity, "submitted_amount_usdc").fillna(num_series(opportunity, "stake_usdc")).fillna(0.0)
    )
    opportunity.loc[~selected_mask, "analysis_amount_usdc"] = 0.0
    opportunity["analysis_entry_price"] = num_series(opportunity, "price")
    opportunity["analysis_shares"] = opportunity["analysis_amount_usdc"] / opportunity["analysis_entry_price"].replace(0.0, pd.NA)
    opportunity["analysis_payout_usdc"] = opportunity["analysis_shares"].where(
        opportunity["predicted_correct"].fillna(False).astype(bool),
        0.0,
    )
    opportunity["analysis_pnl_usdc"] = opportunity["analysis_payout_usdc"] - opportunity["analysis_amount_usdc"]
    opportunity["analysis_return_pct"] = opportunity["analysis_pnl_usdc"] / opportunity["analysis_amount_usdc"].replace(0.0, pd.NA)
    opportunity["missed_opportunity_flag"] = (
        opportunity["resolved"].map(to_bool)
        & opportunity["predicted_correct"].map(to_bool)
    )
    return opportunity


def aggregate_rate(frame: pd.DataFrame, group_col: str) -> List[Dict[str, Any]]:
    if frame.empty or group_col not in frame.columns:
        return []
    subset = frame[frame["resolved"] == True].copy()  # noqa: E712
    if subset.empty:
        return []
    subset["_correct_num"] = subset["predicted_correct"].fillna(False).astype(bool).astype(int)
    grouped = (
        subset.groupby(group_col, dropna=False)
        .agg(resolved_count=("market_id", "count"), correct_count=("_correct_num", "sum"))
        .reset_index()
    )
    grouped["win_rate"] = grouped["correct_count"] / grouped["resolved_count"]
    grouped = grouped.sort_values(by=["resolved_count", group_col], ascending=[False, True]).head(20)
    return grouped.to_dict(orient="records")


def aggregate_q_pred_buckets(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    if frame.empty:
        return []
    subset = frame[(frame["resolved"] == True) & frame["q_pred"].notna()].copy()  # noqa: E712
    if subset.empty:
        return []
    subset["q_pred_num"] = pd.to_numeric(subset["q_pred"], errors="coerce")
    subset = subset[subset["q_pred_num"].notna()].copy()
    if subset.empty:
        return []
    subset["q_bucket"] = subset["q_pred_num"].apply(lambda value: f"{int(value * 10) / 10:.1f}-{min(int(value * 10) / 10 + 0.1, 1.0):.1f}")
    subset["_correct_num"] = subset["predicted_correct"].fillna(False).astype(bool).astype(int)
    grouped = (
        subset.groupby("q_bucket", dropna=False)
        .agg(resolved_count=("market_id", "count"), correct_count=("_correct_num", "sum"), avg_q_pred=("q_pred_num", "mean"))
        .reset_index()
    )
    grouped["realized_rate"] = grouped["correct_count"] / grouped["resolved_count"]
    grouped = grouped.sort_values(by="q_bucket")
    return grouped.to_dict(orient="records")


def aggregate_counts(frame: pd.DataFrame, group_col: str) -> List[Dict[str, Any]]:
    if frame.empty or group_col not in frame.columns:
        return []
    grouped = (
        frame.groupby(group_col, dropna=False)
        .agg(row_count=("market_id", "count"))
        .reset_index()
        .sort_values(by=["row_count", group_col], ascending=[False, True])
        .head(20)
    )
    return grouped.to_dict(orient="records")


def aggregate_edge_buckets(frame: pd.DataFrame) -> List[Dict[str, Any]]:
    if frame.empty or "edge_prob" not in frame.columns:
        return []
    subset = frame[frame["resolved"] == True].copy()  # noqa: E712
    if subset.empty:
        return []
    subset["edge_prob_num"] = pd.to_numeric(subset["edge_prob"], errors="coerce")
    subset["q_pred_num"] = pd.to_numeric(subset["q_pred"], errors="coerce")
    subset = subset[subset["edge_prob_num"].notna()].copy()
    if subset.empty:
        return []
    subset["edge_bucket_floor"] = (subset["edge_prob_num"] * 20).apply(lambda value: int(value) if value >= 0 else int(value) - 1) / 20.0
    subset["edge_bucket"] = subset["edge_bucket_floor"].apply(lambda lower: f"{lower:.2f}-{lower + 0.05:.2f}")
    subset["_correct_num"] = subset["predicted_correct"].fillna(False).astype(bool).astype(int)
    grouped = (
        subset.groupby("edge_bucket", dropna=False)
        .agg(
            resolved_count=("market_id", "count"),
            correct_count=("_correct_num", "sum"),
            avg_edge_prob=("edge_prob_num", "mean"),
            avg_q_pred=("q_pred_num", "mean"),
        )
        .reset_index()
        .sort_values(by="edge_bucket")
    )
    grouped["realized_rate"] = grouped["correct_count"] / grouped["resolved_count"]
    return grouped.to_dict(orient="records")


def aggregate_opportunity_cost(frame: pd.DataFrame, group_col: str) -> List[Dict[str, Any]]:
    if frame.empty or group_col not in frame.columns:
        return []
    subset = frame[frame["resolved"] == True].copy()  # noqa: E712
    if subset.empty:
        return []
    subset["edge_prob_num"] = pd.to_numeric(subset["edge_prob"], errors="coerce")
    subset["q_pred_num"] = pd.to_numeric(subset["q_pred"], errors="coerce")
    subset["_missed_num"] = subset["missed_opportunity_flag"].fillna(False).astype(bool).astype(int)
    grouped = (
        subset.groupby(group_col, dropna=False)
        .agg(
            resolved_count=("market_id", "count"),
            missed_opportunity_count=("_missed_num", "sum"),
            avg_edge_prob=("edge_prob_num", "mean"),
            avg_q_pred=("q_pred_num", "mean"),
        )
        .reset_index()
        .sort_values(by=["missed_opportunity_count", group_col], ascending=[False, True])
        .head(20)
    )
    grouped["missed_opportunity_rate"] = grouped["missed_opportunity_count"] / grouped["resolved_count"].replace(0, pd.NA)
    return grouped.to_dict(orient="records")


def build_trade_performance_summary(frame: pd.DataFrame) -> Dict[str, Any]:
    empty = {
        "resolved_count": 0,
        "win_count": 0,
        "win_rate": 0.0,
        "deployed_amount_usdc": 0.0,
        "realized_payout_usdc": 0.0,
        "realized_pnl_usdc": 0.0,
        "roi": 0.0,
        "avg_pnl_per_trade_usdc": 0.0,
        "avg_return_pct": 0.0,
    }
    if frame.empty:
        return empty

    subset = frame[frame["resolved"] == True].copy()  # noqa: E712
    if subset.empty:
        return empty

    subset["analysis_amount_usdc"] = pd.to_numeric(subset.get("analysis_amount_usdc"), errors="coerce").fillna(0.0)
    subset = subset[subset["analysis_amount_usdc"] > 0.0].copy()
    if subset.empty:
        return empty

    subset["analysis_payout_usdc"] = pd.to_numeric(subset.get("analysis_payout_usdc"), errors="coerce").fillna(0.0)
    subset["analysis_pnl_usdc"] = pd.to_numeric(subset.get("analysis_pnl_usdc"), errors="coerce").fillna(0.0)
    subset["analysis_return_pct"] = pd.to_numeric(subset.get("analysis_return_pct"), errors="coerce")
    subset["_win_num"] = subset["predicted_correct"].fillna(False).astype(bool).astype(int)

    resolved_count = int(len(subset))
    win_count = int(subset["_win_num"].sum())
    deployed = float(subset["analysis_amount_usdc"].sum())
    payout = float(subset["analysis_payout_usdc"].sum())
    pnl = float(subset["analysis_pnl_usdc"].sum())

    return {
        "resolved_count": resolved_count,
        "win_count": win_count,
        "win_rate": round(win_count / resolved_count, 6) if resolved_count > 0 else 0.0,
        "deployed_amount_usdc": round(deployed, 6),
        "realized_payout_usdc": round(payout, 6),
        "realized_pnl_usdc": round(pnl, 6),
        "roi": round(pnl / deployed, 6) if deployed > 0 else 0.0,
        "avg_pnl_per_trade_usdc": round(pnl / resolved_count, 6) if resolved_count > 0 else 0.0,
        "avg_return_pct": round(float(subset["analysis_return_pct"].dropna().mean()), 6)
        if subset["analysis_return_pct"].notna().any()
        else 0.0,
    }


def aggregate_trade_performance(frame: pd.DataFrame, group_col: str) -> List[Dict[str, Any]]:
    if frame.empty or group_col not in frame.columns:
        return []
    subset = frame[frame["resolved"] == True].copy()  # noqa: E712
    if subset.empty:
        return []

    subset["analysis_amount_usdc"] = pd.to_numeric(subset.get("analysis_amount_usdc"), errors="coerce").fillna(0.0)
    subset = subset[subset["analysis_amount_usdc"] > 0.0].copy()
    if subset.empty:
        return []

    subset["analysis_payout_usdc"] = pd.to_numeric(subset.get("analysis_payout_usdc"), errors="coerce").fillna(0.0)
    subset["analysis_pnl_usdc"] = pd.to_numeric(subset.get("analysis_pnl_usdc"), errors="coerce").fillna(0.0)
    subset["_win_num"] = subset["predicted_correct"].fillna(False).astype(bool).astype(int)
    grouped = (
        subset.groupby(group_col, dropna=False)
        .agg(
            resolved_count=("market_id", "count"),
            win_count=("_win_num", "sum"),
            deployed_amount_usdc=("analysis_amount_usdc", "sum"),
            realized_payout_usdc=("analysis_payout_usdc", "sum"),
            realized_pnl_usdc=("analysis_pnl_usdc", "sum"),
        )
        .reset_index()
    )
    grouped["win_rate"] = grouped["win_count"] / grouped["resolved_count"].replace(0, pd.NA)
    grouped["roi"] = grouped["realized_pnl_usdc"] / grouped["deployed_amount_usdc"].replace(0.0, pd.NA)
    grouped = grouped.sort_values(by=["deployed_amount_usdc", group_col], ascending=[False, True]).head(20)
    return grouped.to_dict(orient="records")


def build_order_lifecycle_summary(order_lifecycle: pd.DataFrame) -> Dict[str, Any]:
    if order_lifecycle.empty:
        return {
            "submitted_count": 0,
            "filled_count": 0,
            "partial_fill_count": 0,
            "opened_position_count": 0,
            "cancel_count": 0,
            "rejection_count": 0,
            "expired_count": 0,
            "fill_rate": 0.0,
            "cancel_rate": 0.0,
            "rejection_rate": 0.0,
            "average_order_lifetime_sec": 0.0,
            "average_fill_latency_sec": 0.0,
            "latest_status_counts": {},
        }

    submitted_count = int(len(order_lifecycle))
    filled_count = int(order_lifecycle["full_fill"].fillna(False).astype(bool).sum())
    partial_fill_count = int(order_lifecycle["partial_fill"].fillna(False).astype(bool).sum())
    opened_position_count = int(order_lifecycle["opened_position"].fillna(False).astype(bool).sum())
    cancel_count = int(order_lifecycle["cancel_no_fill"].fillna(False).astype(bool).sum())
    rejection_count = int(order_lifecycle["rejected_no_fill"].fillna(False).astype(bool).sum())
    expired_count = int(order_lifecycle["latest_status"].fillna("").astype(str).str.upper().eq("EXPIRED").sum())
    lifetime_values = pd.to_numeric(order_lifecycle.get("order_lifetime_sec"), errors="coerce")
    fill_latency_values = pd.to_numeric(order_lifecycle.get("fill_latency_sec"), errors="coerce")
    latest_status_counts = order_lifecycle["latest_status"].fillna("UNKNOWN").astype(str).value_counts().to_dict()

    return {
        "submitted_count": submitted_count,
        "filled_count": filled_count,
        "partial_fill_count": partial_fill_count,
        "opened_position_count": opened_position_count,
        "cancel_count": cancel_count,
        "rejection_count": rejection_count,
        "expired_count": expired_count,
        "fill_rate": round((filled_count + partial_fill_count) / submitted_count, 6) if submitted_count > 0 else 0.0,
        "cancel_rate": round(cancel_count / submitted_count, 6) if submitted_count > 0 else 0.0,
        "rejection_rate": round(rejection_count / submitted_count, 6) if submitted_count > 0 else 0.0,
        "average_order_lifetime_sec": round(float(lifetime_values.dropna().mean()), 6) if lifetime_values.notna().any() else 0.0,
        "average_fill_latency_sec": round(float(fill_latency_values.dropna().mean()), 6) if fill_latency_values.notna().any() else 0.0,
        "latest_status_counts": latest_status_counts,
    }

