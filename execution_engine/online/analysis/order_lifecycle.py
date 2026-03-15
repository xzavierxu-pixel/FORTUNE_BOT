"""Order lifecycle assembly for daily label analysis."""

from __future__ import annotations

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.analysis.label_history import LabelAnalysisScope, load_orders_submitted
from execution_engine.online.analysis.label_io import (
    horizon_bucket,
    latest_by_order_attempt,
    num_series,
    str_series,
)
from execution_engine.shared.io import list_run_artifact_paths, read_jsonl_many


def _artifact_paths(cfg: PegConfig, filename: str, scope: LabelAnalysisScope) -> list:
    if scope == "run":
        if not cfg.data_dir.exists():
            return []
        return sorted([path for path in cfg.data_dir.rglob(filename) if path.is_file()])
    return list_run_artifact_paths(cfg.runs_root_dir, filename)


def build_order_lifecycle(cfg: PegConfig, selections: pd.DataFrame, scope: LabelAnalysisScope = "run") -> pd.DataFrame:
    submitted = load_orders_submitted(cfg, scope=scope)
    if submitted.empty:
        return pd.DataFrame()

    latest_orders = latest_by_order_attempt(
        read_jsonl_many(_artifact_paths(cfg, "orders.jsonl", scope))
    )
    latest_frame = pd.DataFrame(latest_orders.values())
    if not latest_frame.empty:
        latest_frame = latest_frame.rename(
            columns={
                "status": "latest_status",
                "status_reason": "latest_status_reason",
                "updated_at_utc": "terminal_at_utc",
                "created_at_utc": "order_created_at_utc",
                "run_id": "latest_run_id",
            }
        )
        for column in [
            "order_attempt_id",
            "latest_status",
            "latest_status_reason",
            "terminal_at_utc",
            "order_created_at_utc",
            "latest_run_id",
        ]:
            if column not in latest_frame.columns:
                latest_frame[column] = pd.NA
        latest_frame = latest_frame[
            [
                "order_attempt_id",
                "latest_status",
                "latest_status_reason",
                "terminal_at_utc",
                "order_created_at_utc",
                "latest_run_id",
            ]
        ]

    fills = read_jsonl_many(_artifact_paths(cfg, "fills.jsonl", scope))
    fill_agg = pd.DataFrame()
    if fills:
        fill_frame = pd.DataFrame(fills)
        fill_frame["fill_amount_usdc"] = pd.to_numeric(fill_frame.get("amount_usdc"), errors="coerce").fillna(0.0)
        fill_frame["fill_shares"] = pd.to_numeric(fill_frame.get("shares"), errors="coerce").fillna(0.0)
        fill_frame = fill_frame.sort_values(by=["order_attempt_id", "filled_at_utc", "fill_id"])
        fill_agg = (
            fill_frame.groupby("order_attempt_id", dropna=False)
            .agg(
                fill_count=("fill_id", "count"),
                filled_amount_usdc=("fill_amount_usdc", "sum"),
                filled_shares=("fill_shares", "sum"),
                first_fill_at_utc=("filled_at_utc", "min"),
                last_fill_at_utc=("filled_at_utc", "max"),
            )
            .reset_index()
        )
        fill_agg["avg_fill_price"] = fill_agg["filled_amount_usdc"] / fill_agg["filled_shares"].replace(0.0, pd.NA)
        fill_agg["avg_fill_price"] = fill_agg["avg_fill_price"].fillna(0.0)

    selection_subset = pd.DataFrame()
    if not selections.empty:
        selection_subset = selections[
            [
                "run_id",
                "market_id",
                "selected_token_id",
                "selected_outcome_label",
                "growth_score",
                "f_exec",
                "q_pred",
                "trade_value_pred",
                "price",
                "horizon_hours",
                "direction_model",
                "position_side",
                "category",
                "domain",
                "market_type",
                "rule_group_key",
                "rule_leaf_id",
                "settlement_key",
                "cluster_key",
            ]
        ].rename(
            columns={
                "selected_token_id": "token_id",
                "selected_outcome_label": "selection_outcome_label",
                "growth_score": "selection_growth_score",
                "f_exec": "selection_f_exec",
                "q_pred": "selection_q_pred",
                "trade_value_pred": "selection_trade_value_pred",
                "price": "selection_price",
                "horizon_hours": "selection_horizon_hours",
                "direction_model": "selection_direction_model",
                "position_side": "selection_position_side",
                "category": "selection_category",
                "domain": "selection_domain",
                "market_type": "selection_market_type",
                "rule_group_key": "selection_rule_group_key",
                "rule_leaf_id": "selection_rule_leaf_id",
                "settlement_key": "selection_settlement_key",
                "cluster_key": "selection_cluster_key",
            }
        )

    lifecycle = submitted.copy()
    if not latest_frame.empty:
        lifecycle = lifecycle.merge(latest_frame, on="order_attempt_id", how="left")
    if not fill_agg.empty:
        lifecycle = lifecycle.merge(fill_agg, on="order_attempt_id", how="left")
    if not selection_subset.empty:
        lifecycle = lifecycle.merge(selection_subset, on=["run_id", "market_id", "token_id"], how="left")

    lifecycle["latest_status"] = str_series(lifecycle, "latest_status").replace("", pd.NA).fillna(str_series(lifecycle, "order_status"))
    lifecycle["selected_outcome_label"] = str_series(lifecycle, "outcome_label").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_outcome_label")
    )
    lifecycle["category"] = str_series(lifecycle, "category").replace("", pd.NA).fillna(str_series(lifecycle, "selection_category"))
    lifecycle["domain"] = str_series(lifecycle, "domain").replace("", pd.NA).fillna(str_series(lifecycle, "selection_domain"))
    lifecycle["market_type"] = str_series(lifecycle, "market_type").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_market_type")
    )
    lifecycle["position_side"] = str_series(lifecycle, "position_side").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_position_side")
    )
    lifecycle["rule_group_key"] = str_series(lifecycle, "rule_group_key").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_rule_group_key")
    )
    lifecycle["rule_leaf_id"] = str_series(lifecycle, "rule_leaf_id").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_rule_leaf_id")
    )
    lifecycle["settlement_key"] = str_series(lifecycle, "settlement_key").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_settlement_key")
    )
    lifecycle["cluster_key"] = str_series(lifecycle, "cluster_key").replace("", pd.NA).fillna(
        str_series(lifecycle, "selection_cluster_key")
    )
    lifecycle["q_pred"] = num_series(lifecycle, "q_pred").fillna(num_series(lifecycle, "selection_q_pred"))
    lifecycle["growth_score"] = num_series(lifecycle, "growth_score").fillna(num_series(lifecycle, "selection_growth_score"))
    lifecycle["f_exec"] = num_series(lifecycle, "f_exec").fillna(num_series(lifecycle, "selection_f_exec"))
    lifecycle["trade_value_pred"] = num_series(lifecycle, "selection_trade_value_pred")
    lifecycle["price"] = num_series(lifecycle, "selection_price")
    lifecycle["horizon_hours"] = num_series(lifecycle, "selection_horizon_hours")
    lifecycle["direction_model"] = num_series(lifecycle, "selection_direction_model")
    lifecycle["submitted_amount_usdc"] = num_series(lifecycle, "submitted_amount_usdc")
    lifecycle["fill_count"] = num_series(lifecycle, "fill_count").fillna(0.0).astype(int)
    lifecycle["filled_amount_usdc"] = num_series(lifecycle, "filled_amount_usdc").fillna(0.0)
    lifecycle["filled_shares"] = num_series(lifecycle, "filled_shares").fillna(0.0)
    lifecycle["avg_fill_price"] = num_series(lifecycle, "avg_fill_price").fillna(0.0)
    lifecycle["horizon_bucket"] = lifecycle["horizon_hours"].apply(horizon_bucket)
    lifecycle["edge_prob"] = lifecycle["q_pred"] - lifecycle["price"]

    submitted_at = pd.to_datetime(str_series(lifecycle, "submitted_at_utc"), utc=True, errors="coerce")
    terminal_at = pd.to_datetime(str_series(lifecycle, "terminal_at_utc"), utc=True, errors="coerce")
    first_fill_at = pd.to_datetime(str_series(lifecycle, "first_fill_at_utc"), utc=True, errors="coerce")
    lifecycle["order_lifetime_sec"] = (terminal_at - submitted_at).dt.total_seconds()
    lifecycle["fill_latency_sec"] = (first_fill_at - submitted_at).dt.total_seconds()

    submitted_amount = lifecycle["submitted_amount_usdc"].fillna(0.0)
    lifecycle["opened_position"] = lifecycle["filled_amount_usdc"] > 0.0
    lifecycle["full_fill"] = lifecycle["opened_position"] & (
        lifecycle["latest_status"].astype(str).str.upper().eq("FILLED")
        | (lifecycle["filled_amount_usdc"] >= (submitted_amount - 1e-9))
    )
    lifecycle["partial_fill"] = lifecycle["opened_position"] & ~lifecycle["full_fill"]
    lifecycle["cancel_no_fill"] = lifecycle["latest_status"].astype(str).str.upper().isin({"CANCELED", "EXPIRED"}) & ~lifecycle["opened_position"]
    lifecycle["rejected_no_fill"] = lifecycle["latest_status"].astype(str).str.upper().isin({"REJECTED", "ERROR"}) & ~lifecycle["opened_position"]
    lifecycle["terminal_status"] = lifecycle["latest_status"].fillna("")
    lifecycle["terminal_reason"] = str_series(lifecycle, "latest_status_reason")

    keep_columns = [
        "run_id",
        "run_date",
        "batch_id",
        "market_id",
        "token_id",
        "selected_outcome_label",
        "order_attempt_id",
        "limit_price",
        "best_bid_at_submit",
        "best_ask_at_submit",
        "tick_size",
        "submitted_amount_usdc",
        "ttl_seconds",
        "submitted_at_utc",
        "latest_status",
        "terminal_status",
        "terminal_reason",
        "terminal_at_utc",
        "fill_count",
        "filled_amount_usdc",
        "filled_shares",
        "avg_fill_price",
        "first_fill_at_utc",
        "last_fill_at_utc",
        "order_lifetime_sec",
        "fill_latency_sec",
        "opened_position",
        "full_fill",
        "partial_fill",
        "cancel_no_fill",
        "rejected_no_fill",
        "q_pred",
        "growth_score",
        "f_exec",
        "trade_value_pred",
        "price",
        "edge_prob",
        "horizon_hours",
        "horizon_bucket",
        "direction_model",
        "position_side",
        "category",
        "domain",
        "market_type",
        "rule_group_key",
        "rule_leaf_id",
        "settlement_key",
        "cluster_key",
        "source_path",
    ]
    available_columns = [column for column in keep_columns if column in lifecycle.columns]
    return lifecycle[available_columns].copy()


