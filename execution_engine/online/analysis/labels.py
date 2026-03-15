"""Resolved label sync and daily analysis orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.analysis.label_history import LabelAnalysisScope, load_resolved_labels, load_selection_history
from execution_engine.online.analysis.label_io import to_bool, write_frame, write_json
from execution_engine.online.analysis.label_metrics import (
    aggregate_counts,
    aggregate_edge_buckets,
    aggregate_opportunity_cost,
    aggregate_q_pred_buckets,
    aggregate_rate,
    aggregate_trade_performance,
    build_executed_analysis,
    build_opportunity_analysis,
    build_order_lifecycle_summary,
    build_trade_performance_summary,
)
from execution_engine.online.analysis.order_lifecycle import build_order_lifecycle
from execution_engine.online.reporting.run_summary import publish_run_summary
from execution_engine.shared.time import to_iso, utc_now


@dataclass(frozen=True)
class LabelAnalysisResult:
    run_manifest_path: Path
    resolved_labels_path: Path
    order_lifecycle_path: Path
    executed_analysis_path: Path
    opportunity_analysis_path: Path
    summary_path: Path
    resolved_label_count: int
    order_lifecycle_count: int
    executed_row_count: int
    opportunity_row_count: int
    executed_resolved_count: int
    opportunity_resolved_count: int


def build_daily_label_analysis(
    cfg: PegConfig,
    *,
    scope: LabelAnalysisScope = "run",
    publish_summary_enabled: bool = True,
) -> LabelAnalysisResult:
    labels = load_resolved_labels(cfg, scope=scope)
    selections = load_selection_history(cfg, scope=scope)
    order_lifecycle = build_order_lifecycle(cfg, selections, scope=scope)
    executed = build_executed_analysis(labels, order_lifecycle)
    opportunity = build_opportunity_analysis(labels, selections, order_lifecycle)

    write_frame(cfg.run_label_order_lifecycle_path, order_lifecycle)
    write_frame(cfg.run_label_executed_analysis_path, executed)
    write_frame(cfg.run_label_opportunity_analysis_path, opportunity)

    lifecycle_summary = build_order_lifecycle_summary(order_lifecycle)
    executed_resolved = int((executed["resolved"] == True).sum()) if not executed.empty else 0  # noqa: E712
    opportunity_resolved = int((opportunity["resolved"] == True).sum()) if not opportunity.empty else 0  # noqa: E712
    matched_not_selected_count = (
        int((~opportunity.get("selected_for_submission", pd.Series(dtype=bool)).map(to_bool)).sum())
        if not opportunity.empty
        else 0
    )
    selected_not_submitted_count = (
        int(
            (
                opportunity.get("selected_for_submission", pd.Series(dtype=bool)).map(to_bool)
                & ~opportunity.get("submitted", pd.Series(dtype=bool)).fillna(False).astype(bool)
            ).sum()
        )
        if not opportunity.empty
        else 0
    )
    submitted_not_filled_count = (
        int(opportunity.get("execution_outcome", pd.Series(dtype=str)).astype(str).eq("submitted_not_filled").sum())
        if not opportunity.empty
        else 0
    )
    selected_opportunity = opportunity[opportunity.get("selected_for_submission", pd.Series(dtype=bool)).map(to_bool)].copy()
    submitted_opportunity = opportunity[opportunity.get("submitted", pd.Series(dtype=bool)).fillna(False).astype(bool)].copy()

    summary = {
        "generated_at_utc": to_iso(utc_now()),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "analysis_scope": scope,
        "label_source_path": str(cfg.rule_engine_raw_markets_path),
        "resolved_labels_path": str(cfg.resolved_labels_path),
        "resolved_label_count": int(len(labels)),
        "order_lifecycle_path": str(cfg.run_label_order_lifecycle_path),
        "order_lifecycle_count": int(len(order_lifecycle)),
        "executed_lifecycle": lifecycle_summary,
        "submitted_count": int(lifecycle_summary["submitted_count"]),
        "filled_count": int(lifecycle_summary["filled_count"]),
        "partial_fill_count": int(lifecycle_summary["partial_fill_count"]),
        "fill_rate": float(lifecycle_summary["fill_rate"]),
        "cancel_rate": float(lifecycle_summary["cancel_rate"]),
        "rejection_rate": float(lifecycle_summary["rejection_rate"]),
        "average_order_lifetime_sec": float(lifecycle_summary["average_order_lifetime_sec"]),
        "average_fill_latency_sec": float(lifecycle_summary["average_fill_latency_sec"]),
        "executed_row_count": int(len(executed)),
        "executed_resolved_count": executed_resolved,
        "executed_win_rate": round(
            float(executed.loc[executed["resolved"] == True, "predicted_correct"].fillna(False).astype(bool).mean())  # noqa: E712
            if executed_resolved > 0
            else 0.0,
            6,
        ),
        "executed_performance": build_trade_performance_summary(executed),
        "opportunity_row_count": int(len(opportunity)),
        "opportunity_resolved_count": opportunity_resolved,
        "opportunity_selected_count": int(
            opportunity.get("selected_for_submission", pd.Series(dtype=bool)).astype(str).str.lower().isin({"1", "true", "yes"}).sum()
        ) if not opportunity.empty else 0,
        "opportunity_submitted_count": int(opportunity.get("submitted", pd.Series(dtype=bool)).astype(bool).sum()) if not opportunity.empty else 0,
        "selected_opportunity_performance": build_trade_performance_summary(selected_opportunity),
        "submitted_opportunity_performance": build_trade_performance_summary(submitted_opportunity),
        "executed_by_category": aggregate_rate(executed, "category"),
        "executed_by_domain": aggregate_rate(executed, "domain"),
        "executed_by_rule_leaf": aggregate_rate(executed, "rule_leaf_id"),
        "executed_by_horizon_bucket": aggregate_rate(executed, "horizon_bucket"),
        "opportunity_by_rule_leaf": aggregate_rate(opportunity, "rule_leaf_id"),
        "opportunity_by_horizon_bucket": aggregate_rate(opportunity, "horizon_bucket"),
        "selected_performance_by_rule_leaf": aggregate_trade_performance(selected_opportunity, "rule_leaf_id"),
        "selected_performance_by_horizon_bucket": aggregate_trade_performance(selected_opportunity, "horizon_bucket"),
        "opportunity_by_selection_reason": aggregate_counts(opportunity, "selection_reason"),
        "opportunity_by_execution_outcome": aggregate_counts(opportunity, "execution_outcome"),
        "opportunity_breakdown": {
            "matched_not_selected_count": matched_not_selected_count,
            "selected_not_submitted_count": selected_not_submitted_count,
            "submitted_not_filled_count": submitted_not_filled_count,
        },
        "opportunity_edge_vs_realized_label": aggregate_edge_buckets(opportunity),
        "opportunity_cost_by_rule_leaf": aggregate_opportunity_cost(opportunity, "rule_leaf_id"),
        "opportunity_cost_by_horizon_bucket": aggregate_opportunity_cost(opportunity, "horizon_bucket"),
        "opportunity_q_pred_calibration": aggregate_q_pred_buckets(opportunity),
    }

    write_json(cfg.run_label_summary_path, summary)
    write_json(
        cfg.run_label_manifest_path,
        {
            **summary,
            "run_label_resolved_labels_path": str(cfg.run_label_resolved_labels_path),
            "run_label_order_lifecycle_path": str(cfg.run_label_order_lifecycle_path),
            "run_label_executed_analysis_path": str(cfg.run_label_executed_analysis_path),
            "run_label_opportunity_analysis_path": str(cfg.run_label_opportunity_analysis_path),
            "run_label_summary_path": str(cfg.run_label_summary_path),
        },
    )

    if publish_summary_enabled:
        publish_run_summary(
            cfg,
            status="label_analysis_completed",
            notes={"label_analysis": summary},
        )

    return LabelAnalysisResult(
        run_manifest_path=cfg.run_label_manifest_path,
        resolved_labels_path=cfg.run_label_resolved_labels_path,
        order_lifecycle_path=cfg.run_label_order_lifecycle_path,
        executed_analysis_path=cfg.run_label_executed_analysis_path,
        opportunity_analysis_path=cfg.run_label_opportunity_analysis_path,
        summary_path=cfg.run_label_summary_path,
        resolved_label_count=len(labels),
        order_lifecycle_count=len(order_lifecycle),
        executed_row_count=len(executed),
        opportunity_row_count=len(opportunity),
        executed_resolved_count=executed_resolved,
        opportunity_resolved_count=opportunity_resolved,
    )



