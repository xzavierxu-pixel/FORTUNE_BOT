"""HTML dashboard orchestration for execution_engine summaries."""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any, Dict, List

from execution_engine.online.reporting.dashboard_sections import (
    format_num,
    render_cards,
    render_daily_rows,
    render_label_analysis_cards,
    render_label_performance_table,
    render_positions_table,
    render_reason_bars,
    render_recent_fills_table,
    render_recent_orders_table,
    render_rows,
    render_status_badges,
    render_top_exposure_table,
)
from execution_engine.online.reporting.dashboard_template import build_dashboard_html


def _render_compact_cards(items: List[tuple[str, Any]]) -> str:
    return "".join(
        f"<div class='card compact'><div class='label'>{escape(label)}</div><div class='value small'>{escape(format_num(value))}</div></div>"
        for label, value in items
    )


def _build_latest_paths(latest: Dict[str, Any], shared_state: Dict[str, Any]) -> str:
    return (
        f"<p><strong>Latest run dir:</strong> {escape(str(latest.get('run_dir', '')))}</p>"
        f"<p><strong>Latest summary:</strong> {escape(str(latest.get('summary_path', '')))}</p>"
        f"<p><strong>Market state cache:</strong> {escape(str(shared_state.get('market_state_cache_path', '')))}</p>"
        f"<p><strong>State snapshot:</strong> {escape(str(shared_state.get('state_snapshot_path', '')))}</p>"
    )


def _build_execution_cards(execution: Dict[str, Any], shared_state: Dict[str, Any]) -> str:
    items = [
        ("This Run Orders", execution.get("run_orders_count", 0)),
        ("This Run Fills", execution.get("run_fills_count", 0)),
        ("This Run Submitted USDC", execution.get("run_submitted_notional_usdc", 0.0)),
        ("This Run Filled USDC", execution.get("run_filled_notional_usdc", 0.0)),
        ("Current Book Open Orders", execution.get("current_open_orders_count", 0)),
        ("Current Book Open USDC", execution.get("current_open_notional_usdc", 0.0)),
        ("Lifetime Filled USDC", execution.get("lifetime_filled_notional_usdc", 0.0)),
        ("This Run Avg Order USDC", execution.get("run_avg_order_usdc", 0.0)),
        ("Pending Markets", shared_state.get("pending_market_count", 0)),
        ("Open Markets", shared_state.get("open_market_count", 0)),
        ("State Open Orders", shared_state.get("state_open_orders_count", 0)),
        ("State Net Exposure", shared_state.get("state_net_exposure_usdc", 0.0)),
    ]
    return _render_compact_cards(items)


def _build_lifecycle_cards(execution: Dict[str, Any], positions: Dict[str, Any]) -> str:
    lifecycle = execution.get("order_lifecycle", {}) if isinstance(execution.get("order_lifecycle"), dict) else {}
    items = [
        ("Total Orders", lifecycle.get("total_orders", 0)),
        ("Filled Orders", lifecycle.get("filled_orders", 0)),
        ("Partial Orders", lifecycle.get("partial_orders", 0)),
        ("Canceled Orders", lifecycle.get("canceled_orders", 0)),
        ("Failed Orders", lifecycle.get("failed_orders", 0)),
        ("Fill Rate", lifecycle.get("fill_rate", 0.0)),
        ("Cancel Rate", lifecycle.get("cancel_rate", 0.0)),
        ("Avg Terminal Sec", lifecycle.get("avg_terminal_lifecycle_sec", 0.0)),
        ("Avg Fill Latency Sec", lifecycle.get("avg_fill_latency_sec", 0.0)),
        ("Open Positions", positions.get("open_positions_count", 0)),
        ("Closed Positions", positions.get("closed_positions_count", 0)),
        ("Open Position USDC", positions.get("open_position_notional_usdc", 0.0)),
    ]
    return _render_compact_cards(items)


def _build_exposure_tables(execution: Dict[str, Any]) -> str:
    return (
        "<div class='grid-three'>"
        f"{render_top_exposure_table(execution.get('top_open_market_exposure', []), 'Top Open Market Exposure')}"
        f"{render_top_exposure_table(execution.get('top_open_category_exposure', []), 'Top Open Category Exposure')}"
        f"{render_top_exposure_table(execution.get('top_open_side_exposure', []), 'Top Open Side Exposure')}"
        "</div>"
    )


def _build_position_tables(positions: Dict[str, Any]) -> str:
    return (
        "<div class='grid-two'>"
        f"<div>{render_positions_table(positions.get('open_positions', []), 'Open Positions', 'open')}</div>"
        f"<div>{render_positions_table(positions.get('closed_positions', []), 'Closed Positions', 'closed')}</div>"
        "</div>"
    )


def _build_latest_context(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    if not rows:
        return {
            "latest_paths_html": "",
            "latest_statuses_html": "",
            "latest_execution_cards_html": "",
            "latest_lifecycle_cards_html": "",
            "latest_label_cards_html": "",
            "latest_label_tables_html": "",
            "latest_exposures_html": "",
            "latest_positions_html": "",
            "latest_orders_table_html": "",
            "latest_fills_table_html": "",
        }

    latest = rows[0]
    execution = latest.get("execution", {}) if isinstance(latest.get("execution"), dict) else {}
    positions = execution.get("positions", {}) if isinstance(execution.get("positions"), dict) else {}
    shared_state = latest.get("shared_state", {}) if isinstance(latest.get("shared_state"), dict) else {}
    return {
        "latest_paths_html": _build_latest_paths(latest, shared_state),
        "latest_statuses_html": render_status_badges(latest),
        "latest_execution_cards_html": _build_execution_cards(execution, shared_state),
        "latest_lifecycle_cards_html": _build_lifecycle_cards(execution, positions),
        "latest_label_cards_html": render_label_analysis_cards(latest),
        "latest_label_tables_html": (
            "<div class='grid-two'>"
            f"{render_label_performance_table(latest, 'selected_performance_by_horizon_bucket', 'Selected Performance By Horizon')}"
            f"{render_label_performance_table(latest, 'selected_performance_by_rule_leaf', 'Selected Performance By Rule Leaf')}"
            "</div>"
        ),
        "latest_exposures_html": _build_exposure_tables(execution),
        "latest_positions_html": _build_position_tables(positions),
        "latest_orders_table_html": render_recent_orders_table(latest),
        "latest_fills_table_html": render_recent_fills_table(latest),
    }


def write_dashboard(path: Path, rows: List[Dict[str, Any]]) -> None:
    latest_context = _build_latest_context(rows)
    html = build_dashboard_html(
        path=path,
        cards_html=render_cards(rows),
        latest_paths_html=latest_context["latest_paths_html"],
        latest_statuses_html=latest_context["latest_statuses_html"],
        latest_execution_cards_html=latest_context["latest_execution_cards_html"],
        latest_lifecycle_cards_html=latest_context["latest_lifecycle_cards_html"],
        latest_label_cards_html=latest_context["latest_label_cards_html"],
        latest_label_tables_html=latest_context["latest_label_tables_html"],
        latest_exposures_html=latest_context["latest_exposures_html"],
        latest_positions_html=latest_context["latest_positions_html"],
        latest_orders_table_html=latest_context["latest_orders_table_html"],
        latest_fills_table_html=latest_context["latest_fills_table_html"],
        daily_rows_html=render_daily_rows(rows),
        reason_bars_html=render_reason_bars(rows),
        rows_html=render_rows(rows),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")

