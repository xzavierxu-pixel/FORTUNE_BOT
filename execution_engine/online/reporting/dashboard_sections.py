"""Reusable dashboard section renderers for execution_engine summaries."""

from __future__ import annotations

from html import escape
from typing import Any, Dict, List, Tuple


def format_num(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def render_cards(rows: List[Dict[str, Any]]) -> str:
    total_runs = len(rows)
    today = rows[0]["run_date"] if rows else ""
    today_runs = sum(1 for row in rows if row.get("run_date") == today) if today else 0
    latest = rows[0] if rows else {}
    latest_counts = latest.get("counts", {}) if isinstance(latest.get("counts"), dict) else {}
    total_orders = sum(int((row.get("counts") or {}).get("orders", 0)) for row in rows)
    total_snapshots = sum(int((row.get("counts") or {}).get("normalized_snapshots", 0)) for row in rows)
    cards = [
        ("Total Runs", total_runs),
        ("Runs Today", today_runs),
        ("Latest Snapshots", latest_counts.get("normalized_snapshots", 0)),
        ("Latest Orders", latest_counts.get("orders", 0)),
        ("Total Orders", total_orders),
        ("Total Snapshots", total_snapshots),
        ("Latest Run", latest.get("run_id", "-")),
    ]
    return "".join(
        f"<div class='card'><div class='label'>{escape(label)}</div><div class='value'>{escape(format_num(value))}</div></div>"
        for label, value in cards
    )


def render_rows(rows: List[Dict[str, Any]]) -> str:
    rendered: List[str] = []
    for row in rows[:100]:
        counts = row.get("counts", {}) if isinstance(row.get("counts"), dict) else {}
        metrics = row.get("metrics", {}) if isinstance(row.get("metrics"), dict) else {}
        rendered.append(
            "<tr>"
            f"<td>{escape(str(row.get('run_date', '')))}</td>"
            f"<td>{escape(str(row.get('run_id', '')))}</td>"
            f"<td>{escape(str(row.get('run_mode', '')))}</td>"
            f"<td>{escape(str(row.get('status', '')))}</td>"
            f"<td>{escape('yes' if row.get('dry_run') else 'no')}</td>"
            f"<td>{escape(format_num(counts.get('normalized_snapshots', 0)))}</td>"
            f"<td>{escape(format_num(counts.get('rule_hits', 0)))}</td>"
            f"<td>{escape(format_num(counts.get('selection_decisions', 0)))}</td>"
            f"<td>{escape(format_num(counts.get('orders', 0)))}</td>"
            f"<td>{escape(format_num(counts.get('fills', 0)))}</td>"
            f"<td>{escape(format_num(counts.get('rejections', 0)))}</td>"
            f"<td>{escape(format_num(metrics.get('orders_sent', 0)))}</td>"
            f"<td>{escape(str(row.get('generated_at_utc', '')))}</td>"
            f"<td class='path'>{escape(str(row.get('run_dir', '')))}</td>"
            "</tr>"
        )
    return "".join(rendered)


def aggregate_daily(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    daily: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        run_date = str(row.get("run_date", "") or "")
        if not run_date:
            continue
        counts = row.get("counts", {}) if isinstance(row.get("counts"), dict) else {}
        bucket = daily.setdefault(
            run_date,
            {
                "run_date": run_date,
                "runs": 0,
                "snapshots": 0,
                "rule_hits": 0,
                "selections": 0,
                "orders": 0,
                "fills": 0,
                "rejections": 0,
            },
        )
        bucket["runs"] += 1
        bucket["snapshots"] += int(counts.get("normalized_snapshots", 0))
        bucket["rule_hits"] += int(counts.get("rule_hits", 0))
        bucket["selections"] += int(counts.get("selection_decisions", 0))
        bucket["orders"] += int(counts.get("orders", 0))
        bucket["fills"] += int(counts.get("fills", 0))
        bucket["rejections"] += int(counts.get("rejections", 0))
    return sorted(daily.values(), key=lambda item: item["run_date"], reverse=True)


def aggregate_rejection_reasons(rows: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    reason_counts: Dict[str, int] = {}
    for row in rows:
        reasons = row.get("rejection_reasons", {}) if isinstance(row.get("rejection_reasons"), dict) else {}
        for reason, count in reasons.items():
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + int(count)
    return sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:12]


def render_status_badges(latest: Dict[str, Any]) -> str:
    execution = latest.get("execution", {}) if isinstance(latest.get("execution"), dict) else {}
    statuses = execution.get("run_latest_order_status_counts", {}) if isinstance(execution.get("run_latest_order_status_counts"), dict) else {}
    if not statuses:
        return "<p class='sub'>No orders created in this run.</p>"
    return "".join(
        f"<span class='badge'><strong>{escape(str(key))}</strong> {escape(str(value))}</span>"
        for key, value in sorted(statuses.items())
    )


def render_top_exposure_table(items: List[Dict[str, Any]], title: str) -> str:
    if not items:
        return f"<div><h3>{escape(title)}</h3><p class='sub'>No exposure yet.</p></div>"
    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item.get('key', '')))}</td>"
        f"<td>{escape(format_num(item.get('amount_usdc', 0.0)))}</td>"
        "</tr>"
        for item in items
    )
    return (
        f"<div><h3>{escape(title)}</h3>"
        "<table><thead><tr><th>Key</th><th>USDC</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


def render_recent_orders_table(latest: Dict[str, Any]) -> str:
    execution = latest.get("execution", {}) if isinstance(latest.get("execution"), dict) else {}
    orders = execution.get("recent_orders", []) if isinstance(execution.get("recent_orders"), list) else []
    if not orders:
        return "<p class='sub'>No order history yet.</p>"
    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item.get('updated_at_utc', '')))}</td>"
        f"<td>{escape(str(item.get('market_id', '')))}</td>"
        f"<td>{escape(str(item.get('action', '')))}</td>"
        f"<td>{escape(str(item.get('position_side', '')))}</td>"
        f"<td>{escape(format_num(item.get('amount_usdc', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('price_limit', 0.0)))}</td>"
        f"<td>{escape(str(item.get('status', '')))}</td>"
        f"<td>{escape(str(item.get('status_reason', '') or ''))}</td>"
        "</tr>"
        for item in orders[:15]
    )
    return (
        "<table><thead><tr>"
        "<th>Updated</th><th>Market</th><th>Action</th><th>Side</th><th>USDC</th><th>Price</th><th>Status</th><th>Reason</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )


def render_recent_fills_table(latest: Dict[str, Any]) -> str:
    execution = latest.get("execution", {}) if isinstance(latest.get("execution"), dict) else {}
    fills = execution.get("recent_fills", []) if isinstance(execution.get("recent_fills"), list) else []
    if not fills:
        return "<p class='sub'>No fills yet.</p>"
    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item.get('filled_at_utc', '')))}</td>"
        f"<td>{escape(str(item.get('market_id', '')))}</td>"
        f"<td>{escape(str(item.get('action', '')))}</td>"
        f"<td>{escape(str(item.get('position_side', '')))}</td>"
        f"<td>{escape(format_num(item.get('amount_usdc', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('price', 0.0)))}</td>"
        f"<td>{escape(str(item.get('category', '') or ''))}</td>"
        "</tr>"
        for item in fills[:15]
    )
    return (
        "<table><thead><tr>"
        "<th>Filled</th><th>Market</th><th>Action</th><th>Side</th><th>USDC</th><th>Price</th><th>Category</th>"
        f"</tr></thead><tbody>{rows}</tbody></table>"
    )


def render_positions_table(items: List[Dict[str, Any]], title: str, kind: str) -> str:
    if not items:
        return f"<div><h3>{escape(title)}</h3><p class='sub'>No {escape(kind)} positions.</p></div>"
    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item.get('market_id', '')))}</td>"
        f"<td>{escape(str(item.get('position_side', '')))}</td>"
        f"<td>{escape(format_num(item.get('open_cost_usdc', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('open_shares', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('avg_entry_price', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('realized_pnl_usdc', 0.0)))}</td>"
        f"<td>{escape(str(item.get('last_fill_at_utc', '')))}</td>"
        "</tr>"
        for item in items[:15]
    )
    return (
        f"<div><h3>{escape(title)}</h3><table><thead><tr>"
        "<th>Market</th><th>Side</th><th>Open USDC</th><th>Open Shares</th><th>Avg Entry</th><th>Realized PnL</th><th>Last Fill</th>"
        f"</tr></thead><tbody>{rows}</tbody></table></div>"
    )


def render_daily_rows(rows: List[Dict[str, Any]]) -> str:
    rendered: List[str] = []
    for row in aggregate_daily(rows)[:30]:
        rendered.append(
            "<tr>"
            f"<td>{escape(str(row['run_date']))}</td>"
            f"<td>{escape(format_num(row['runs']))}</td>"
            f"<td>{escape(format_num(row['snapshots']))}</td>"
            f"<td>{escape(format_num(row['rule_hits']))}</td>"
            f"<td>{escape(format_num(row['selections']))}</td>"
            f"<td>{escape(format_num(row['orders']))}</td>"
            f"<td>{escape(format_num(row['fills']))}</td>"
            f"<td>{escape(format_num(row['rejections']))}</td>"
            "</tr>"
        )
    return "".join(rendered)


def render_reason_bars(rows: List[Dict[str, Any]]) -> str:
    reasons = aggregate_rejection_reasons(rows)
    if not reasons:
        return "<p class='sub'>No rejection data yet.</p>"
    max_count = max(count for _, count in reasons) or 1
    rendered: List[str] = []
    for reason, count in reasons:
        width = int((count / max_count) * 100)
        rendered.append(
            "<div class='bar-row'>"
            f"<div class='bar-label'>{escape(reason)}</div>"
            "<div class='bar-track'>"
            f"<div class='bar-fill' style='width:{width}%'></div>"
            "</div>"
            f"<div class='bar-value'>{escape(str(count))}</div>"
            "</div>"
        )
    return "".join(rendered)


def _label_analysis(latest: Dict[str, Any]) -> Dict[str, Any]:
    notes = latest.get("notes", {}) if isinstance(latest.get("notes"), dict) else {}
    return notes.get("label_analysis", {}) if isinstance(notes.get("label_analysis"), dict) else {}


def render_label_analysis_cards(latest: Dict[str, Any]) -> str:
    analysis = _label_analysis(latest)
    if not analysis:
        return "<p class='sub'>No label analysis attached to this run.</p>"
    selected = analysis.get("selected_opportunity_performance", {}) if isinstance(analysis.get("selected_opportunity_performance"), dict) else {}
    submitted = analysis.get("submitted_opportunity_performance", {}) if isinstance(analysis.get("submitted_opportunity_performance"), dict) else {}
    executed = analysis.get("executed_performance", {}) if isinstance(analysis.get("executed_performance"), dict) else {}
    items = [
        ("Resolved Labels", analysis.get("resolved_label_count", 0)),
        ("Resolved Opps", analysis.get("opportunity_resolved_count", 0)),
        ("Selected Win Rate", selected.get("win_rate", 0.0)),
        ("Selected ROI", selected.get("roi", 0.0)),
        ("Selected PnL", selected.get("realized_pnl_usdc", 0.0)),
        ("Submitted ROI", submitted.get("roi", 0.0)),
        ("Submitted PnL", submitted.get("realized_pnl_usdc", 0.0)),
        ("Executed ROI", executed.get("roi", 0.0)),
        ("Executed PnL", executed.get("realized_pnl_usdc", 0.0)),
    ]
    return "".join(
        f"<div class='card compact'><div class='label'>{escape(label)}</div><div class='value small'>{escape(format_num(value))}</div></div>"
        for label, value in items
    )


def render_label_performance_table(latest: Dict[str, Any], field: str, title: str) -> str:
    analysis = _label_analysis(latest)
    rows = analysis.get(field, []) if isinstance(analysis.get(field), list) else []
    if not rows:
        return f"<div><h3>{escape(title)}</h3><p class='sub'>No resolved performance rows.</p></div>"
    group_key = next(
        (
            key
            for key in rows[0].keys()
            if key not in {"resolved_count", "win_count", "deployed_amount_usdc", "realized_payout_usdc", "realized_pnl_usdc", "win_rate", "roi"}
        ),
        "group",
    )
    body = "".join(
        "<tr>"
        f"<td>{escape(str(item.get(group_key, '')))}</td>"
        f"<td>{escape(format_num(item.get('resolved_count', 0)))}</td>"
        f"<td>{escape(format_num(item.get('win_rate', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('deployed_amount_usdc', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('realized_pnl_usdc', 0.0)))}</td>"
        f"<td>{escape(format_num(item.get('roi', 0.0)))}</td>"
        "</tr>"
        for item in rows[:12]
    )
    return (
        f"<div><h3>{escape(title)}</h3><table><thead><tr>"
        f"<th>{escape(group_key)}</th><th>Resolved</th><th>Win Rate</th><th>USDC</th><th>PnL</th><th>ROI</th>"
        f"</tr></thead><tbody>{body}</tbody></table></div>"
    )
