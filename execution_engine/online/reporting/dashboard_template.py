"""Dashboard HTML template for execution_engine."""

from __future__ import annotations

from html import escape
from pathlib import Path


def build_dashboard_html(
    *,
    path: Path,
    cards_html: str,
    latest_paths_html: str,
    latest_statuses_html: str,
    latest_execution_cards_html: str,
    latest_lifecycle_cards_html: str,
    latest_label_cards_html: str,
    latest_label_tables_html: str,
    latest_exposures_html: str,
    latest_positions_html: str,
    latest_orders_table_html: str,
    latest_fills_table_html: str,
    daily_rows_html: str,
    reason_bars_html: str,
    rows_html: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Execution Engine Dashboard</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f4efe6;
      --panel: #fffdf8;
      --ink: #1d1d1f;
      --muted: #6b6257;
      --line: #d7cfbf;
      --accent: #0f766e;
      --accent-soft: #d5eeeb;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top, #fff8ec 0%, var(--bg) 52%, #ebe2d4 100%);
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 32px;
    }}
    .hero {{
      display: grid;
      gap: 20px;
      margin-bottom: 24px;
    }}
    h1 {{
      margin: 0;
      font-size: 42px;
      line-height: 1;
      letter-spacing: -0.03em;
    }}
    .sub {{
      color: var(--muted);
      font-size: 16px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }}
    .card, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: 0 10px 30px rgba(74, 58, 42, 0.08);
    }}
    .card {{
      padding: 18px 20px;
    }}
    .card.compact {{
      padding: 14px 16px;
    }}
    .label {{
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      margin-bottom: 8px;
    }}
    .value {{
      font-size: 30px;
      font-weight: 700;
    }}
    .value.small {{
      font-size: 24px;
    }}
    .panel {{
      padding: 20px;
      margin-top: 20px;
    }}
    .grid-two {{
      display: grid;
      grid-template-columns: 1.1fr 0.9fr;
      gap: 20px;
    }}
    .grid-three {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 18px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .path {{
      max-width: 420px;
      word-break: break-all;
      color: #51493f;
    }}
    .stamp {{
      display: inline-block;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }}
    .badge {{
      display: inline-flex;
      gap: 8px;
      align-items: center;
      margin: 6px 8px 0 0;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #f6f0e6;
      font-size: 12px;
      color: #3d352c;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: 190px 1fr 56px;
      gap: 12px;
      align-items: center;
      margin-bottom: 12px;
    }}
    .bar-label, .bar-value {{
      font-size: 13px;
      color: #51493f;
    }}
    .bar-track {{
      height: 12px;
      border-radius: 999px;
      background: #efe5d8;
      overflow: hidden;
      border: 1px solid #ddcfbe;
    }}
    .bar-fill {{
      height: 100%;
      background: linear-gradient(90deg, #0f766e, #19a39a);
    }}
    @media (max-width: 1100px) {{
      .grid-two {{
        grid-template-columns: 1fr;
      }}
      .grid-three {{
        grid-template-columns: 1fr;
      }}
      .bar-row {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div>
        <div class="stamp">execution_engine</div>
        <h1>Run Dashboard</h1>
        <p class="sub">Daily run buckets under execution_engine/data/runs with a rolling summary index.</p>
      </div>
      <div class="cards">{cards_html}</div>
    </section>
    <section class="panel">
      <h2>Latest</h2>
      {latest_paths_html}
      <div>{latest_statuses_html}</div>
      <p><strong>Dashboard file:</strong> {escape(str(path))}</p>
    </section>
    <section class="panel">
      <h2>Execution Snapshot</h2>
      <div class="cards">{latest_execution_cards_html}</div>
    </section>
    <section class="panel">
      <h2>Order Lifecycle</h2>
      <div class="cards">{latest_lifecycle_cards_html}</div>
    </section>
    <section class="panel">
      <h2>Label Analysis</h2>
      <div class="cards">{latest_label_cards_html}</div>
      {latest_label_tables_html}
    </section>
    <section class="panel">
      <h2>Current Exposure</h2>
      {latest_exposures_html}
    </section>
    <section class="panel">
      <h2>Position Snapshot</h2>
      {latest_positions_html}
    </section>
    <section class="panel">
      <h2>Recent Orders</h2>
      {latest_orders_table_html}
    </section>
    <section class="panel">
      <h2>Recent Fills</h2>
      {latest_fills_table_html}
    </section>
    <section class="grid-two">
      <div class="panel">
        <h2>Daily Aggregates</h2>
        <table>
          <thead>
            <tr>
              <th>Date</th>
              <th>Runs</th>
              <th>Snapshots</th>
              <th>Rule Hits</th>
              <th>Selections</th>
              <th>Orders</th>
              <th>Fills</th>
              <th>Rejects</th>
            </tr>
          </thead>
          <tbody>{daily_rows_html}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Rejection Reasons</h2>
        {reason_bars_html}
      </div>
    </section>
    <section class="panel">
      <h2>Recent Runs</h2>
      <table>
        <thead>
          <tr>
            <th>Date</th>
            <th>Run</th>
            <th>Mode</th>
            <th>Status</th>
            <th>Dry</th>
            <th>Snapshots</th>
            <th>Rule Hits</th>
            <th>Selections</th>
            <th>Orders</th>
            <th>Fills</th>
            <th>Rejects</th>
            <th>orders_sent</th>
            <th>Generated</th>
            <th>Run Dir</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </section>
  </div>
</body>
</html>
"""
