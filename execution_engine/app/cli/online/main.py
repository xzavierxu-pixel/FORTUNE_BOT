"""Unified CLI for the online execution pipeline."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[4]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from execution_engine.online.analysis.labels import build_daily_label_analysis
from execution_engine.online.execution.monitor import monitor_order_lifecycle
from execution_engine.online.pipeline.submit_window import run_submit_window
from execution_engine.online.streaming.manager import stream_market_data
from execution_engine.runtime.config import load_config


def _print_frame_head(path: Path, print_head: int) -> None:
    if print_head <= 0 or not path.exists():
        return
    import pandas as pd

    try:
        frame = pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        frame = pd.DataFrame()
    if not frame.empty:
        print(frame.head(print_head).to_string(index=False))


def _cmd_stream_market_data(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "market_stream"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    result = asyncio.run(
        stream_market_data(
            cfg,
            asset_ids=args.asset_id,
            market_limit=args.market_limit or None,
            market_offset=max(args.market_offset, 0),
            duration_sec=args.duration_sec,
        )
    )

    print(f"subscribed_token_count={result.subscribed_token_count}")
    print(f"shard_count={result.shard_count}")
    print(f"websocket_message_count={result.websocket_message_count}")
    print(f"raw_event_count={result.raw_event_count}")
    print(f"token_state_count={result.token_state_count}")
    print(f"duration_sec={result.duration_sec:.3f}")
    for key, value in sorted(result.event_counts.items()):
        print(f"event_{key}={value}")
    print(f"run_manifest={result.run_manifest_path}")
    print(f"shared_token_state={result.shared_token_state_path}")
    print(f"shared_token_state_json={result.shared_token_state_json_path}")
    print(f"run_token_state={result.run_token_state_path}")
    _print_frame_head(result.run_token_state_path, args.print_head)


def _cmd_monitor_orders(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "order_monitor"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    result = monitor_order_lifecycle(
        cfg,
        sleep_sec=max(args.sleep_sec, 0),
        publish_summary_enabled=True,
    )

    print(f"sleep_sec={result.sleep_sec}")
    print(f"latest_order_count={result.latest_order_count}")
    print(f"open_order_count={result.open_order_count}")
    print(f"fill_count={result.fill_count}")
    print(f"open_position_count={result.open_position_count}")
    print(f"exported_submit_dirs={result.exported_submit_dirs}")
    print(f"exported_fill_rows={result.exported_fill_rows}")
    print(f"exported_cancel_rows={result.exported_cancel_rows}")
    print(f"exported_open_position_rows={result.exported_open_position_rows}")
    print(f"exported_open_position_event_rows={result.exported_open_position_event_rows}")
    print(f"shared_latest_order_count={result.shared_latest_order_count}")
    print(f"shared_fill_count={result.shared_fill_count}")
    print(f"shared_cancel_count={result.shared_cancel_count}")
    print(f"shared_open_position_count={result.shared_open_position_count}")
    print(f"shared_opened_position_event_count={result.shared_opened_position_event_count}")
    print(f"exit_candidate_count={result.exit_candidate_count}")
    print(f"exit_submitted_count={result.exit_submitted_count}")
    print(f"settlement_close_count={result.settlement_close_count}")
    print(f"canceled_exit_order_count={result.canceled_exit_order_count}")
    for key, value in sorted(result.order_status_counts.items()):
        print(f"status_{key}={value}")
    print(f"manifest={result.run_manifest_path}")


def _cmd_label_analysis(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "label_analysis"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    result = build_daily_label_analysis(cfg, scope=args.scope, publish_summary_enabled=True)

    print(f"resolved_label_count={result.resolved_label_count}")
    print(f"order_lifecycle_count={result.order_lifecycle_count}")
    print(f"executed_row_count={result.executed_row_count}")
    print(f"executed_resolved_count={result.executed_resolved_count}")
    print(f"opportunity_row_count={result.opportunity_row_count}")
    print(f"opportunity_resolved_count={result.opportunity_resolved_count}")
    print(f"analysis_scope={args.scope}")
    print(f"manifest={result.run_manifest_path}")
    print(f"resolved_labels={result.resolved_labels_path}")
    print(f"order_lifecycle={result.order_lifecycle_path}")
    print(f"executed_analysis={result.executed_analysis_path}")
    print(f"opportunity_analysis={result.opportunity_analysis_path}")
    print(f"summary={result.summary_path}")


def _cmd_run_submit_window(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "submit_window"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    result = run_submit_window(
        cfg,
        max_pages=args.max_pages or None,
    )

    print(f"page_count={result.page_count}")
    print(f"expanded_market_count={result.expanded_market_count}")
    print(f"direct_candidate_count={result.direct_candidate_count}")
    print(f"submitted_order_count={result.submitted_order_count}")
    print(f"underfilled_batch_count={result.underfilled_batch_count}")
    print(f"underfilled_batch_avg_size={result.underfilled_batch_avg_size:.3f}")
    print(f"post_submit_monitor_status={result.post_submit_monitor_status}")
    print(f"post_submit_open_order_count={result.post_submit_open_order_count}")
    print(f"post_submit_fill_count={result.post_submit_fill_count}")
    print(f"post_submit_open_position_count={result.post_submit_open_position_count}")
    print(f"final_status={result.final_status}")
    for page in result.pages:
        print(f"page_{page.page_offset}_events={page.event_count}")
        print(f"page_{page.page_offset}_expanded={page.expanded_market_count}")
        print(f"page_{page.page_offset}_candidates={page.direct_candidate_count}")
        print(f"page_{page.page_offset}_submitted={page.submitted_count}")
    print(f"manifest={result.run_manifest_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified CLI for the online execution pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    stream = subparsers.add_parser("stream-market-data", description="Stream market websocket data.")
    stream.add_argument("--run-id", default=None)
    stream.add_argument("--asset-id", action="append", default=[])
    stream.add_argument("--market-limit", type=int, default=0)
    stream.add_argument("--market-offset", type=int, default=0)
    stream.add_argument("--duration-sec", type=int, default=60)
    stream.add_argument("--print-head", type=int, default=5)
    stream.set_defaults(handler=_cmd_stream_market_data)

    monitor = subparsers.add_parser("monitor-orders", description="Monitor and reconcile order lifecycle state.")
    monitor.add_argument("--run-id", default=None)
    monitor.add_argument("--sleep-sec", type=int, default=0)
    monitor.set_defaults(handler=_cmd_monitor_orders)

    labels = subparsers.add_parser("label-analysis-daily", description="Build resolved-label sync and daily analysis artifacts.")
    labels.add_argument("--run-id", default=None)
    labels.add_argument("--scope", choices=["run", "all"], default="run")
    labels.set_defaults(handler=_cmd_label_analysis)

    submit_window = subparsers.add_parser("run-submit-window", description="Run the direct page-based online submit loop.")
    submit_window.add_argument("--run-id", default=None)
    submit_window.add_argument("--max-pages", type=int, default=0)
    submit_window.set_defaults(handler=_cmd_run_submit_window)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
