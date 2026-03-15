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
from execution_engine.online.execution.submission import submit_hourly_selection
from execution_engine.online.pipeline.cycle import run_hourly_cycle
from execution_engine.online.scoring.hourly import score_hourly_snapshots
from execution_engine.online.streaming.manager import stream_market_data
from execution_engine.online.universe.refresh import refresh_current_universe
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


def _cmd_refresh_universe(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "universe_refresh"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id

    cfg = load_config()
    result = refresh_current_universe(cfg, max_markets=args.max_markets)

    print(f"fetched_markets={result.fetched_markets}")
    print(f"eligible_markets={result.eligible_markets}")
    print(f"excluded_for_expiry={result.excluded_for_expiry}")
    print(f"excluded_for_structure={result.excluded_for_structure}")
    print(f"excluded_for_positions={result.excluded_for_positions}")
    for key, value in sorted(result.exclusion_breakdown.items()):
        print(f"exclude_{key}={value}")
    print(f"current_universe={result.current_universe_path}")
    print(f"current_manifest={result.current_manifest_path}")
    print(f"run_universe={result.run_universe_path}")
    print(f"run_manifest={result.run_manifest_path}")
    _print_frame_head(result.current_universe_path, args.print_head)


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


def _cmd_score_hourly(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "snapshot_score"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id
    if args.universe_csv:
        os.environ["PEG_UNIVERSE_CURRENT_PATH"] = args.universe_csv
    if args.token_state_csv:
        os.environ["PEG_TOKEN_STATE_CURRENT_PATH"] = args.token_state_csv

    cfg = load_config()
    result = score_hourly_snapshots(
        cfg,
        market_limit=args.market_limit or None,
        market_offset=max(args.market_offset, 0),
    )

    print(f"source_market_count={result.source_market_count}")
    print(f"live_universe_market_count={result.live_universe_market_count}")
    for key, value in sorted(result.live_universe_filter_breakdown.items()):
        print(f"live_universe_{key}={value}")
    print(f"rule_horizon_eligible_count={result.horizon_eligible_count}")
    print(f"rule_coverage_eligible_count={result.rule_coverage_eligible_count}")
    print(f"processed_market_count={result.processed_market_count}")
    print(f"snapshot_count={result.snapshot_count}")
    print(f"rule_hit_count={result.rule_hit_count}")
    print(f"model_output_count={result.model_output_count}")
    print(f"selected_count={result.selected_count}")
    for key, value in sorted(result.processing_reason_counts.items()):
        print(f"process_{key}={value}")
    for key, value in sorted(result.selection_reason_counts.items()):
        print(f"select_{key}={value}")
    print(f"manifest={result.run_manifest_path}")
    print(f"processed_markets={result.processed_markets_path}")
    print(f"normalized_snapshots={result.normalized_snapshots_path}")
    print(f"feature_inputs={result.feature_inputs_path}")
    print(f"rule_hits={result.rule_hits_path}")
    print(f"model_outputs={result.model_outputs_path}")
    print(f"selection={result.selection_path}")
    _print_frame_head(result.selection_path, args.print_head)


def _cmd_submit_hourly(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "submit_hourly"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id
    if args.selection_csv:
        os.environ["PEG_RUN_SNAPSHOT_SELECTION_PATH"] = args.selection_csv
    if args.token_state_csv:
        os.environ["PEG_TOKEN_STATE_CURRENT_PATH"] = args.token_state_csv

    cfg = load_config()
    selection_path = Path(args.selection_csv) if args.selection_csv else None
    token_state_path = Path(args.token_state_csv) if args.token_state_csv else None
    result = submit_hourly_selection(
        cfg,
        selection_path=selection_path,
        token_state_path=token_state_path,
        max_orders=args.max_orders or None,
    )

    print(f"total_selected_rows={result.total_selected_rows}")
    print(f"attempted_count={result.attempted_count}")
    print(f"submitted_count={result.submitted_count}")
    print(f"rejection_count={result.rejection_count}")
    for key, value in sorted(result.status_counts.items()):
        print(f"status_{key}={value}")
    print(f"manifest={result.run_manifest_path}")
    print(f"attempts={result.attempts_path}")
    _print_frame_head(result.attempts_path, args.print_head)


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


def _cmd_run_hourly_cycle(args: argparse.Namespace) -> None:
    os.environ["PEG_RUN_MODE"] = "hourly_cycle"
    if args.run_id:
        os.environ["PEG_RUN_ID"] = args.run_id
    if args.universe_csv:
        os.environ["PEG_UNIVERSE_CURRENT_PATH"] = args.universe_csv

    cfg = load_config()
    result = run_hourly_cycle(
        cfg,
        refresh_universe_enabled=not args.skip_refresh_universe,
        stream_duration_sec=max(args.stream_duration_sec, 0),
        max_batches=args.max_batches or None,
        market_limit=args.market_limit or None,
        submit_enabled=not args.skip_submit,
        run_monitor_enabled=not args.skip_monitor,
        monitor_sleep_sec=max(args.monitor_sleep_sec, 0),
        skip_stream=args.skip_stream,
    )

    print(f"source_market_count={result.source_market_count}")
    print(f"after_state_filter_count={result.after_state_filter_count}")
    print(f"rule_horizon_eligible_count={result.horizon_eligible_count}")
    print(f"rule_coverage_eligible_count={result.rule_coverage_eligible_count}")
    print(f"eligible_market_count={result.eligible_market_count}")
    print(f"batch_count={result.batch_count}")
    print(f"processed_market_count={result.processed_market_count}")
    print(f"selected_market_count={result.selected_market_count}")
    print(f"submitted_order_count={result.submitted_order_count}")
    print(f"skipped_submit={'1' if result.skipped_submit else '0'}")
    if result.universe_result is not None:
        print(f"refreshed_eligible_markets={result.universe_result.eligible_markets}")
    for batch in result.batches:
        print(f"{batch.batch_id}_markets={batch.market_count}")
        print(f"{batch.batch_id}_selected={batch.score_result.selected_count}")
        print(f"{batch.batch_id}_submitted={0 if batch.submit_result is None else batch.submit_result.submitted_count}")
        print(f"{batch.batch_id}_dir={batch.batch_dir}")
    if result.monitor_result is not None:
        print(f"monitor_open_orders={result.monitor_result.open_order_count}")
        print(f"monitor_open_positions={result.monitor_result.open_position_count}")
    print(f"manifest={result.run_manifest_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified CLI for the online execution pipeline.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    refresh = subparsers.add_parser("refresh-universe", description="Refresh the shared online market universe.")
    refresh.add_argument("--run-id", default=None)
    refresh.add_argument("--max-markets", type=int, default=None)
    refresh.add_argument("--print-head", type=int, default=5)
    refresh.set_defaults(handler=_cmd_refresh_universe)

    stream = subparsers.add_parser("stream-market-data", description="Stream market websocket data.")
    stream.add_argument("--run-id", default=None)
    stream.add_argument("--asset-id", action="append", default=[])
    stream.add_argument("--market-limit", type=int, default=0)
    stream.add_argument("--market-offset", type=int, default=0)
    stream.add_argument("--duration-sec", type=int, default=60)
    stream.add_argument("--print-head", type=int, default=5)
    stream.set_defaults(handler=_cmd_stream_market_data)

    score = subparsers.add_parser("score-hourly", description="Run the hourly snapshot scoring job.")
    score.add_argument("--run-id", default=None)
    score.add_argument("--market-limit", type=int, default=0)
    score.add_argument("--market-offset", type=int, default=0)
    score.add_argument("--universe-csv", default=None)
    score.add_argument("--token-state-csv", default=None)
    score.add_argument("--print-head", type=int, default=5)
    score.set_defaults(handler=_cmd_score_hourly)

    submit = subparsers.add_parser("submit-hourly", description="Submit hourly selection decisions.")
    submit.add_argument("--run-id", default=None)
    submit.add_argument("--selection-csv", default=None)
    submit.add_argument("--token-state-csv", default=None)
    submit.add_argument("--max-orders", type=int, default=0)
    submit.add_argument("--print-head", type=int, default=5)
    submit.set_defaults(handler=_cmd_submit_hourly)

    monitor = subparsers.add_parser("monitor-orders", description="Monitor and reconcile order lifecycle state.")
    monitor.add_argument("--run-id", default=None)
    monitor.add_argument("--sleep-sec", type=int, default=0)
    monitor.set_defaults(handler=_cmd_monitor_orders)

    labels = subparsers.add_parser("label-analysis-daily", description="Build resolved-label sync and daily analysis artifacts.")
    labels.add_argument("--run-id", default=None)
    labels.add_argument("--scope", choices=["run", "all"], default="run")
    labels.set_defaults(handler=_cmd_label_analysis)

    cycle = subparsers.add_parser("run-hourly-cycle", description="Run the end-to-end hourly online cycle.")
    cycle.add_argument("--run-id", default=None)
    cycle.add_argument("--universe-csv", default=None)
    cycle.add_argument("--stream-duration-sec", type=int, default=20)
    cycle.add_argument("--max-batches", type=int, default=0)
    cycle.add_argument("--market-limit", type=int, default=0)
    cycle.add_argument("--monitor-sleep-sec", type=int, default=0)
    cycle.add_argument("--skip-refresh-universe", action="store_true")
    cycle.add_argument("--skip-stream", action="store_true")
    cycle.add_argument("--skip-submit", action="store_true")
    cycle.add_argument("--skip-monitor", action="store_true")
    cycle.set_defaults(handler=_cmd_run_hourly_cycle)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.handler(args)


if __name__ == "__main__":
    main()
