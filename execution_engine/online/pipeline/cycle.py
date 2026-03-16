"""Batch orchestration for the hourly online pipeline."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List
import asyncio
import json

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.pipeline.eligibility import EligibleUniverseResult, evaluate_online_universe
from execution_engine.online.execution.monitor import monitor_order_lifecycle
from execution_engine.online.execution.positions import load_open_market_ids, load_pending_market_ids
from execution_engine.online.scoring.hourly import SnapshotScoreResult, score_hourly_snapshots
from execution_engine.online.scoring.snapshot_builder import load_market_frame
from execution_engine.online.streaming.manager import StreamRunResult, stream_market_data
from execution_engine.online.execution.submission import SubmitHourlyResult, submit_hourly_selection
from execution_engine.online.universe.refresh import UniverseRefreshResult, refresh_current_universe
from execution_engine.online.reporting.run_summary import publish_run_summary
from execution_engine.shared.time import to_iso, utc_now


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _build_batch_cfg(base_cfg: PegConfig, batch_dir: Path, batch_universe_path: Path) -> PegConfig:
    batch_cfg = replace(
        base_cfg,
        universe_current_path=batch_universe_path,
        run_stream_manifest_path=batch_dir / "market_stream" / "manifest.json",
        run_stream_token_state_path=batch_dir / "market_stream" / "token_state.csv",
        run_snapshot_score_manifest_path=batch_dir / "snapshot_score" / "manifest.json",
        run_snapshot_processed_markets_path=batch_dir / "snapshot_score" / "processed_markets.csv",
        run_snapshot_raw_inputs_path=batch_dir / "snapshot_score" / "raw_snapshot_inputs.jsonl",
        run_snapshot_normalized_path=batch_dir / "snapshot_score" / "normalized_snapshots.csv",
        run_snapshot_feature_inputs_path=batch_dir / "snapshot_score" / "feature_inputs.csv",
        run_snapshot_rule_hits_path=batch_dir / "snapshot_score" / "rule_hits.csv",
        run_snapshot_model_outputs_path=batch_dir / "snapshot_score" / "model_outputs.csv",
        run_snapshot_selection_path=batch_dir / "snapshot_score" / "selection_decisions.csv",
        run_submit_manifest_path=batch_dir / "submit_hourly" / "manifest.json",
        run_submit_attempts_path=batch_dir / "submit_hourly" / "submission_attempts.csv",
        run_submit_orders_submitted_path=batch_dir / "submit_hourly" / "orders_submitted.jsonl",
    )
    batch_cfg.ensure_dirs()
    return batch_cfg


@dataclass(frozen=True)
class HourlyCycleBatchResult:
    batch_id: str
    batch_dir: Path
    market_count: int
    reference_token_count: int
    stream_result: StreamRunResult | None
    score_result: SnapshotScoreResult
    submit_result: SubmitHourlyResult | None


@dataclass(frozen=True)
class HourlyCycleResult:
    run_manifest_path: Path
    batch_count: int
    source_market_count: int
    live_universe_market_count: int
    after_state_filter_count: int
    horizon_eligible_count: int
    rule_coverage_eligible_count: int
    eligible_market_count: int
    processed_market_count: int
    selected_market_count: int
    submitted_order_count: int
    skipped_submit: bool
    batches: List[HourlyCycleBatchResult]
    universe_result: UniverseRefreshResult | None
    monitor_result: Any | None


def run_hourly_cycle(
    cfg: PegConfig,
    *,
    refresh_universe_enabled: bool = True,
    stream_duration_sec: int = 20,
    max_batches: int | None = None,
    market_limit: int | None = None,
    submit_enabled: bool = True,
    run_monitor_enabled: bool = True,
    monitor_sleep_sec: int = 0,
    skip_stream: bool = False,
) -> HourlyCycleResult:
    monitor_before = monitor_order_lifecycle(cfg, sleep_sec=0, publish_summary_enabled=False)
    universe_result = refresh_current_universe(cfg) if refresh_universe_enabled else None
    universe = load_market_frame(cfg.universe_current_path)
    excluded_market_ids = load_open_market_ids(cfg) | load_pending_market_ids(cfg)
    eligible_result: EligibleUniverseResult = evaluate_online_universe(
        cfg,
        universe,
        excluded_market_ids=excluded_market_ids,
    )
    eligible = eligible_result.frame
    if market_limit is not None and market_limit > 0:
        eligible = eligible.head(market_limit).reset_index(drop=True)

    batches: List[HourlyCycleBatchResult] = []
    if not eligible.empty:
        batch_size = max(cfg.online_market_batch_size, 1)
        for batch_index, start_index in enumerate(range(0, len(eligible), batch_size), start=1):
            if max_batches is not None and batch_index > max_batches:
                break
            batch_id = f"batch_{batch_index:03d}"
            batch_frame = eligible.iloc[start_index:start_index + batch_size].copy().reset_index(drop=True)
            if batch_frame.empty:
                continue
            batch_dir = cfg.data_dir / "hourly_cycle" / "batches" / batch_id
            batch_universe_path = batch_dir / "universe.csv"
            _write_frame(batch_universe_path, batch_frame)
            batch_cfg = _build_batch_cfg(cfg, batch_dir, batch_universe_path)

            stream_result = None
            if not skip_stream:
                asset_ids = []
                for column in ["token_0_id", "token_1_id", "selected_reference_token_id"]:
                    if column not in batch_frame.columns:
                        continue
                    asset_ids.extend(
                        str(token_id)
                        for token_id in batch_frame[column].astype(str).tolist()
                        if str(token_id).strip()
                    )
                if asset_ids:
                    stream_result = asyncio.run(
                        stream_market_data(
                            batch_cfg,
                            asset_ids=asset_ids,
                            duration_sec=stream_duration_sec,
                        )
                    )

            score_result = score_hourly_snapshots(batch_cfg)
            submit_result = submit_hourly_selection(batch_cfg) if submit_enabled else None

            batches.append(
                HourlyCycleBatchResult(
                    batch_id=batch_id,
                    batch_dir=batch_dir,
                    market_count=len(batch_frame),
                    reference_token_count=int(batch_frame["selected_reference_token_id"].astype(str).nunique()),
                    stream_result=stream_result,
                    score_result=score_result,
                    submit_result=submit_result,
                )
            )

    monitor_result = None
    if run_monitor_enabled:
        monitor_result = monitor_order_lifecycle(
            cfg,
            sleep_sec=max(monitor_sleep_sec, 0),
            publish_summary_enabled=False,
        )

    processed_market_count = sum(batch.score_result.processed_market_count for batch in batches)
    selected_market_count = sum(batch.score_result.selected_count for batch in batches)
    submitted_order_count = sum(
        0 if batch.submit_result is None else batch.submit_result.submitted_count
        for batch in batches
    )

    manifest = {
        "generated_at_utc": to_iso(utc_now()),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "refresh_universe_enabled": refresh_universe_enabled,
        "stream_duration_sec": int(max(stream_duration_sec, 0)),
        "skip_stream": bool(skip_stream),
        "submit_enabled": bool(submit_enabled),
        "run_monitor_enabled": bool(run_monitor_enabled),
        "monitor_sleep_sec": int(max(monitor_sleep_sec, 0)),
        "source_market_count": int(eligible_result.source_market_count),
        "live_universe_market_count": int(eligible_result.live_universe_market_count),
        "live_universe_filter_breakdown": eligible_result.live_universe_filter_breakdown,
        "after_state_filter_count": int(eligible_result.after_state_filter_count),
        "rule_horizon_eligible_count": int(eligible_result.horizon_eligible_count),
        "rule_coverage_eligible_count": int(eligible_result.rule_coverage_eligible_count),
        "rule_coverage_required": bool(cfg.online_require_rule_coverage),
        "eligible_market_count": int(len(eligible)),
        "batch_count": int(len(batches)),
        "processed_market_count": int(processed_market_count),
        "selected_market_count": int(selected_market_count),
        "submitted_order_count": int(submitted_order_count),
        "universe_path": str(cfg.universe_current_path),
        "batches": [
            {
                "batch_id": batch.batch_id,
                "batch_dir": str(batch.batch_dir),
                "market_count": int(batch.market_count),
                "reference_token_count": int(batch.reference_token_count),
                "stream_manifest_path": str(batch.stream_result.run_manifest_path) if batch.stream_result else "",
                "score_manifest_path": str(batch.score_result.run_manifest_path),
                "submit_manifest_path": str(batch.submit_result.run_manifest_path) if batch.submit_result else "",
                "processed_market_count": int(batch.score_result.processed_market_count),
                "selected_count": int(batch.score_result.selected_count),
                "submitted_count": 0 if batch.submit_result is None else int(batch.submit_result.submitted_count),
            }
            for batch in batches
        ],
        "monitor_before": None if monitor_before is None else {
            "latest_order_count": int(monitor_before.latest_order_count),
            "open_order_count": int(monitor_before.open_order_count),
            "open_position_count": int(monitor_before.open_position_count),
        },
        "monitor_after": None if monitor_result is None else {
            "latest_order_count": int(monitor_result.latest_order_count),
            "open_order_count": int(monitor_result.open_order_count),
            "open_position_count": int(monitor_result.open_position_count),
        },
    }
    _write_manifest(cfg.run_hourly_cycle_manifest_path, manifest)

    publish_run_summary(
        cfg,
        status="hourly_cycle_completed",
        notes={"hourly_cycle": manifest},
    )

    return HourlyCycleResult(
        run_manifest_path=cfg.run_hourly_cycle_manifest_path,
        batch_count=len(batches),
        source_market_count=eligible_result.source_market_count,
        live_universe_market_count=eligible_result.live_universe_market_count,
        after_state_filter_count=eligible_result.after_state_filter_count,
        horizon_eligible_count=eligible_result.horizon_eligible_count,
        rule_coverage_eligible_count=eligible_result.rule_coverage_eligible_count,
        eligible_market_count=len(eligible),
        processed_market_count=processed_market_count,
        selected_market_count=selected_market_count,
        submitted_order_count=submitted_order_count,
        skipped_submit=not submit_enabled,
        batches=batches,
        universe_result=universe_result,
        monitor_result=monitor_result,
    )



