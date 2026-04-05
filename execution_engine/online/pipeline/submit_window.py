"""Direct page-based online submit pipeline."""

from __future__ import annotations

from dataclasses import dataclass, replace
import os
from pathlib import Path
import shutil
import subprocess
import sys
from time import perf_counter
from typing import Dict, List
import json

import pandas as pd

from execution_engine.online.execution.monitor import OrderMonitorResult, monitor_order_lifecycle
from execution_engine.online.execution.positions import load_open_market_ids, load_pending_market_ids
from execution_engine.online.execution.submission import SubmitSelectionResult, _empty_result_noop, submit_selected_orders
from execution_engine.online.pipeline.candidate_queue import CandidateBatch, DirectCandidateQueue
from execution_engine.online.pipeline.eligibility import apply_structural_coarse_filter
from execution_engine.online.pipeline.lifecycle import (
    record_candidate_frame,
    record_pass_complete,
)
from execution_engine.online.pipeline.prewarm import OnlineRuntimeContainer, build_runtime_container
from execution_engine.online.reporting.deferred_writer import DeferredWriter
from execution_engine.online.reporting.candidate_audit import build_candidate_audit
from execution_engine.online.reporting.run_summary import publish_run_summary
from execution_engine.online.scoring.live import LiveInferenceResult, run_live_inference
from execution_engine.online.scoring.selection import allocate_candidates, build_selection_decisions, select_target_side
from execution_engine.online.streaming.manager import StreamRunResult, stream_market_data
from execution_engine.online.universe.page_source import EventPageResult, fetch_event_page
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.run_state import acquire_submit_phase, read_submit_phase
from execution_engine.runtime.state import StateStore
from execution_engine.shared.metrics import load_metrics, save_metrics
from execution_engine.shared.time import bj_now_iso, to_bj_iso, to_iso, utc_now


def _write_manifest(path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _debug_artifacts_enabled(cfg: PegConfig) -> bool:
    return str(getattr(cfg, "artifact_policy", "minimal") or "minimal").strip().lower() == "debug"


def _now_fields(prefix: str) -> Dict[str, str]:
    now_utc = utc_now()
    return {
        f"{prefix}_utc": to_iso(now_utc),
        f"{prefix}_bj": to_bj_iso(now_utc),
    }


def _merge_unique_columns(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    if left.empty:
        return right.copy()
    if right.empty:
        return left.copy()
    addable = right.loc[:, [column for column in right.columns if column not in left.columns]]
    return pd.concat([left.reset_index(drop=True), addable.reset_index(drop=True)], axis=1)


def _concat_row_frames(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return incoming.reset_index(drop=True).copy()
    if incoming.empty:
        return existing.reset_index(drop=True).copy()

    ordered_columns = list(existing.columns)
    ordered_columns.extend(column for column in incoming.columns if column not in ordered_columns)
    left = existing.reindex(columns=ordered_columns)
    right = incoming.reindex(columns=ordered_columns)
    return pd.concat([left.reset_index(drop=True), right.reset_index(drop=True)], ignore_index=True)


def _write_selection_snapshot(path, selection: pd.DataFrame) -> None:
    if selection.empty:
        return
    existing = pd.DataFrame()
    if path.exists():
        try:
            existing = pd.read_csv(path, dtype=str)
        except pd.errors.EmptyDataError:
            existing = pd.DataFrame()
    combined = _concat_row_frames(existing, selection.copy())
    dedupe_keys = [
        column
        for column in ["run_id", "batch_id", "market_id", "selected_token_id", "rule_group_key", "rule_leaf_id"]
        if column in combined.columns
    ]
    if dedupe_keys:
        for column in dedupe_keys:
            combined[column] = combined[column].fillna("").astype(str)
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(path, index=False)


def _load_csv_frame(path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def _append_csv_snapshot(path, frame: pd.DataFrame, *, dedupe_keys: list[str] | None = None) -> None:
    if frame.empty:
        return
    incoming = frame.reset_index(drop=True).copy()
    existing = _load_csv_frame(path)
    combined = _concat_row_frames(existing, incoming)
    keys = [column for column in (dedupe_keys or []) if column in combined.columns]
    if keys:
        for column in keys:
            combined[column] = combined[column].fillna("").astype(str)
        combined = combined.drop_duplicates(subset=keys, keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(path, index=False)


def _append_jsonl_snapshot(path, frame: pd.DataFrame) -> None:
    if frame.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    records = json.loads(frame.reset_index(drop=True).to_json(orient="records", date_format="iso"))
    with path.open("a", encoding="utf-8") as handle:
        for row in records:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def _with_batch_metadata(frame: pd.DataFrame, cfg: PegConfig, batch_id: str) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.reset_index(drop=True).copy()
    if "run_id" not in out.columns:
        out.insert(0, "run_id", cfg.run_id)
    else:
        out["run_id"] = out["run_id"].fillna("").astype(str)
        out.loc[out["run_id"] == "", "run_id"] = cfg.run_id
    if "batch_id" not in out.columns:
        out.insert(1 if "run_id" in out.columns else 0, "batch_id", batch_id)
    else:
        out["batch_id"] = out["batch_id"].fillna("").astype(str)
        out.loc[out["batch_id"] == "", "batch_id"] = batch_id
    return out


def _write_snapshot_score_manifest(cfg: PegConfig) -> None:
    debug_enabled = _debug_artifacts_enabled(cfg)
    payload = {
        "generated_at_utc": to_iso(utc_now()),
        "generated_at_bj": bj_now_iso(),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "artifact_policy": str(getattr(cfg, "artifact_policy", "minimal") or "minimal"),
        "debug_artifacts_enabled": debug_enabled,
        "selection_decisions_path": str(cfg.run_snapshot_selection_path),
        "selection_decision_count": int(len(_load_csv_frame(cfg.run_snapshot_selection_path))),
    }
    if debug_enabled:
        payload.update(
            {
                "processed_markets_path": str(cfg.run_snapshot_processed_markets_path),
                "raw_snapshot_inputs_path": str(cfg.run_snapshot_raw_inputs_path),
                "normalized_snapshots_path": str(cfg.run_snapshot_normalized_path),
                "feature_inputs_path": str(cfg.run_snapshot_feature_inputs_path),
                "rule_hits_path": str(cfg.run_snapshot_rule_hits_path),
                "model_outputs_path": str(cfg.run_snapshot_model_outputs_path),
                "processed_market_count": int(len(_load_csv_frame(cfg.run_snapshot_processed_markets_path))),
                "normalized_snapshot_count": int(len(_load_csv_frame(cfg.run_snapshot_normalized_path))),
                "feature_input_count": int(len(_load_csv_frame(cfg.run_snapshot_feature_inputs_path))),
                "rule_hit_count": int(len(_load_csv_frame(cfg.run_snapshot_rule_hits_path))),
                "model_output_count": int(len(_load_csv_frame(cfg.run_snapshot_model_outputs_path))),
            }
        )
    _write_manifest(cfg.run_snapshot_score_manifest_path, payload)


def _persist_batch_training_artifacts(
    runtime: OnlineRuntimeContainer,
    batch: CandidateBatch,
    inference_result: LiveInferenceResult,
    selection: pd.DataFrame,
) -> None:
    cfg = runtime.cfg
    batch_id = batch.batch_id
    processed_markets = _with_batch_metadata(batch.frame, cfg, batch_id)
    raw_snapshot_inputs = _with_batch_metadata(inference_result.live_filter.eligible, cfg, batch_id)
    normalized_snapshots = _with_batch_metadata(inference_result.snapshots, cfg, batch_id)
    rule_hits = _with_batch_metadata(inference_result.rule_model.rule_hits, cfg, batch_id)
    feature_inputs = _with_batch_metadata(inference_result.rule_model.feature_inputs, cfg, batch_id)
    model_outputs = _with_batch_metadata(select_target_side(inference_result.rule_model.model_outputs), cfg, batch_id)
    selection_snapshot = _with_batch_metadata(selection, cfg, batch_id)

    if _debug_artifacts_enabled(cfg):
        _append_csv_snapshot(
            cfg.run_snapshot_processed_markets_path,
            processed_markets,
            dedupe_keys=["run_id", "batch_id", "market_id", "selected_reference_token_id"],
        )
        _append_jsonl_snapshot(cfg.run_snapshot_raw_inputs_path, raw_snapshot_inputs)
        _append_csv_snapshot(
            cfg.run_snapshot_normalized_path,
            normalized_snapshots,
            dedupe_keys=["run_id", "batch_id", "market_id", "snapshot_time"],
        )
        _append_csv_snapshot(
            cfg.run_snapshot_rule_hits_path,
            rule_hits,
            dedupe_keys=["run_id", "batch_id", "market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        )
        _append_csv_snapshot(
            cfg.run_snapshot_feature_inputs_path,
            feature_inputs,
            dedupe_keys=["run_id", "batch_id", "market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        )
        _append_csv_snapshot(
            cfg.run_snapshot_model_outputs_path,
            model_outputs,
            dedupe_keys=["run_id", "batch_id", "market_id", "snapshot_time", "rule_group_key", "rule_leaf_id"],
        )
    _write_selection_snapshot(cfg.run_snapshot_selection_path, selection_snapshot)
    _write_snapshot_score_manifest(cfg)


def _write_post_submit_output(
    runtime: OnlineRuntimeContainer,
    inference_result: LiveInferenceResult,
    selection: pd.DataFrame,
    attempt_frame: pd.DataFrame,
) -> None:
    if not _debug_artifacts_enabled(runtime.cfg):
        return
    model_outputs = select_target_side(inference_result.rule_model.model_outputs).reset_index(drop=True)
    feature_inputs = inference_result.rule_model.feature_inputs.reset_index(drop=True)
    if model_outputs.empty and feature_inputs.empty:
        return
    output = _merge_unique_columns(feature_inputs, model_outputs)
    if not selection.empty:
        selection_merge_keys = [
            column
            for column in ["run_id", "batch_id", "market_id", "rule_group_key", "rule_leaf_id"]
            if column in output.columns and column in selection.columns
        ]
        if selection_merge_keys:
            output = output.merge(
                selection,
                on=selection_merge_keys,
                how="left",
                suffixes=("", "_selection"),
            )
        else:
            output = _merge_unique_columns(output, selection.reset_index(drop=True))
    if not attempt_frame.empty:
        attempts = attempt_frame.copy()
        if "market_id" in attempts.columns and "token_id" in attempts.columns:
            attempts = attempts.drop_duplicates(subset=["market_id", "token_id"], keep="last")
            attempts = attempts.rename(
                columns={
                    "token_id": "selected_token_id",
                    "status": "submit_status",
                    "best_bid": "submit_best_bid",
                    "best_ask": "submit_best_ask",
                    "tick_size": "submit_tick_size",
                    "limit_price": "submit_limit_price",
                    "reference_price": "submit_reference_price",
                    "price_cap": "submit_price_cap",
                    "stake_usdc": "submit_stake_usdc",
                    "quote_source": "submit_quote_source",
                }
            )
            merge_keys = [column for column in ["market_id", "selected_token_id"] if column in output.columns and column in attempts.columns]
            if merge_keys:
                output = output.merge(
                    attempts[
                        [
                            column
                            for column in [
                                *merge_keys,
                                "submit_status",
                                "submit_best_bid",
                                "submit_best_ask",
                                "submit_tick_size",
                                "submit_limit_price",
                                "submit_reference_price",
                                "submit_price_cap",
                                "submit_stake_usdc",
                                "submit_quote_source",
                                "decision_id",
                                "order_attempt_id",
                            ]
                            if column in attempts.columns
                        ]
                    ],
                    on=merge_keys,
                    how="left",
                )
    now_utc = utc_now()
    output["post_submit_recorded_at_utc"] = to_iso(now_utc)
    output["post_submit_recorded_at_bj"] = to_bj_iso(now_utc)
    existing = pd.DataFrame()
    if runtime.cfg.run_submit_post_submit_features_path.exists():
        try:
            existing = pd.read_csv(runtime.cfg.run_submit_post_submit_features_path, dtype=str)
        except pd.errors.EmptyDataError:
            existing = pd.DataFrame()
    combined = _concat_row_frames(existing, output)
    runtime.cfg.run_submit_post_submit_features_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(runtime.cfg.run_submit_post_submit_features_path, index=False)


@dataclass(frozen=True)
class SubmitWindowBatchResult:
    batch_id: str
    market_count: int
    live_eligible_count: int
    live_price_miss_count: int
    live_spread_too_wide_count: int
    live_state_missing_count: int
    live_state_stale_count: int
    invalid_price_count: int
    selected_count: int
    submitted_count: int
    underfilled: bool
    candidate_column_count: int
    snapshot_column_count: int
    stream_latency_ms: float
    inference_latency_ms: float
    avg_token_state_age_sec: float
    stream_result: StreamRunResult
    inference_result: LiveInferenceResult
    submit_result: SubmitSelectionResult


@dataclass(frozen=True)
class SubmitWindowPageResult:
    page_offset: int
    event_count: int
    expanded_market_count: int
    structural_reject_count: int
    state_reject_count: int
    direct_candidate_count: int
    submitted_count: int
    fetch_latency_ms: float
    batches: List[SubmitWindowBatchResult]


@dataclass(frozen=True)
class SubmitWindowResult:
    run_manifest_path: str
    final_status: str
    submit_phase_status: str
    page_count: int
    expanded_market_count: int
    direct_candidate_count: int
    submitted_order_count: int
    submit_rejection_count: int
    underfilled_batch_count: int
    underfilled_batch_avg_size: float
    metrics: Dict[str, float]
    post_submit_monitor_status: str
    post_submit_monitor_manifest_path: str
    post_submit_latest_order_count: int
    post_submit_open_order_count: int
    post_submit_fill_count: int
    post_submit_open_position_count: int
    post_submit_exit_candidate_count: int
    post_submit_exit_submitted_count: int
    post_submit_settlement_close_count: int
    post_submit_canceled_exit_order_count: int
    submit_phase_started_at_bj: str
    submit_phase_finished_at_bj: str
    post_submit_started_at_bj: str
    post_submit_finished_at_bj: str
    pages: List[SubmitWindowPageResult]


def _monitor_summary_defaults(enabled: bool) -> Dict[str, object]:
    return {
        "post_submit_monitor_enabled": bool(enabled),
        "post_submit_monitor_status": "skipped",
        "post_submit_monitor_error": "",
        "post_submit_monitor_manifest_path": "",
        "post_submit_latest_order_count": 0,
        "post_submit_open_order_count": 0,
        "post_submit_fill_count": 0,
        "post_submit_open_position_count": 0,
        "post_submit_exit_candidate_count": 0,
        "post_submit_exit_submitted_count": 0,
        "post_submit_settlement_close_count": 0,
        "post_submit_canceled_exit_order_count": 0,
    }


def _monitor_summary_from_result(result: OrderMonitorResult) -> Dict[str, object]:
    return {
        "post_submit_monitor_enabled": True,
        "post_submit_monitor_status": "success",
        "post_submit_monitor_error": "",
        "post_submit_monitor_manifest_path": str(result.run_manifest_path),
        "post_submit_latest_order_count": int(result.latest_order_count),
        "post_submit_open_order_count": int(result.open_order_count),
        "post_submit_fill_count": int(result.fill_count),
        "post_submit_open_position_count": int(result.open_position_count),
        "post_submit_exit_candidate_count": int(result.exit_candidate_count),
        "post_submit_exit_submitted_count": int(result.exit_submitted_count),
        "post_submit_settlement_close_count": int(result.settlement_close_count),
        "post_submit_canceled_exit_order_count": int(result.canceled_exit_order_count),
    }


def _build_submit_window_manifest(
    cfg: PegConfig,
    *,
    funnel_payload: Dict[str, object],
    metrics_payload: Dict[str, float],
    page_results: List[SubmitWindowPageResult],
    expanded_market_count: int,
    structural_reject_count: int,
    state_reject_count: int,
    direct_candidate_count: int,
    live_eligible_count: int,
    live_price_miss_count: int,
    live_spread_too_wide_count: int,
    live_state_missing_count: int,
    live_state_stale_count: int,
    invalid_price_count: int,
    selected_count: int,
    submit_attempted_count: int,
    submitted_order_count: int,
    submit_rejection_count: int,
    underfilled_batch_count: int,
    underfilled_batch_avg_size: float,
    final_status: str,
    submit_phase_status: str,
    monitor_summary: Dict[str, object],
    submit_phase_started_at_bj: str,
    submit_phase_finished_at_bj: str,
) -> Dict[str, object]:
    return {
        "generated_at_utc": to_iso(utc_now()),
        "generated_at_bj": bj_now_iso(),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "submit_stage_status": submit_phase_status,
        "submit_phase_started_at_bj": submit_phase_started_at_bj,
        "submit_phase_finished_at_bj": submit_phase_finished_at_bj,
        "funnel": funnel_payload,
        "page_count": int(len(page_results)),
        "expanded_market_count": int(expanded_market_count),
        "structural_reject_count": int(structural_reject_count),
        "state_reject_count": int(state_reject_count),
        "direct_candidate_count": int(direct_candidate_count),
        "live_eligible_count": int(live_eligible_count),
        "live_price_miss_count": int(live_price_miss_count),
        "live_spread_too_wide_count": int(live_spread_too_wide_count),
        "live_state_missing_count": int(live_state_missing_count),
        "live_state_stale_count": int(live_state_stale_count),
        "invalid_price_count": int(invalid_price_count),
        "selected_count": int(selected_count),
        "submit_attempted_count": int(submit_attempted_count),
        "submitted_order_count": int(submitted_order_count),
        "submit_rejection_count": int(submit_rejection_count),
        "selection_decisions_path": str(cfg.run_snapshot_selection_path),
        "post_submit_features_path": str(cfg.run_submit_post_submit_features_path),
        "underfilled_batch_count": int(underfilled_batch_count),
        "underfilled_batch_avg_size": float(underfilled_batch_avg_size),
        "final_status": final_status,
        "metrics": metrics_payload,
        **monitor_summary,
        "pages": [
            {
                "page_offset": page.page_offset,
                "event_count": page.event_count,
                "expanded_market_count": page.expanded_market_count,
                "structural_reject_count": page.structural_reject_count,
                "state_reject_count": page.state_reject_count,
                "direct_candidate_count": page.direct_candidate_count,
                "submitted_count": page.submitted_count,
                "fetch_latency_ms": round(page.fetch_latency_ms, 3),
                "batches": [
                    {
                        "batch_id": batch.batch_id,
                        "market_count": batch.market_count,
                        "live_eligible_count": batch.live_eligible_count,
                        "live_price_miss_count": batch.live_price_miss_count,
                        "live_spread_too_wide_count": batch.live_spread_too_wide_count,
                        "live_state_missing_count": batch.live_state_missing_count,
                        "live_state_stale_count": batch.live_state_stale_count,
                        "invalid_price_count": batch.invalid_price_count,
                        "selected_count": batch.selected_count,
                        "submitted_count": batch.submitted_count,
                        "underfilled": batch.underfilled,
                        "stream_latency_ms": round(batch.stream_latency_ms, 3),
                        "inference_latency_ms": round(batch.inference_latency_ms, 3),
                        "avg_token_state_age_sec": round(batch.avg_token_state_age_sec, 3),
                        "candidate_column_count": batch.candidate_column_count,
                        "snapshot_column_count": batch.snapshot_column_count,
                        "quote_lookup_latency_ms": round(batch.submit_result.quote_lookup_latency_ms, 3),
                        "gamma_to_submit_latency_ms": round(batch.submit_result.gamma_to_submit_latency_ms, 3),
                        "selection_to_submit_latency_ms": round(batch.submit_result.selection_to_submit_latency_ms, 3),
                    }
                    for batch in page.batches
                ],
            }
            for page in page_results
        ],
    }


def _publish_submit_window_summary(
    cfg: PegConfig,
    *,
    final_status: str,
    manifest: Dict[str, object],
    monitor_summary: Dict[str, object],
) -> None:
    summary_manifest = {key: value for key, value in manifest.items() if key != "pages"}
    publish_run_summary(
        cfg,
        status=final_status,
        notes={
            "submit_window": summary_manifest,
            "post_submit_monitor": {
                key: monitor_summary[key]
                for key in sorted(monitor_summary)
            },
        },
    )


def _spawn_async_post_submit(cfg: PegConfig) -> bool:
    systemd_run = shutil.which("systemd-run")
    if not cfg.submit_window_run_monitor_after or not cfg.submit_window_async_post_submit or not systemd_run:
        return False
    env_args: list[str] = []
    for key, value in os.environ.items():
        if key.startswith("PEG_") or key.startswith("FORTUNE_BOT_"):
            env_args.append(f"--setenv={key}={value}")
    env_args.extend(
        [
            f"--setenv=PEG_RUN_ID={cfg.run_id}",
            f"--setenv=PEG_RUN_DATE={cfg.run_date}",
            f"--setenv=PEG_RUN_MODE={cfg.run_mode}",
        ]
    )
    unit_name = f"fortune-bot-submit-window-post-submit-{cfg.run_id}".lower().replace("_", "-")
    command = [
        systemd_run,
        f"--unit={unit_name}",
        "--collect",
        "--property=Type=exec",
        f"--property=WorkingDirectory={Path(__file__).resolve().parents[3]}",
        *env_args,
        sys.executable,
        "-m",
        "execution_engine.app.cli.online.main",
        "run-submit-window-post-submit",
        "--run-id",
        cfg.run_id,
        "--run-date",
        cfg.run_date,
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except Exception:
        return False
    return True


def complete_post_submit_monitor(cfg: PegConfig) -> SubmitWindowResult:
    if not cfg.run_submit_window_manifest_path.exists():
        raise FileNotFoundError(f"submit-window manifest missing: {cfg.run_submit_window_manifest_path}")
    manifest = json.loads(cfg.run_submit_window_manifest_path.read_text(encoding="utf-8"))
    monitor_summary, monitor_error = _run_post_submit_monitor(cfg)
    final_status = "completed"
    if str(monitor_summary["post_submit_monitor_status"]) == "failed":
        final_status = "completed_with_post_submit_failure"
    manifest.update(monitor_summary)
    manifest.update(
        {
            "generated_at_utc": to_iso(utc_now()),
            "generated_at_bj": bj_now_iso(),
            "final_status": final_status,
            "post_submit_started_at_bj": str(manifest.get("post_submit_started_at_bj") or bj_now_iso()),
            "post_submit_finished_at_bj": bj_now_iso(),
        }
    )
    _write_manifest(cfg.run_submit_window_manifest_path, manifest)
    _publish_submit_window_summary(cfg, final_status=final_status, manifest=manifest, monitor_summary=monitor_summary)
    if monitor_error is not None and cfg.submit_window_fail_on_monitor_error:
        raise RuntimeError("post-submit monitor failed") from monitor_error
    return SubmitWindowResult(
        run_manifest_path=str(cfg.run_submit_window_manifest_path),
        final_status=final_status,
        submit_phase_status=str(manifest.get("submit_stage_status") or "completed"),
        page_count=int(manifest.get("page_count", 0) or 0),
        expanded_market_count=int(manifest.get("expanded_market_count", 0) or 0),
        direct_candidate_count=int(manifest.get("direct_candidate_count", 0) or 0),
        submitted_order_count=int(manifest.get("submitted_order_count", 0) or 0),
        submit_rejection_count=int(manifest.get("submit_rejection_count", 0) or 0),
        underfilled_batch_count=int(manifest.get("underfilled_batch_count", 0) or 0),
        underfilled_batch_avg_size=float(manifest.get("underfilled_batch_avg_size", 0.0) or 0.0),
        metrics={key: float(value) for key, value in dict(manifest.get("metrics") or {}).items()},
        post_submit_monitor_status=str(monitor_summary["post_submit_monitor_status"]),
        post_submit_monitor_manifest_path=str(monitor_summary["post_submit_monitor_manifest_path"]),
        post_submit_latest_order_count=int(monitor_summary["post_submit_latest_order_count"]),
        post_submit_open_order_count=int(monitor_summary["post_submit_open_order_count"]),
        post_submit_fill_count=int(monitor_summary["post_submit_fill_count"]),
        post_submit_open_position_count=int(monitor_summary["post_submit_open_position_count"]),
        post_submit_exit_candidate_count=int(monitor_summary["post_submit_exit_candidate_count"]),
        post_submit_exit_submitted_count=int(monitor_summary["post_submit_exit_submitted_count"]),
        post_submit_settlement_close_count=int(monitor_summary["post_submit_settlement_close_count"]),
        post_submit_canceled_exit_order_count=int(monitor_summary["post_submit_canceled_exit_order_count"]),
        submit_phase_started_at_bj=str(manifest.get("submit_phase_started_at_bj") or ""),
        submit_phase_finished_at_bj=str(manifest.get("submit_phase_finished_at_bj") or ""),
        post_submit_started_at_bj=str(manifest.get("post_submit_started_at_bj") or ""),
        post_submit_finished_at_bj=str(manifest.get("post_submit_finished_at_bj") or ""),
        pages=[],
    )


def _process_batch(runtime: OnlineRuntimeContainer, batch: CandidateBatch) -> SubmitWindowBatchResult:
    import asyncio

    record_candidate_frame(
        runtime.cfg,
        batch.frame,
        state="BATCH_ASSIGNED",
        reason_column="coarse_filter_reason",
        batch_id_column="batch_id",
        token_column="selected_reference_token_id",
    )
    stream_started = perf_counter()
    stream_result = asyncio.run(
        stream_market_data(
            runtime.cfg,
            asset_ids=[
                str(token_id)
                for token_id in batch.frame.get("selected_reference_token_id", pd.Series(dtype=str)).astype(str).tolist()
                if str(token_id).strip()
            ],
            duration_sec=max(int(runtime.cfg.online_stream_duration_sec), 0),
        )
    )
    stream_latency_ms = (perf_counter() - stream_started) * 1000.0
    token_state = pd.DataFrame(stream_result.token_state_records)
    inference_started = perf_counter()
    inference_result = run_live_inference(runtime, batch.frame, token_state)
    inference_latency_ms = (perf_counter() - inference_started) * 1000.0
    if not inference_result.live_filter.rejected.empty:
        for rejected_row in inference_result.live_filter.rejected.to_dict(orient="records"):
            record_candidate_frame(
                runtime.cfg,
                pd.DataFrame([rejected_row]),
                state=str(rejected_row.get("live_filter_state") or "LIVE_REJECTED"),
                reason_column="live_filter_reason",
                batch_id_column="batch_id",
                token_column="selected_reference_token_id",
            )
    if inference_result.live_filter.eligible.empty:
        live_state_counts = inference_result.live_filter.state_counts
        _persist_batch_training_artifacts(runtime, batch, inference_result, pd.DataFrame())
        return SubmitWindowBatchResult(
            batch_id=batch.batch_id,
            market_count=int(len(batch.frame)),
            live_eligible_count=0,
            live_price_miss_count=int(live_state_counts.get("LIVE_PRICE_MISS", 0)),
            live_spread_too_wide_count=int(live_state_counts.get("LIVE_SPREAD_TOO_WIDE", 0)),
            live_state_missing_count=int(live_state_counts.get("LIVE_STATE_MISSING", 0)),
            live_state_stale_count=int(live_state_counts.get("LIVE_STATE_STALE", 0)),
            invalid_price_count=int(live_state_counts.get("INVALID_PRICE", 0)),
            selected_count=0,
            submitted_count=0,
            underfilled=bool(len(batch.frame) < max(int(runtime.cfg.online_market_batch_size), 1)),
            candidate_column_count=int(len(batch.frame.columns)),
            snapshot_column_count=int(len(inference_result.snapshots.columns)),
            stream_latency_ms=stream_latency_ms,
            inference_latency_ms=inference_latency_ms,
            avg_token_state_age_sec=0.0,
            stream_result=stream_result,
            inference_result=inference_result,
            submit_result=_empty_result_noop(runtime.cfg, status="empty_live_eligible"),
        )
    model_outputs = select_target_side(inference_result.rule_model.model_outputs)
    if not model_outputs.empty:
        record_candidate_frame(
            runtime.cfg,
            model_outputs,
            state="INFERRED",
            batch_id_column="batch_id",
            token_column="selected_reference_token_id",
        )
    viable_candidates = select_target_side(inference_result.rule_model.viable_candidates)
    if viable_candidates.empty:
        live_state_counts = inference_result.live_filter.state_counts
        token_age_series = pd.to_numeric(inference_result.live_filter.eligible.get("token_state_age_sec"), errors="coerce")
        _persist_batch_training_artifacts(runtime, batch, inference_result, pd.DataFrame())
        return SubmitWindowBatchResult(
            batch_id=batch.batch_id,
            market_count=int(len(batch.frame)),
            live_eligible_count=int(len(inference_result.live_filter.eligible)),
            live_price_miss_count=int(live_state_counts.get("LIVE_PRICE_MISS", 0)),
            live_spread_too_wide_count=int(live_state_counts.get("LIVE_SPREAD_TOO_WIDE", 0)),
            live_state_missing_count=int(live_state_counts.get("LIVE_STATE_MISSING", 0)),
            live_state_stale_count=int(live_state_counts.get("LIVE_STATE_STALE", 0)),
            invalid_price_count=int(live_state_counts.get("INVALID_PRICE", 0)),
            selected_count=0,
            submitted_count=0,
            underfilled=bool(len(batch.frame) < max(int(runtime.cfg.online_market_batch_size), 1)),
            candidate_column_count=int(len(batch.frame.columns)),
            snapshot_column_count=int(len(inference_result.snapshots.columns)),
            stream_latency_ms=stream_latency_ms,
            inference_latency_ms=inference_latency_ms,
            avg_token_state_age_sec=float(token_age_series.dropna().mean()) if token_age_series is not None and not token_age_series.dropna().empty else 0.0,
            stream_result=stream_result,
            inference_result=inference_result,
            submit_result=_empty_result_noop(runtime.cfg, status="empty_viable_candidates"),
        )
    state = StateStore(runtime.cfg)
    selected = (
        allocate_candidates(
            viable_candidates,
            runtime.cfg,
            state,
            runtime.rule_runtime.backtest_config,
        )
        if not viable_candidates.empty
        else pd.DataFrame()
    )
    selection = build_selection_decisions(model_outputs, selected, runtime.cfg)
    if selection.empty:
        live_state_counts = inference_result.live_filter.state_counts
        token_age_series = pd.to_numeric(inference_result.live_filter.eligible.get("token_state_age_sec"), errors="coerce")
        _persist_batch_training_artifacts(runtime, batch, inference_result, selection)
        return SubmitWindowBatchResult(
            batch_id=batch.batch_id,
            market_count=int(len(batch.frame)),
            live_eligible_count=int(len(inference_result.live_filter.eligible)),
            live_price_miss_count=int(live_state_counts.get("LIVE_PRICE_MISS", 0)),
            live_spread_too_wide_count=int(live_state_counts.get("LIVE_SPREAD_TOO_WIDE", 0)),
            live_state_missing_count=int(live_state_counts.get("LIVE_STATE_MISSING", 0)),
            live_state_stale_count=int(live_state_counts.get("LIVE_STATE_STALE", 0)),
            invalid_price_count=int(live_state_counts.get("INVALID_PRICE", 0)),
            selected_count=0,
            submitted_count=0,
            underfilled=bool(len(batch.frame) < max(int(runtime.cfg.online_market_batch_size), 1)),
            candidate_column_count=int(len(batch.frame.columns)),
            snapshot_column_count=int(len(inference_result.snapshots.columns)),
            stream_latency_ms=stream_latency_ms,
            inference_latency_ms=inference_latency_ms,
            avg_token_state_age_sec=float(token_age_series.dropna().mean()) if token_age_series is not None and not token_age_series.dropna().empty else 0.0,
            stream_result=stream_result,
            inference_result=inference_result,
            submit_result=_empty_result_noop(runtime.cfg, status="empty_selection"),
        )
    if not selection.empty:
        selected_rows = selection[selection["selected_for_submission"].map(lambda value: str(value).strip().lower() in {"1", "true", "yes", "y", "on"})].copy()
        rejected_rows = selection[~selection.index.isin(selected_rows.index)].copy()
        record_candidate_frame(
            runtime.cfg,
            selected_rows,
            state="SELECTED_FOR_SUBMISSION",
            reason_column="selection_reason",
            batch_id_column="batch_id",
            token_column="selected_token_id",
        )
        record_candidate_frame(
            runtime.cfg,
            rejected_rows,
            state="INFERRED",
            reason_column="selection_reason",
            batch_id_column="batch_id",
            token_column="selected_token_id",
        )
    submit_result = submit_selected_orders(runtime.cfg, selection, token_state)
    attempt_frame = pd.DataFrame()
    if runtime.cfg.run_submit_attempts_path.exists():
        try:
            attempt_frame = pd.read_csv(runtime.cfg.run_submit_attempts_path, dtype=str)
        except pd.errors.EmptyDataError:
            attempt_frame = pd.DataFrame()
    if not attempt_frame.empty:
        latest_batch_attempts = attempt_frame[attempt_frame["market_id"].astype(str).isin(batch.frame["market_id"].astype(str))].copy()
        submitted_mask = latest_batch_attempts["status"].astype(str).str.upper().isin({"DRY_RUN_SUBMITTED", "NEW", "ACKED", "FILLED"})
        record_candidate_frame(
            runtime.cfg,
            latest_batch_attempts[submitted_mask],
            state="SUBMITTED",
            reason_column="status",
            token_column="token_id",
        )
        record_candidate_frame(
            runtime.cfg,
            latest_batch_attempts[~submitted_mask],
            state="SUBMISSION_REJECTED",
            reason_column="status",
            token_column="token_id",
        )
    _persist_batch_training_artifacts(runtime, batch, inference_result, selection)
    _write_post_submit_output(runtime, inference_result, selection, latest_batch_attempts if not attempt_frame.empty else pd.DataFrame())
    live_state_counts = inference_result.live_filter.state_counts
    token_age_series = pd.to_numeric(inference_result.live_filter.eligible.get("token_state_age_sec"), errors="coerce")
    return SubmitWindowBatchResult(
        batch_id=batch.batch_id,
        market_count=int(len(batch.frame)),
        live_eligible_count=int(len(inference_result.live_filter.eligible)),
        live_price_miss_count=int(live_state_counts.get("LIVE_PRICE_MISS", 0)),
        live_spread_too_wide_count=int(live_state_counts.get("LIVE_SPREAD_TOO_WIDE", 0)),
        live_state_missing_count=int(live_state_counts.get("LIVE_STATE_MISSING", 0)),
        live_state_stale_count=int(live_state_counts.get("LIVE_STATE_STALE", 0)),
        invalid_price_count=int(live_state_counts.get("INVALID_PRICE", 0)),
        selected_count=int(
            len(
                selection[
                    selection["selected_for_submission"].map(
                        lambda value: str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
                    )
                ]
            )
            if not selection.empty
            else 0
        ),
        submitted_count=int(submit_result.submitted_count),
        underfilled=bool(len(batch.frame) < max(int(runtime.cfg.online_market_batch_size), 1)),
        candidate_column_count=int(len(batch.frame.columns)),
        snapshot_column_count=int(len(inference_result.snapshots.columns)),
        stream_latency_ms=stream_latency_ms,
        inference_latency_ms=inference_latency_ms,
        avg_token_state_age_sec=float(token_age_series.dropna().mean()) if token_age_series is not None and not token_age_series.dropna().empty else 0.0,
        stream_result=stream_result,
        inference_result=inference_result,
        submit_result=submit_result,
    )


def _process_page(
    runtime: OnlineRuntimeContainer,
    page: EventPageResult,
    *,
    fetch_latency_ms: float,
) -> SubmitWindowPageResult:
    record_candidate_frame(runtime.cfg, page.markets, state="NEW_PAGE_MARKET", page_offset=page.page_offset)
    excluded_market_ids = load_open_market_ids(runtime.cfg) | load_pending_market_ids(runtime.cfg)
    structural = apply_structural_coarse_filter(
        runtime.cfg,
        page.markets,
        runtime.rules_frame,
        excluded_market_ids=excluded_market_ids,
    )
    if not structural.rejected.empty:
        structural_rejected = structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STRUCTURAL_REJECT"].copy()
        state_rejected = structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STATE_REJECT"].copy()
        record_candidate_frame(
            runtime.cfg,
            structural_rejected,
            state="STRUCTURAL_REJECT",
            reason_column="coarse_filter_reason",
            page_offset=page.page_offset,
        )
        record_candidate_frame(
            runtime.cfg,
            state_rejected,
            state="STATE_REJECT",
            reason_column="coarse_filter_reason",
            page_offset=page.page_offset,
        )
    record_candidate_frame(
        runtime.cfg,
        structural.direct_candidates,
        state="DIRECT_CANDIDATE",
        reason_column="coarse_filter_reason",
        token_column="selected_reference_token_id",
        page_offset=page.page_offset,
    )
    queue = DirectCandidateQueue(runtime.cfg.online_market_batch_size)
    if structural.direct_candidates.empty:
        return SubmitWindowPageResult(
            page_offset=page.page_offset,
            event_count=page.event_count,
            expanded_market_count=page.expanded_market_count,
            structural_reject_count=int(
                len(structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STRUCTURAL_REJECT"])
            ),
            state_reject_count=int(
                len(structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STATE_REJECT"])
            ),
            direct_candidate_count=0,
            submitted_count=0,
            fetch_latency_ms=fetch_latency_ms,
            batches=[],
        )
    batches = queue.add_frame(structural.direct_candidates)
    final_batch = queue.flush()
    if final_batch is not None:
        batches.append(final_batch)
    batch_results = [_process_batch(runtime, batch) for batch in batches]
    return SubmitWindowPageResult(
        page_offset=page.page_offset,
        event_count=page.event_count,
        expanded_market_count=page.expanded_market_count,
        structural_reject_count=int(
            len(structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STRUCTURAL_REJECT"])
        ),
        state_reject_count=int(
            len(structural.rejected[structural.rejected["coarse_filter_state"].astype(str) == "STATE_REJECT"])
        ),
        direct_candidate_count=int(len(structural.direct_candidates)),
        submitted_count=sum(batch.submit_result.submitted_count for batch in batch_results),
        fetch_latency_ms=fetch_latency_ms,
        batches=batch_results,
    )


def _update_metrics(cfg: PegConfig, payload: Dict[str, float]) -> None:
    metrics = load_metrics(cfg.metrics_path)
    metrics.update({key: float(value) for key, value in payload.items()})
    save_metrics(cfg.metrics_path, metrics)


def _run_post_submit_monitor(cfg: PegConfig) -> tuple[Dict[str, object], Exception | None]:
    if not cfg.submit_window_run_monitor_after:
        return _monitor_summary_defaults(enabled=False), None

    try:
        result = monitor_order_lifecycle(
            cfg,
            sleep_sec=max(int(cfg.submit_window_monitor_sleep_sec), 0),
            publish_summary_enabled=False,
        )
    except Exception as exc:
        summary = _monitor_summary_defaults(enabled=True)
        summary["post_submit_monitor_status"] = "failed"
        summary["post_submit_monitor_error"] = str(exc)
        return summary, exc

    return _monitor_summary_from_result(result), None


def _run_submit_window_sync_impl(cfg: PegConfig, *, max_pages: int | None = None) -> SubmitWindowResult:
    runtime = build_runtime_container(cfg)
    deferred_writer = DeferredWriter(cfg)

    page_results: List[SubmitWindowPageResult] = []
    expanded_market_count = 0
    direct_candidate_count = 0
    submitted_order_count = 0
    submit_rejection_count = 0
    underfilled_sizes: List[int] = []
    seen_market_ids: set[str] = set()
    fetch_latencies_ms: List[float] = []
    stream_latencies_ms: List[float] = []
    inference_latencies_ms: List[float] = []
    deferred_io_latencies_ms: List[float] = []
    critical_path_candidate_column_count = 0
    critical_path_snapshot_column_count = 0
    structural_reject_count = 0
    state_reject_count = 0
    live_eligible_count = 0
    live_price_miss_count = 0
    live_spread_too_wide_count = 0
    live_state_missing_count = 0
    live_state_stale_count = 0
    invalid_price_count = 0
    selected_count = 0
    submit_attempted_count = 0
    token_state_age_weighted_sum = 0.0
    token_state_age_weight = 0
    quote_lookup_latency_weighted_sum = 0.0
    quote_lookup_latency_weight = 0
    gamma_to_submit_latency_weighted_sum = 0.0
    gamma_to_submit_latency_weight = 0
    selection_to_submit_latency_weighted_sum = 0.0
    selection_to_submit_latency_weight = 0
    spread_gate_reject_count = 0
    submit_status_counts: Dict[str, int] = {}

    page_limit = max(int(cfg.online_gamma_event_page_size), 1)
    page_offset = 0
    page_index = 0
    while True:
        page_index += 1
        if max_pages is not None and page_index > max_pages:
            break
        fetch_started = perf_counter()
        page = fetch_event_page(cfg, offset=page_offset, limit=page_limit, seen_market_ids=seen_market_ids)
        fetch_latency_ms = (perf_counter() - fetch_started) * 1000.0
        fetch_latencies_ms.append(fetch_latency_ms)
        if page.event_count == 0:
            break
        for market_id in page.markets.get("market_id", pd.Series(dtype=str)).astype(str).tolist():
            if market_id:
                seen_market_ids.add(market_id)
        page_result = _process_page(runtime, page, fetch_latency_ms=fetch_latency_ms)
        page_results.append(page_result)
        expanded_market_count += page_result.expanded_market_count
        structural_reject_count += page_result.structural_reject_count
        state_reject_count += page_result.state_reject_count
        direct_candidate_count += page_result.direct_candidate_count
        submitted_order_count += page_result.submitted_count
        for batch in page_result.batches:
            if batch.underfilled:
                underfilled_sizes.append(batch.market_count)
            stream_latencies_ms.append(batch.stream_latency_ms)
            inference_latencies_ms.append(batch.inference_latency_ms)
            live_eligible_count += batch.live_eligible_count
            live_spread_too_wide_count += batch.live_spread_too_wide_count
            live_state_missing_count += batch.live_state_missing_count
            live_state_stale_count += batch.live_state_stale_count
            invalid_price_count += batch.invalid_price_count
            selected_count += batch.selected_count
            submit_attempted_count += batch.submit_result.attempted_count
            if batch.live_eligible_count > 0:
                token_state_age_weighted_sum += batch.avg_token_state_age_sec * batch.live_eligible_count
                token_state_age_weight += batch.live_eligible_count
            critical_path_candidate_column_count = max(critical_path_candidate_column_count, batch.candidate_column_count)
            critical_path_snapshot_column_count = max(critical_path_snapshot_column_count, batch.snapshot_column_count)
            live_price_miss_count += batch.live_price_miss_count
            submit_rejection_count += batch.submit_result.rejection_count
            for status, count in batch.submit_result.status_counts.items():
                normalized_status = str(status or "UNKNOWN")
                submit_status_counts[normalized_status] = submit_status_counts.get(normalized_status, 0) + int(count)
            if batch.submit_result.quote_lookup_count > 0:
                quote_lookup_latency_weighted_sum += (
                    batch.submit_result.quote_lookup_latency_ms * batch.submit_result.quote_lookup_count
                )
                quote_lookup_latency_weight += batch.submit_result.quote_lookup_count
            if batch.submit_result.submitted_count > 0:
                gamma_to_submit_latency_weighted_sum += (
                    batch.submit_result.gamma_to_submit_latency_ms * batch.submit_result.submitted_count
                )
                gamma_to_submit_latency_weight += batch.submit_result.submitted_count
                selection_to_submit_latency_weighted_sum += (
                    batch.submit_result.selection_to_submit_latency_ms * batch.submit_result.submitted_count
                )
                selection_to_submit_latency_weight += batch.submit_result.submitted_count
            spread_gate_reject_count += batch.submit_result.spread_gate_reject_count
        deferred_started = perf_counter()
        deferred_writer.write_report(
            {
                "generated_at_utc": to_iso(utc_now()),
                "generated_at_bj": bj_now_iso(),
                "page_offset": page_result.page_offset,
                "event_count": page_result.event_count,
                "expanded_market_count": page_result.expanded_market_count,
                "direct_candidate_count": page_result.direct_candidate_count,
                "submitted_count": page_result.submitted_count,
            }
        )
        deferred_io_latencies_ms.append((perf_counter() - deferred_started) * 1000.0)
        record_pass_complete(
            cfg,
            page.markets.get("market_id", pd.Series(dtype=str)).astype(str).tolist(),
            page_offset=page.page_offset,
        )
        if not page.has_more:
            break
        page_offset += page_limit

    underfilled_batch_count = len(underfilled_sizes)
    underfilled_batch_avg_size = (
        sum(underfilled_sizes) / underfilled_batch_count if underfilled_batch_count else 0.0
    )
    live_rejected_count = (
        live_price_miss_count
        + live_spread_too_wide_count
        + live_state_missing_count
        + live_state_stale_count
        + invalid_price_count
    )
    unaccounted_live_stage_count = max(
        direct_candidate_count - live_eligible_count - live_rejected_count,
        0,
    )
    selected_not_attempted_count = max(selected_count - submit_attempted_count, 0)
    live_eligible_not_selected_count = max(live_eligible_count - selected_count, 0)
    metrics_payload = {
        "gamma_event_page_fetch_latency_ms": sum(fetch_latencies_ms) / len(fetch_latencies_ms) if fetch_latencies_ms else 0.0,
        "expanded_market_count": float(expanded_market_count),
        "structural_reject_count": float(structural_reject_count),
        "state_reject_count": float(state_reject_count),
        "direct_candidate_count": float(direct_candidate_count),
        "underfilled_batch_count": float(underfilled_batch_count),
        "underfilled_batch_avg_size": float(underfilled_batch_avg_size),
        "stream_latency_ms": sum(stream_latencies_ms) / len(stream_latencies_ms) if stream_latencies_ms else 0.0,
        "token_state_age_sec": token_state_age_weighted_sum / token_state_age_weight if token_state_age_weight else 0.0,
        "live_eligible_count": float(live_eligible_count),
        "live_price_miss_count": float(live_price_miss_count),
        "live_spread_too_wide_count": float(live_spread_too_wide_count),
        "live_state_missing_count": float(live_state_missing_count),
        "live_state_stale_count": float(live_state_stale_count),
        "invalid_price_count": float(invalid_price_count),
        "selected_count": float(selected_count),
        "submit_attempted_count": float(submit_attempted_count),
        "inference_latency_ms": sum(inference_latencies_ms) / len(inference_latencies_ms) if inference_latencies_ms else 0.0,
        "selection_to_submit_latency_ms": (
            selection_to_submit_latency_weighted_sum / selection_to_submit_latency_weight
            if selection_to_submit_latency_weight
            else 0.0
        ),
        "gamma_to_submit_latency_ms": (
            gamma_to_submit_latency_weighted_sum / gamma_to_submit_latency_weight
            if gamma_to_submit_latency_weight
            else 0.0
        ),
        "submit_success_count": float(submitted_order_count),
        "submit_rejection_count": float(submit_rejection_count),
        "deferred_io_latency_ms": sum(deferred_io_latencies_ms) / len(deferred_io_latencies_ms) if deferred_io_latencies_ms else 0.0,
        "submit_quote_lookup_latency_ms": (
            quote_lookup_latency_weighted_sum / quote_lookup_latency_weight if quote_lookup_latency_weight else 0.0
        ),
        "spread_gate_reject_count": float(spread_gate_reject_count),
        "live_stage_unaccounted_count": float(unaccounted_live_stage_count),
        "live_eligible_not_selected_count": float(live_eligible_not_selected_count),
        "selected_not_attempted_count": float(selected_not_attempted_count),
        "critical_path_candidate_column_count": float(critical_path_candidate_column_count),
        "critical_path_snapshot_column_count": float(critical_path_snapshot_column_count),
        "deferred_artifact_reconstruction_count": 0.0,
    }
    funnel_payload = {
        "stage0_expanded_market_count": int(expanded_market_count),
        "stage1_structural_reject_count": int(structural_reject_count),
        "stage1_state_reject_count": int(state_reject_count),
        "stage1_direct_candidate_count": int(direct_candidate_count),
        "stage2_live_eligible_count": int(live_eligible_count),
        "stage2_live_price_miss_count": int(live_price_miss_count),
        "stage2_live_spread_too_wide_count": int(live_spread_too_wide_count),
        "stage2_live_state_missing_count": int(live_state_missing_count),
        "stage2_live_state_stale_count": int(live_state_stale_count),
        "stage2_invalid_price_count": int(invalid_price_count),
        "stage2_unaccounted_count": int(unaccounted_live_stage_count),
        "stage3_selected_count": int(selected_count),
        "stage3_live_eligible_not_selected_count": int(live_eligible_not_selected_count),
        "stage4_submit_attempted_count": int(submit_attempted_count),
        "stage4_selected_not_attempted_count": int(selected_not_attempted_count),
        "stage4_submit_success_count": int(submitted_order_count),
        "stage4_submit_rejection_count": int(submit_rejection_count),
        "stage4_submit_status_counts": {
            key: int(value)
            for key, value in sorted(submit_status_counts.items())
        },
    }
    post_submit_started_at_bj = bj_now_iso()
    monitor_summary, monitor_error = _run_post_submit_monitor(cfg)
    post_submit_finished_at_bj = bj_now_iso() if cfg.submit_window_run_monitor_after else ""
    final_status = "completed"
    if str(monitor_summary["post_submit_monitor_status"]) == "failed":
        final_status = "completed_with_post_submit_failure"
    audit_result = build_candidate_audit(cfg)
    manifest = {
        "generated_at_utc": to_iso(utc_now()),
        "generated_at_bj": bj_now_iso(),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "submit_stage_status": "completed",
        "post_submit_started_at_bj": post_submit_started_at_bj if cfg.submit_window_run_monitor_after else "",
        "post_submit_finished_at_bj": post_submit_finished_at_bj,
        "funnel": funnel_payload,
        "page_count": int(len(page_results)),
        "expanded_market_count": int(expanded_market_count),
        "structural_reject_count": int(structural_reject_count),
        "state_reject_count": int(state_reject_count),
        "direct_candidate_count": int(direct_candidate_count),
        "live_eligible_count": int(live_eligible_count),
        "live_price_miss_count": int(live_price_miss_count),
        "live_spread_too_wide_count": int(live_spread_too_wide_count),
        "live_state_missing_count": int(live_state_missing_count),
        "live_state_stale_count": int(live_state_stale_count),
        "invalid_price_count": int(invalid_price_count),
        "selected_count": int(selected_count),
        "submit_attempted_count": int(submit_attempted_count),
        "submitted_order_count": int(submitted_order_count),
        "submit_rejection_count": int(submit_rejection_count),
        "selection_decisions_path": str(cfg.run_snapshot_selection_path),
        "audit_market_path": audit_result.market_audit_path,
        "audit_funnel_summary_path": audit_result.funnel_summary_path,
        "audit_market_count": int(audit_result.market_count),
        "audit_candidate_event_count": int(audit_result.candidate_event_count),
        "underfilled_batch_count": int(underfilled_batch_count),
        "underfilled_batch_avg_size": float(underfilled_batch_avg_size),
        "final_status": final_status,
        "metrics": metrics_payload,
        **monitor_summary,
        "pages": [
            {
                "page_offset": page.page_offset,
                "event_count": page.event_count,
                "expanded_market_count": page.expanded_market_count,
                "structural_reject_count": page.structural_reject_count,
                "state_reject_count": page.state_reject_count,
                "direct_candidate_count": page.direct_candidate_count,
                "submitted_count": page.submitted_count,
                "fetch_latency_ms": round(page.fetch_latency_ms, 3),
                "batches": [
                    {
                        "batch_id": batch.batch_id,
                        "market_count": batch.market_count,
                        "live_eligible_count": batch.live_eligible_count,
                        "live_price_miss_count": batch.live_price_miss_count,
                        "live_spread_too_wide_count": batch.live_spread_too_wide_count,
                        "live_state_missing_count": batch.live_state_missing_count,
                        "live_state_stale_count": batch.live_state_stale_count,
                        "invalid_price_count": batch.invalid_price_count,
                        "selected_count": batch.selected_count,
                        "submitted_count": batch.submitted_count,
                        "underfilled": batch.underfilled,
                        "stream_latency_ms": round(batch.stream_latency_ms, 3),
                        "inference_latency_ms": round(batch.inference_latency_ms, 3),
                        "avg_token_state_age_sec": round(batch.avg_token_state_age_sec, 3),
                        "candidate_column_count": batch.candidate_column_count,
                        "snapshot_column_count": batch.snapshot_column_count,
                        "quote_lookup_latency_ms": round(batch.submit_result.quote_lookup_latency_ms, 3),
                        "gamma_to_submit_latency_ms": round(batch.submit_result.gamma_to_submit_latency_ms, 3),
                        "selection_to_submit_latency_ms": round(batch.submit_result.selection_to_submit_latency_ms, 3),
                    }
                    for batch in page.batches
                ],
            }
            for page in page_results
        ],
    }
    if _debug_artifacts_enabled(cfg):
        manifest["post_submit_features_path"] = str(cfg.run_submit_post_submit_features_path)
    _write_manifest(cfg.run_submit_window_manifest_path, manifest)
    _update_metrics(cfg, metrics_payload)
    summary_manifest = {key: value for key, value in manifest.items() if key != "pages"}
    publish_run_summary(
        cfg,
        status=final_status,
        notes={
            "submit_window": summary_manifest,
            "post_submit_monitor": {
                key: monitor_summary[key]
                for key in sorted(monitor_summary)
            },
        },
    )
    if monitor_error is not None and cfg.submit_window_fail_on_monitor_error:
        raise RuntimeError("post-submit monitor failed") from monitor_error
    return SubmitWindowResult(
        run_manifest_path=str(cfg.run_submit_window_manifest_path),
        final_status=final_status,
        submit_phase_status="completed",
        page_count=len(page_results),
        expanded_market_count=expanded_market_count,
        direct_candidate_count=direct_candidate_count,
        submitted_order_count=submitted_order_count,
        submit_rejection_count=submit_rejection_count,
        underfilled_batch_count=underfilled_batch_count,
        underfilled_batch_avg_size=underfilled_batch_avg_size,
        metrics=metrics_payload,
        post_submit_monitor_status=str(monitor_summary["post_submit_monitor_status"]),
        post_submit_monitor_manifest_path=str(monitor_summary["post_submit_monitor_manifest_path"]),
        post_submit_latest_order_count=int(monitor_summary["post_submit_latest_order_count"]),
        post_submit_open_order_count=int(monitor_summary["post_submit_open_order_count"]),
        post_submit_fill_count=int(monitor_summary["post_submit_fill_count"]),
        post_submit_open_position_count=int(monitor_summary["post_submit_open_position_count"]),
        post_submit_exit_candidate_count=int(monitor_summary["post_submit_exit_candidate_count"]),
        post_submit_exit_submitted_count=int(monitor_summary["post_submit_exit_submitted_count"]),
        post_submit_settlement_close_count=int(monitor_summary["post_submit_settlement_close_count"]),
        post_submit_canceled_exit_order_count=int(monitor_summary["post_submit_canceled_exit_order_count"]),
        submit_phase_started_at_bj="",
        submit_phase_finished_at_bj="",
        post_submit_started_at_bj=post_submit_started_at_bj if cfg.submit_window_run_monitor_after else "",
        post_submit_finished_at_bj=post_submit_finished_at_bj,
        pages=page_results,
    )


def _rewrite_submit_window_manifest(
    cfg: PegConfig,
    *,
    submit_phase_started_at_bj: str,
    submit_phase_finished_at_bj: str,
    post_submit_status: str,
    post_submit_started_at_bj: str = "",
    post_submit_finished_at_bj: str = "",
) -> dict[str, object]:
    manifest = json.loads(cfg.run_submit_window_manifest_path.read_text(encoding="utf-8"))
    monitor_summary = _monitor_summary_defaults(enabled=cfg.submit_window_run_monitor_after)
    for key in monitor_summary:
        if key in manifest:
            monitor_summary[key] = manifest[key]
    manifest.update(
        {
            "generated_at_utc": to_iso(utc_now()),
            "generated_at_bj": bj_now_iso(),
            "submit_stage_status": "completed",
            "submit_phase_started_at_bj": submit_phase_started_at_bj,
            "submit_phase_finished_at_bj": submit_phase_finished_at_bj,
            "post_submit_started_at_bj": post_submit_started_at_bj,
            "post_submit_finished_at_bj": post_submit_finished_at_bj,
            "post_submit_monitor_enabled": cfg.submit_window_run_monitor_after,
            "post_submit_monitor_status": post_submit_status,
            "post_submit_monitor_error": "" if post_submit_status == "scheduled" else str(manifest.get("post_submit_monitor_error") or ""),
        }
    )
    if post_submit_status == "scheduled":
        manifest["final_status"] = "completed_post_submit_scheduled"
    _write_manifest(cfg.run_submit_window_manifest_path, manifest)
    _publish_submit_window_summary(
        cfg,
        final_status=str(manifest.get("final_status") or "completed"),
        manifest=manifest,
        monitor_summary={
            **monitor_summary,
            "post_submit_monitor_status": str(manifest.get("post_submit_monitor_status") or post_submit_status),
        },
    )
    return manifest


def run_submit_window(cfg: PegConfig, *, max_pages: int | None = None) -> SubmitWindowResult:
    submit_phase_started_at_bj = bj_now_iso()
    existing_phase = read_submit_phase(cfg.submit_phase_lock_path)
    if existing_phase.active:
        monitor_summary = _monitor_summary_defaults(enabled=cfg.submit_window_run_monitor_after)
        monitor_summary["post_submit_monitor_status"] = "not_started"
        monitor_summary["post_submit_monitor_error"] = "previous_submit_phase_active"
        skipped_manifest = {
            "generated_at_utc": to_iso(utc_now()),
            "generated_at_bj": bj_now_iso(),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "submit_stage_status": "skipped_previous_submit_phase_active",
            "submit_phase_started_at_bj": submit_phase_started_at_bj,
            "submit_phase_finished_at_bj": submit_phase_started_at_bj,
            "final_status": "skipped_previous_submit_phase_active",
            "blocking_submit_phase": existing_phase.payload,
            "metrics": {},
            **monitor_summary,
            "pages": [],
        }
        _write_manifest(cfg.run_submit_window_manifest_path, skipped_manifest)
        _publish_submit_window_summary(
            cfg,
            final_status="skipped_previous_submit_phase_active",
            manifest=skipped_manifest,
            monitor_summary=monitor_summary,
        )
        return SubmitWindowResult(
            run_manifest_path=str(cfg.run_submit_window_manifest_path),
            final_status="skipped_previous_submit_phase_active",
            submit_phase_status="skipped_previous_submit_phase_active",
            page_count=0,
            expanded_market_count=0,
            direct_candidate_count=0,
            submitted_order_count=0,
            submit_rejection_count=0,
            underfilled_batch_count=0,
            underfilled_batch_avg_size=0.0,
            metrics={},
            post_submit_monitor_status="not_started",
            post_submit_monitor_manifest_path="",
            post_submit_latest_order_count=0,
            post_submit_open_order_count=0,
            post_submit_fill_count=0,
            post_submit_open_position_count=0,
            post_submit_exit_candidate_count=0,
            post_submit_exit_submitted_count=0,
            post_submit_settlement_close_count=0,
            post_submit_canceled_exit_order_count=0,
            submit_phase_started_at_bj=submit_phase_started_at_bj,
            submit_phase_finished_at_bj=submit_phase_started_at_bj,
            post_submit_started_at_bj="",
            post_submit_finished_at_bj="",
            pages=[],
        )

    run_cfg = cfg
    want_async_post_submit = cfg.submit_window_run_monitor_after and cfg.submit_window_async_post_submit
    if want_async_post_submit:
        run_cfg = replace(cfg, submit_window_run_monitor_after=False)

    with acquire_submit_phase(cfg.submit_phase_lock_path, run_id=cfg.run_id, run_mode=cfg.run_mode):
        result = _run_submit_window_sync_impl(run_cfg, max_pages=max_pages)
    submit_phase_finished_at_bj = bj_now_iso()

    if want_async_post_submit and _spawn_async_post_submit(cfg):
        _rewrite_submit_window_manifest(
            cfg,
            submit_phase_started_at_bj=submit_phase_started_at_bj,
            submit_phase_finished_at_bj=submit_phase_finished_at_bj,
            post_submit_status="scheduled",
            post_submit_started_at_bj=bj_now_iso(),
            post_submit_finished_at_bj="",
        )
        return SubmitWindowResult(
            run_manifest_path=result.run_manifest_path,
            final_status="completed_post_submit_scheduled",
            submit_phase_status="completed",
            page_count=result.page_count,
            expanded_market_count=result.expanded_market_count,
            direct_candidate_count=result.direct_candidate_count,
            submitted_order_count=result.submitted_order_count,
            submit_rejection_count=result.submit_rejection_count,
            underfilled_batch_count=result.underfilled_batch_count,
            underfilled_batch_avg_size=result.underfilled_batch_avg_size,
            metrics=result.metrics,
            post_submit_monitor_status="scheduled",
            post_submit_monitor_manifest_path="",
            post_submit_latest_order_count=0,
            post_submit_open_order_count=0,
            post_submit_fill_count=0,
            post_submit_open_position_count=0,
            post_submit_exit_candidate_count=0,
            post_submit_exit_submitted_count=0,
            post_submit_settlement_close_count=0,
            post_submit_canceled_exit_order_count=0,
            submit_phase_started_at_bj=submit_phase_started_at_bj,
            submit_phase_finished_at_bj=submit_phase_finished_at_bj,
            post_submit_started_at_bj=bj_now_iso(),
            post_submit_finished_at_bj="",
            pages=result.pages,
        )

    manifest = _rewrite_submit_window_manifest(
        cfg,
        submit_phase_started_at_bj=submit_phase_started_at_bj,
        submit_phase_finished_at_bj=submit_phase_finished_at_bj,
        post_submit_status=result.post_submit_monitor_status,
        post_submit_started_at_bj=result.post_submit_started_at_bj,
        post_submit_finished_at_bj=result.post_submit_finished_at_bj,
    )
    return SubmitWindowResult(
        run_manifest_path=result.run_manifest_path,
        final_status=str(manifest.get("final_status") or result.final_status),
        submit_phase_status="completed",
        page_count=result.page_count,
        expanded_market_count=result.expanded_market_count,
        direct_candidate_count=result.direct_candidate_count,
        submitted_order_count=result.submitted_order_count,
        submit_rejection_count=result.submit_rejection_count,
        underfilled_batch_count=result.underfilled_batch_count,
        underfilled_batch_avg_size=result.underfilled_batch_avg_size,
        metrics=result.metrics,
        post_submit_monitor_status=result.post_submit_monitor_status,
        post_submit_monitor_manifest_path=result.post_submit_monitor_manifest_path,
        post_submit_latest_order_count=result.post_submit_latest_order_count,
        post_submit_open_order_count=result.post_submit_open_order_count,
        post_submit_fill_count=result.post_submit_fill_count,
        post_submit_open_position_count=result.post_submit_open_position_count,
        post_submit_exit_candidate_count=result.post_submit_exit_candidate_count,
        post_submit_exit_submitted_count=result.post_submit_exit_submitted_count,
        post_submit_settlement_close_count=result.post_submit_settlement_close_count,
        post_submit_canceled_exit_order_count=result.post_submit_canceled_exit_order_count,
        submit_phase_started_at_bj=submit_phase_started_at_bj,
        submit_phase_finished_at_bj=submit_phase_finished_at_bj,
        post_submit_started_at_bj=result.post_submit_started_at_bj,
        post_submit_finished_at_bj=result.post_submit_finished_at_bj,
        pages=result.pages,
    )
