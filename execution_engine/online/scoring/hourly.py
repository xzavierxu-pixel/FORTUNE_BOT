"""Hourly snapshot scoring orchestration for the online execution pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
import json

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.state import StateStore
from execution_engine.online.scoring.annotations import build_online_annotations
from execution_engine.online.pipeline.eligibility import evaluate_online_universe
from execution_engine.online.scoring.rule_runtime import evaluate_matched_snapshots, load_rule_runtime
from execution_engine.online.scoring.rules import load_rules_frame
from execution_engine.online.scoring.selection import (
    allocate_candidates,
    build_selection_decisions,
    select_target_side,
)
from execution_engine.online.scoring.snapshot_builder import (
    build_online_market_context,
    build_snapshot_inputs,
    load_market_frame,
)
from execution_engine.shared.io import append_jsonl


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


@dataclass(frozen=True)
class SnapshotScoreResult:
    run_manifest_path: Path
    processed_markets_path: Path
    normalized_snapshots_path: Path
    feature_inputs_path: Path
    rule_hits_path: Path
    model_outputs_path: Path
    selection_path: Path
    source_market_count: int
    live_universe_market_count: int
    live_universe_filter_breakdown: Dict[str, int]
    horizon_eligible_count: int
    rule_coverage_eligible_count: int
    processed_market_count: int
    snapshot_count: int
    rule_hit_count: int
    model_output_count: int
    selected_count: int
    selection_reason_counts: Dict[str, int]
    processing_reason_counts: Dict[str, int]


def _write_empty_outputs(cfg: PegConfig) -> None:
    empty = pd.DataFrame()
    _write_frame(cfg.run_snapshot_processed_markets_path, empty)
    _write_frame(cfg.run_snapshot_normalized_path, empty)
    _write_frame(cfg.run_snapshot_feature_inputs_path, empty)
    _write_frame(cfg.run_snapshot_rule_hits_path, empty)
    _write_frame(cfg.run_snapshot_model_outputs_path, empty)
    _write_frame(cfg.run_snapshot_selection_path, empty)


def _result(
    cfg: PegConfig,
    eligibility,
    *,
    processed_market_count: int,
    snapshot_count: int,
    rule_hit_count: int,
    model_output_count: int,
    selected_count: int,
    selection_reason_counts: Dict[str, int],
    processing_reason_counts: Dict[str, int],
) -> SnapshotScoreResult:
    return SnapshotScoreResult(
        run_manifest_path=cfg.run_snapshot_score_manifest_path,
        processed_markets_path=cfg.run_snapshot_processed_markets_path,
        normalized_snapshots_path=cfg.run_snapshot_normalized_path,
        feature_inputs_path=cfg.run_snapshot_feature_inputs_path,
        rule_hits_path=cfg.run_snapshot_rule_hits_path,
        model_outputs_path=cfg.run_snapshot_model_outputs_path,
        selection_path=cfg.run_snapshot_selection_path,
        source_market_count=eligibility.source_market_count,
        live_universe_market_count=eligibility.live_universe_market_count,
        live_universe_filter_breakdown=eligibility.live_universe_filter_breakdown,
        horizon_eligible_count=eligibility.horizon_eligible_count,
        rule_coverage_eligible_count=eligibility.rule_coverage_eligible_count,
        processed_market_count=processed_market_count,
        snapshot_count=snapshot_count,
        rule_hit_count=rule_hit_count,
        model_output_count=model_output_count,
        selected_count=selected_count,
        selection_reason_counts=selection_reason_counts,
        processing_reason_counts=processing_reason_counts,
    )


def score_hourly_snapshots(
    cfg: PegConfig,
    *,
    market_limit: int | None = None,
    market_offset: int = 0,
) -> SnapshotScoreResult:
    source_universe = load_market_frame(cfg.universe_current_path)
    eligibility = evaluate_online_universe(cfg, source_universe)
    eligible_universe = eligibility.frame

    if eligible_universe.empty:
        _write_empty_outputs(cfg)
        _write_manifest(
            cfg.run_snapshot_score_manifest_path,
            {
                "generated_at_utc": _to_iso(_utc_now()),
                "run_id": cfg.run_id,
                "run_mode": cfg.run_mode,
                "status": "empty_universe",
                "source_universe_path": str(cfg.universe_current_path),
                "source_token_state_path": str(cfg.token_state_current_path),
                "source_market_count": eligibility.source_market_count,
                "live_universe_market_count": eligibility.live_universe_market_count,
                "live_universe_filter_breakdown": eligibility.live_universe_filter_breakdown,
                "rule_horizon_eligible_count": eligibility.horizon_eligible_count,
                "rule_coverage_eligible_count": eligibility.rule_coverage_eligible_count,
                "rule_coverage_required": bool(cfg.online_require_rule_coverage),
            },
        )
        return _result(
            cfg,
            eligibility,
            processed_market_count=0,
            snapshot_count=0,
            rule_hit_count=0,
            model_output_count=0,
            selected_count=0,
            selection_reason_counts={},
            processing_reason_counts={"empty_universe": 1},
        )

    snapshot_inputs = build_snapshot_inputs(
        cfg,
        eligible_universe,
        market_limit=market_limit,
        market_offset=market_offset,
    )
    processed = snapshot_inputs.processed
    snapshots = snapshot_inputs.snapshots
    active_markets = snapshot_inputs.active_markets
    raw_inputs = snapshot_inputs.raw_inputs
    processing_counts = snapshot_inputs.processing_counts

    _write_frame(cfg.run_snapshot_processed_markets_path, processed)
    cfg.run_snapshot_raw_inputs_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_snapshot_raw_inputs_path.write_text("", encoding="utf-8")
    for record in raw_inputs:
        append_jsonl(cfg.run_snapshot_raw_inputs_path, record)

    if snapshots.empty:
        _write_frame(cfg.run_snapshot_normalized_path, snapshots)
        _write_frame(cfg.run_snapshot_feature_inputs_path, pd.DataFrame())
        _write_frame(cfg.run_snapshot_rule_hits_path, pd.DataFrame())
        _write_frame(cfg.run_snapshot_model_outputs_path, pd.DataFrame())
        _write_frame(cfg.run_snapshot_selection_path, pd.DataFrame())
        _write_manifest(
            cfg.run_snapshot_score_manifest_path,
            {
                "generated_at_utc": _to_iso(_utc_now()),
                "run_id": cfg.run_id,
                "run_mode": cfg.run_mode,
                "status": "no_snapshot_inputs",
                "source_universe_path": str(cfg.universe_current_path),
                "source_token_state_path": str(cfg.token_state_current_path),
                "source_market_count": eligibility.source_market_count,
                "live_universe_market_count": eligibility.live_universe_market_count,
                "live_universe_filter_breakdown": eligibility.live_universe_filter_breakdown,
                "rule_horizon_eligible_count": eligibility.horizon_eligible_count,
                "rule_coverage_eligible_count": eligibility.rule_coverage_eligible_count,
                "rule_coverage_required": bool(cfg.online_require_rule_coverage),
                "processing_reason_counts": processing_counts,
            },
        )
        return _result(
            cfg,
            eligibility,
            processed_market_count=len(processed),
            snapshot_count=0,
            rule_hit_count=0,
            model_output_count=0,
            selected_count=0,
            selection_reason_counts={},
            processing_reason_counts=processing_counts,
        )

    _write_frame(cfg.run_snapshot_normalized_path, snapshots)

    token_state = load_market_frame(cfg.token_state_current_path)
    token_state_by_token = {
        str(row.get("token_id") or ""): row
        for row in token_state.to_dict(orient="records")
        if str(row.get("token_id") or "")
    }
    runtime = load_rule_runtime(cfg)
    rules_frame = load_rules_frame(cfg)
    market_context = build_online_market_context(active_markets, token_state_by_token)
    annotations = build_online_annotations(cfg, active_markets)
    market_feature_cache = runtime.build_market_feature_cache(market_context, annotations)
    matched = runtime.match_rules(snapshots, rules_frame)
    rule_model = evaluate_matched_snapshots(
        cfg,
        runtime,
        matched,
        market_feature_cache,
        rules_frame,
    )

    _write_frame(cfg.run_snapshot_rule_hits_path, rule_model.rule_hits)
    _write_frame(cfg.run_snapshot_feature_inputs_path, rule_model.feature_inputs)

    if rule_model.rule_hits.empty:
        _write_frame(cfg.run_snapshot_model_outputs_path, pd.DataFrame())
        _write_frame(cfg.run_snapshot_selection_path, pd.DataFrame())
        _write_manifest(
            cfg.run_snapshot_score_manifest_path,
            {
                "generated_at_utc": _to_iso(_utc_now()),
                "run_id": cfg.run_id,
                "run_mode": cfg.run_mode,
                "status": "no_rule_hits",
                "source_market_count": eligibility.source_market_count,
                "live_universe_market_count": eligibility.live_universe_market_count,
                "live_universe_filter_breakdown": eligibility.live_universe_filter_breakdown,
                "rule_horizon_eligible_count": eligibility.horizon_eligible_count,
                "rule_coverage_eligible_count": eligibility.rule_coverage_eligible_count,
                "rule_coverage_required": bool(cfg.online_require_rule_coverage),
                "snapshot_count": len(snapshots),
                "processing_reason_counts": processing_counts,
            },
        )
        return _result(
            cfg,
            eligibility,
            processed_market_count=len(processed),
            snapshot_count=len(snapshots),
            rule_hit_count=0,
            model_output_count=0,
            selected_count=0,
            selection_reason_counts={},
            processing_reason_counts=processing_counts,
        )

    model_outputs = select_target_side(rule_model.model_outputs)
    _write_frame(cfg.run_snapshot_model_outputs_path, model_outputs)

    state = StateStore(cfg)
    viable_candidates = select_target_side(rule_model.viable_candidates)
    selected = allocate_candidates(
        viable_candidates,
        cfg,
        state,
        runtime.backtest_config,
    ) if not viable_candidates.empty else pd.DataFrame()
    selection = build_selection_decisions(model_outputs, selected, cfg)
    _write_frame(cfg.run_snapshot_selection_path, selection)

    selection_reason_counts = (
        selection["selection_reason"].astype(str).value_counts().to_dict()
        if not selection.empty and "selection_reason" in selection.columns
        else {}
    )
    _write_manifest(
        cfg.run_snapshot_score_manifest_path,
        {
            "generated_at_utc": _to_iso(_utc_now()),
            "run_id": cfg.run_id,
            "run_mode": cfg.run_mode,
            "source_universe_path": str(cfg.universe_current_path),
            "source_token_state_path": str(cfg.token_state_current_path),
            "source_market_count": eligibility.source_market_count,
            "live_universe_market_count": eligibility.live_universe_market_count,
            "live_universe_filter_breakdown": eligibility.live_universe_filter_breakdown,
            "rule_horizon_eligible_count": eligibility.horizon_eligible_count,
            "rule_coverage_eligible_count": eligibility.rule_coverage_eligible_count,
            "rule_coverage_required": bool(cfg.online_require_rule_coverage),
            "processed_market_count": int(len(processed)),
            "snapshot_count": int(len(snapshots)),
            "rule_hit_count": int(len(rule_model.rule_hits)),
            "model_output_count": int(len(model_outputs)),
            "selected_count": int(len(selected)),
            "processing_reason_counts": processing_counts,
            "selection_reason_counts": selection_reason_counts,
        },
    )

    return _result(
        cfg,
        eligibility,
        processed_market_count=len(processed),
        snapshot_count=len(snapshots),
        rule_hit_count=len(rule_model.rule_hits),
        model_output_count=len(model_outputs),
        selected_count=len(selected),
        selection_reason_counts=selection_reason_counts,
        processing_reason_counts=processing_counts,
    )


