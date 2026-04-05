"""Retention and compaction helpers for run-scoped artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List
import json

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.time import bj_now_iso, to_iso, utc_now

DEBUG_ONLY_FILES = {
    "processed_markets.csv",
    "raw_snapshot_inputs.jsonl",
    "normalized_snapshots.csv",
    "feature_inputs.csv",
    "rule_hits.csv",
    "model_outputs.csv",
    "post_submit_model_features.csv",
}

FULL_RETENTION_REMOVABLE_FILES = {
    "submission_attempts.csv",
    "token_state.csv",
    "current_universe.csv",
}

CORE_RETAINED_FILES = {
    "run_summary.json",
    "selection_decisions.csv",
    "orders_submitted.jsonl",
    "manifest.json",
    "summary.json",
    "resolved_labels.csv",
    "order_lifecycle.csv",
    "executed_analysis.csv",
    "opportunity_analysis.csv",
}


@dataclass(frozen=True)
class ArtifactCompactionResult:
    scanned_run_count: int
    compacted_run_count: int
    deleted_file_count: int
    deleted_dir_count: int
    deleted_paths: List[str]
    manifest_path: Path


def _parse_run_date(path: Path) -> date | None:
    try:
        return datetime.strptime(path.name, "%Y-%m-%d").date()
    except ValueError:
        return None


def _remove_empty_dirs(root: Path) -> int:
    removed = 0
    for directory in sorted([path for path in root.rglob("*") if path.is_dir()], key=lambda value: len(value.parts), reverse=True):
        try:
            next(directory.iterdir())
        except StopIteration:
            directory.rmdir()
            removed += 1
        except OSError:
            continue
    return removed


def _iter_run_dirs(runs_root_dir: Path) -> Iterable[Path]:
    if not runs_root_dir.exists():
        return []
    run_dirs: List[Path] = []
    for day_dir in sorted([path for path in runs_root_dir.iterdir() if path.is_dir()]):
        for run_dir in sorted([path for path in day_dir.iterdir() if path.is_dir()]):
            run_dirs.append(run_dir)
    return run_dirs


def compact_run_artifacts(
    cfg: PegConfig,
    *,
    today: date | None = None,
    full_retention_days: int | None = None,
    debug_retention_days: int | None = None,
) -> ArtifactCompactionResult:
    effective_today = today or date.today()
    full_days = max(int(full_retention_days if full_retention_days is not None else cfg.artifact_retention_full_days), 0)
    debug_days = max(int(debug_retention_days if debug_retention_days is not None else cfg.artifact_retention_debug_days), 0)
    deleted_paths: List[str] = []
    scanned_run_count = 0
    compacted_run_count = 0
    deleted_file_count = 0
    deleted_dir_count = 0

    for run_dir in _iter_run_dirs(cfg.runs_root_dir):
        run_date = _parse_run_date(run_dir.parent)
        if run_date is None:
            continue
        scanned_run_count += 1
        age_days = max((effective_today - run_date).days, 0)
        if age_days < debug_days:
            continue

        run_deleted = 0
        for path in sorted([candidate for candidate in run_dir.rglob("*") if candidate.is_file()]):
            name = path.name
            delete = False
            if name in DEBUG_ONLY_FILES:
                delete = True
            elif age_days >= full_days and name in FULL_RETENTION_REMOVABLE_FILES and name not in CORE_RETAINED_FILES:
                delete = True
            if not delete:
                continue
            try:
                path.unlink()
            except OSError:
                continue
            deleted_paths.append(str(path))
            deleted_file_count += 1
            run_deleted += 1

        if run_deleted > 0:
            compacted_run_count += 1
            deleted_dir_count += _remove_empty_dirs(run_dir)

    payload = {
        "generated_at_utc": to_iso(utc_now()),
        "generated_at_bj": bj_now_iso(),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "artifact_policy": str(getattr(cfg, "artifact_policy", "minimal") or "minimal"),
        "full_retention_days": full_days,
        "debug_retention_days": debug_days,
        "scanned_run_count": scanned_run_count,
        "compacted_run_count": compacted_run_count,
        "deleted_file_count": deleted_file_count,
        "deleted_dir_count": deleted_dir_count,
        "deleted_paths": deleted_paths,
    }
    manifest_path = cfg.data_dir / "retention" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return ArtifactCompactionResult(
        scanned_run_count=scanned_run_count,
        compacted_run_count=compacted_run_count,
        deleted_file_count=deleted_file_count,
        deleted_dir_count=deleted_dir_count,
        deleted_paths=deleted_paths,
        manifest_path=manifest_path,
    )
