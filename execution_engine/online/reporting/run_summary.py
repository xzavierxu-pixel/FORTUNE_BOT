"""Run summary orchestration for execution_engine."""

from __future__ import annotations

from typing import Any, Dict, Optional

from execution_engine.runtime.config import PegConfig
from execution_engine.online.reporting.dashboard import write_dashboard
from execution_engine.online.reporting.summary_io import (
    bj_now_summary_iso,
    load_json,
    read_index,
    utc_now_iso,
    write_json,
    write_jsonl,
)
from execution_engine.online.reporting.summary_metrics import (
    build_counts,
    build_execution_metrics,
    build_rejection_reasons,
    build_shared_state,
)


def _build_summary_payload(
    cfg: PegConfig,
    status: str,
    counts_override: Optional[Dict[str, int]] = None,
    notes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    counts = build_counts(cfg)
    if counts_override:
        counts.update({key: int(value) for key, value in counts_override.items()})
    metrics = load_json(cfg.metrics_path) or {}
    rejection_reasons = build_rejection_reasons(cfg)
    execution_metrics = build_execution_metrics(cfg)
    shared_state = build_shared_state(cfg)
    return {
        "run_id": cfg.run_id,
        "run_date": cfg.run_date,
        "run_mode": cfg.run_mode,
        "dry_run": cfg.dry_run,
        "status": status,
        "generated_at_utc": utc_now_iso(),
        "generated_at_bj": bj_now_summary_iso(),
        "run_dir": str(cfg.data_dir),
        "summary_path": str(cfg.run_summary_path),
        "metrics_path": str(cfg.metrics_path),
        "dashboard_path": str(cfg.summary_dashboard_path),
        "counts": counts,
        "metrics": metrics,
        "rejection_reasons": rejection_reasons,
        "execution": execution_metrics,
        "shared_state": shared_state,
        "notes": notes or {},
    }


def _upsert_index(cfg: PegConfig, summary: Dict[str, Any]) -> list[Dict[str, Any]]:
    rows = read_index(cfg.summary_index_path)
    run_key = str(summary["run_dir"])
    remaining = [row for row in rows if str(row.get("run_dir", "")) != run_key]
    remaining.append(summary)
    remaining.sort(key=lambda row: (str(row.get("run_date", "")), str(row.get("generated_at_utc", ""))), reverse=True)
    write_jsonl(cfg.summary_index_path, remaining)
    return remaining


def publish_run_summary(
    cfg: PegConfig,
    status: str,
    counts_override: Optional[Dict[str, int]] = None,
    notes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    summary = _build_summary_payload(cfg, status=status, counts_override=counts_override, notes=notes)
    write_json(cfg.run_summary_path, summary)
    rows = _upsert_index(cfg, summary)
    write_dashboard(cfg.summary_dashboard_path, rows)
    return summary


