"""Historical data loaders for daily label analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Literal, Set

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.online.analysis.label_io import derive_run_meta, load_csv, write_frame
from execution_engine.shared.io import list_artifact_paths_recursive, read_jsonl_many


LabelAnalysisScope = Literal["run", "all"]


def _list_artifact_paths(cfg: PegConfig, filename: str, scope: LabelAnalysisScope) -> List[Path]:
    if scope == "run":
        if not cfg.data_dir.exists():
            return []
        return sorted([path for path in cfg.data_dir.rglob(filename) if path.is_file()])
    return list_artifact_paths_recursive(cfg.runs_root_dir, filename)


RESOLVED_LABEL_COLUMNS = [
    "market_id",
    "resolved_outcome_label",
    "resolved_outcome_index",
    "label_category",
    "label_domain",
    "resolved_closed_time_utc",
    "label_source_updated_at_utc",
]


def load_orders_submitted(cfg: PegConfig, scope: LabelAnalysisScope = "run") -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    for path in _list_artifact_paths(cfg, "orders_submitted.jsonl", scope):
        meta = {"run_id": cfg.run_id, "run_date": cfg.run_date} if scope == "run" else derive_run_meta(cfg.runs_root_dir, path)
        for row in read_jsonl_many([path]):
            merged = dict(row)
            merged.setdefault("run_id", meta["run_id"])
            merged["run_date"] = meta["run_date"]
            merged["source_path"] = str(path)
            rows.append(merged)
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    if "order_status" in frame.columns:
        frame = frame[~frame["order_status"].fillna("").astype(str).str.upper().eq("DRY_RUN_SUBMITTED")].copy()
    return frame.reset_index(drop=True)


def load_selection_history(cfg: PegConfig, scope: LabelAnalysisScope = "run") -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in _list_artifact_paths(cfg, "selection_decisions.csv", scope):
        frame = load_csv(path)
        if frame.empty:
            continue
        meta = {"run_id": cfg.run_id, "run_date": cfg.run_date} if scope == "run" else derive_run_meta(cfg.runs_root_dir, path)
        if "run_id" not in frame.columns or frame["run_id"].fillna("").eq("").all():
            frame["run_id"] = meta["run_id"]
        frame["run_date"] = meta["run_date"]
        frame["source_path"] = str(path)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    selected_series = combined["selected_for_submission"] if "selected_for_submission" in combined.columns else pd.Series("", index=combined.index)
    combined["selected_rank"] = selected_series.astype(str).str.lower().map({"true": 1, "1": 1, "yes": 1}).fillna(0)
    growth_series = combined["growth_score"] if "growth_score" in combined.columns else pd.Series("", index=combined.index)
    combined["growth_score_num"] = pd.to_numeric(growth_series, errors="coerce").fillna(-999999.0)
    combined = combined.sort_values(
        by=["run_id", "market_id", "selected_token_id", "selected_rank", "growth_score_num"],
        ascending=[True, True, True, False, False],
    ).drop_duplicates(subset=["run_id", "market_id", "selected_token_id"], keep="first")
    return combined.drop(columns=["selected_rank", "growth_score_num"], errors="ignore").reset_index(drop=True)


def load_scanned_market_ids(cfg: PegConfig, scope: LabelAnalysisScope = "run") -> Set[str]:
    market_ids: Set[str] = set()

    # Keep label coverage aligned with the current submit-window production path.
    # Legacy universe/hourly-cycle artifacts are intentionally ignored here.
    for filename in ["selection_decisions.csv"]:
        for path in _list_artifact_paths(cfg, filename, scope):
            frame = load_csv(path)
            if frame.empty or "market_id" not in frame.columns:
                continue
            market_ids.update(
                str(value).strip()
                for value in frame["market_id"].dropna().astype(str)
                if str(value).strip()
            )

    for path in _list_artifact_paths(cfg, "orders_submitted.jsonl", scope):
        for row in read_jsonl_many([path]):
            market_id = str(row.get("market_id") or "").strip()
            if market_id:
                market_ids.add(market_id)

    for filename in ["decisions.jsonl", "events.jsonl"]:
        for path in _list_artifact_paths(cfg, filename, scope):
            for row in read_jsonl_many([path]):
                market_id = str(row.get("market_id") or "").strip()
                if market_id:
                    market_ids.add(market_id)

    return market_ids


def load_resolved_labels(cfg: PegConfig, scope: LabelAnalysisScope = "run") -> pd.DataFrame:
    source_path = cfg.rule_engine_raw_markets_path
    if not source_path.exists():
        empty = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, empty)
        write_frame(cfg.run_label_resolved_labels_path, empty)
        return empty

    scanned_market_ids = load_scanned_market_ids(cfg, scope=scope)
    if not scanned_market_ids:
        empty = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, empty)
        write_frame(cfg.run_label_resolved_labels_path, empty)
        return empty

    expected_cols = {
        "market_id",
        "winning_outcome_label",
        "winning_outcome_index",
        "category",
        "domain",
        "closedTime",
        "batch_fetched_at",
    }
    frame = pd.read_csv(source_path, dtype=str, usecols=lambda col: col in expected_cols)
    if frame.empty:
        write_frame(cfg.resolved_labels_path, frame)
        write_frame(cfg.run_label_resolved_labels_path, frame)
        return frame

    frame["market_id"] = frame["market_id"].fillna("").astype(str)
    frame = frame[frame["market_id"].isin(scanned_market_ids)].copy()
    if frame.empty:
        frame = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, frame)
        write_frame(cfg.run_label_resolved_labels_path, frame)
        return frame

    frame = frame.rename(
        columns={
            "winning_outcome_label": "resolved_outcome_label",
            "winning_outcome_index": "resolved_outcome_index",
            "category": "label_category",
            "domain": "label_domain",
            "closedTime": "resolved_closed_time_utc",
            "batch_fetched_at": "label_source_updated_at_utc",
        }
    )
    frame["resolved_outcome_label"] = frame["resolved_outcome_label"].fillna("").astype(str)
    frame = frame[frame["resolved_outcome_label"].str.strip() != ""].copy()
    if frame.empty:
        frame = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, frame)
        write_frame(cfg.run_label_resolved_labels_path, frame)
        return frame

    sort_columns = ["market_id"]
    if "label_source_updated_at_utc" in frame.columns:
        sort_columns.append("label_source_updated_at_utc")
    frame = frame.sort_values(by=sort_columns).drop_duplicates(subset=["market_id"], keep="last").reset_index(drop=True)
    write_frame(cfg.resolved_labels_path, frame)
    write_frame(cfg.run_label_resolved_labels_path, frame)
    return frame


