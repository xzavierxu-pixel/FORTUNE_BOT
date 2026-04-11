"""Historical data loaders for daily label analysis."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Literal, Set
from urllib.parse import urlparse

import pandas as pd

from execution_engine.integrations.providers.gamma_provider import GammaMarketProvider
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

_MARKET_FETCH_CHUNK_SIZE = 50


def _parse_string_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _parse_float_list(value: Any) -> List[float]:
    if isinstance(value, list):
        raw_values = value
    elif value is None:
        raw_values = []
    else:
        text = str(value).strip()
        if not text:
            raw_values = []
        else:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = []
            raw_values = parsed if isinstance(parsed, list) else []
    parsed_values: List[float] = []
    for item in raw_values:
        try:
            parsed_values.append(float(item))
        except (TypeError, ValueError):
            parsed_values.append(float("nan"))
    return parsed_values


def _infer_label_domain(row: Dict[str, Any]) -> str:
    value = str(row.get("domain") or "").strip()
    if value:
        return value
    resolution_source = str(row.get("resolutionSource") or "").strip()
    if not resolution_source:
        return ""
    parsed = urlparse(resolution_source)
    return parsed.netloc or parsed.path or ""


def _resolve_market_payload(row: Dict[str, Any]) -> Dict[str, str] | None:
    market_id = str(row.get("market_id") or row.get("id") or "").strip()
    if not market_id:
        return None

    outcomes = _parse_string_list(row.get("outcomes"))
    outcome_prices = _parse_float_list(row.get("outcomePrices"))
    if not outcomes or len(outcomes) != len(outcome_prices):
        return None

    best_index = -1
    best_price = float("-inf")
    for index, price in enumerate(outcome_prices):
        if pd.isna(price):
            continue
        if price > best_price:
            best_index = index
            best_price = price
    if best_index < 0 or best_price <= 0.6:
        return None

    return {
        "market_id": market_id,
        "resolved_outcome_label": str(outcomes[best_index]),
        "resolved_outcome_index": str(best_index),
        "label_category": str(row.get("category") or ""),
        "label_domain": _infer_label_domain(row),
        "resolved_closed_time_utc": str(row.get("closedTime") or row.get("updatedAt") or ""),
        "label_source_updated_at_utc": str(row.get("updatedAt") or ""),
    }


def _fetch_market_payloads(cfg: PegConfig, market_ids: Set[str]) -> List[Dict[str, Any]]:
    provider = GammaMarketProvider(cfg.gamma_base_url, timeout_sec=cfg.clob_request_timeout_sec)
    normalized_ids = sorted(str(market_id).strip() for market_id in market_ids if str(market_id).strip())
    rows: List[Dict[str, Any]] = []

    for start in range(0, len(normalized_ids), _MARKET_FETCH_CHUNK_SIZE):
        chunk = normalized_ids[start : start + _MARKET_FETCH_CHUNK_SIZE]
        batch = provider.fetch_markets_by_ids(chunk)
        batch_by_id = {
            str(row.get("market_id") or row.get("id") or "").strip(): row
            for row in batch
            if isinstance(row, dict) and str(row.get("market_id") or row.get("id") or "").strip()
        }
        rows.extend(batch_by_id.values())
        missing_ids = [market_id for market_id in chunk if market_id not in batch_by_id]
        for market_id in missing_ids:
            fallback = provider.fetch_market_by_id(market_id)
            if isinstance(fallback, dict):
                rows.append(fallback)
    return rows


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
    scanned_market_ids = load_scanned_market_ids(cfg, scope=scope)
    if not scanned_market_ids:
        empty = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, empty)
        write_frame(cfg.run_label_resolved_labels_path, empty)
        return empty

    payload_rows = _fetch_market_payloads(cfg, scanned_market_ids)
    resolved_rows = [row for row in (_resolve_market_payload(payload) for payload in payload_rows) if row is not None]
    frame = pd.DataFrame(resolved_rows, columns=RESOLVED_LABEL_COLUMNS)
    if frame.empty:
        frame = pd.DataFrame(columns=RESOLVED_LABEL_COLUMNS)
        write_frame(cfg.resolved_labels_path, frame)
        write_frame(cfg.run_label_resolved_labels_path, frame)
        return frame

    frame["market_id"] = frame["market_id"].fillna("").astype(str)
    frame["resolved_outcome_label"] = frame["resolved_outcome_label"].fillna("").astype(str)
    frame = frame[frame["resolved_outcome_label"].str.strip() != ""].copy()
    frame = frame.sort_values(by=["market_id", "label_source_updated_at_utc"]).drop_duplicates(
        subset=["market_id"], keep="last"
    ).reset_index(drop=True)
    write_frame(cfg.resolved_labels_path, frame)
    write_frame(cfg.run_label_resolved_labels_path, frame)
    return frame


