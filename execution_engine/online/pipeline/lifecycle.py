"""Append-only candidate lifecycle state events for submit passes."""

from __future__ import annotations

from typing import Any, Dict, Iterable

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.io import append_jsonl
from execution_engine.shared.time import to_iso, utc_now


def record_candidate_state(
    cfg: PegConfig,
    *,
    market_id: str,
    state: str,
    reason: str = "",
    batch_id: str = "",
    token_id: str = "",
    page_offset: int | None = None,
    extra: Dict[str, Any] | None = None,
) -> None:
    payload = {
        "event_time_utc": to_iso(utc_now()),
        "event_type": "CANDIDATE_STATE",
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "market_id": str(market_id or ""),
        "batch_id": str(batch_id or ""),
        "token_id": str(token_id or ""),
        "candidate_state": str(state or ""),
        "reason": str(reason or ""),
    }
    if page_offset is not None:
        payload["page_offset"] = int(page_offset)
    if extra:
        payload.update(extra)
    append_jsonl(cfg.events_path, payload)


def record_candidate_frame(
    cfg: PegConfig,
    frame: pd.DataFrame,
    *,
    state: str,
    reason_column: str | None = None,
    batch_id_column: str | None = None,
    token_column: str | None = None,
    page_offset: int | None = None,
) -> None:
    if frame.empty:
        return
    for row in frame.to_dict(orient="records"):
        record_candidate_state(
            cfg,
            market_id=str(row.get("market_id") or ""),
            state=state,
            reason="" if reason_column is None else str(row.get(reason_column) or ""),
            batch_id="" if batch_id_column is None else str(row.get(batch_id_column) or ""),
            token_id="" if token_column is None else str(row.get(token_column) or ""),
            page_offset=page_offset,
        )


def record_pass_complete(
    cfg: PegConfig,
    market_ids: Iterable[str],
    *,
    page_offset: int | None = None,
) -> None:
    for market_id in market_ids:
        market_value = str(market_id or "")
        if not market_value:
            continue
        record_candidate_state(
            cfg,
            market_id=market_value,
            state="PASS_COMPLETE",
            reason="page_processing_complete",
            page_offset=page_offset,
        )
