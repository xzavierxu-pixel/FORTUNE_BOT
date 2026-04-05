"""Run-level candidate funnel audit built from candidate-state events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import json

import pandas as pd

from execution_engine.runtime.config import PegConfig
from execution_engine.shared.io import read_jsonl
from execution_engine.shared.time import bj_now_iso, to_iso, utc_now

TERMINAL_STATES = {
    "STRUCTURAL_REJECT",
    "STATE_REJECT",
    "LIVE_PRICE_MISS",
    "LIVE_SPREAD_TOO_WIDE",
    "LIVE_STATE_MISSING",
    "LIVE_STATE_STALE",
    "INVALID_PRICE",
    "SELECTED_FOR_SUBMISSION",
    "SUBMISSION_REJECTED",
    "SUBMITTED",
}

STAGE_ORDER = [
    "NEW_PAGE_MARKET",
    "STRUCTURAL_REJECT",
    "STATE_REJECT",
    "DIRECT_CANDIDATE",
    "BATCH_ASSIGNED",
    "LIVE_STATE_MISSING",
    "LIVE_STATE_STALE",
    "LIVE_SPREAD_TOO_WIDE",
    "INVALID_PRICE",
    "LIVE_PRICE_MISS",
    "INFERRED",
    "SELECTED_FOR_SUBMISSION",
    "SUBMISSION_REJECTED",
    "SUBMITTED",
    "PASS_COMPLETE",
]


@dataclass(frozen=True)
class CandidateAuditResult:
    market_audit_path: str
    funnel_summary_path: str
    market_count: int
    candidate_event_count: int


def _candidate_event_frame(cfg: PegConfig) -> pd.DataFrame:
    rows = [
        row
        for row in read_jsonl(cfg.events_path)
        if str(row.get("event_type") or "") == "CANDIDATE_STATE"
        and str(row.get("run_id") or "") == str(cfg.run_id)
        and str(row.get("run_mode") or "") == str(cfg.run_mode)
    ]
    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    for column in ["market_id", "batch_id", "token_id", "candidate_state", "reason", "event_time_utc"]:
        if column not in frame.columns:
            frame[column] = ""
    if "page_offset" not in frame.columns:
        frame["page_offset"] = pd.NA
    frame["candidate_state"] = frame["candidate_state"].fillna("").astype(str)
    frame["reason"] = frame["reason"].fillna("").astype(str)
    frame["market_id"] = frame["market_id"].fillna("").astype(str)
    frame["event_time_utc"] = frame["event_time_utc"].fillna("").astype(str)
    frame = frame[frame["market_id"].str.strip() != ""].copy()
    return frame.sort_values(by=["market_id", "event_time_utc", "candidate_state"]).reset_index(drop=True)


def _reason_counts(frame: pd.DataFrame) -> Dict[str, int]:
    if frame.empty or "reason" not in frame.columns:
        return {}
    reasons = frame["reason"].fillna("").astype(str).str.strip()
    reasons = reasons[reasons != ""]
    if reasons.empty:
        return {}
    return {str(key): int(value) for key, value in reasons.value_counts().sort_index().items()}


def build_candidate_audit(cfg: PegConfig) -> CandidateAuditResult:
    events = _candidate_event_frame(cfg)
    market_audit_path = cfg.run_audit_market_path
    funnel_summary_path = cfg.run_audit_funnel_summary_path
    market_audit_path.parent.mkdir(parents=True, exist_ok=True)
    funnel_summary_path.parent.mkdir(parents=True, exist_ok=True)

    if events.empty:
        pd.DataFrame(
            columns=[
                "run_id",
                "run_mode",
                "market_id",
                "event_count",
                "first_state",
                "terminal_state",
                "terminal_reason",
                "selected_for_submission",
                "submitted",
                "filtered_before_submission",
                "state_path",
                "reason_path",
            ]
        ).to_csv(market_audit_path, index=False)
        funnel_summary_path.write_text(
            json.dumps(
                {
                    "generated_at_utc": to_iso(utc_now()),
                    "generated_at_bj": bj_now_iso(),
                    "run_id": cfg.run_id,
                    "run_mode": cfg.run_mode,
                    "candidate_event_count": 0,
                    "market_count": 0,
                    "market_funnel": [],
                    "final_state_counts": {},
                    "final_reason_counts": {},
                },
                ensure_ascii=True,
                indent=2,
            ),
            encoding="utf-8",
        )
        return CandidateAuditResult(
            market_audit_path=str(market_audit_path),
            funnel_summary_path=str(funnel_summary_path),
            market_count=0,
            candidate_event_count=0,
        )

    grouped_rows: List[Dict[str, Any]] = []
    for market_id, market_events in events.groupby("market_id", sort=True):
        market_events = market_events.reset_index(drop=True)
        first = market_events.iloc[0]
        terminal_events = market_events[market_events["candidate_state"].isin(TERMINAL_STATES)]
        terminal = terminal_events.iloc[-1] if not terminal_events.empty else market_events.iloc[-1]
        states = market_events["candidate_state"].astype(str).tolist()
        reasons = [value for value in market_events["reason"].astype(str).tolist() if value]
        selected = any(state == "SELECTED_FOR_SUBMISSION" for state in states)
        submitted = any(state == "SUBMITTED" for state in states)
        grouped_rows.append(
            {
                "run_id": cfg.run_id,
                "run_mode": cfg.run_mode,
                "market_id": str(market_id),
                "event_count": int(len(market_events)),
                "page_offset": terminal.get("page_offset"),
                "batch_id": str(terminal.get("batch_id") or ""),
                "token_id": str(terminal.get("token_id") or ""),
                "first_state": str(first.get("candidate_state") or ""),
                "terminal_state": str(terminal.get("candidate_state") or ""),
                "terminal_reason": str(terminal.get("reason") or ""),
                "selected_for_submission": bool(selected),
                "submitted": bool(submitted),
                "filtered_before_submission": bool(not submitted and str(terminal.get("candidate_state") or "") != "PASS_COMPLETE"),
                "first_event_time_utc": str(first.get("event_time_utc") or ""),
                "terminal_event_time_utc": str(terminal.get("event_time_utc") or ""),
                "state_path": " > ".join(states),
                "reason_path": " > ".join(reasons),
            }
        )

    market_audit = pd.DataFrame(grouped_rows).sort_values(by=["market_id"]).reset_index(drop=True)
    market_audit.to_csv(market_audit_path, index=False)

    stage_rank = {state: index for index, state in enumerate(STAGE_ORDER)}
    market_funnel = []
    for state in STAGE_ORDER:
        stage_events = events[events["candidate_state"] == state].copy()
        if stage_events.empty:
            continue
        unique_markets = stage_events["market_id"].astype(str).nunique()
        market_funnel.append(
            {
                "stage": state.lower(),
                "row_count": int(len(stage_events)),
                "unique_markets": int(unique_markets),
                "reason_counts": _reason_counts(stage_events),
                "stage_rank": int(stage_rank.get(state, 999)),
            }
        )

    final_state_counts = {
        str(key): int(value)
        for key, value in market_audit["terminal_state"].astype(str).value_counts().sort_index().items()
    }
    final_reason_counts = _reason_counts(
        market_audit.rename(columns={"terminal_reason": "reason"})[["reason"]]
    )
    funnel_summary_path.write_text(
        json.dumps(
            {
                "generated_at_utc": to_iso(utc_now()),
                "generated_at_bj": bj_now_iso(),
                "run_id": cfg.run_id,
                "run_mode": cfg.run_mode,
                "candidate_event_count": int(len(events)),
                "market_count": int(len(market_audit)),
                "market_audit_path": str(market_audit_path),
                "market_funnel": market_funnel,
                "final_state_counts": final_state_counts,
                "final_reason_counts": final_reason_counts,
            },
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )
    return CandidateAuditResult(
        market_audit_path=str(market_audit_path),
        funnel_summary_path=str(funnel_summary_path),
        market_count=int(len(market_audit)),
        candidate_event_count=int(len(events)),
    )
