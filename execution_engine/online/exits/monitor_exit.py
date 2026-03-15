"""Orchestrate exit-order submission and settlement fallback."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from execution_engine.integrations.trading.clob_client import ClobClient
from execution_engine.online.exits.settlement import SettlementCloseResult, settle_resolved_positions
from execution_engine.online.exits.submit_exit import ExitSubmissionResult, submit_pending_exit_orders
from execution_engine.runtime.config import PegConfig


@dataclass(frozen=True)
class ExitMonitorResult:
    submitted_count: int
    candidate_count: int
    settlement_close_count: int
    canceled_exit_order_count: int
    status_counts: Dict[str, int]
    exit_manifest_path: str
    settlement_path: str


def manage_exit_lifecycle(
    cfg: PegConfig,
    *,
    clob_client: ClobClient | None = None,
) -> ExitMonitorResult:
    submission: ExitSubmissionResult = submit_pending_exit_orders(cfg, clob_client=clob_client)
    settlement: SettlementCloseResult = settle_resolved_positions(cfg)
    return ExitMonitorResult(
        submitted_count=submission.submitted_count,
        candidate_count=submission.candidate_count,
        settlement_close_count=settlement.closed_count,
        canceled_exit_order_count=settlement.canceled_exit_order_count,
        status_counts=submission.status_counts,
        exit_manifest_path=str(submission.run_manifest_path),
        settlement_path=str(settlement.settlements_path),
    )
