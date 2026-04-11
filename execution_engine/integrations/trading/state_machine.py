"""Order state machine definitions."""

from __future__ import annotations

from typing import Dict, Set

ORDER_STATES: Set[str] = {
    "NEW",
    "SENT",
    "ACKED",
    "DELAYED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCEL_REQUESTED",
    "CANCELED",
    "EXPIRED",
    "REJECTED",
    "ERROR",
    "DRY_RUN_SUBMITTED",
}

TERMINAL_STATES: Set[str] = {"CANCELED", "REJECTED", "EXPIRED", "FILLED", "ERROR"}

TRANSITIONS: Dict[str, Set[str]] = {
    "NEW": {"SENT", "REJECTED", "ERROR", "CANCEL_REQUESTED", "EXPIRED"},
    "SENT": {"ACKED", "REJECTED", "ERROR", "CANCEL_REQUESTED", "EXPIRED"},
    "ACKED": {"PARTIALLY_FILLED", "FILLED", "CANCEL_REQUESTED", "EXPIRED", "ERROR"},
    "DELAYED": {"ACKED", "PARTIALLY_FILLED", "FILLED", "CANCEL_REQUESTED", "EXPIRED", "ERROR"},
    "PARTIALLY_FILLED": {"PARTIALLY_FILLED", "FILLED", "CANCEL_REQUESTED", "EXPIRED", "ERROR"},
    "CANCEL_REQUESTED": {"CANCELED", "EXPIRED", "ERROR"},
    "DRY_RUN_SUBMITTED": {"ACKED", "FILLED", "CANCELED", "EXPIRED", "ERROR", "CANCEL_REQUESTED"},
}


def can_transition(current: str, target: str) -> bool:
    if current == target:
        return True
    allowed = TRANSITIONS.get(current, set())
    return target in allowed
