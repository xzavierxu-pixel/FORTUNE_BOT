"""Runtime coordination for submit-window phases."""

from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
import json
import os
import sys

from execution_engine.shared.time import bj_now_iso, to_iso, utc_now

if os.name == "nt":
    import msvcrt
else:
    import fcntl


@dataclass(frozen=True)
class SubmitPhaseStatus:
    active: bool
    payload: dict[str, object]


class SubmitPhaseGuard(AbstractContextManager["SubmitPhaseGuard"]):
    def __init__(self, path: Path, handle, payload: dict[str, object]) -> None:
        self.path = path
        self._handle = handle
        self.payload = payload

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self._handle.close()
        finally:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
        return None


def _lock_handle(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        if os.name == "nt":
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise
    return handle


def _read_payload(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def acquire_submit_phase(path: Path, *, run_id: str, run_mode: str) -> SubmitPhaseGuard:
    handle = _lock_handle(path)
    payload = {
        "run_id": run_id,
        "run_mode": run_mode,
        "pid": os.getpid(),
        "started_at_utc": to_iso(utc_now()),
        "started_at_bj": bj_now_iso(),
        "host_pid_marker": f"{os.getpid()}@{sys.platform}",
    }
    handle.seek(0)
    handle.truncate()
    handle.write(json.dumps(payload, ensure_ascii=True, indent=2))
    handle.flush()
    return SubmitPhaseGuard(path, handle, payload)


def read_submit_phase(path: Path) -> SubmitPhaseStatus:
    try:
        payload = _read_payload(path)
    except PermissionError:
        return SubmitPhaseStatus(active=True, payload={})
    if not payload:
        if path.exists():
            try:
                handle = _lock_handle(path)
            except OSError:
                return SubmitPhaseStatus(active=True, payload={})
            handle.close()
        return SubmitPhaseStatus(active=False, payload={})
    try:
        handle = _lock_handle(path)
    except OSError:
        return SubmitPhaseStatus(active=True, payload=payload)
    handle.close()
    return SubmitPhaseStatus(active=False, payload=payload)
