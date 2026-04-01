#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def bj_now() -> str:
    return datetime.now(BEIJING_TZ).isoformat()


def default_state_dir() -> Path:
    raw = os.getenv("FORTUNE_BOT_STATE_DIR", "")
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "state"


def job_status_path(state_dir: Path, job: str) -> Path:
    return state_dir / "jobs" / f"{job}.json"


def load_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def mark_start(state_dir: Path, job: str, run_id: str) -> None:
    path = job_status_path(state_dir, job)
    payload = load_payload(path)
    payload.update(
        {
            "job": job,
            "host": socket.gethostname(),
            "last_run_id": run_id,
            "last_start_utc": utc_now(),
            "last_start_bj": bj_now(),
            "last_status": "running",
            "last_pid": os.getpid(),
        }
    )
    write_payload(path, payload)


def mark_finish(state_dir: Path, job: str, run_id: str, exit_code: int) -> None:
    path = job_status_path(state_dir, job)
    payload = load_payload(path)
    finished_at_utc = utc_now()
    finished_at_bj = bj_now()
    payload.update(
        {
            "job": job,
            "host": socket.gethostname(),
            "last_run_id": run_id,
            "last_end_utc": finished_at_utc,
            "last_end_bj": finished_at_bj,
            "last_exit_code": int(exit_code),
            "last_status": "success" if int(exit_code) == 0 else "failed",
        }
    )
    if int(exit_code) == 0:
        payload["last_success_utc"] = finished_at_utc
        payload["last_success_bj"] = finished_at_bj
    write_payload(path, payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Record deployment job status.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser("start")
    start.add_argument("--job", required=True)
    start.add_argument("--run-id", required=True)

    finish = subparsers.add_parser("finish")
    finish.add_argument("--job", required=True)
    finish.add_argument("--run-id", required=True)
    finish.add_argument("--exit-code", type=int, required=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    state_dir = default_state_dir()
    if args.command == "start":
        mark_start(state_dir, args.job, args.run_id)
        return
    mark_finish(state_dir, args.job, args.run_id, args.exit_code)


if __name__ == "__main__":
    main()
