#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import smtplib
import socket
import ssl
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def bj_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def bj_now_iso() -> str:
    return bj_now().isoformat()


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def to_bj(value: str) -> str:
    return parse_utc(value).astimezone(BEIJING_TZ).isoformat()


def state_dir() -> Path:
    raw = os.getenv("FORTUNE_BOT_STATE_DIR", "")
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "state"


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def env_list(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def monitored_jobs() -> dict[str, int]:
    jobs: dict[str, int] = {}
    for key, value in os.environ.items():
        if not key.startswith("CHECK_") or not key.endswith("_MAX_AGE_SEC"):
            continue
        job = key[len("CHECK_") : -len("_MAX_AGE_SEC")].lower()
        jobs[job] = int(value)
    return jobs


def issue_key(kind: str, name: str) -> str:
    return f"{kind}:{name}"


@dataclass(frozen=True)
class Issue:
    key: str
    subject: str
    body: str


def check_unit_issues() -> list[Issue]:
    issues: list[Issue] = []
    for unit in env_list("CHECK_REQUIRED_UNITS"):
        result = subprocess.run(
            ["systemctl", "is-active", unit],
            capture_output=True,
            text=True,
            check=False,
        )
        status = (result.stdout or result.stderr).strip()
        if result.returncode != 0 or status != "active":
            issues.append(
                Issue(
                    key=issue_key("unit", unit),
                    subject=f"systemd unit not active: {unit}",
                    body=f"Required systemd unit `{unit}` is not active.\nCurrent status output: {status or 'unknown'}",
                )
            )
    return issues


def check_job_issues(base_state_dir: Path) -> list[Issue]:
    issues: list[Issue] = []
    jobs_dir = base_state_dir / "jobs"
    now = utc_now()
    for job, max_age_sec in monitored_jobs().items():
        path = jobs_dir / f"{job}.json"
        if not path.exists():
            issues.append(
                Issue(
                    key=issue_key("job_missing", job),
                    subject=f"missing job heartbeat: {job}",
                    body=f"Job heartbeat file is missing: {path}",
                )
            )
            continue

        payload = load_json(path)
        status = str(payload.get("last_status") or "unknown")
        if status == "failed":
            issues.append(
                Issue(
                    key=issue_key("job_failed", job),
                    subject=f"job failed: {job}",
                    body=json.dumps(payload, ensure_ascii=True, indent=2),
                )
            )
            continue

        success_raw = str(payload.get("last_success_utc") or "")
        if not success_raw:
            issues.append(
                Issue(
                    key=issue_key("job_never_succeeded", job),
                    subject=f"job never succeeded: {job}",
                    body=json.dumps(payload, ensure_ascii=True, indent=2),
                )
            )
            continue

        age_sec = int((now - parse_utc(success_raw)).total_seconds())
        if age_sec > max_age_sec:
            issues.append(
                Issue(
                    key=issue_key("job_stale", job),
                    subject=f"job stale: {job}",
                    body=(
                        f"Last success for `{job}` is older than allowed.\n"
                        f"last_success_utc={success_raw}\n"
                        f"last_success_bj={to_bj(success_raw)}\n"
                        f"age_sec={age_sec}\n"
                        f"max_age_sec={max_age_sec}\n"
                        f"payload={json.dumps(payload, ensure_ascii=True, indent=2)}"
                    ),
                )
            )
    return issues


def send_email(subject: str, body: str) -> None:
    host = os.getenv("SMTP_HOST", "").strip()
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    sender = os.getenv("ALERT_EMAIL_FROM", username).strip()
    recipient = os.getenv("ALERT_EMAIL_TO", "").strip()
    port = env_int("SMTP_PORT", 465)
    use_ssl = os.getenv("SMTP_USE_SSL", "1").strip().lower() in {"1", "true", "yes", "on"}
    subject_prefix = os.getenv("ALERT_SUBJECT_PREFIX", "[version3]").strip()

    if not host or not username or not password or not sender or not recipient:
        raise RuntimeError("SMTP configuration is incomplete")

    message = EmailMessage()
    message["Subject"] = f"{subject_prefix} {subject}"
    message["From"] = sender
    message["To"] = recipient
    message.set_content(
        f"Host: {socket.gethostname()}\nTime-BJ: {bj_now_iso()}\nTime-UTC: {utc_now_iso()}\n\n{body}",
    )

    if use_ssl:
        with smtplib.SMTP_SSL(host, port, context=ssl.create_default_context()) as server:
            server.login(username, password)
            server.send_message(message)
        return

    with smtplib.SMTP(host, port) as server:
        server.starttls(context=ssl.create_default_context())
        server.login(username, password)
        server.send_message(message)


def should_send(base_state_dir: Path, key: str) -> bool:
    cooldown_sec = env_int("ALERT_COOLDOWN_SEC", 3600)
    alert_path = base_state_dir / "alerts" / f"{key}.json"
    payload = load_json(alert_path)
    last_sent_raw = str(payload.get("last_sent_utc") or "")
    if not last_sent_raw:
        return True
    age_sec = int((utc_now() - parse_utc(last_sent_raw)).total_seconds())
    return age_sec >= cooldown_sec


def mark_sent(base_state_dir: Path, key: str) -> None:
    alert_path = base_state_dir / "alerts" / f"{key}.json"
    write_json(
        alert_path,
        {
            "last_sent_utc": utc_now_iso(),
            "last_sent_bj": bj_now_iso(),
        },
    )


def main() -> None:
    base_state_dir = state_dir()
    issues = check_unit_issues() + check_job_issues(base_state_dir)
    if not issues:
        return

    for issue in issues:
        if not should_send(base_state_dir, issue.key):
            continue
        send_email(issue.subject, issue.body)
        mark_sent(base_state_dir, issue.key)


if __name__ == "__main__":
    main()
