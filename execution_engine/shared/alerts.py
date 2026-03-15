"""Alert helpers for PEG."""

from __future__ import annotations

from typing import Dict

from .io import append_jsonl


def record_alert(path, record: Dict[str, object]) -> None:
    append_jsonl(path, record)
