"""Structured logging for PEG."""

from __future__ import annotations

from typing import Dict

from .io import append_jsonl


def log_structured(path, record: Dict[str, object]) -> None:
    append_jsonl(path, record)
