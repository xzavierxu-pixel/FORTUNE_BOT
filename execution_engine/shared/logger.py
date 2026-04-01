"""Structured logging for PEG."""

from __future__ import annotations

from typing import Dict

from .io import append_jsonl
from .time import with_bj_timestamp_fields


def log_structured(path, record: Dict[str, object]) -> None:
    payload = with_bj_timestamp_fields(record, source_key="created_at_utc", target_key="created_at_bj")
    payload = with_bj_timestamp_fields(payload, source_key="updated_at_utc", target_key="updated_at_bj")
    payload = with_bj_timestamp_fields(payload, source_key="filled_at_utc", target_key="filled_at_bj")
    payload = with_bj_timestamp_fields(payload, target_key="logged_at_bj")
    append_jsonl(path, payload)
