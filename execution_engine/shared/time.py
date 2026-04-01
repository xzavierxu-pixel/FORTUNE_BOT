"""Time helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def bj_now() -> datetime:
    return datetime.now(BEIJING_TZ)


def parse_utc(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def to_bj_iso(dt: datetime) -> str:
    return dt.astimezone(BEIJING_TZ).isoformat()


def bj_now_iso() -> str:
    return to_bj_iso(bj_now())


def bj_today_str() -> str:
    return bj_now().strftime("%Y-%m-%d")


def with_bj_timestamp_fields(
    payload: dict[str, object],
    *,
    source_key: str | None = None,
    target_key: str | None = None,
) -> dict[str, object]:
    out = dict(payload)
    if source_key and target_key:
        value = out.get(source_key)
        if isinstance(value, str) and value:
            try:
                out[target_key] = to_bj_iso(parse_utc(value))
                return out
            except ValueError:
                pass
    if target_key and target_key not in out:
        out[target_key] = bj_now_iso()
    return out
