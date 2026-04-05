"""Raw event buffering and manifest output for market streaming."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Sequence
import json

from execution_engine.runtime.config import PegConfig
from execution_engine.online.streaming.utils import to_iso
from execution_engine.online.streaming.token_state import TokenSubscriptionTarget, format_token_state_frame, serialize_targets


class RawEventBuffer:
    def __init__(self, root_dir: Path, flush_events: int, *, enabled: bool = True) -> None:
        self.root_dir = root_dir
        self.enabled = enabled
        self.flush_events = max(flush_events, 1)
        self._buffers: Dict[Path, List[str]] = defaultdict(list)
        self.raw_event_count = 0

    def append(self, shard_id: int, received_at: datetime, payload: Any) -> None:
        if not self.enabled:
            return
        path = self.root_dir / received_at.strftime("%Y-%m-%d") / received_at.strftime("%H") / f"shard_{shard_id:02d}.jsonl"
        record = {
            "received_at_utc": to_iso(received_at),
            "shard_id": shard_id,
            "payload": payload,
        }
        self._buffers[path].append(json.dumps(record, ensure_ascii=True))
        self.raw_event_count += 1
        if len(self._buffers[path]) >= self.flush_events:
            self.flush_path(path)

    def flush_path(self, path: Path) -> None:
        lines = self._buffers.get(path)
        if not lines:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
            handle.write("\n")
        self._buffers[path] = []

    def flush_all(self) -> None:
        for path in list(self._buffers.keys()):
            self.flush_path(path)


def write_stream_manifest(
    cfg: PegConfig,
    *,
    started_at: datetime,
    completed_at: datetime,
    last_message_at: datetime | None,
    targets: Sequence[TokenSubscriptionTarget],
    state_by_token: Dict[str, Dict[str, Any]],
    raw_writer: RawEventBuffer,
    message_count: int,
    event_counts: Dict[str, int],
    shard_stats: Dict[int, Dict[str, Any]],
) -> None:
    frame = format_token_state_frame(state_by_token)
    manifest = {
        "generated_at_utc": to_iso(completed_at),
        "run_id": cfg.run_id,
        "run_mode": cfg.run_mode,
        "started_at_utc": to_iso(started_at),
        "completed_at_utc": to_iso(completed_at),
        "duration_sec": round((completed_at - started_at).total_seconds(), 3),
        "websocket_url": cfg.online_market_ws_url,
        "subscribed_token_count": len(targets),
        "token_state_count": len(frame),
        "shard_count": len(shard_stats),
        "max_tokens_per_connection": cfg.online_market_ws_max_tokens_per_connection,
        "websocket_message_count": message_count,
        "raw_event_count": raw_writer.raw_event_count,
        "event_counts": dict(sorted(event_counts.items())),
        "last_message_at_utc": to_iso(last_message_at) if last_message_at else "",
        "source_universe_path": str(cfg.universe_current_path),
        "shared_token_state_path": str(cfg.token_state_current_path),
        "shared_token_state_json_path": str(cfg.token_state_current_json_path),
        "run_token_state_path": str(cfg.run_stream_token_state_path),
        "raw_event_root_dir": str(cfg.shared_ws_raw_dir) if cfg.online_market_ws_raw_enabled else "",
        "raw_capture_enabled": bool(cfg.online_market_ws_raw_enabled),
        "targets": serialize_targets(targets),
        "shards": shard_stats,
    }
    cfg.run_stream_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_stream_manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


