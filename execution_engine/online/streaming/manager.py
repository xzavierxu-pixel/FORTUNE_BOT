"""WebSocket market stream manager for reference-token state ingestion."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence
import json
import time

import websockets
from websockets.exceptions import ConnectionClosed

from execution_engine.runtime.config import PegConfig
from execution_engine.online.streaming.io import RawEventBuffer, write_stream_manifest
from execution_engine.online.streaming.state import ingest_event
from execution_engine.online.streaming.utils import chunked, resolve_stream_targets, to_iso, utc_now
from execution_engine.online.streaming.token_state import (
    TokenSubscriptionTarget,
    build_initial_token_state,
    format_token_state_frame,
    write_token_state_outputs,
)


@dataclass(frozen=True)
class StreamRunResult:
    run_manifest_path: Path
    shared_token_state_path: Path
    shared_token_state_json_path: Path
    run_token_state_path: Path
    subscribed_token_count: int
    shard_count: int
    websocket_message_count: int
    raw_event_count: int
    token_state_count: int
    duration_sec: float
    event_counts: Dict[str, int]
    token_state_records: List[Dict[str, Any]]


class MarketStreamManager:
    def __init__(self, cfg: PegConfig, targets: Sequence[TokenSubscriptionTarget]) -> None:
        self.cfg = cfg
        self.targets = list(targets)
        self.state_by_token: Dict[str, Dict[str, Any]] = {
            target.token_id: build_initial_token_state(target) for target in self.targets
        }
        self.raw_writer = RawEventBuffer(
            cfg.shared_ws_raw_dir,
            cfg.online_market_ws_raw_flush_events,
            enabled=cfg.online_market_ws_raw_enabled,
        )
        self.stop_event = asyncio.Event()
        self.started_at = utc_now()
        self.completed_at: Any = None
        self.last_message_at = None
        self.message_count = 0
        self.event_counts: Dict[str, int] = defaultdict(int)
        self.shard_stats: Dict[int, Dict[str, Any]] = {}
        self._dirty_state = bool(self.targets)
        self._last_state_flush_monotonic = 0.0

    async def run(self, duration_sec: int = 60) -> StreamRunResult:
        token_chunk_size = max(self.cfg.online_market_ws_max_tokens_per_connection, 1)
        target_shards = chunked(self.targets, token_chunk_size)
        self.shard_stats = {
            shard_id: {
                "token_count": len(shard_targets),
                "token_ids": [target.token_id for target in shard_targets],
                "message_count": 0,
                "reconnect_count": 0,
                "last_message_at_utc": "",
                "last_error": "",
            }
            for shard_id, shard_targets in enumerate(target_shards)
        }

        if not self.targets:
            self._flush_state(force=True)
            self.completed_at = utc_now()
            write_stream_manifest(
                self.cfg,
                started_at=self.started_at,
                completed_at=self.completed_at,
                last_message_at=self.last_message_at,
                targets=self.targets,
                state_by_token=self.state_by_token,
                raw_writer=self.raw_writer,
                message_count=self.message_count,
                event_counts=self.event_counts,
                shard_stats=self.shard_stats,
            )
            return StreamRunResult(
                run_manifest_path=self.cfg.run_stream_manifest_path,
                shared_token_state_path=self.cfg.token_state_current_path,
                shared_token_state_json_path=self.cfg.token_state_current_json_path,
                run_token_state_path=self.cfg.run_stream_token_state_path,
                subscribed_token_count=0,
                shard_count=0,
                websocket_message_count=0,
                raw_event_count=0,
                token_state_count=0,
                duration_sec=0.0,
                event_counts={},
                token_state_records=[],
            )

        tasks = [asyncio.create_task(self._run_shard(shard_id, shard_targets)) for shard_id, shard_targets in enumerate(target_shards)]
        flush_task = asyncio.create_task(self._flush_loop())

        try:
            if duration_sec > 0:
                await asyncio.sleep(duration_sec)
            else:
                await self.stop_event.wait()
        finally:
            self.stop_event.set()
            for task in tasks:
                task.cancel()
            flush_task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(flush_task, return_exceptions=True)
            self.raw_writer.flush_all()
            self._flush_state(force=True)
            self.completed_at = utc_now()
            write_stream_manifest(
                self.cfg,
                started_at=self.started_at,
                completed_at=self.completed_at,
                last_message_at=self.last_message_at,
                targets=self.targets,
                state_by_token=self.state_by_token,
                raw_writer=self.raw_writer,
                message_count=self.message_count,
                event_counts=self.event_counts,
                shard_stats=self.shard_stats,
            )

        duration = (self.completed_at - self.started_at).total_seconds() if self.completed_at else 0.0
        frame = format_token_state_frame(self.state_by_token)
        return StreamRunResult(
            run_manifest_path=self.cfg.run_stream_manifest_path,
            shared_token_state_path=self.cfg.token_state_current_path,
            shared_token_state_json_path=self.cfg.token_state_current_json_path,
            run_token_state_path=self.cfg.run_stream_token_state_path,
            subscribed_token_count=len(self.targets),
            shard_count=len(target_shards),
            websocket_message_count=self.message_count,
            raw_event_count=self.raw_writer.raw_event_count,
            token_state_count=len(frame),
            duration_sec=duration,
            event_counts=dict(sorted(self.event_counts.items())),
            token_state_records=frame.to_dict(orient="records"),
        )

    async def _run_shard(self, shard_id: int, targets: Sequence[TokenSubscriptionTarget]) -> None:
        if not targets:
            return

        subscription = {
            "type": "market",
            "assets_ids": [target.token_id for target in targets],
        }

        while not self.stop_event.is_set():
            websocket = None
            ping_task = None
            try:
                self.shard_stats[shard_id]["reconnect_count"] += 1
                websocket = await websockets.connect(
                    self.cfg.online_market_ws_url,
                    open_timeout=self.cfg.online_market_ws_connect_timeout_sec,
                    ping_interval=None,
                    max_size=None,
                )
                await websocket.send(json.dumps(subscription, ensure_ascii=True))
                ping_task = asyncio.create_task(self._ping_loop(websocket))
                while not self.stop_event.is_set():
                    raw_message = await asyncio.wait_for(
                        websocket.recv(),
                        timeout=self.cfg.online_market_ws_idle_timeout_sec,
                    )
                    self._handle_message(shard_id, raw_message, utc_now())
            except asyncio.CancelledError:
                raise
            except (TimeoutError, ConnectionClosed, OSError, websockets.WebSocketException) as exc:
                self.shard_stats[shard_id]["last_error"] = f"{type(exc).__name__}: {exc}"
                if self.stop_event.is_set():
                    break
                await asyncio.sleep(self.cfg.online_market_ws_reconnect_backoff_sec)
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    await asyncio.gather(ping_task, return_exceptions=True)
                if websocket is not None:
                    try:
                        await websocket.close()
                    except Exception:
                        pass

    async def _ping_loop(self, websocket: Any) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(self.cfg.online_market_ws_ping_interval_sec)
            await websocket.send("PING")

    def _handle_message(self, shard_id: int, raw_message: Any, received_at) -> None:
        self.message_count += 1
        shard_stats = self.shard_stats.setdefault(shard_id, {})
        shard_stats["message_count"] = int(shard_stats.get("message_count", 0)) + 1
        shard_stats["last_message_at_utc"] = to_iso(received_at)
        self.last_message_at = received_at

        raw_text = raw_message.decode("utf-8", errors="replace") if isinstance(raw_message, bytes) else str(raw_message)
        if raw_text in {"PING", "PONG"}:
            heartbeat_type = raw_text.lower()
            self.event_counts[heartbeat_type] += 1
            self.raw_writer.append(shard_id, received_at, {"event_type": heartbeat_type, "payload": raw_text})
            return

        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError:
            self.event_counts["non_json"] += 1
            self.raw_writer.append(shard_id, received_at, {"event_type": "non_json", "payload": raw_text})
            return

        payloads = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if not isinstance(item, dict):
                self.event_counts["non_object_payload"] += 1
                self.raw_writer.append(shard_id, received_at, {"event_type": "non_object_payload", "payload": item})
                continue
            event_type = str(item.get("event_type") or item.get("type") or "unknown")
            self.event_counts[event_type] += 1
            self.raw_writer.append(shard_id, received_at, item)
            if ingest_event(self.state_by_token, event_type, item, received_at):
                self._dirty_state = True

    async def _flush_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(self.cfg.online_market_ws_state_flush_sec)
            self._flush_state(force=False)

    def _flush_state(self, force: bool) -> None:
        if not force and not self._dirty_state:
            return
        now = time.monotonic()
        if not force and now - self._last_state_flush_monotonic < self.cfg.online_market_ws_state_flush_sec:
            return
        frame = format_token_state_frame(self.state_by_token)
        write_token_state_outputs(self.cfg, frame, generated_at_utc=to_iso(utc_now()))
        self._dirty_state = False
        self._last_state_flush_monotonic = now


async def stream_market_data(
    cfg: PegConfig,
    *,
    asset_ids: Iterable[str] | None = None,
    market_limit: int | None = None,
    market_offset: int = 0,
    duration_sec: int = 60,
) -> StreamRunResult:
    targets = resolve_stream_targets(
        cfg,
        asset_ids=asset_ids,
        market_limit=market_limit,
        market_offset=market_offset,
    )
    manager = MarketStreamManager(cfg, targets)
    return await manager.run(duration_sec=duration_sec)


