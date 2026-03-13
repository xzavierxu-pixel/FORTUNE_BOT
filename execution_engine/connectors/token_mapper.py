"""Map condition_id + outcome_index to CLOB token_id using Gamma API."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
import json
import time

from .gamma_provider import GammaMarketProvider


@dataclass
class TokenCacheEntry:
    clob_token_ids: List[str]
    updated_at: float


class TokenMapper:
    def __init__(self, base_url: str, cache_path: Path, ttl_sec: int, timeout_sec: int) -> None:
        self.gamma = GammaMarketProvider(base_url, timeout_sec=timeout_sec)
        self.cache_path = cache_path
        self.ttl_sec = ttl_sec
        self._cache: Dict[str, TokenCacheEntry] = {}
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        if not self.cache_path.exists():
            self._cache = {}
            self._loaded = True
            return
        with self.cache_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        cache: Dict[str, TokenCacheEntry] = {}
        for condition_id, entry in raw.items():
            cache[condition_id] = TokenCacheEntry(
                clob_token_ids=[str(x) for x in entry.get("clob_token_ids", [])],
                updated_at=float(entry.get("updated_at", 0.0)),
            )
        self._cache = cache
        self._loaded = True

    def _save(self) -> None:
        payload = {
            key: {
                "clob_token_ids": entry.clob_token_ids,
                "updated_at": entry.updated_at,
            }
            for key, entry in self._cache.items()
        }
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with self.cache_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def _is_fresh(self, entry: TokenCacheEntry) -> bool:
        if entry.updated_at <= 0:
            return False
        if self.ttl_sec <= 0:
            return True
        return (time.time() - entry.updated_at) <= self.ttl_sec

    def get_token_id(self, condition_id: str, outcome_index: int) -> str:
        self._load()
        entry = self._cache.get(condition_id)
        if entry is None or not self._is_fresh(entry):
            markets = self.gamma.fetch_markets_by_condition(condition_id)
            if not markets:
                raise KeyError(f"Gamma market not found for condition_id={condition_id}")
            market = markets[0]
            clob_tokens = market.get("clobTokenIds") or market.get("clob_token_ids") or []
            entry = TokenCacheEntry(
                clob_token_ids=[str(x) for x in clob_tokens],
                updated_at=time.time(),
            )
            self._cache[condition_id] = entry
            self._save()

        if outcome_index < 0 or outcome_index >= len(entry.clob_token_ids):
            raise KeyError(
                f"Outcome index {outcome_index} not in clobTokenIds for condition_id={condition_id}"
            )
        return entry.clob_token_ids[outcome_index]
