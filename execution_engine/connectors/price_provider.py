"""Mid price providers."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Optional, Tuple

from ..execution.clob_client import ClobClient


class FileMidPriceProvider:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._cache = None

    def _load(self) -> None:
        if not self.path.exists():
            self._cache = {}
            return
        with self.path.open("r", encoding="utf-8") as handle:
            self._cache = json.load(handle)

    def get(self, market_id: str) -> Tuple[float, Optional[float], Optional[float]]:
        if self._cache is None:
            self._load()
        entry = self._cache.get(str(market_id), {}) if self._cache is not None else {}
        if not entry:
            raise KeyError(f"mid price not found for market_id={market_id}")
        mid = float(entry.get("mid"))
        spread = entry.get("spread")
        depth = entry.get("depth_usdc")
        return mid, (float(spread) if spread is not None else None), (float(depth) if depth is not None else None)


def _extract_price(entry: Any) -> Optional[float]:
    if entry is None:
        return None
    if isinstance(entry, dict):
        value = entry.get("price") or entry.get("p")
        if value is None and "0" in entry:
            value = entry.get("0")
        return None if value is None else float(value)
    if isinstance(entry, (list, tuple)) and entry:
        return float(entry[0])
    try:
        return float(entry)
    except (TypeError, ValueError):
        return None


class ClobMidPriceProvider:
    def __init__(self, clob_client: ClobClient) -> None:
        self.clob_client = clob_client

    def get(self, token_id: str) -> Tuple[float, Optional[float], Optional[float]]:
        book = self.clob_client.get_order_book(token_id)
        bids = book.get("bids") if isinstance(book, dict) else None
        asks = book.get("asks") if isinstance(book, dict) else None

        best_bid = _extract_price(bids[0]) if bids else None
        best_ask = _extract_price(asks[0]) if asks else None

        mid = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2.0
        else:
            mid = self.clob_client.get_midpoint(token_id)

        if mid is None:
            raise KeyError(f"midpoint not available for token_id={token_id}")

        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
        return float(mid), (float(spread) if spread is not None else None), None
