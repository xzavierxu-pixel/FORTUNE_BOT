"""File-based nonce manager (single-process)."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Optional


class NonceManager:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._nonce: Optional[int] = None

    def _load(self) -> None:
        if not self.path.exists():
            self._nonce = 0
            return
        with self.path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        self._nonce = int(data.get("nonce", 0))

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump({"nonce": self._nonce}, handle)

    def peek(self) -> int:
        if self._nonce is None:
            self._load()
        return int(self._nonce or 0)

    def next_nonce(self) -> int:
        if self._nonce is None:
            self._load()
        self._nonce = int(self._nonce or 0) + 1
        self._save()
        return int(self._nonce)
