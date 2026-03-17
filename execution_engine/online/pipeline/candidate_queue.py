"""Direct micro-batch assembly for current-pass candidates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass(frozen=True)
class CandidateBatch:
    batch_id: str
    frame: pd.DataFrame


class DirectCandidateQueue:
    def __init__(self, batch_size: int) -> None:
        self.batch_size = max(int(batch_size), 1)
        self._pending: List[dict] = []
        self._batch_index = 0

    def add_frame(self, frame: pd.DataFrame) -> List[CandidateBatch]:
        emitted: List[CandidateBatch] = []
        if frame.empty:
            return emitted
        for row in frame.to_dict(orient="records"):
            self._pending.append(row)
            if len(self._pending) >= self.batch_size:
                emitted.append(self._emit())
        return emitted

    def flush(self) -> CandidateBatch | None:
        if not self._pending:
            return None
        return self._emit()

    def _emit(self) -> CandidateBatch:
        self._batch_index += 1
        rows = self._pending[: self.batch_size]
        self._pending = self._pending[self.batch_size :]
        frame = pd.DataFrame(rows).reset_index(drop=True)
        batch_id = f"batch_{self._batch_index:03d}"
        if not frame.empty:
            frame["batch_id"] = batch_id
        return CandidateBatch(batch_id=batch_id, frame=frame)
