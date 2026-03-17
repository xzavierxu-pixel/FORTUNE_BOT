"""Optional noncritical reporting for the online submit pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import json

from execution_engine.runtime.config import PegConfig


class DeferredWriter:
    def __init__(self, cfg: PegConfig) -> None:
        self.cfg = cfg
        self.enabled = bool(cfg.online_deferred_artifacts_enabled)

    def write_report(self, payload: Dict[str, Any]) -> Path | None:
        if not self.enabled:
            return None
        path = self.cfg.run_deferred_reports_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        return path
