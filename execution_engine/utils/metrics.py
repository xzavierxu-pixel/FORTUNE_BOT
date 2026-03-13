"""Metrics helpers for PEG."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Dict


def load_metrics(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_metrics(path: Path, metrics: Dict[str, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, ensure_ascii=True, indent=2)


def increment_metric(path: Path, key: str, value: float = 1.0) -> None:
    metrics = load_metrics(path)
    metrics[key] = float(metrics.get(key, 0.0)) + value
    save_metrics(path, metrics)
