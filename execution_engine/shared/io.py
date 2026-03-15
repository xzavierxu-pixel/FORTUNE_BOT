"""File-based IO helpers for PEG."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Any, Callable, Dict, Iterable, List


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")


def read_jsonl_many(paths: Iterable[Path]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        rows.extend(read_jsonl(path))
    return rows


def list_run_artifact_paths(runs_root_dir: Path, filename: str) -> List[Path]:
    if not runs_root_dir.exists():
        return []
    return sorted(
        [
            path
            for path in runs_root_dir.glob(f"*/*/{filename}")
            if path.is_file()
        ]
    )


def list_artifact_paths_recursive(root_dir: Path, filename: str) -> List[Path]:
    if not root_dir.exists():
        return []
    return sorted([path for path in root_dir.rglob(filename) if path.is_file()])


def index_by(rows: Iterable[Dict[str, Any]], key_fn: Callable[[Dict[str, Any]], str]) -> Dict[str, Dict[str, Any]]:
    indexed: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        indexed[key_fn(row)] = row
    return indexed
