from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import write_json
from rule_baseline.training.groupkey_reports import (
    build_runtime_report_markdown,
    build_runtime_report_payload,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GroupKey runtime coverage and bundle footprint report.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=2000)
    parser.add_argument("--recent-days", type=int, default=14)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    parser.add_argument("--unknown-group-preview-limit", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_rows = None if args.max_rows <= 0 else args.max_rows
    recent_days = None if args.recent_days <= 0 else args.recent_days

    payload = build_runtime_report_payload(
        artifact_mode=args.artifact_mode,
        max_rows=max_rows,
        recent_days=recent_days,
        split_reference_end=args.split_reference_end,
        history_start=args.history_start,
        unknown_group_preview_limit=args.unknown_group_preview_limit,
    )

    docs_dir = Path("polymarket_rule_engine/docs")
    docs_dir.mkdir(parents=True, exist_ok=True)
    json_path = docs_dir / "groupkey_runtime_report.json"
    markdown_path = docs_dir / "groupkey_runtime_report.md"

    write_json(json_path, payload)
    markdown_path.write_text(build_runtime_report_markdown(payload), encoding="utf-8")

    print(f"[INFO] Wrote runtime report json to {json_path}")
    print(f"[INFO] Wrote runtime report markdown to {markdown_path}")


if __name__ == "__main__":
    main()
