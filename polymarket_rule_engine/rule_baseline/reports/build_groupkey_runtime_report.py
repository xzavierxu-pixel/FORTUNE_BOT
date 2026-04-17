from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.reports.groupkey_reports import (
    build_runtime_report_markdown,
    build_runtime_report_payload,
)
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GroupKey runtime coverage and bundle footprint report.")
    parser.add_argument("--unknown-group-preview-limit", type=int, default=20)
    parser.add_argument("--pipeline-config", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    max_rows = pipeline_config.max_rows
    recent_days = pipeline_config.recent_days

    payload = build_runtime_report_payload(
        artifact_mode=pipeline_config.artifact_mode,
        max_rows=max_rows,
        recent_days=recent_days,
        split_reference_end=pipeline_config.split.split_reference_end,
        history_start=pipeline_config.split.history_start,
        unknown_group_preview_limit=args.unknown_group_preview_limit,
        split_config=pipeline_config.split,
    )
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)
    report_dir = artifact_paths.docs_groupkey_reports_dir
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = artifact_paths.groupkey_runtime_report_json_path
    markdown_path = artifact_paths.groupkey_runtime_report_markdown_path

    write_json(json_path, payload)
    markdown_path.write_text(build_runtime_report_markdown(payload), encoding="utf-8")

    print(f"[INFO] Wrote runtime report json to {json_path}")
    print(f"[INFO] Wrote runtime report markdown to {markdown_path}")


if __name__ == "__main__":
    main()
