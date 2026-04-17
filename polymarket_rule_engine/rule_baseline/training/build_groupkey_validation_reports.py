from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.reports.groupkey_reports import write_groupkey_reports
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GroupKey migration and consistency markdown reports.")
    parser.add_argument("--pipeline-config", type=str, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    outputs = write_groupkey_reports(pipeline_config.artifact_mode)
    for name, path in outputs.items():
        print(f"[INFO] Wrote {name} report to {path}")


if __name__ == "__main__":
    main()
