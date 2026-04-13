from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.training.groupkey_reports import write_groupkey_reports


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GroupKey migration and consistency markdown reports.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outputs = write_groupkey_reports(args.artifact_mode)
    for name, path in outputs.items():
        print(f"[INFO] Wrote {name} report to {path}")


if __name__ == "__main__":
    main()
