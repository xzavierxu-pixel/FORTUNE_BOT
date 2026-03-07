from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]


def main() -> None:
    command = [
        sys.executable,
        "rule_baseline/workflow/run_pipeline.py",
        "--artifact-mode",
        "online",
        "--skip-backtest",
        "--skip-baselines",
    ]
    subprocess.run(command, cwd=ROOT_DIR, check=True)


if __name__ == "__main__":
    main()
