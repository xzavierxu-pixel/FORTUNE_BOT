from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the end-to-end Polymarket research or online pipeline.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--target-mode", choices=["q", "residual_q", "expected_pnl", "expected_roi"], default="q")
    parser.add_argument(
        "--calibration-mode",
        choices=[
            "valid_isotonic",
            "valid_sigmoid",
            "domain_valid_isotonic",
            "horizon_valid_isotonic",
            "cv_isotonic",
            "cv_sigmoid",
            "none",
        ],
        default="valid_isotonic",
    )
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--walk-forward-windows", type=int, default=3)
    parser.add_argument("--walk-forward-step-days", type=int, default=None)
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-snapshots", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--skip-backtest", action="store_true")
    parser.add_argument("--skip-baselines", action="store_true")
    return parser.parse_args()


def add_common_args(command: list[str], args: argparse.Namespace) -> list[str]:
    if args.artifact_mode:
        command.extend(["--artifact-mode", args.artifact_mode])
    if args.max_rows is not None:
        command.extend(["--max-rows", str(args.max_rows)])
    if args.recent_days is not None:
        command.extend(["--recent-days", str(args.recent_days)])
    return command


def run_step(label: str, command: list[str]) -> None:
    print(f"[PIPELINE] {label}: {' '.join(command)}")
    subprocess.run(command, cwd=ROOT_DIR, check=True)


def main() -> None:
    args = parse_args()

    if not args.skip_fetch:
        run_step("Fetch raw markets", [sys.executable, "rule_baseline/data_collection/fetch_raw_events.py"])

    if not args.skip_snapshots:
        run_step("Build snapshots", [sys.executable, "rule_baseline/data_collection/build_snapshots.py"])

    train_rules_cmd = add_common_args(
        [sys.executable, "rule_baseline/training/train_rules_naive_output_rule.py"],
        args,
    )
    run_step("Train rules", train_rules_cmd)

    train_model_cmd = add_common_args(
        [
            sys.executable,
            "rule_baseline/training/train_snapshot_model.py",
            "--calibration-mode",
            args.calibration_mode,
            "--target-mode",
            args.target_mode,
        ],
        args,
    )
    run_step("Train model", train_model_cmd)

    if not args.skip_analysis:
        for label, script_path in [
            ("Analyze calibration", "rule_baseline/analysis/analyze_q_model_calibration.py"),
            ("Analyze alpha", "rule_baseline/analysis/analyze_alpha_quadrant.py"),
            ("Analyze rules alpha", "rule_baseline/analysis/analyze_rules_alpha_quadrant.py"),
        ]:
            command = add_common_args([sys.executable, script_path], args)
            run_step(label, command)

    if args.artifact_mode == "offline" and not args.skip_backtest:
        command = add_common_args([sys.executable, "rule_baseline/backtesting/backtest_portfolio_qmodel.py"], args)
        run_step("Backtest q-model", command)

    if args.artifact_mode == "offline" and not args.skip_baselines:
        baseline_cmd = add_common_args(
            [
                sys.executable,
                "rule_baseline/analysis/compare_baseline_families.py",
                "--walk-forward-windows",
                str(args.walk_forward_windows),
            ],
            args,
        )
        if args.walk_forward_step_days is not None:
            baseline_cmd.extend(["--walk-forward-step-days", str(args.walk_forward_step_days)])
        run_step("Compare baselines", baseline_cmd)


if __name__ == "__main__":
    main()
