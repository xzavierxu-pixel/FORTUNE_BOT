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
            "grouped_isotonic",
            "global_isotonic",
            "grouped_sigmoid",
            "global_sigmoid",
            "none",
        ],
        default="global_isotonic",
    )
    parser.add_argument("--grouped-calibration-column", type=str, default="horizon_hours")
    parser.add_argument("--grouped-calibration-min-rows", type=int, default=20)
    parser.add_argument("--random-seed", type=int, default=21)
    parser.add_argument("--predictor-time-limit", type=int, default=300)
    parser.add_argument("--num-bag-folds", type=int, default=None)
    parser.add_argument("--num-bag-sets", type=int, default=None)
    parser.add_argument("--num-stack-levels", type=int, default=None)
    parser.add_argument("--auto-stack", dest="auto_stack", action="store_true")
    parser.add_argument("--no-auto-stack", dest="auto_stack", action="store_false")
    parser.set_defaults(auto_stack=None)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--walk-forward-windows", type=int, default=3)
    parser.add_argument("--walk-forward-step-days", type=int, default=None)
    parser.add_argument("--date-start", type=str, default=None)
    parser.add_argument("--date-end", type=str, default=None)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--skip-fetch", action="store_true")
    parser.add_argument("--skip-annotations", action="store_true")
    parser.add_argument("--skip-snapshots", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    parser.add_argument("--skip-backtest", action="store_true")
    parser.add_argument("--skip-baselines", action="store_true")
    parser.add_argument("--full-refresh-fetch", action="store_true")
    parser.add_argument("--full-refresh-snapshots", action="store_true")
    return parser.parse_args()


def add_common_args(command: list[str], args: argparse.Namespace) -> list[str]:
    if args.artifact_mode:
        command.extend(["--artifact-mode", args.artifact_mode])
    if args.max_rows is not None:
        command.extend(["--max-rows", str(args.max_rows)])
    if args.recent_days is not None:
        command.extend(["--recent-days", str(args.recent_days)])
    if args.split_reference_end is not None:
        command.extend(["--split-reference-end", args.split_reference_end])
    if args.date_start is not None:
        command.extend(["--history-start", args.date_start])
    return command


def add_data_args(command: list[str], args: argparse.Namespace) -> list[str]:
    if args.artifact_mode:
        command.extend(["--artifact-mode", args.artifact_mode])
    if args.max_rows is not None:
        command.extend(["--max-rows", str(args.max_rows)])
    if args.recent_days is not None:
        command.extend(["--recent-days", str(args.recent_days)])
    return command


def add_mode_only_args(command: list[str], args: argparse.Namespace) -> list[str]:
    if args.artifact_mode:
        command.extend(["--artifact-mode", args.artifact_mode])
    return command


def add_analysis_args(command: list[str], args: argparse.Namespace) -> list[str]:
    command = add_mode_only_args(command, args)
    if args.split_reference_end is not None:
        command.extend(["--split-reference-end", args.split_reference_end])
    if args.date_start is not None:
        command.extend(["--history-start", args.date_start])
    return command


def run_step(label: str, command: list[str]) -> None:
    print(f"[PIPELINE] {label}: {' '.join(command)}")
    subprocess.run(command, cwd=ROOT_DIR, check=True)


def main() -> None:
    args = parse_args()

    if not args.skip_fetch:
        fetch_cmd = [sys.executable, "rule_baseline/data_collection/fetch_raw_events.py"]
        if args.full_refresh_fetch:
            fetch_cmd.append("--full-refresh")
        if args.date_start is not None:
            fetch_cmd.extend(["--date-start", args.date_start])
        if args.date_end is not None:
            fetch_cmd.extend(["--date-end", args.date_end])
        run_step("Fetch raw markets", fetch_cmd)

    if not args.skip_annotations:
        annotation_cmd = [sys.executable, "rule_baseline/domain_extractor/build_market_annotations.py"]
        run_step("Build market annotations", annotation_cmd)

    if not args.skip_snapshots:
        snapshot_cmd = [sys.executable, "rule_baseline/data_collection/build_snapshots.py"]
        if args.full_refresh_snapshots:
            snapshot_cmd.append("--full-refresh")
        run_step("Build snapshots", snapshot_cmd)

    train_rules_cmd = add_common_args(
        [sys.executable, "rule_baseline/training/train_rules_naive_output_rule.py"],
        args,
    )
    run_step("Train rules", train_rules_cmd)

    export_features_cmd = add_common_args(
        [
            sys.executable,
            "rule_baseline/training/export_features.py",
            "--calibration-mode",
            args.calibration_mode,
            "--grouped-calibration-column",
            args.grouped_calibration_column,
            "--grouped-calibration-min-rows",
            str(args.grouped_calibration_min_rows),
            "--random-seed",
            str(args.random_seed),
            "--predictor-time-limit",
            str(args.predictor_time_limit),
            "--target-mode",
            args.target_mode,
        ],
        args,
    )
    run_step("Export features", export_features_cmd)

    train_model_cmd = add_common_args(
        [
            sys.executable,
            "rule_baseline/training/train_snapshot_model.py",
            "--calibration-mode",
            args.calibration_mode,
            "--grouped-calibration-column",
            args.grouped_calibration_column,
            "--grouped-calibration-min-rows",
            str(args.grouped_calibration_min_rows),
            "--random-seed",
            str(args.random_seed),
            "--predictor-time-limit",
            str(args.predictor_time_limit),
            "--target-mode",
            args.target_mode,
        ],
        args,
    )
    if args.num_bag_folds is not None:
        train_model_cmd.extend(["--num-bag-folds", str(args.num_bag_folds)])
    if args.num_bag_sets is not None:
        train_model_cmd.extend(["--num-bag-sets", str(args.num_bag_sets)])
    if args.num_stack_levels is not None:
        train_model_cmd.extend(["--num-stack-levels", str(args.num_stack_levels)])
    if args.auto_stack is True:
        train_model_cmd.append("--auto-stack")
    elif args.auto_stack is False:
        train_model_cmd.append("--no-auto-stack")
    run_step("Train model", train_model_cmd)

    validation_reports_cmd = add_mode_only_args(
        [sys.executable, "rule_baseline/training/build_groupkey_validation_reports.py"],
        args,
    )
    run_step("Build GroupKey validation reports", validation_reports_cmd)

    if not args.skip_analysis:
        analysis_steps = [
            ("Analyze calibration", "rule_baseline/analysis/analyze_q_model_calibration.py", add_mode_only_args),
            ("Analyze alpha", "rule_baseline/analysis/analyze_alpha_quadrant.py", add_mode_only_args),
            ("Analyze rules alpha", "rule_baseline/analysis/analyze_rules_alpha_quadrant.py", add_analysis_args),
        ]
        for label, script_path, arg_builder in analysis_steps:
            command = arg_builder([sys.executable, script_path], args)
            run_step(label, command)

    if args.artifact_mode == "offline" and not args.skip_backtest:
        command = add_common_args([sys.executable, "rule_baseline/backtesting/backtest_execution_parity.py"], args)
        run_step("Backtest execution parity", command)

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
