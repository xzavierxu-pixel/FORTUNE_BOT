from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.snapshots import prepare_rule_training_frame
from rule_baseline.training.history_features import summarize_history_features, write_history_feature_artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build standalone hierarchical history feature artifacts.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    parser.add_argument("--min-price", type=float, default=0.2)
    parser.add_argument("--max-price", type=float, default=0.8)
    parser.add_argument("--price-bin-step", type=float, default=0.1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    df, _, _ = prepare_rule_training_frame(
        artifact_mode=args.artifact_mode,
        max_rows=args.max_rows,
        recent_days=args.recent_days,
        split_reference_end=args.split_reference_end,
        history_start_override=args.history_start,
        min_price=args.min_price,
        max_price=args.max_price,
        price_bin_step=args.price_bin_step,
    )
    history_feature_frames = summarize_history_features(df)
    write_history_feature_artifacts(history_feature_frames, artifact_paths.history_feature_paths)
    for level_name, path in artifact_paths.history_feature_paths.items():
        rows = len(history_feature_frames[level_name])
        print(f"[INFO] Saved {rows} {level_name} history rows to {path}")


if __name__ == "__main__":
    main()
