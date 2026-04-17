from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.snapshots import prepare_rule_training_frame
from rule_baseline.history.history_features import summarize_history_features, write_history_feature_artifacts
from rule_baseline.workflow.pipeline_config import load_pipeline_runtime_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build standalone hierarchical history feature artifacts.")
    parser.add_argument("--pipeline-config", type=str, default=None)
    parser.add_argument("--min-price", type=float, default=0.2)
    parser.add_argument("--max-price", type=float, default=0.8)
    parser.add_argument("--price-bin-step", type=float, default=0.1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pipeline_config = load_pipeline_runtime_config(args.pipeline_config)
    artifact_paths = build_artifact_paths(pipeline_config.artifact_mode)
    df, _, _ = prepare_rule_training_frame(
        artifact_mode=pipeline_config.artifact_mode,
        max_rows=pipeline_config.max_rows,
        recent_days=pipeline_config.recent_days,
        split_reference_end=pipeline_config.split.split_reference_end,
        history_start_override=pipeline_config.split.history_start,
        split_config=pipeline_config.split,
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
