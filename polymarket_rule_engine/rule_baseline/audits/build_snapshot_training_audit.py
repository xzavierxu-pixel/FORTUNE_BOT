from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged
from rule_baseline.datasets.snapshots import load_online_parity_snapshots, load_raw_markets
from rule_baseline.datasets.splits import assign_dataset_split, compute_artifact_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache, preprocess_features
from rule_baseline.features.annotation_normalization import build_normalization_manifest, normalize_market_annotations
from rule_baseline.features.snapshot_semantics import online_feature_columns
from rule_baseline.audits.snapshot_training_audit import (
    build_snapshot_training_audit_payload,
    write_snapshot_training_audit,
)
from rule_baseline.training.train_snapshot_model import (
    DROP_COLS,
    TRAIN_PRICE_MAX,
    TRAIN_PRICE_MIN,
    add_training_targets,
    load_rules,
    load_serving_feature_bundle,
    match_snapshots_to_rules,
)
from rule_baseline.utils import config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build snapshot training funnel audit artifacts.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    parser.add_argument("--split-reference-end", type=str, default=None)
    parser.add_argument("--history-start", type=str, default=None)
    parser.add_argument("--random-sample-rows", type=int, default=None)
    parser.add_argument("--random-sample-seed", type=int, default=21)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)

    rebuild_canonical_merged()
    snapshots_loaded = load_online_parity_snapshots(
        min_price=TRAIN_PRICE_MIN,
        max_price=TRAIN_PRICE_MAX,
        max_rows=args.max_rows,
        recent_days=args.recent_days,
    )
    snapshots_quality = snapshots_loaded[snapshots_loaded["quality_pass"]].copy()
    if args.random_sample_rows is not None and args.random_sample_rows > 0 and len(snapshots_quality) > args.random_sample_rows:
        snapshots_quality = snapshots_quality.sample(args.random_sample_rows, random_state=args.random_sample_seed).copy()
    split = compute_artifact_split(
        snapshots_quality,
        artifact_mode=args.artifact_mode,
        reference_end=args.split_reference_end,
        history_start_override=args.history_start,
    )
    snapshots_assigned = assign_dataset_split(snapshots_quality, split)
    allowed_splits = ["train", "valid", "test"] if args.artifact_mode == "offline" else ["train", "valid"]
    snapshots_assigned = snapshots_assigned[snapshots_assigned["dataset_split"].isin(allowed_splits)].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations_raw = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    normalization_manifest = build_normalization_manifest(market_annotations_raw)
    market_annotations = normalize_market_annotations(
        market_annotations_raw,
        vocabulary_manifest=normalization_manifest,
    )
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules_df = load_rules(artifact_paths.rules_path)
    matched = match_snapshots_to_rules(snapshots_assigned, market_annotations, rules_df)
    serving_feature_bundle = load_serving_feature_bundle(artifact_paths)
    if serving_feature_bundle is not None and not matched.empty:
        from rule_baseline.features.serving import attach_serving_features

        matched = attach_serving_features(
            matched,
            serving_feature_bundle,
            price_column="price",
            horizon_column="horizon_hours",
        )
    df_feat = preprocess_features(matched, market_feature_cache) if not matched.empty else matched
    if not df_feat.empty:
        df_feat = add_training_targets(df_feat)
    feature_columns = online_feature_columns([column for column in df_feat.columns if column not in DROP_COLS]) if not df_feat.empty else []
    payload = build_snapshot_training_audit_payload(
        artifact_paths=artifact_paths,
        snapshots_loaded=snapshots_loaded,
        snapshots_quality=snapshots_quality,
        snapshots_assigned=snapshots_assigned,
        df_feat=df_feat,
        feature_columns=feature_columns,
        rules_df=rules_df,
        sample_config={
            "max_rows": args.max_rows,
            "recent_days": args.recent_days,
            "random_sample_rows": args.random_sample_rows,
            "random_sample_seed": args.random_sample_seed,
            "split_reference_end": args.split_reference_end,
            "history_start": args.history_start,
        },
    )
    write_snapshot_training_audit(artifact_paths=artifact_paths, payload=payload)
    print(f"[INFO] Wrote snapshot training audit json to {artifact_paths.snapshot_training_audit_json_path}")
    print(f"[INFO] Wrote snapshot training audit markdown to {artifact_paths.snapshot_training_audit_markdown_path}")


if __name__ == "__main__":
    main()
