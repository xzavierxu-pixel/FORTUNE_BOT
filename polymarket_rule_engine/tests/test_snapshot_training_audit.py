import os
import sys
import unittest
from tempfile import TemporaryDirectory
from pathlib import Path

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.training.snapshot_training_audit import (
    build_snapshot_training_audit_payload,
    write_snapshot_training_audit,
)


class SnapshotTrainingAuditTest(unittest.TestCase):
    def test_build_payload_tracks_funnel_and_artifacts(self) -> None:
        artifact_paths = build_artifact_paths("offline")
        snapshots_loaded = pd.DataFrame(
            [
                {"market_id": "m1", "snapshot_time": "2026-01-01T00:00:00Z", "domain": "a", "category": "SPORTS", "market_type": "other"},
                {"market_id": "m2", "snapshot_time": "2026-01-01T01:00:00Z", "domain": "b", "category": "POLITICS", "market_type": "other"},
            ]
        )
        snapshots_quality = snapshots_loaded.iloc[[0]].copy()
        snapshots_assigned = snapshots_quality.copy()
        df_feat = snapshots_quality.assign(dataset_split="train", y=1, price=0.5)
        rules_df = pd.DataFrame([{"group_key": "a|SPORTS|other"}])

        with TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "naive_all_leaves_report.csv"
            pd.DataFrame(
                [
                    {"group_key": "a|SPORTS|other", "selection_status": "keep"},
                    {"group_key": "b|POLITICS|other", "selection_status": "drop"},
                ]
            ).to_csv(report_path, index=False)
            local_paths = artifact_paths
            object.__setattr__(local_paths, "rule_report_path", report_path)
            object.__setattr__(local_paths, "snapshot_training_audit_json_path", Path(tmpdir) / "snapshot_training_funnel.json")
            object.__setattr__(local_paths, "snapshot_training_audit_markdown_path", Path(tmpdir) / "snapshot_training_funnel.md")

            payload = build_snapshot_training_audit_payload(
                artifact_paths=local_paths,
                snapshots_loaded=snapshots_loaded,
                snapshots_quality=snapshots_quality,
                snapshots_assigned=snapshots_assigned,
                df_feat=df_feat,
                feature_columns=["price"],
                rules_df=rules_df,
                sample_config={"random_sample_rows": 100000},
            )
            write_snapshot_training_audit(artifact_paths=local_paths, payload=payload)

            self.assertEqual(payload["matching_summary"]["matched_training_rows"], 1)
            self.assertEqual(payload["matching_summary"]["kept_but_unmatched_rows"], 0)
            self.assertEqual(payload["training_frame"]["model_feature_shape"], [1, 1])
            self.assertTrue(local_paths.snapshot_training_audit_json_path.exists())
            self.assertTrue(local_paths.snapshot_training_audit_markdown_path.exists())


if __name__ == "__main__":
    unittest.main()
