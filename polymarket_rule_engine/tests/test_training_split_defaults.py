import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.splits import select_preferred_split
from rule_baseline.workflow.pipeline_config import default_pipeline_config_path, resolve_pipeline_config


class TrainingSplitDefaultsTest(unittest.TestCase):
    def test_pipeline_resolves_offline_three_way_split(self) -> None:
        pipeline_config = resolve_pipeline_config(
            artifact_mode="offline",
            max_rows=None,
            recent_days=None,
            history_start="2026-01-01T00:00:00Z",
            split_reference_end="2026-04-16T00:00:00Z",
            offline_validation_days=30,
            offline_test_days=30,
            train_sample_rows=None,
        )

        self.assertEqual(pipeline_config.split.train_start, "2026-01-01T00:00:00+00:00")
        self.assertEqual(pipeline_config.split.train_end, "2026-02-14T23:59:59+00:00")
        self.assertEqual(pipeline_config.split.valid_start, "2026-02-15T00:00:00+00:00")
        self.assertEqual(pipeline_config.split.valid_end, "2026-03-16T23:59:59+00:00")
        self.assertEqual(pipeline_config.split.test_start, "2026-03-17T00:00:00+00:00")
        self.assertEqual(pipeline_config.split.test_end, "2026-04-16T00:00:00+00:00")
        self.assertEqual(pipeline_config.split.allowed_splits, ("train", "valid", "test"))
        self.assertEqual(pipeline_config.publish.prediction_publish_split, "test")

    def test_pipeline_resolves_online_full_train_config(self) -> None:
        pipeline_config = resolve_pipeline_config(
            artifact_mode="online",
            max_rows=None,
            recent_days=None,
            history_start="2026-01-01T00:00:00Z",
            split_reference_end="2026-04-16T00:00:00Z",
            train_sample_rows=None,
        )

        self.assertEqual(pipeline_config.split.train_start, "2026-01-01T00:00:00+00:00")
        self.assertEqual(pipeline_config.split.train_end, "2026-04-16T00:00:00+00:00")
        self.assertIsNone(pipeline_config.split.valid_start)
        self.assertIsNone(pipeline_config.split.valid_end)
        self.assertIsNone(pipeline_config.split.test_start)
        self.assertIsNone(pipeline_config.split.test_end)
        self.assertEqual(pipeline_config.split.allowed_splits, ("train",))
        self.assertEqual(pipeline_config.publish.prediction_publish_split, "train")

    def test_pipeline_sampling_has_no_default_cap(self) -> None:
        pipeline_config = resolve_pipeline_config(
            artifact_mode="offline",
            max_rows=None,
            recent_days=None,
            history_start="2026-01-01T00:00:00Z",
            split_reference_end="2026-04-16T00:00:00Z",
            train_sample_rows=None,
            train_sample_seed=21,
        )

        self.assertIsNone(pipeline_config.sampling.train_sample_rows)
        self.assertEqual(pipeline_config.sampling.train_sample_seed, 21)
        self.assertEqual(pipeline_config.sampling.train_sample_scope, "train_only")

    def test_select_preferred_split_prefers_test_then_valid(self) -> None:
        df = pd.DataFrame(
            [
                {"dataset_split": "train", "value": 1},
                {"dataset_split": "valid", "value": 2},
                {"dataset_split": "test", "value": 3},
            ]
        )

        split_name, selected = select_preferred_split(df)

        self.assertEqual(split_name, "test")
        self.assertEqual(selected["value"].tolist(), [3])

    def test_default_pipeline_config_path_uses_offline_audit_dir(self) -> None:
        path = default_pipeline_config_path("offline")
        self.assertTrue(str(path).endswith(os.path.join("data", "offline", "audit", "pipeline_runtime_config.json")))


if __name__ == "__main__":
    unittest.main()
