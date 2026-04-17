import os
import shutil
import sys
import unittest
from pathlib import Path
from uuid import uuid4

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.history.history_features import (
    LEVEL_DEFINITIONS,
    load_history_feature_artifacts,
    summarize_history_features,
    validate_materialized_history_artifacts,
    write_history_feature_artifacts,
)


class HistoryFeatureArtifactsTest(unittest.TestCase):
    def _make_tempdir(self) -> Path:
        path = Path("polymarket_rule_engine/tests/_tmp_runtime") / f"history-{uuid4().hex}"
        path.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

    def test_history_features_can_be_persisted_and_reloaded_per_level(self) -> None:
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "price": 0.40,
                    "y": 1,
                    "horizon_hours": 6,
                    "closedTime": "2025-12-01T00:00:00Z",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "m2",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "price": 0.60,
                    "y": 0,
                    "horizon_hours": 12,
                    "closedTime": "2026-04-02T00:00:00Z",
                    "snapshot_time": "2026-04-02T00:00:00Z",
                },
                {
                    "market_id": "m3",
                    "domain": "other.com",
                    "category": "POLITICS",
                    "market_type": "other",
                    "price": 0.55,
                    "y": 1,
                    "horizon_hours": 24,
                    "closedTime": "2026-04-03T00:00:00Z",
                    "snapshot_time": "2026-04-03T00:00:00Z",
                },
            ]
        )

        history_frames = summarize_history_features(snapshots)
        self.assertEqual(set(history_frames), set(LEVEL_DEFINITIONS))
        self.assertIn("global_expanding_logloss_mean", history_frames["global"].columns)
        self.assertIn("global_expanding_bias_p50", history_frames["global"].columns)
        self.assertNotIn("global_expanding_logloss_max", history_frames["global"].columns)
        self.assertNotIn("global_expanding_bias_min", history_frames["global"].columns)
        self.assertIn("full_group_recent_90days_bias_mean", history_frames["full_group"].columns)
        example_group = history_frames["full_group"][history_frames["full_group"]["level_key"] == "example.com|SPORTS|moneyline"].iloc[0]
        self.assertEqual(float(example_group["full_group_recent_90days_snapshot_count"]), 1.0)

        tmpdir = self._make_tempdir()
        with self.subTest("persist_and_reload"):
            paths = {
                level_name: tmpdir / f"history_features_{level_name}.parquet"
                for level_name in LEVEL_DEFINITIONS
            }
            write_history_feature_artifacts(history_frames, paths)
            reloaded = load_history_feature_artifacts(paths)

        self.assertEqual(set(reloaded), set(LEVEL_DEFINITIONS))
        self.assertEqual(len(reloaded["global"]), 1)
        self.assertEqual(len(reloaded["full_group"]), 2)
        self.assertIn("domain_recent_90days_brier_mean", reloaded["domain"].columns)
        self.assertIn("domain_recent_90days_bias_p50", reloaded["domain"].columns)
        self.assertIn("category_x_market_type_expanding_abs_bias_p90", reloaded["category_x_market_type"].columns)
        self.assertNotIn("category_x_market_type_expanding_abs_bias_max", reloaded["category_x_market_type"].columns)

    def test_validate_materialized_history_artifacts_fails_when_any_level_is_missing(self) -> None:
        tmpdir = self._make_tempdir()
        with self.subTest("missing_level_validation"):
            paths = {
                level_name: tmpdir / f"history_features_{level_name}.parquet"
                for level_name in LEVEL_DEFINITIONS
            }
            for level_name, path in paths.items():
                if level_name == "market_type":
                    continue
                pd.DataFrame([{"level_key": level_name, "value": 1.0}]).to_parquet(path, index=False)

            with self.assertRaisesRegex(FileNotFoundError, "market_type"):
                validate_materialized_history_artifacts(paths)


if __name__ == "__main__":
    unittest.main()
