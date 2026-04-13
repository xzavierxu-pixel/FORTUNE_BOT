import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.training.history_features import (
    LEVEL_DEFINITIONS,
    load_history_feature_artifacts,
    summarize_history_features,
    write_history_feature_artifacts,
)


class HistoryFeatureArtifactsTest(unittest.TestCase):
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
                    "snapshot_time": "2026-04-03T00:00:00Z",
                },
            ]
        )

        history_frames = summarize_history_features(snapshots)
        self.assertEqual(set(history_frames), set(LEVEL_DEFINITIONS))
        self.assertIn("global_expanding_logloss_mean", history_frames["global"].columns)
        self.assertIn("full_group_recent_50_bias_mean", history_frames["full_group"].columns)

        with TemporaryDirectory() as tmpdir:
            paths = {
                level_name: Path(tmpdir) / f"history_features_{level_name}.parquet"
                for level_name in LEVEL_DEFINITIONS
            }
            write_history_feature_artifacts(history_frames, paths)
            reloaded = load_history_feature_artifacts(paths)

        self.assertEqual(set(reloaded), set(LEVEL_DEFINITIONS))
        self.assertEqual(len(reloaded["global"]), 1)
        self.assertEqual(len(reloaded["full_group"]), 2)
        self.assertIn("domain_recent_200_brier_mean", reloaded["domain"].columns)
        self.assertIn("category_x_market_type_expanding_abs_bias_p90", reloaded["category_x_market_type"].columns)


if __name__ == "__main__":
    unittest.main()
