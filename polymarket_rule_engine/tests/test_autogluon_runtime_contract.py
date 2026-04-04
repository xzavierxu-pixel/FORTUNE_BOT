import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.training.train_rules_naive_output_rule import evaluate_rule_candidate


class UnifiedRuleLogicTest(unittest.TestCase):
    def test_online_rule_selection_uses_same_full_history_semantics_as_offline(self) -> None:
        row = pd.Series(
            {
                "domain": "example.com",
                "category": "SPORTS",
                "market_type": "moneyline",
                "price_bin": "0.40-0.50",
                "horizon_bin": "1-2h",
                "n_all": 30,
                "wins_all": 24,
                "p_mean_all": 0.45,
                "edge_std_mean_all": 0.2,
                "n_train": 5,
                "wins_train": 4,
                "p_mean_train": 0.45,
                "edge_std_mean_train": 0.2,
                "n_valid": 4,
                "wins_valid": 3,
                "p_mean_valid": 0.45,
                "edge_std_mean_valid": 0.2,
            }
        )

        offline_rule, offline_status = evaluate_rule_candidate(row, "offline")
        online_rule, online_status = evaluate_rule_candidate(row, "online")

        self.assertEqual(offline_status, "selected")
        self.assertEqual(online_status, "selected")
        self.assertEqual(offline_rule["leaf_id"], online_rule["leaf_id"])
        self.assertEqual(offline_rule["direction"], online_rule["direction"])
        self.assertAlmostEqual(float(offline_rule["q_full"]), 0.8, places=6)
        self.assertAlmostEqual(float(offline_rule["q_full"]), float(online_rule["q_full"]), places=6)
        self.assertAlmostEqual(
            float(offline_rule["edge_lower_bound_full"]),
            float(online_rule["edge_lower_bound_full"]),
            places=6,
        )


class ArtifactBundlePathTest(unittest.TestCase):
    def test_model_path_points_to_runtime_bundle(self) -> None:
        paths = build_artifact_paths("offline")
        self.assertEqual(paths.model_path.name, "q_model_bundle")
        self.assertEqual(paths.model_bundle_dir, paths.model_path)
        self.assertEqual(paths.legacy_model_path.name, "ensemble_snapshot_q.pkl")


if __name__ == "__main__":
    unittest.main()
