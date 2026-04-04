import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.models.autogluon_qmodel import fit_autogluon_q_model
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


class AutoGluonMetadataContractTest(unittest.TestCase):
    def test_runtime_manifest_records_seed_and_bagging_controls(self) -> None:
        df_train = pd.DataFrame(
            {
                "price": [0.3, 0.7, 0.35, 0.65],
                "domain": ["a", "b", "a", "b"],
                "horizon_hours": [1, 1, 2, 2],
                "y": [0, 1, 0, 1],
            }
        )
        df_valid = pd.DataFrame(
            {
                "price": [0.4, 0.6],
                "domain": ["a", "b"],
                "horizon_hours": [1, 2],
                "y": [0, 1],
            }
        )

        class FakePredictor:
            model_best = "WeightedEnsemble_L2"

            def __init__(self, **kwargs):
                self.kwargs = kwargs

            def fit(self, **kwargs):
                self.fit_kwargs = kwargs
                return self

            def predict_proba(self, data, as_pandas=False, as_multiclass=False):
                return [0.2] * len(data)

        with TemporaryDirectory() as tmpdir:
            with unittest.mock.patch(
                "rule_baseline.models.autogluon_qmodel._load_tabular_predictor_class",
                return_value=FakePredictor,
            ):
                result = fit_autogluon_q_model(
                    df_train=df_train,
                    df_valid=df_valid,
                    feature_columns=["price", "domain", "horizon_hours"],
                    bundle_dir=Path(tmpdir) / "bundle",
                    artifact_mode="offline",
                    split_boundaries={},
                    random_seed=314,
                    num_bag_folds=5,
                    num_bag_sets=2,
                    num_stack_levels=1,
                    auto_stack=False,
                )

        self.assertEqual(result.runtime_manifest["random_seed"], 314)
        self.assertEqual(result.runtime_manifest["num_bag_folds"], 5)
        self.assertEqual(result.runtime_manifest["num_bag_sets"], 2)
        self.assertEqual(result.runtime_manifest["num_stack_levels"], 1)
        self.assertFalse(result.runtime_manifest["auto_stack"])


if __name__ == "__main__":
    unittest.main()
