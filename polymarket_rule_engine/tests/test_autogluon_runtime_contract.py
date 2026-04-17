import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.features.annotation_normalization import build_normalization_manifest
from rule_baseline.models.autogluon_qmodel import _coerce_feature_frame, fit_autogluon_q_model
from rule_baseline.training.train_rules_naive_output_rule import evaluate_rule_candidate


class UnifiedRuleLogicTest(unittest.TestCase):
    def test_rule_candidate_uses_group_level_keep_status_and_exact_horizon_hour(self) -> None:
        row = pd.Series(
            {
                "domain": "example.com",
                "category": "SPORTS",
                "market_type": "moneyline",
                "group_key": "example.com|SPORTS|moneyline",
                "price_bin": "0.40-0.50",
                "horizon_hours": 2,
                "n_full": 30,
                "wins_full": 24,
                "p_full": 0.45,
                "edge_std_full_raw": 0.2,
                "group_unique_markets": 30,
                "group_snapshot_rows": 30,
                "global_total_unique_markets": 300,
                "global_total_snapshot_rows": 1800,
                "group_market_share_global": 0.1,
                "group_snapshot_share_global": 0.0166666667,
                "group_median_logloss": 0.62,
                "group_median_brier": 0.21,
                "global_group_logloss_q25": 0.31,
                "global_group_brier_q25": 0.09,
                "group_direction": 1,
                "selection_status": "keep",
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
        self.assertEqual(int(offline_rule["horizon_hours"]), 2)
        self.assertEqual((offline_rule["h_min"], offline_rule["h_max"]), (1.5, 3.0))


class ArtifactBundlePathTest(unittest.TestCase):
    def test_model_path_points_to_runtime_bundle(self) -> None:
        paths = build_artifact_paths("offline")
        self.assertEqual(paths.model_path.name, "q_model_bundle_deploy")
        self.assertEqual(paths.model_bundle_dir, paths.model_path)
        self.assertEqual(paths.full_model_bundle_dir.name, "q_model_bundle_full")
        self.assertEqual(paths.legacy_model_path.name, "ensemble_snapshot_q.pkl")
        self.assertIn("global", paths.history_feature_paths)
        self.assertIn("full_group", paths.history_feature_paths)
        self.assertEqual(paths.history_feature_paths["global"].name, "history_features_global.parquet")
        self.assertEqual(paths.history_feature_paths["full_group"].name, "history_features_full_group.parquet")


class AutoGluonFeatureCoercionTest(unittest.TestCase):
    def test_coerce_feature_frame_compacts_numeric_and_categorical_types(self) -> None:
        df = pd.DataFrame(
            {
                "price": ["0.45", "0.60", None],
                "spread": [0.01, "0.02", "bad"],
                "domain": ["sports", None, "politics"],
                "market_type": ["moneyline", "spread", None],
            }
        )

        coerced = _coerce_feature_frame(
            df,
            feature_columns=["price", "spread", "domain", "market_type"],
            numeric_columns=["price", "spread"],
            categorical_columns=["domain", "market_type"],
        )

        self.assertEqual(str(coerced["price"].dtype), "float32")
        self.assertEqual(str(coerced["spread"].dtype), "float32")
        self.assertEqual(str(coerced["domain"].dtype), "category")
        self.assertEqual(str(coerced["market_type"].dtype), "category")
        self.assertEqual(float(coerced.loc[2, "price"]), 0.0)
        self.assertEqual(float(coerced.loc[2, "spread"]), 0.0)
        self.assertEqual(str(coerced.loc[1, "domain"]), "UNKNOWN")
        self.assertEqual(str(coerced.loc[2, "market_type"]), "UNKNOWN")


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
            last_created = None
            loaded_paths = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                FakePredictor.last_created = self

            def fit(self, **kwargs):
                self.fit_kwargs = kwargs
                return self

            def clone_for_deployment(self, path, model="best", return_clone=False, dirs_exist_ok=False):
                Path(path).mkdir(parents=True, exist_ok=True)
                return path

            @classmethod
            def load(cls, path):
                cls.loaded_paths.append(str(path))
                return cls.last_created

            def predict_proba(self, data, as_pandas=False, as_multiclass=False):
                return [0.2] * len(data)

        with TemporaryDirectory() as tmpdir:
            with patch(
                "rule_baseline.models.autogluon_qmodel._load_tabular_predictor_class",
                return_value=FakePredictor,
            ):
                result = fit_autogluon_q_model(
                    df_train=df_train,
                    df_valid=df_valid,
                    feature_columns=["price", "domain", "horizon_hours"],
                    required_critical_columns=["price"],
                    required_noncritical_columns=["domain", "horizon_hours"],
                    feature_semantics_version="decision_time_v1",
                    normalization_manifest=build_normalization_manifest(
                        pd.DataFrame([{"market_id": "m1", "domain": "a"}])
                    ),
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
        self.assertEqual(
            result.runtime_manifest["training_recipe"]["predictor_hyperparameters"],
            {"GBM": {"verbosity": -1}, "CAT": {}, "XGB": {}},
        )
        self.assertEqual(result.deploy_bundle_dir.name, "bundle")
        self.assertEqual(result.full_bundle_dir.name, "q_model_bundle_full")
        self.assertEqual(result.runtime_manifest["feature_semantics_version"], "decision_time_v1")
        self.assertEqual(result.runtime_manifest["normalization_manifest"]["manifest_version"], 1)
        self.assertEqual(result.feature_contract.required_critical_columns, ("price",))


if __name__ == "__main__":
    unittest.main()
