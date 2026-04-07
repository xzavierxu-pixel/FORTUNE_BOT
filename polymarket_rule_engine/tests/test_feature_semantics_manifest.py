import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.features.annotation_normalization import build_normalization_manifest
from rule_baseline.models.autogluon_qmodel import fit_autogluon_q_model


class FeatureSemanticsManifestTest(unittest.TestCase):
    def test_runtime_bundle_writes_standalone_normalization_manifest(self) -> None:
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
                return cls.last_created

            def predict_proba(self, data, as_pandas=False, as_multiclass=False):
                return [0.2] * len(data)

        normalization_manifest = build_normalization_manifest(
            pd.DataFrame([{"market_id": "m1", "domain": "a"}])
        )

        with TemporaryDirectory() as tmpdir:
            bundle_dir = Path(tmpdir) / "bundle"
            full_bundle_dir = Path(tmpdir) / "bundle_full"
            with patch(
                "rule_baseline.models.autogluon_qmodel._load_tabular_predictor_class",
                return_value=FakePredictor,
            ):
                fit_autogluon_q_model(
                    df_train=df_train,
                    df_valid=df_valid,
                    feature_columns=["price", "domain", "horizon_hours"],
                    required_critical_columns=["price"],
                    required_noncritical_columns=["domain", "horizon_hours"],
                    feature_semantics_version="decision_time_v1",
                    normalization_manifest=normalization_manifest,
                    bundle_dir=bundle_dir,
                    full_bundle_dir=full_bundle_dir,
                    artifact_mode="offline",
                    split_boundaries={},
                )

            deploy_manifest = json.loads((bundle_dir / "normalization_manifest.json").read_text(encoding="utf-8"))
            full_manifest = json.loads((full_bundle_dir / "normalization_manifest.json").read_text(encoding="utf-8"))
            runtime_manifest = json.loads((bundle_dir / "runtime_manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(deploy_manifest, normalization_manifest)
        self.assertEqual(full_manifest, normalization_manifest)
        self.assertEqual(runtime_manifest["normalization_manifest"], normalization_manifest)
        self.assertEqual(runtime_manifest["feature_semantics_version"], "decision_time_v1")


if __name__ == "__main__":
    unittest.main()
