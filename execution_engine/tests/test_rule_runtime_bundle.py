import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

sys.path.append(os.path.abspath("polymarket_rule_engine"))
sys.path.append(os.path.abspath("."))

from execution_engine.online.scoring.rule_runtime import get_feature_contract, load_model_payload
import rule_baseline.models.runtime_adapter as runtime_adapter


class RuleRuntimeBundleLoadingTest(unittest.TestCase):
    def test_load_model_payload_reads_runtime_bundle_and_exposes_feature_contract(self) -> None:
        with TemporaryDirectory() as tmpdir:
            bundle_dir = Path(tmpdir) / "q_model_bundle"
            predictor_dir = bundle_dir / "predictor"
            predictor_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "runtime_manifest.json").write_text(
                json.dumps(
                    {
                        "artifact_version": 1,
                        "model_family": "autogluon_tabular_q",
                        "target_mode": "q",
                        "predictor_path": "predictor",
                    }
                ),
                encoding="utf-8",
            )
            (bundle_dir / "feature_contract.json").write_text(
                json.dumps(
                    {
                        "feature_columns": ["price", "domain", "horizon_hours"],
                        "numeric_columns": ["price", "horizon_hours"],
                        "categorical_columns": ["domain"],
                    }
                ),
                encoding="utf-8",
            )
            (bundle_dir / "calibration" / "calibrator_meta.json").parent.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "calibration" / "calibrator_meta.json").write_text(
                json.dumps({"type": "none", "mode": "none"}),
                encoding="utf-8",
            )

            fake_predictor = SimpleNamespace(persist=lambda: None)
            cfg = SimpleNamespace(
                rule_engine_dir=Path.cwd() / "polymarket_rule_engine",
                rule_engine_model_path=bundle_dir,
            )

            with patch.object(runtime_adapter, "_load_tabular_predictor", return_value=fake_predictor):
                artifact = load_model_payload(cfg)

            contract = get_feature_contract(artifact)
            self.assertEqual(artifact.backend, "autogluon_q_bundle")
            self.assertEqual(artifact.target_mode, "q")
            self.assertEqual(contract.feature_columns, ("price", "domain", "horizon_hours"))
            self.assertEqual(contract.numeric_columns, ("price", "horizon_hours"))
            self.assertEqual(contract.categorical_columns, ("domain",))

    def test_load_model_payload_rejects_non_q_target_mode_for_live_runtime(self) -> None:
        with TemporaryDirectory() as tmpdir:
            bundle_dir = Path(tmpdir) / "q_model_bundle"
            predictor_dir = bundle_dir / "predictor"
            predictor_dir.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "runtime_manifest.json").write_text(
                json.dumps(
                    {
                        "artifact_version": 1,
                        "model_family": "autogluon_tabular_q",
                        "target_mode": "residual_q",
                        "predictor_path": "predictor",
                    }
                ),
                encoding="utf-8",
            )
            (bundle_dir / "feature_contract.json").write_text(
                json.dumps(
                    {
                        "feature_columns": ["price"],
                        "numeric_columns": ["price"],
                        "categorical_columns": [],
                    }
                ),
                encoding="utf-8",
            )
            (bundle_dir / "calibration" / "calibrator_meta.json").parent.mkdir(parents=True, exist_ok=True)
            (bundle_dir / "calibration" / "calibrator_meta.json").write_text(
                json.dumps({"type": "none", "mode": "none"}),
                encoding="utf-8",
            )

            fake_predictor = SimpleNamespace(persist=lambda: None)
            cfg = SimpleNamespace(
                rule_engine_dir=Path.cwd() / "polymarket_rule_engine",
                rule_engine_model_path=bundle_dir,
            )

            with patch.object(runtime_adapter, "_load_tabular_predictor", return_value=fake_predictor):
                with self.assertRaisesRegex(ValueError, "only supports q-model artifacts"):
                    load_model_payload(cfg)


if __name__ == "__main__":
    unittest.main()
