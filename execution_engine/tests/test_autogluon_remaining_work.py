import os
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
sys.path.append(os.path.abspath(str(ROOT_DIR / "polymarket_rule_engine")))

from rule_baseline.backtesting.backtest_execution_parity import compute_decision_parity_summary
from rule_baseline.models.tree_ensembles import (
    ProbabilityBlendCalibrator,
    apply_probability_calibrator,
    fit_probability_calibrator,
)


class AutoGluonRemainingWorkTest(unittest.TestCase):
    def test_beta_calibration_outputs_unit_interval_probs(self) -> None:
        raw = np.asarray([0.05, 0.2, 0.4, 0.7, 0.9], dtype=float)
        y = np.asarray([0, 0, 0, 1, 1], dtype=int)

        calibrator = fit_probability_calibrator(raw, y, "beta")
        calibrated = apply_probability_calibrator(calibrator, raw)

        self.assertEqual(calibrated.shape, raw.shape)
        self.assertTrue(np.all(calibrated >= 0.0))
        self.assertTrue(np.all(calibrated <= 1.0))

    def test_blend_calibrator_preserves_raw_when_no_base_calibrator(self) -> None:
        raw = np.asarray([0.2, 0.8], dtype=float)
        blend = ProbabilityBlendCalibrator(alpha=0.25, base_calibrator=None)

        blended = apply_probability_calibrator(blend, raw)

        np.testing.assert_allclose(blended, raw)

    def test_decision_parity_summary_reports_overlap(self) -> None:
        ref = pd.DataFrame(
            [
                {"market_id": "m1", "snapshot_time": "2026-04-01T00:00:00Z", "q_pred": 0.6, "edge_prob": 0.1},
                {"market_id": "m2", "snapshot_time": "2026-04-01T01:00:00Z", "q_pred": 0.7, "edge_prob": 0.2},
            ]
        )
        cmp = pd.DataFrame(
            [
                {"market_id": "m1", "snapshot_time": "2026-04-01T00:00:00Z", "q_pred": 0.65, "edge_prob": 0.12},
                {"market_id": "m3", "snapshot_time": "2026-04-01T02:00:00Z", "q_pred": 0.55, "edge_prob": 0.08},
            ]
        )

        summary = compute_decision_parity_summary(ref, cmp)

        self.assertEqual(summary["selected_overlap_count"], 1)
        self.assertEqual(summary["reference_only_count"], 1)
        self.assertEqual(summary["comparison_only_count"], 1)
        self.assertAlmostEqual(summary["q_pred_abs_diff_mean"], 0.05, places=6)


if __name__ == "__main__":
    unittest.main()
