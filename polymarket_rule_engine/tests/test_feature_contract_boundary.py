import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.contracts import classify_feature_columns
from rule_baseline.datasets.snapshots import build_snapshot_base
from rule_baseline.features.market_feature_builders import extract_market_features


class FeatureContractBoundaryTest(unittest.TestCase):
    def test_identity_monitoring_and_control_columns_stay_out_of_model_contract(self) -> None:
        classified = classify_feature_columns(
            [
                "price",
                "domain",
                "token_0_id",
                "token_1_id",
                "selected_reference_token_id",
                "outcome_0_label",
                "selected_reference_outcome_label",
                "selected_reference_side_index",
                "group_match_found",
                "fine_match_found",
                "used_group_fallback_only",
                "group_decision",
                "stale_quote_flag",
                "category_override_flag",
                "dataset_split",
                "y",
            ]
        )

        self.assertEqual(classified.model_feature_columns, ["price", "domain"])
        self.assertIn("token_0_id", classified.metadata_columns)
        self.assertIn("selected_reference_side_index", classified.metadata_columns)
        self.assertIn("group_match_found", classified.monitoring_columns)
        self.assertIn("stale_quote_flag", classified.monitoring_columns)
        self.assertIn("dataset_split", classified.control_columns)
        self.assertIn("y", classified.control_columns)

    def test_stale_quote_rows_fail_quality_pass_but_remain_auditable(self) -> None:
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "price": 0.45,
                    "horizon_hours": 4,
                    "y": 1,
                    "delta_hours": 0.0,
                    "closedTime": "2026-04-02T00:00:00Z",
                    "startDate": "2026-04-01T00:00:00Z",
                    "endDate": "2026-04-02T00:00:00Z",
                    "selected_quote_offset_sec": 600,
                    "selected_quote_points_in_window": 1,
                    "selected_quote_side": "right",
                    "stale_quote_flag": True,
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                }
            ]
        )

        out = build_snapshot_base(snapshots=snapshots, raw_markets=None, market_annotations=None)

        self.assertFalse(bool(out.iloc[0]["quality_pass"]))
        self.assertTrue(bool(out.iloc[0]["stale_quote_flag"]))

    def test_sparse_entertainment_flags_are_no_longer_generated(self) -> None:
        features = extract_market_features(
            {
                "question": "Will this entertainment awards show break ratings records?",
                "description": "Entertainment market",
            }
        )

        self.assertNotIn("cat_entertainment", features)
        self.assertNotIn("cat_entertainment_str", features)


if __name__ == "__main__":
    unittest.main()
