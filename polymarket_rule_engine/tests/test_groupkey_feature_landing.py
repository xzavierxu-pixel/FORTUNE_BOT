import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.features.tabular import preprocess_features


class GroupKeyFeatureLandingTest(unittest.TestCase):
    def test_preprocess_features_keeps_group_quality_and_rule_prior_columns(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "price": 0.45,
                    "horizon_hours": 6,
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "group_key": "example.com|SPORTS|other",
                    "q_full": 0.61,
                    "p_full": 0.45,
                    "edge_full": 0.16,
                    "edge_std_full": 0.08,
                    "edge_lower_bound_full": 0.09,
                    "rule_score": 0.09,
                    "n_full": 40,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "group_unique_markets": 30,
                    "group_snapshot_rows": 180,
                    "global_total_unique_markets": 300,
                    "global_total_snapshot_rows": 1800,
                    "group_market_share_global": 0.1,
                    "group_snapshot_share_global": 0.1,
                    "group_median_logloss": 0.62,
                    "group_median_brier": 0.21,
                    "global_group_logloss_q25": 0.31,
                    "global_group_brier_q25": 0.09,
                    "group_decision": "keep",
                }
            ]
        )
        market_feature_cache = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "source_host": "example.com",
                }
            ]
        )

        out = preprocess_features(df, market_feature_cache)

        self.assertIn("group_unique_markets", out.columns)
        self.assertIn("group_median_logloss", out.columns)
        self.assertIn("group_median_brier", out.columns)
        self.assertIn("global_group_logloss_q25", out.columns)
        self.assertIn("global_group_brier_q25", out.columns)
        self.assertIn("group_decision", out.columns)
        self.assertIn("group_logloss_gap_q25", out.columns)
        self.assertIn("group_brier_gap_q25", out.columns)
        self.assertIn("group_quality_pass_q25", out.columns)
        self.assertIn("rule_edge_over_std", out.columns)
        self.assertIn("group_rule_score_x_edge_lower", out.columns)
        self.assertIn("domain_category_key", out.columns)
        self.assertIn("domain_market_type_key", out.columns)
        self.assertIn("category_market_type_key", out.columns)
        self.assertIn("domain_is_unknown", out.columns)
        self.assertIn("rule_price_width", out.columns)
        self.assertIn("rule_horizon_width", out.columns)
        self.assertIn("rule_edge_buffer", out.columns)
        self.assertIn("rule_confidence_ratio", out.columns)
        self.assertIn("rule_support_log1p", out.columns)
        self.assertIn("group_market_share_global", out.columns)
        self.assertIn("group_snapshot_share_global", out.columns)
        self.assertIn("group_share_x_logloss_gap", out.columns)

        row = out.iloc[0]
        self.assertAlmostEqual(float(row["group_logloss_gap_q25"]), 0.31, places=6)
        self.assertAlmostEqual(float(row["group_brier_gap_q25"]), 0.12, places=6)
        self.assertEqual(float(row["group_quality_pass_q25"]), 1.0)
        self.assertEqual(str(row["group_decision"]), "keep")
        self.assertGreater(float(row["rule_edge_over_std"]), 1.9)
        self.assertEqual(str(row["domain_category_key"]), "example.com|SPORTS")
        self.assertEqual(float(row["domain_is_unknown"]), 0.0)
        self.assertAlmostEqual(float(row["rule_price_width"]), 0.0, places=6)
        self.assertAlmostEqual(float(row["rule_horizon_width"]), 4.0, places=6)
        self.assertAlmostEqual(float(row["rule_edge_buffer"]), 0.07, places=6)
        self.assertGreater(float(row["rule_support_log1p"]), 3.0)
        self.assertAlmostEqual(float(row["group_market_share_global"]), 0.1, places=6)
        self.assertAlmostEqual(float(row["group_snapshot_share_global"]), 0.1, places=6)
        self.assertAlmostEqual(float(row["group_share_x_logloss_gap"]), 0.031, places=6)


if __name__ == "__main__":
    unittest.main()
