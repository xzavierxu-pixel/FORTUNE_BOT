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
                    "n_full": 40,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "group_decision": "keep",
                    "group_feature_full_group_expanding_market_count": 30,
                    "group_feature_full_group_expanding_snapshot_count": 180,
                    "group_feature_full_group_expanding_logloss_p50": 0.62,
                    "group_feature_full_group_expanding_brier_p50": 0.21,
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

        self.assertIn("group_feature_full_group_expanding_market_count", out.columns)
        self.assertIn("group_feature_full_group_expanding_logloss_p50", out.columns)
        self.assertIn("group_feature_full_group_expanding_brier_p50", out.columns)
        self.assertIn("group_decision", out.columns)
        self.assertIn("rule_edge_over_std", out.columns)
        self.assertIn("domain_category_key", out.columns)
        self.assertIn("domain_market_type_key", out.columns)
        self.assertIn("category_market_type_key", out.columns)
        self.assertIn("domain_is_unknown", out.columns)
        self.assertIn("rule_horizon_width", out.columns)
        self.assertIn("rule_edge_buffer", out.columns)
        self.assertIn("rule_confidence_ratio", out.columns)
        self.assertIn("rule_support_log1p", out.columns)
        self.assertNotIn("group_market_share_global", out.columns)
        self.assertNotIn("group_snapshot_share_global", out.columns)
        self.assertNotIn("group_logloss_gap_q25", out.columns)
        self.assertNotIn("group_brier_gap_q25", out.columns)
        
        row = out.iloc[0]
        self.assertEqual(str(row["group_decision"]), "keep")
        self.assertGreater(float(row["rule_edge_over_std"]), 1.9)
        self.assertEqual(str(row["domain_category_key"]), "example.com|SPORTS")
        self.assertEqual(float(row["domain_is_unknown"]), 0.0)
        self.assertAlmostEqual(float(row["rule_horizon_width"]), 4.0, places=6)
        self.assertAlmostEqual(float(row["rule_edge_buffer"]), 0.07, places=6)
        self.assertGreater(float(row["rule_support_log1p"]), 3.0)


if __name__ == "__main__":
    unittest.main()
