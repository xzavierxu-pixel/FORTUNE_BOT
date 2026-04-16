import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.features.serving import ServingFeatureBundle
from rule_baseline.training.train_snapshot_model import build_feature_table


class ServingFeatureIntegrationTest(unittest.TestCase):
    def test_build_feature_table_keeps_serving_features_for_training(self) -> None:
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                    "price": 0.45,
                    "horizon_hours": 7,
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "y": 1,
                }
            ]
        )
        market_annotations = snapshots[["market_id", "domain", "category", "market_type"]].copy()
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
        rules = pd.DataFrame(
            [
                {
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "leaf_id": 101,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "horizon_hours": 6,
                    "q_full": 0.63,
                    "p_full": 0.45,
                    "edge_full": 0.18,
                    "edge_std_full": 0.09,
                    "edge_lower_bound_full": 0.12,
                    "rule_score": 0.12,
                    "direction": 1,
                    "group_key": "example.com|SPORTS|other",
                    "n_full": 30,
                    "group_unique_markets": 20,
                    "group_snapshot_rows": 100,
                    "group_median_logloss": 0.5,
                    "group_median_brier": 0.2,
                    "global_group_logloss_q25": 0.3,
                    "global_group_brier_q25": 0.1,
                    "group_decision": "keep",
                }
            ]
        )
        serving_bundle = ServingFeatureBundle(
            fine_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|other",
                        "price_bin": "0.40-0.50",
                        "horizon_hours": 6,
                        "leaf_id": 101,
                        "direction": 1,
                        "q_full": 0.63,
                        "p_full": 0.45,
                        "edge_full": 0.18,
                        "edge_std_full": 0.09,
                        "edge_lower_bound_full": 0.12,
                        "rule_score": 0.12,
                        "n_full": 30,
                        "rule_price_center": 0.45,
                        "rule_price_width": 0.1,
                        "rule_horizon_center": 7.0,
                        "rule_horizon_width": 4.0,
                        "rule_edge_buffer": 0.06,
                        "rule_confidence_ratio": 1.33,
                        "rule_support_log1p": 3.43,
                        "rule_snapshot_support_log1p": 3.43,
                        "hist_price_x_full_group_expanding_bias": 0.018,
                        "rule_edge_minus_full_group_expanding_bias": 0.14,
                        "rule_edge_minus_domain_expanding_bias": 0.13,
                        "rule_edge_minus_category_expanding_bias": 0.125,
                        "rule_edge_minus_market_type_expanding_bias": 0.12,
                        "rule_edge_minus_domain_x_category_expanding_bias": 0.115,
                        "rule_edge_minus_domain_x_market_type_expanding_bias": 0.11,
                        "rule_edge_minus_category_x_market_type_expanding_bias": 0.105,
                        "rule_score_minus_domain_expanding_logloss": -0.20,
                        "rule_score_minus_category_expanding_logloss": -0.22,
                        "rule_score_minus_market_type_expanding_logloss": -0.18,
                        "rule_score_minus_domain_x_category_expanding_logloss": -0.16,
                        "rule_score_minus_domain_x_market_type_expanding_logloss": -0.14,
                        "rule_score_minus_category_x_market_type_expanding_logloss": -0.12,
                        "price_x_full_group_expanding_abs_bias_tail_spread": 0.09,
                    }
                ]
            ),
            group_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|other",
                        "group_decision": "keep",
                        "group_default_q_full": 0.61,
                        "group_default_edge_full": 0.16,
                        "full_group_expanding_bias_mean": 0.04,
                        "full_group_recent_90days_vs_expanding_logloss_gap": 0.03,
                        "full_group_recent_90days_vs_expanding_bias_zscore": 0.40,
                        "full_group_recent_90days_tail_instability_ratio": 1.20,
                        "group_default_rule_edge_minus_domain_x_category_expanding_bias": 0.10,
                        "group_default_rule_score_minus_domain_x_market_type_expanding_logloss": -0.11,
                        "group_default_hist_price_x_full_group_expanding_bias": 0.017,
                    }
                ]
            ),
            defaults_manifest={
                "fine_feature_defaults": {
                    "q_full": {"group_column": "group_default_q_full"},
                    "edge_full": {"group_column": "group_default_edge_full"},
                    "hist_price_x_full_group_expanding_bias": {"group_column": "group_default_hist_price_x_full_group_expanding_bias"},
                    "rule_edge_minus_domain_expanding_bias": {"group_column": "group_default_rule_edge_minus_domain_expanding_bias"},
                    "rule_edge_minus_domain_x_category_expanding_bias": {"group_column": "group_default_rule_edge_minus_domain_x_category_expanding_bias"},
                    "rule_score_minus_domain_x_market_type_expanding_logloss": {"group_column": "group_default_rule_score_minus_domain_x_market_type_expanding_logloss"},
                }
            },
        )

        out = build_feature_table(
            snapshots,
            market_feature_cache,
            market_annotations,
            rules,
            serving_feature_bundle=serving_bundle,
        )

        self.assertEqual(len(out), 1)
        self.assertIn("fine_feature_q_full", out.columns)
        self.assertIn("fine_feature_edge_full", out.columns)
        self.assertIn("group_feature_full_group_expanding_bias_mean", out.columns)
        self.assertIn("group_feature_full_group_recent_90days_vs_expanding_logloss_gap", out.columns)
        self.assertIn("group_feature_full_group_recent_90days_vs_expanding_bias_zscore", out.columns)
        self.assertIn("group_feature_full_group_recent_90days_tail_instability_ratio", out.columns)
        self.assertIn("fine_feature_hist_price_x_full_group_expanding_bias", out.columns)
        self.assertIn("fine_feature_rule_edge_minus_domain_expanding_bias", out.columns)
        self.assertIn("fine_feature_rule_edge_minus_domain_x_category_expanding_bias", out.columns)
        self.assertIn("fine_feature_rule_score_minus_domain_expanding_logloss", out.columns)
        self.assertIn("fine_feature_rule_score_minus_category_expanding_logloss", out.columns)
        self.assertIn("fine_feature_rule_score_minus_domain_x_market_type_expanding_logloss", out.columns)
        self.assertIn("fine_feature_price_x_full_group_expanding_abs_bias_tail_spread", out.columns)
        self.assertIn("fine_match_found", out.columns)
        self.assertEqual(float(out.loc[0, "fine_feature_q_full"]), 0.63)
        self.assertEqual(float(out.loc[0, "fine_feature_edge_full"]), 0.18)
        self.assertEqual(float(out.loc[0, "group_feature_full_group_expanding_bias_mean"]), 0.04)
        self.assertEqual(float(out.loc[0, "group_feature_full_group_recent_90days_vs_expanding_logloss_gap"]), 0.03)
        self.assertEqual(float(out.loc[0, "group_feature_full_group_recent_90days_vs_expanding_bias_zscore"]), 0.40)
        self.assertEqual(float(out.loc[0, "group_feature_full_group_recent_90days_tail_instability_ratio"]), 1.20)
        self.assertEqual(float(out.loc[0, "fine_feature_hist_price_x_full_group_expanding_bias"]), 0.018)
        self.assertEqual(float(out.loc[0, "fine_feature_rule_edge_minus_domain_expanding_bias"]), 0.13)
        self.assertEqual(float(out.loc[0, "fine_feature_rule_edge_minus_domain_x_category_expanding_bias"]), 0.115)
        self.assertEqual(float(out.loc[0, "fine_feature_rule_score_minus_domain_expanding_logloss"]), -0.20)
        self.assertEqual(float(out.loc[0, "fine_feature_rule_score_minus_category_expanding_logloss"]), -0.22)
        self.assertEqual(float(out.loc[0, "fine_feature_rule_score_minus_domain_x_market_type_expanding_logloss"]), -0.14)
        self.assertEqual(float(out.loc[0, "fine_feature_price_x_full_group_expanding_abs_bias_tail_spread"]), 0.09)
        self.assertEqual(float(out.loc[0, "fine_match_found"]), 1.0)
        self.assertEqual(int(out.loc[0, "rounded_horizon_hours"]), 6)


if __name__ == "__main__":
    unittest.main()
