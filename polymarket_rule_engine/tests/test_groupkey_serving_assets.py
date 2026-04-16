import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.training.train_rules_naive_output_rule import (
    build_fine_serving_features,
    build_group_serving_features,
    summarize_history_features,
)


class GroupKeyServingAssetsTest(unittest.TestCase):
    def test_group_defaults_are_aggregated_from_group_key_rows(self) -> None:
        rules = pd.DataFrame(
            [
                {
                    "group_key": "example.com|SPORTS|other",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "leaf_id": 1,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "direction": 1,
                    "q_full": 0.6,
                    "p_full": 0.45,
                    "edge_full": 0.15,
                    "edge_std_full": 0.10,
                    "edge_lower_bound_full": 0.11,
                    "rule_score": 0.11,
                    "n_full": 20,
                    "horizon_hours": 6,
                    "group_unique_markets": 30,
                    "group_snapshot_rows": 60,
                    "global_total_unique_markets": 200,
                    "global_total_snapshot_rows": 600,
                    "group_market_share_global": 0.15,
                    "group_snapshot_share_global": 0.10,
                    "group_median_logloss": 0.5,
                    "group_median_brier": 0.2,
                    "global_group_logloss_q25": 0.3,
                    "global_group_brier_q25": 0.1,
                    "group_decision": "keep",
                },
                {
                    "group_key": "example.com|SPORTS|other",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "leaf_id": 2,
                    "price_min": 0.5,
                    "price_max": 0.6,
                    "h_min": 9.0,
                    "h_max": 18.0,
                    "direction": 1,
                    "q_full": 0.8,
                    "p_full": 0.55,
                    "edge_full": 0.25,
                    "edge_std_full": 0.20,
                    "edge_lower_bound_full": 0.18,
                    "rule_score": 0.18,
                    "n_full": 40,
                    "horizon_hours": 12,
                    "group_unique_markets": 30,
                    "group_snapshot_rows": 60,
                    "global_total_unique_markets": 200,
                    "global_total_snapshot_rows": 600,
                    "group_market_share_global": 0.15,
                    "group_snapshot_share_global": 0.10,
                    "group_median_logloss": 0.5,
                    "group_median_brier": 0.2,
                    "global_group_logloss_q25": 0.3,
                    "global_group_brier_q25": 0.1,
                    "group_decision": "keep",
                },
            ]
        )
        history_df = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.45,
                    "y": 1,
                    "horizon_hours": 6,
                    "closedTime": "2026-04-01T00:00:00Z",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "m2",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.55,
                    "y": 0,
                    "horizon_hours": 12,
                    "closedTime": "2026-04-02T00:00:00Z",
                    "snapshot_time": "2026-04-02T00:00:00Z",
                },
            ]
        )

        history = summarize_history_features(history_df)
        group, defaults_manifest = build_group_serving_features(rules, history)
        fine = build_fine_serving_features(rules, group_features=group)

        self.assertEqual(len(fine), 2)
        self.assertEqual(len(group), 1)
        self.assertIn("full_group_expanding_bias_mean", group.columns)
        self.assertIn("full_group_expanding_bias_p50", group.columns)
        self.assertIn("full_group_expanding_bias_std", group.columns)
        self.assertIn("full_group_expanding_abs_bias_p75", group.columns)
        self.assertIn("full_group_expanding_brier_p90", group.columns)
        self.assertIn("full_group_expanding_logloss_std", group.columns)
        self.assertIn("full_group_recent_90days_logloss_p50", group.columns)
        self.assertIn("full_group_recent_90days_bias_p50", group.columns)
        self.assertIn("full_group_recent_90days_vs_expanding_logloss_gap", group.columns)
        self.assertIn("full_group_recent_90days_vs_expanding_bias_zscore", group.columns)
        self.assertIn("full_group_recent_90days_vs_expanding_logloss_zscore", group.columns)
        self.assertIn("full_group_expanding_logloss_tail_spread", group.columns)
        self.assertIn("full_group_recent_90days_tail_instability_ratio", group.columns)
        self.assertIn("full_group_vs_domain_logloss_gap", group.columns)
        self.assertIn("hist_price_x_full_group_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_full_group_expanding_bias", fine.columns)
        self.assertIn("rule_score_minus_recent_90days_logloss", fine.columns)
        self.assertIn("rule_edge_minus_domain_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_category_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_market_type_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_domain_x_category_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_domain_x_market_type_expanding_bias", fine.columns)
        self.assertIn("rule_edge_minus_category_x_market_type_expanding_bias", fine.columns)
        self.assertIn("rule_score_minus_domain_expanding_logloss", fine.columns)
        self.assertIn("rule_score_minus_category_expanding_logloss", fine.columns)
        self.assertIn("rule_score_minus_market_type_expanding_logloss", fine.columns)
        self.assertIn("rule_score_minus_domain_x_category_expanding_logloss", fine.columns)
        self.assertIn("rule_score_minus_domain_x_market_type_expanding_logloss", fine.columns)
        self.assertIn("rule_score_minus_category_x_market_type_expanding_logloss", fine.columns)
        self.assertIn("price_x_full_group_expanding_abs_bias_tail_spread", fine.columns)
        self.assertIn("q_full", defaults_manifest)
        self.assertEqual(defaults_manifest["q_full"]["group_column"], "group_default_q_full")
        self.assertIn("hist_price_x_full_group_expanding_bias", defaults_manifest)
        self.assertIn("rule_edge_minus_domain_expanding_bias", defaults_manifest)
        self.assertIn("rule_score_minus_category_expanding_logloss", defaults_manifest)
        self.assertIn("rule_edge_minus_domain_x_category_expanding_bias", defaults_manifest)
        self.assertIn("rule_score_minus_domain_x_market_type_expanding_logloss", defaults_manifest)
        self.assertAlmostEqual(float(group.loc[0, "group_default_q_full"]), (0.6 * 20 + 0.8 * 40) / 60, places=6)
        self.assertAlmostEqual(float(group.loc[0, "group_default_edge_full"]), (0.15 * 20 + 0.25 * 40) / 60, places=6)
        self.assertAlmostEqual(float(group.loc[0, "group_default_n_full"]), 60.0, places=6)
        self.assertGreaterEqual(float(group.loc[0, "full_group_expanding_abs_bias_p75"]), 0.45)
        self.assertGreaterEqual(float(group.loc[0, "full_group_expanding_logloss_max"]), 0.59)
        self.assertGreaterEqual(float(group.loc[0, "full_group_expanding_logloss_tail_spread"]), 0.0)
        self.assertGreaterEqual(float(group.loc[0, "full_group_recent_90days_tail_instability_ratio"]), 0.0)
        self.assertAlmostEqual(float(group.loc[0, "full_group_recent_90days_vs_expanding_bias_zscore"]), 0.0, places=6)
        self.assertAlmostEqual(float(group.loc[0, "full_group_recent_90days_vs_expanding_logloss_zscore"]), 0.0, places=6)
        self.assertGreaterEqual(float(fine.loc[0, "hist_price_x_full_group_expanding_bias"]), 0.0)

    def test_matched_rule_aggregate_features_are_built_per_grain(self) -> None:
        rules = pd.DataFrame(
            [
                {
                    "group_key": "a.com|SPORTS|other",
                    "domain": "a.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "leaf_id": 1,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "direction": 1,
                    "q_full": 0.60,
                    "p_full": 0.45,
                    "edge_full": 0.15,
                    "edge_std_full": 0.10,
                    "edge_lower_bound_full": 0.11,
                    "rule_score": 0.12,
                    "n_full": 20,
                    "horizon_hours": 6,
                    "group_unique_markets": 30,
                    "group_snapshot_rows": 60,
                    "global_total_unique_markets": 200,
                    "global_total_snapshot_rows": 600,
                    "group_market_share_global": 0.15,
                    "group_snapshot_share_global": 0.10,
                    "group_median_logloss": 0.5,
                    "group_median_brier": 0.2,
                    "global_group_logloss_q25": 0.3,
                    "global_group_brier_q25": 0.1,
                    "group_decision": "keep",
                },
                {
                    "group_key": "b.com|SPORTS|other",
                    "domain": "b.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "leaf_id": 2,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "direction": 1,
                    "q_full": 0.70,
                    "p_full": 0.45,
                    "edge_full": 0.25,
                    "edge_std_full": 0.20,
                    "edge_lower_bound_full": 0.18,
                    "rule_score": 0.22,
                    "n_full": 40,
                    "horizon_hours": 6,
                    "group_unique_markets": 30,
                    "group_snapshot_rows": 60,
                    "global_total_unique_markets": 200,
                    "global_total_snapshot_rows": 600,
                    "group_market_share_global": 0.15,
                    "group_snapshot_share_global": 0.10,
                    "group_median_logloss": 0.5,
                    "group_median_brier": 0.2,
                    "global_group_logloss_q25": 0.3,
                    "global_group_brier_q25": 0.1,
                    "group_decision": "keep",
                },
            ]
        )
        history_df = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "a.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.45,
                    "y": 1,
                    "horizon_hours": 6,
                    "closedTime": "2026-04-01T00:00:00Z",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "m2",
                    "domain": "b.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.45,
                    "y": 0,
                    "horizon_hours": 6,
                    "closedTime": "2026-04-02T00:00:00Z",
                    "snapshot_time": "2026-04-02T00:00:00Z",
                },
            ]
        )

        history = summarize_history_features(history_df)
        group, defaults_manifest = build_group_serving_features(rules, history)
        fine = build_fine_serving_features(rules, group_features=group)

        row_a = fine[fine["group_key"] == "a.com|SPORTS|other"].iloc[0]
        self.assertEqual(int(row_a["rule_full_group_key_matched_rule_count"]), 1)
        self.assertEqual(int(row_a["rule_domain_matched_rule_count"]), 1)
        self.assertEqual(int(row_a["rule_category_matched_rule_count"]), 2)
        self.assertEqual(int(row_a["rule_market_type_matched_rule_count"]), 2)
        self.assertAlmostEqual(float(row_a["rule_category_max_edge_full"]), 0.25, places=6)
        self.assertAlmostEqual(float(row_a["rule_category_mean_edge_full"]), 0.20, places=6)
        self.assertAlmostEqual(float(row_a["rule_category_sum_n_full"]), 60.0, places=6)
        self.assertIn("rule_category_matched_rule_count", defaults_manifest)
        self.assertEqual(defaults_manifest["rule_category_max_edge_full"]["aggregation"], "max")


if __name__ == "__main__":
    unittest.main()
