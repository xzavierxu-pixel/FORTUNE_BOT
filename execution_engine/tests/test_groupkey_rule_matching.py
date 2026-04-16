import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
sys.path.append(os.path.abspath("polymarket_rule_engine"))

from execution_engine.online.scoring.rules import (
    ServingFeatureBundle,
    attach_serving_features,
    build_group_default_rule_hits,
    score_frame_group_rule_coverage,
    score_frame_rule_coverage,
)
from rule_baseline.backtesting.backtest_portfolio_qmodel import load_rules, match_rules


class GroupKeyRuleMatchingTest(unittest.TestCase):
    def test_snapshot_matching_prefers_exact_horizon_hour_when_available(self) -> None:
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.45,
                    "horizon_hours": 6,
                },
                {
                    "market_id": "m2",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.45,
                    "horizon_hours": 5,
                },
            ]
        )
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
                    "horizon_hours": 6,
                    "rule_score": 0.1,
                    "direction": 1,
                    "q_full": 0.6,
                    "edge_full": 0.1,
                    "edge_std_full": 0.1,
                    "edge_lower_bound_full": 0.05,
                    "n_full": 20,
                }
            ]
        )

        matched = match_rules(snapshots, rules)

        self.assertEqual(list(matched["market_id"]), ["m1"])
        self.assertEqual(list(matched["rule_group_key"]), ["example.com|SPORTS|other"])

    def test_live_rule_coverage_uses_band_matching_for_continuous_remaining_hours(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "remaining_hours": 7.2,
                    "live_mid_price": 0.46,
                }
            ]
        )
        rules = pd.DataFrame(
            [
                {
                    "group_key": "example.com|SPORTS|other",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "h_min": 5.0,
                    "h_max": 9.0,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "horizon_hours": 6,
                }
            ]
        )

        scored = score_frame_rule_coverage(
            frame,
            rules,
            horizon_column="remaining_hours",
            price_column="live_mid_price",
        )

        self.assertEqual(int(scored.loc[0, "rule_coverage_match_count"]), 1)
        self.assertTrue(bool(scored.loc[0, "rule_coverage_exact_match"]))

    def test_group_rule_coverage_uses_only_domain_category_market_type(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "remaining_hours": 48.0,
                    "live_mid_price": 0.91,
                }
            ]
        )
        rules = pd.DataFrame(
            [
                {
                    "group_key": "example.com|SPORTS|other",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "h_min": 1.0,
                    "h_max": 12.0,
                    "price_min": 0.4,
                    "price_max": 0.5,
                }
            ]
        )

        scored = score_frame_group_rule_coverage(frame, rules)

        self.assertEqual(int(scored.loc[0, "group_rule_coverage_match_count"]), 1)
        self.assertTrue(bool(scored.loc[0, "group_rule_coverage_exact_match"]))
        self.assertEqual(scored.loc[0, "group_rule_key"], "example.com|SPORTS|other")

    def test_load_rules_preserves_horizon_hours_column(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "trading_rules.csv"
            pd.DataFrame(
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
                        "edge_std_full": 0.1,
                        "edge_lower_bound_full": 0.08,
                        "rule_score": 0.08,
                        "n_full": 20,
                        "horizon_hours": 6,
                    }
                ]
            ).to_csv(path, index=False)

            loaded = load_rules(path)

            self.assertIn("horizon_hours", loaded.columns)
            self.assertEqual(int(loaded.loc[0, "horizon_hours"]), 6)

    def test_attach_serving_features_falls_back_to_group_defaults_on_fine_miss(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "remaining_hours": 9.2,
                    "live_mid_price": 0.46,
                }
            ]
        )
        bundle = ServingFeatureBundle(
            fine_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|other",
                        "price_bin": "0.40-0.50",
                        "horizon_hours": 6,
                        "leaf_id": 123,
                        "direction": 1,
                        "q_full": 0.7,
                        "p_full": 0.45,
                        "edge_full": 0.25,
                        "edge_std_full": 0.1,
                        "edge_lower_bound_full": 0.2,
                        "rule_score": 0.2,
                        "n_full": 50,
                        "rule_price_center": 0.45,
                        "rule_price_width": 0.1,
                        "rule_horizon_center": 7.0,
                        "rule_horizon_width": 4.0,
                        "rule_edge_buffer": 0.05,
                        "rule_confidence_ratio": 2.0,
                        "rule_support_log1p": 3.9,
                        "rule_snapshot_support_log1p": 3.9,
                    }
                ]
            ),
            group_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|other",
                        "group_decision": "keep",
                        "group_default_q_full": 0.61,
                        "group_default_p_full": 0.44,
                        "group_default_edge_full": 0.17,
                        "group_default_edge_std_full": 0.08,
                        "group_default_edge_lower_bound_full": 0.12,
                        "group_default_rule_score": 0.12,
                        "group_default_n_full": 140.0,
                        "group_default_rule_price_center": 0.5,
                        "group_default_rule_price_width": 0.1,
                        "group_default_rule_horizon_center": 8.0,
                        "group_default_rule_horizon_width": 6.0,
                        "group_default_rule_edge_buffer": 0.05,
                        "group_default_rule_confidence_ratio": 1.5,
                        "group_default_rule_support_log1p": 4.0,
                        "group_default_rule_snapshot_support_log1p": 4.0,
                        "group_default_direction": 1,
                        "group_default_leaf_id": "__GROUP_DEFAULT__|example.com|SPORTS|other",
                    }
                ]
            ),
            defaults_manifest={
                "fine_feature_defaults": {
                    "leaf_id": {"group_column": "group_default_leaf_id"},
                    "direction": {"group_column": "group_default_direction"},
                    "q_full": {"group_column": "group_default_q_full"},
                    "p_full": {"group_column": "group_default_p_full"},
                    "edge_full": {"group_column": "group_default_edge_full"},
                    "edge_std_full": {"group_column": "group_default_edge_std_full"},
                    "edge_lower_bound_full": {"group_column": "group_default_edge_lower_bound_full"},
                    "rule_score": {"group_column": "group_default_rule_score"},
                    "n_full": {"group_column": "group_default_n_full"},
                    "rule_price_center": {"group_column": "group_default_rule_price_center"},
                    "rule_price_width": {"group_column": "group_default_rule_price_width"},
                    "rule_horizon_center": {"group_column": "group_default_rule_horizon_center"},
                    "rule_horizon_width": {"group_column": "group_default_rule_horizon_width"},
                    "rule_edge_buffer": {"group_column": "group_default_rule_edge_buffer"},
                    "rule_confidence_ratio": {"group_column": "group_default_rule_confidence_ratio"},
                    "rule_support_log1p": {"group_column": "group_default_rule_support_log1p"},
                    "rule_snapshot_support_log1p": {"group_column": "group_default_rule_snapshot_support_log1p"},
                }
            },
        )

        enriched = attach_serving_features(
            frame,
            bundle,
            price_column="live_mid_price",
            horizon_column="remaining_hours",
        )

        self.assertFalse(bool(enriched.loc[0, "fine_match_found"]))
        self.assertTrue(bool(enriched.loc[0, "group_match_found"]))
        self.assertTrue(bool(enriched.loc[0, "used_group_fallback_only"]))
        self.assertEqual(
            enriched.loc[0, "fine_feature_leaf_id"],
            "__GROUP_DEFAULT__|example.com|SPORTS|other",
        )
        self.assertAlmostEqual(float(enriched.loc[0, "fine_feature_edge_full"]), 0.17, places=6)
        self.assertAlmostEqual(float(enriched.loc[0, "fine_feature_q_full"]), 0.61, places=6)

    def test_build_group_default_rule_hits_constructs_model_time_fallback_row(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "snapshot_time": "2026-04-01T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "other",
                    "price": 0.91,
                    "horizon_hours": 48.0,
                }
            ]
        )
        bundle = ServingFeatureBundle(
            fine_features=pd.DataFrame(),
            group_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|other",
                        "group_decision": "keep",
                        "group_default_leaf_id": "__GROUP_DEFAULT__|example.com|SPORTS|other",
                        "group_default_direction": 1,
                        "group_default_q_full": 0.61,
                        "group_default_p_full": 0.44,
                        "group_default_edge_full": 0.17,
                        "group_default_edge_std_full": 0.08,
                        "group_default_edge_lower_bound_full": 0.12,
                        "group_default_rule_score": 0.12,
                        "group_default_n_full": 140.0,
                    }
                ]
            ),
            defaults_manifest={},
        )

        fallback_hits = build_group_default_rule_hits(frame, bundle)

        self.assertEqual(len(fallback_hits), 1)
        self.assertEqual(fallback_hits.loc[0, "rule_group_key"], "example.com|SPORTS|other")
        self.assertEqual(int(fallback_hits.loc[0, "rule_leaf_id"]), -1)
        self.assertEqual(int(fallback_hits.loc[0, "rule_direction"]), 1)
        self.assertAlmostEqual(float(fallback_hits.loc[0, "edge_full"]), 0.17, places=6)
        self.assertEqual(fallback_hits.loc[0, "rule_match_reason"], "group_default_fallback")


if __name__ == "__main__":
    unittest.main()
