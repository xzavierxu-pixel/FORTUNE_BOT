import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.models.runtime_bundle import FeatureContract
from rule_baseline.training.groupkey_reports import (
    build_consistency_report_markdown,
    build_migration_validation_markdown,
    build_runtime_report_markdown,
)


class GroupKeyValidationReportsTest(unittest.TestCase):
    def test_build_migration_validation_markdown_contains_summary_and_distribution(self) -> None:
        rule_funnel_summary = {
            "snapshot_funnel": [
                {"stage": "after_snapshot_quality", "snapshot_rows": 120, "unique_markets": 40, "snapshot_rows_delta": None, "unique_markets_delta": None},
                {"stage": "after_rule_selection", "snapshot_rows": 75, "unique_markets": 25, "snapshot_rows_delta": -45, "unique_markets_delta": -15},
            ],
            "rule_selection": {
                "selection_status_market_impact": [
                    {"selection_status": "keep", "snapshot_rows": 75, "unique_markets": 25},
                    {"selection_status": "drop", "snapshot_rows": 45, "unique_markets": 15},
                ]
            },
        }
        report_df = pd.DataFrame(
            [
                {"group_key": "a|SPORTS|other", "domain": "a", "category": "SPORTS", "market_type": "other", "selection_status": "keep", "group_snapshot_rows": 50, "group_unique_markets": 20},
                {"group_key": "b|POLITICS|other", "domain": "b", "category": "POLITICS", "market_type": "other", "selection_status": "drop", "group_snapshot_rows": 30, "group_unique_markets": 10},
            ]
        )
        rules_df = pd.DataFrame([{"group_key": "a|SPORTS|other"}, {"group_key": "a|SPORTS|other"}])

        markdown = build_migration_validation_markdown(
            rule_funnel_summary=rule_funnel_summary,
            report_df=report_df,
            rules_df=rules_df,
        )

        self.assertIn("# GroupKey Migration Validation", markdown)
        self.assertIn("- snapshot_rows_before=120", markdown)
        self.assertIn("- snapshot_rows_after=75", markdown)
        self.assertIn("- retained_group_key_count=1", markdown)
        self.assertIn("- retained_rule_row_count=2", markdown)
        self.assertIn("### Domain Distribution", markdown)
        self.assertIn("domain=a, selection_status=keep", markdown)

    def test_build_consistency_report_markdown_detects_missing_defaults_and_contract_gaps(self) -> None:
        feature_contract = FeatureContract(
            feature_columns=(
                "price",
                "group_feature_full_group_expanding_bias_mean",
                "group_feature_full_group_recent_50_tail_instability_ratio",
                "fine_feature_q_full",
                "fine_feature_rule_edge_minus_domain_x_category_expanding_bias",
                "fine_feature_missing_from_assets",
            ),
            numeric_columns=("price", "group_feature_full_group_expanding_bias_mean", "fine_feature_q_full"),
            categorical_columns=(),
            required_critical_columns=("price",),
            required_noncritical_columns=(
                "group_feature_full_group_expanding_bias_mean",
                "group_feature_full_group_recent_50_tail_instability_ratio",
                "fine_feature_q_full",
                "fine_feature_rule_edge_minus_domain_x_category_expanding_bias",
                "fine_feature_missing_from_assets",
            ),
        )
        group_features = pd.DataFrame(
            [
                {
                    "group_key": "a|SPORTS|other",
                    "full_group_expanding_bias_mean": 0.1,
                    "full_group_recent_50_tail_instability_ratio": 1.2,
                    "unused_group_metric": 9.0,
                }
            ]
        )
        fine_features = pd.DataFrame(
            [
                {
                    "group_key": "a|SPORTS|other",
                    "price_bin": "0.40-0.50",
                    "horizon_hours": 6,
                    "q_full": 0.6,
                    "rule_edge_minus_domain_x_category_expanding_bias": 0.1,
                    "rule_score_minus_domain_x_market_type_expanding_logloss": -0.2,
                }
            ]
        )
        defaults_manifest = {
            "fine_feature_defaults": {
                "q_full": {"group_column": "group_default_q_full"},
                "rule_edge_minus_domain_x_category_expanding_bias": {"group_column": "group_default_rule_edge_minus_domain_x_category_expanding_bias"},
            }
        }

        markdown = build_consistency_report_markdown(
            feature_contract=feature_contract,
            group_features=group_features,
            fine_features=fine_features,
            defaults_manifest=defaults_manifest,
        )

        self.assertIn("# GroupKey Consistency Report", markdown)
        self.assertIn("- missing_contract_columns_in_assets=1", markdown)
        self.assertIn("- bundle_missing_asset_backed_columns=2", markdown)
        self.assertIn("- unused_asset_columns_not_in_contract=2", markdown)
        self.assertIn("- fine_only_columns_missing_fallback=1", markdown)
        self.assertIn("### Bundle Missing Asset-Backed Serving Columns", markdown)
        self.assertIn("fine_feature_missing_from_assets", markdown)
        self.assertIn("group_feature_unused_group_metric", markdown)
        self.assertIn("fine_feature_rule_score_minus_domain_x_market_type_expanding_logloss", markdown)

    def test_build_runtime_report_markdown_contains_rates_and_unknown_groups(self) -> None:
        payload = {
            "artifact_mode": "offline",
            "sample_filters": {
                "max_rows": 2000,
                "recent_days": 14,
                "split_reference_end": None,
                "history_start": None,
            },
            "snapshot_rows_after_quality_and_split": 600,
            "matched_rule_rows": 420,
            "matched_group_keys": 12,
            "group_features_rows": 90,
            "fine_features_rows": 180,
            "defaults_manifest_entries": 30,
            "bundle_load_seconds": 0.1234,
            "attach_seconds": 0.4567,
            "group_features_memory_mb": 1.25,
            "fine_features_memory_mb": 2.5,
            "group_match_rate": 0.80,
            "fine_match_rate": 0.55,
            "group_fallback_only_rate": 0.25,
            "unknown_group_rate": 0.20,
            "split_rows": [
                {
                    "dataset_split": "test",
                    "rows": 120,
                    "group_match_rate": 0.75,
                    "fine_match_rate": 0.50,
                    "group_fallback_only_rate": 0.25,
                }
            ],
            "unknown_group_preview": [
                {"group_key": "example.com|SPORTS|other", "rows": 22},
            ],
        }

        markdown = build_runtime_report_markdown(payload)

        self.assertIn("# GroupKey Runtime Report", markdown)
        self.assertIn("- matched_rule_rows=420", markdown)
        self.assertIn("- fine_match_rate=0.5500", markdown)
        self.assertIn("### By Split Coverage", markdown)
        self.assertIn("dataset_split=test, rows=120", markdown)
        self.assertIn("### Unknown Group Preview", markdown)
        self.assertIn("group_key=example.com|SPORTS|other, rows=22", markdown)


if __name__ == "__main__":
    unittest.main()
