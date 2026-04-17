import os
import sys
import unittest

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.reports.build_groupkey_feature_inventory import (
    _docs_dir,
    _alias_candidates,
    apply_inventory_override,
    build_inventory,
    build_inventory_summary_markdown,
    classify_blueprint_pending_row,
)


class GroupKeyFeatureInventoryTest(unittest.TestCase):
    def test_docs_dir_resolves_to_repo_docs(self) -> None:
        docs_dir = _docs_dir()
        self.assertTrue(str(docs_dir).endswith("polymarket_rule_engine\\docs"))
        self.assertEqual(docs_dir.name, "docs")

    def test_classify_blueprint_pending_row_uses_final_disposition_states(self) -> None:
        interaction_row = {
            "feature_name": "ix_full_recent50_minus_expanding_bias",
            "notes": "建议交给树模型自动学习，但显式交互常能加速收敛",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        categorical_row = {
            "feature_name": "domain_id_hash",
            "notes": "可做 categorical 或 target encoding",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        normal_row = {
            "feature_name": "full_group_recent_90days_tail_instability_ratio",
            "notes": "Need tail spread normalized by median or std.",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        unsupported_row = {
            "feature_name": "delta_logit_1h_vs_12h",
            "notes": "更适合极端概率",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        shrinkage_row = {
            "feature_name": "global_recent_90days_bias_se",
            "notes": "建议与 count/shrinkage 一起使用",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        one_hot_row = {
            "feature_name": "category_is_sports",
            "notes": "",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
        }
        history_share_row = {
            "feature_name": "domain_history_share_expanding",
            "notes": "需严格历史滚动",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
            "implemented_in": "",
            "match_quality": "",
            "matched_feature_name": "",
            "serving_asset": "",
        }

        self.assertEqual(classify_blueprint_pending_row(interaction_row)["status"], "intentionally_excluded")
        self.assertEqual(classify_blueprint_pending_row(categorical_row)["status"], "intentionally_excluded")
        self.assertEqual(classify_blueprint_pending_row(normal_row)["status"], "pending_implementation")
        self.assertEqual(classify_blueprint_pending_row(unsupported_row)["status"], "unsupported_now")
        self.assertEqual(classify_blueprint_pending_row(shrinkage_row)["status"], "unsupported_now")
        self.assertEqual(classify_blueprint_pending_row(one_hot_row)["status"], "intentionally_excluded")
        derived = classify_blueprint_pending_row(history_share_row)
        self.assertEqual(derived["status"], "intentionally_excluded")
        self.assertEqual(derived["audit_class"], "E_intentionally_excluded")

    def test_alias_candidates_cover_structural_prefix_variants(self) -> None:
        candidates = _alias_candidates("domain_category_recent_90days_abs_bias_q75")
        self.assertIn("domain_x_category_recent_90days_abs_bias_q75", candidates)
        self.assertIn("domain_x_category_recent_90days_abs_bias_p75", candidates)

    def test_apply_inventory_override_replaces_status_and_appends_notes(self) -> None:
        row = {
            "feature_name": "category_id",
            "source_table": "blueprint",
            "status": "pending_implementation",
            "audit_class": "B_keep_but_later",
            "notes": "建议直接 categorical",
        }
        overridden = apply_inventory_override(
            row,
            {
                ("category_id", "blueprint"): {
                    "status": "intentionally_excluded",
                    "audit_class": "E_intentionally_excluded",
                    "notes_append": "Override note",
                }
            },
        )
        self.assertEqual(overridden["status"], "intentionally_excluded")
        self.assertEqual(overridden["audit_class"], "E_intentionally_excluded")
        self.assertIn("Override note", overridden["notes"])

    def test_build_inventory_uses_final_statuses_and_summary_markdown(self) -> None:
        inventory = build_inventory("offline")
        statuses = set(inventory["status"].astype(str))

        self.assertNotIn("pending", statuses)
        self.assertIn("implemented_exact", statuses)
        self.assertIn("intentionally_excluded", statuses)
        self.assertIn("unsupported_now", statuses)
        self.assertTrue({"implemented_approximate", "pending_implementation"} & statuses)

        summary = build_inventory_summary_markdown(inventory)
        self.assertIn("# GroupKey Feature Inventory Summary", summary)
        self.assertIn("- implemented_exact=", summary)
        self.assertTrue(
            "- implemented_approximate=" in summary or "- pending_implementation=" in summary
        )
        self.assertIn("- unsupported_now=", summary)


if __name__ == "__main__":
    unittest.main()
