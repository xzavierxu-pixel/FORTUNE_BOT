import os
import shutil
import sys
import unittest
from pathlib import Path
from uuid import uuid4

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.audits.rule_generation_audit import (
    build_rule_generation_audit_payload,
    write_rule_generation_audit,
)


class RuleGenerationAuditTest(unittest.TestCase):
    def _make_tempdir(self) -> Path:
        path = Path("polymarket_rule_engine/tests/_tmp_runtime") / f"rule-audit-{uuid4().hex}"
        path.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(path, ignore_errors=True))
        return path

    def test_build_payload_and_write_outputs_include_history_artifacts(self) -> None:
        artifact_paths = build_artifact_paths("offline")

        rules_df = pd.DataFrame(
            [
                {"group_key": "a|SPORTS|other", "price_min": 0.4, "price_max": 0.5, "horizon_hours": 6},
                {"group_key": "a|SPORTS|other", "price_min": 0.5, "price_max": 0.6, "horizon_hours": 12},
            ]
        )
        report_df = pd.DataFrame(
            [
                {"group_key": "a|SPORTS|other", "selection_status": "keep", "group_snapshot_rows": 40, "group_unique_markets": 20},
                {"group_key": "b|POLITICS|other", "selection_status": "drop", "group_snapshot_rows": 10, "group_unique_markets": 6},
            ]
        )
        group_features = pd.DataFrame([{"group_key": "a|SPORTS|other", "group_match_found_default": 1}])
        fine_features = pd.DataFrame(
            [{"group_key": "a|SPORTS|other", "price_bin": "0.40-0.50", "horizon_hours": 6, "q_full": 0.6}]
        )
        funnel_summary = {
            "snapshot_funnel": [
                {"stage": "after_snapshot_quality", "snapshot_rows": 60, "unique_markets": 30},
                {"stage": "after_rule_selection", "snapshot_rows": 40, "unique_markets": 20},
            ],
            "rule_selection": {
                "selected_rule_count": 2,
                "rule_bucket_status_counts": {"keep": 2},
                "selection_status_market_impact": [
                    {"selection_status": "keep", "snapshot_rows": 40, "unique_markets": 20},
                    {"selection_status": "drop", "snapshot_rows": 20, "unique_markets": 10},
                ],
            },
        }

        tmpdir = self._make_tempdir()
        with self.subTest("payload_and_outputs"):
            local_paths = artifact_paths
            edge_dir = tmpdir / "edge"
            audit_dir = tmpdir / "audit"
            edge_dir.mkdir(parents=True, exist_ok=True)
            audit_dir.mkdir(parents=True, exist_ok=True)

            object.__setattr__(local_paths, "edge_dir", edge_dir)
            object.__setattr__(local_paths, "audit_dir", audit_dir)
            object.__setattr__(local_paths, "rules_path", edge_dir / "trading_rules.csv")
            object.__setattr__(local_paths, "group_serving_features_path", edge_dir / "group_serving_features.parquet")
            object.__setattr__(local_paths, "fine_serving_features_path", edge_dir / "fine_serving_features.parquet")
            object.__setattr__(local_paths, "serving_feature_defaults_path", edge_dir / "serving_feature_defaults.json")
            object.__setattr__(
                local_paths,
                "history_feature_paths",
                {level_name: edge_dir / path.name for level_name, path in local_paths.history_feature_paths.items()},
            )
            object.__setattr__(local_paths, "rule_report_path", audit_dir / "all_trading_rule_audit_report.csv")
            object.__setattr__(local_paths, "rule_funnel_summary_path", audit_dir / "rule_funnel_summary.json")
            object.__setattr__(local_paths, "rule_generation_audit_json_path", audit_dir / "rule_generation_audit.json")
            object.__setattr__(local_paths, "rule_generation_audit_markdown_path", audit_dir / "rule_generation_audit.md")
            object.__setattr__(local_paths, "artifact_inventory_json_path", audit_dir / "artifact_inventory.json")
            object.__setattr__(local_paths, "artifact_inventory_markdown_path", audit_dir / "artifact_inventory.md")

            rules_df.to_csv(local_paths.rules_path, index=False)
            report_df.to_csv(local_paths.rule_report_path, index=False)
            group_features.to_parquet(local_paths.group_serving_features_path, index=False)
            fine_features.to_parquet(local_paths.fine_serving_features_path, index=False)
            local_paths.serving_feature_defaults_path.write_text(
                '{"fallback_policy":"group_key_aggregates","fine_feature_defaults":{}}',
                encoding="utf-8",
            )
            for level_name, path in local_paths.history_feature_paths.items():
                pd.DataFrame([{"level_key": level_name, "metric": 1.0}]).to_parquet(path, index=False)

            payload = build_rule_generation_audit_payload(
                artifact_paths=local_paths,
                rules_df=rules_df,
                report_df=report_df,
                group_serving_features=group_features,
                fine_serving_features=fine_features,
                rule_funnel_summary=funnel_summary,
                split_summary={"rows_by_split": {"train": 60}},
                rule_training_summary={"selected_rules": 2},
                debug_filters={"max_rows": 1000},
            )
            write_rule_generation_audit(artifact_paths=local_paths, payload=payload)

            self.assertEqual(payload["rules_summary"]["rule_rows"], 2)
            self.assertEqual(payload["rules_summary"]["kept_group_keys"], 1)
            self.assertEqual(payload["artifact_summary"]["missing_history_artifacts"], [])
            self.assertIn("schema_preview", payload["artifact_inventory"][0])
            self.assertIn("modified_time_utc", payload["artifact_inventory"][0])
            self.assertTrue(local_paths.rule_generation_audit_json_path.exists())
            self.assertTrue(local_paths.rule_generation_audit_markdown_path.exists())
            self.assertTrue(local_paths.artifact_inventory_json_path.exists())
            self.assertTrue(local_paths.artifact_inventory_markdown_path.exists())
            markdown = local_paths.rule_generation_audit_markdown_path.read_text(encoding="utf-8")
            self.assertIn("# Rule Generation Audit", markdown)
            self.assertIn("history_full_group", markdown)
            inventory_markdown = local_paths.artifact_inventory_markdown_path.read_text(encoding="utf-8")
            self.assertIn("# Offline Artifact Inventory", inventory_markdown)
            self.assertIn("artifact=history_full_group", inventory_markdown)


if __name__ == "__main__":
    unittest.main()
