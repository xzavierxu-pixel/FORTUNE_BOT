import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from execution_engine.online.reporting.execution_audit import build_run_execution_audit


class ExecutionAuditReportingTest(unittest.TestCase):
    def test_build_run_execution_audit_is_run_scoped(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = SimpleNamespace(
                data_dir=root,
                run_snapshot_selection_path=root / "snapshot_score" / "selection_decisions.csv",
                run_submit_attempts_path=root / "submit_hourly" / "submission_attempts.csv",
                run_submit_orders_submitted_path=root / "submit_hourly" / "orders_submitted.jsonl",
                rejections_path=root / "rejections.jsonl",
                orders_path=root / "orders.jsonl",
            )
            (root / "snapshot_score").mkdir(parents=True, exist_ok=True)
            (root / "submit_hourly").mkdir(parents=True, exist_ok=True)
            (root / "exits").mkdir(parents=True, exist_ok=True)
            cfg.run_snapshot_selection_path.write_text(
                "market_id\nm1\nm2\n",
                encoding="utf-8",
            )
            cfg.run_submit_attempts_path.write_text(
                "market_id,status\nm1,DRY_RUN_SUBMITTED\nm2,MISSING_LIVE_QUOTE\nm3,LIMIT_PRICE_OUTSIDE_RULE_RANGE\n",
                encoding="utf-8",
            )
            cfg.run_submit_orders_submitted_path.write_text(
                json.dumps({"order_attempt_id": "a1", "order_status": "DRY_RUN_SUBMITTED"}) + "\n",
                encoding="utf-8",
            )
            (root / "submit_hourly" / "cancels.jsonl").write_text(
                json.dumps({"order_attempt_id": "a0", "terminal_status": "CANCELED"}) + "\n",
                encoding="utf-8",
            )
            (root / "submit_hourly" / "fills.jsonl").write_text(
                json.dumps({"fill_id": "f1"}) + "\n",
                encoding="utf-8",
            )
            (root / "submit_hourly" / "opened_positions.jsonl").write_text(
                json.dumps({"entry_order_attempt_id": "a1"}) + "\n",
                encoding="utf-8",
            )
            (root / "submit_hourly" / "opened_position_events.jsonl").write_text(
                json.dumps({"order_attempt_id": "a1", "event_type": "OPENED_POSITION"}) + "\n",
                encoding="utf-8",
            )
            (root / "exits" / "manifest.json").write_text(
                json.dumps(
                    {
                        "candidate_count": 2,
                        "submitted_count": 1,
                        "settlement_close_count": 3,
                        "canceled_exit_order_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            (root / "exits" / "orders_submitted.jsonl").write_text(
                json.dumps({"exit_order_attempt_id": "e1", "status": "DRY_RUN_SUBMITTED"}) + "\n",
                encoding="utf-8",
            )
            cfg.rejections_path.write_text(
                json.dumps({"market_id": "m2", "reason_code": "MISSING_LIVE_QUOTE"}) + "\n",
                encoding="utf-8",
            )
            cfg.orders_path.write_text(
                "\n".join(
                    [
                        json.dumps({"order_attempt_id": "a1", "status": "DRY_RUN_SUBMITTED", "updated_at_utc": "2026-04-05T10:00:00Z"}),
                        json.dumps({"order_attempt_id": "a2", "status": "CANCEL_REQUESTED", "updated_at_utc": "2026-04-05T10:01:00Z"}),
                        json.dumps({"order_attempt_id": "a2", "status": "CANCELED", "updated_at_utc": "2026-04-05T10:02:00Z"}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            audit = build_run_execution_audit(cfg)

            self.assertEqual(audit["selection_count"], 2)
            self.assertEqual(audit["submit_only"]["attempted_count"], 3)
            self.assertEqual(audit["submit_only"]["attempt_status_counts"]["DRY_RUN_SUBMITTED"], 1)
            self.assertEqual(audit["submit_only"]["submitted_count"], 1)
            self.assertEqual(audit["monitor_only"]["cancel_count"], 1)
            self.assertEqual(audit["monitor_only"]["fill_count"], 1)
            self.assertEqual(audit["monitor_only"]["opened_position_count"], 1)
            self.assertEqual(audit["monitor_only"]["opened_position_event_count"], 1)
            self.assertEqual(audit["monitor_only"]["latest_run_order_count"], 2)
            self.assertEqual(audit["monitor_only"]["latest_run_order_status_counts"]["CANCELED"], 1)
            self.assertEqual(audit["rejections"]["count"], 1)
            self.assertEqual(audit["rejections"]["reason_counts"]["MISSING_LIVE_QUOTE"], 1)
            self.assertEqual(audit["exit_lifecycle"]["candidate_count"], 2)
            self.assertEqual(audit["exit_lifecycle"]["submitted_count"], 1)
            self.assertEqual(audit["exit_lifecycle"]["settlement_close_count"], 3)
            self.assertEqual(audit["exit_lifecycle"]["canceled_exit_order_count"], 1)


if __name__ == "__main__":
    unittest.main()
