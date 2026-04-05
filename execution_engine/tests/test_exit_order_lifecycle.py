import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from execution_engine.online.exits.settlement import settle_resolved_positions
from execution_engine.online.exits.submit_exit import submit_pending_exit_orders
from execution_engine.shared.io import read_jsonl


class ExitLifecycleTest(unittest.TestCase):
    def test_submit_pending_exit_orders_ignores_terminal_exit_and_resubmits(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "2026-04-01" / "RUN_1"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "orders.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "order_attempt_id": "entry-1",
                                "action": "BUY",
                                "execution_phase": "ENTRY",
                                "created_at_utc": "2026-04-01T00:00:00Z",
                                "expiration_seconds": 60,
                                "market_id": "m1",
                                "token_id": "t1",
                                "outcome_index": 0,
                            }
                        ),
                        json.dumps(
                            {
                                "order_attempt_id": "exit-old",
                                "action": "SELL",
                                "execution_phase": "EXIT",
                                "status": "CANCELED",
                                "parent_order_attempt_id": "entry-1",
                                "created_at_utc": "2026-04-01T00:02:00Z",
                            }
                        ),
                    ]
                ),
                encoding="utf-8",
            )
            open_positions_path = root / "open_positions.jsonl"
            open_positions_path.write_text(
                json.dumps(
                    {
                        "market_id": "m1",
                        "token_id": "t1",
                        "outcome_index": 0,
                        "entry_order_attempt_id": "entry-1",
                        "filled_shares": 10.0,
                        "filled_amount_usdc": 5.0,
                        "status": "OPEN",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = SimpleNamespace(
                runs_root_dir=root,
                data_dir=run_dir,
                run_id="RUN_1",
                run_mode="test",
                nonce_path=root / "nonce.json",
                orders_path=root / "orders_live.jsonl",
                decisions_path=root / "decisions.jsonl",
                events_path=root / "events.jsonl",
                logs_path=root / "logs.jsonl",
                metrics_path=root / "metrics.json",
                rejections_path=root / "rejections.jsonl",
                fills_path=root / "fills.jsonl",
                open_positions_path=open_positions_path,
                dry_run=True,
                clob_enabled=False,
                order_ttl_sec=300,
                order_usdc=5.0,
                min_time_to_close_sec=0,
            )
            with (
                patch("execution_engine.online.exits.submit_exit.StateStore"),
                patch("execution_engine.online.exits.submit_exit.record_decision_created"),
                patch("execution_engine.online.exits.submit_exit.record_order_submitted"),
            ):
                result = submit_pending_exit_orders(cfg)

            self.assertEqual(result.candidate_count, 1)
            self.assertEqual(result.submitted_count, 1)

    def test_settlement_close_calls_cancel_order_for_live_exit(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "2026-04-01" / "RUN_1"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "orders.jsonl").write_text(
                json.dumps(
                    {
                        "order_attempt_id": "exit-1",
                        "execution_phase": "EXIT",
                        "status": "ACKED",
                        "parent_order_attempt_id": "entry-1",
                        "clob_order_id": "clob-exit-1",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            resolved_labels_path = root / "resolved_labels.csv"
            resolved_labels_path.write_text(
                "market_id,resolved_outcome_index,resolved_closed_time_utc\nm1,0,2026-04-02T00:00:00Z\n",
                encoding="utf-8",
            )
            open_positions_path = root / "open_positions.jsonl"
            open_positions_path.write_text(
                json.dumps(
                    {
                        "market_id": "m1",
                        "token_id": "t1",
                        "outcome_index": 0,
                        "outcome_label": "YES",
                        "entry_order_attempt_id": "entry-1",
                        "filled_shares": 10.0,
                        "filled_amount_usdc": 4.0,
                        "status": "OPEN",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            class DummyState:
                def record_fill(self, record):
                    return None

                def record_event(self, record):
                    return None

            class DummyClient:
                def __init__(self):
                    self.canceled = []

                def cancel_order(self, order_id: str):
                    self.canceled.append(order_id)

            client = DummyClient()
            cfg = SimpleNamespace(
                runs_root_dir=root,
                resolved_labels_path=resolved_labels_path,
                open_positions_path=open_positions_path,
                data_dir=run_dir,
                run_id="RUN_1",
                fills_path=root / "fills.jsonl",
                events_path=root / "events.jsonl",
                logs_path=root / "logs.jsonl",
                state_snapshot_path=root / "state_snapshot.json",
                orders_path=root / "orders_live.jsonl",
                dry_run=False,
                clob_enabled=True,
            )
            with (
                patch("execution_engine.online.exits.settlement.StateStore", return_value=DummyState()),
                patch("execution_engine.online.exits.settlement.build_clob_client", return_value=client),
            ):
                result = settle_resolved_positions(cfg)

            self.assertEqual(result.canceled_exit_order_count, 1)
            self.assertEqual(client.canceled, ["clob-exit-1"])
            recorded_orders = read_jsonl(cfg.orders_path)
            self.assertEqual([row["status"] for row in recorded_orders], ["CANCEL_REQUESTED", "CANCELED"])


if __name__ == "__main__":
    unittest.main()
