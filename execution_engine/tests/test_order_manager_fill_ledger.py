import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from execution_engine.integrations.trading.order_manager import reconcile, submit_order
from execution_engine.shared.io import read_jsonl


class _ImmediateFillClient:
    def place_order(self, payload):
        return {
            "status": "FILLED",
            "order_id": "clob-1",
            "raw": {
                "price": payload["price"],
                "size": payload["size"],
                "created_at": "2026-04-11T00:00:01Z",
            },
        }


class _ReconcileClient:
    def get_open_orders(self):
        return []

    def get_fills(self):
        return [
            {
                "trade_id": "trade-1",
                "order_id": "clob-1",
                "price": 0.5,
                "size": 10.0,
                "created_at": "2026-04-11T00:00:02Z",
            }
        ]


class OrderManagerFillLedgerTest(unittest.TestCase):
    def test_submit_order_records_fill_when_exchange_returns_immediate_fill(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = SimpleNamespace(
                dry_run=False,
                order_ttl_sec=3600,
                min_time_to_close_sec=0,
                fills_path=root / "fills.jsonl",
                logs_path=root / "logs.jsonl",
                metrics_path=root / "metrics.json",
                run_id="RUN_1",
            )
            decision = {
                "decision_id": "decision-1",
                "market_id": "market-1",
                "outcome_index": 0,
                "action": "BUY",
                "order_type": "LIMIT",
                "price_limit": 0.5,
                "amount_usdc": 5.0,
                "market_close_time_utc": "2026-04-11T01:00:00Z",
                "category": "SPORTS",
                "domain": "example.com",
                "market_type": "no_yes",
                "source_host": "example.com",
                "event_id": "event-1",
                "position_side": "OUTCOME_0",
                "rule_group_key": "g",
                "rule_leaf_id": 1,
                "q_pred": 0.7,
                "f_star": 0.2,
                "edge_prob": 0.2,
                "settlement_key": "2026-04-11",
                "cluster_key": "cluster-1",
                "token_id": "token-1",
                "outcome_label": "Yes",
                "best_bid_at_submit": 0.49,
                "best_ask_at_submit": 0.5,
                "tick_size": 0.01,
                "execution_phase": "ENTRY",
                "parent_order_attempt_id": None,
            }
            signal = {
                "order_attempt_id": "attempt-1",
                "order_size_shares": 10.0,
            }

            order = submit_order(cfg, decision, signal, clob_client=_ImmediateFillClient(), token_id="token-1")

            self.assertEqual(order["status"], "FILLED")
            fills = read_jsonl(cfg.fills_path)
            self.assertEqual(len(fills), 1)
            fill = fills[0]
            self.assertEqual(fill["order_attempt_id"], "attempt-1")
            self.assertEqual(fill["clob_order_id"], "clob-1")
            self.assertEqual(fill["amount_usdc"], 5.0)
            self.assertEqual(fill["shares"], 10.0)
            self.assertEqual(fill["price"], 0.5)

    def test_reconcile_skips_duplicate_fill_when_order_already_fully_accounted(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            run_dir = root / "runs" / "2026-04-11" / "RUN_1"
            run_dir.mkdir(parents=True, exist_ok=True)
            (run_dir / "orders.jsonl").write_text(
                json.dumps(
                    {
                        "order_attempt_id": "attempt-1",
                        "clob_order_id": "clob-1",
                        "decision_id": "decision-1",
                        "run_id": "RUN_1",
                        "market_id": "market-1",
                        "outcome_index": 0,
                        "action": "BUY",
                        "amount_usdc": 5.0,
                        "price_limit": 0.5,
                        "status": "FILLED",
                        "created_at_utc": "2026-04-11T00:00:00Z",
                        "updated_at_utc": "2026-04-11T00:00:01Z",
                        "token_id": "token-1",
                        "outcome_label": "Yes",
                        "position_side": "OUTCOME_0",
                        "execution_phase": "ENTRY",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (run_dir / "fills.jsonl").write_text(
                json.dumps(
                    {
                        "fill_id": "immediate:attempt-1",
                        "order_attempt_id": "attempt-1",
                        "clob_order_id": "clob-1",
                        "decision_id": "decision-1",
                        "run_id": "RUN_1",
                        "market_id": "market-1",
                        "outcome_index": 0,
                        "action": "BUY",
                        "amount_usdc": 5.0,
                        "price": 0.5,
                        "shares": 10.0,
                        "pnl_usdc": 0.0,
                        "filled_at_utc": "2026-04-11T00:00:01Z",
                        "token_id": "token-1",
                        "outcome_label": "Yes",
                        "position_side": "OUTCOME_0",
                        "execution_phase": "ENTRY",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            cfg = SimpleNamespace(
                dry_run=False,
                runs_root_dir=root / "runs",
                fills_path=run_dir / "fills.jsonl",
                orders_path=run_dir / "orders.jsonl",
                logs_path=run_dir / "logs.jsonl",
                metrics_path=run_dir / "metrics.json",
            )

            with (
                patch("execution_engine.online.execution.positions.load_open_position_rows", return_value=[]),
                patch("execution_engine.online.execution.positions.rebuild_open_positions_ledger", return_value=[]),
            ):
                reconcile(cfg, clob_client=_ReconcileClient())

            fills = read_jsonl(cfg.fills_path)
            self.assertEqual(len(fills), 1)
            self.assertEqual(fills[0]["fill_id"], "immediate:attempt-1")


if __name__ == "__main__":
    unittest.main()
