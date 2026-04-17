import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import sys

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
RULE_ENGINE_DIR = ROOT_DIR / "polymarket_rule_engine"
if str(RULE_ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(RULE_ENGINE_DIR))

from execution_engine.online.execution import submission


def _cfg(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        run_id="RUN_001",
        run_mode="submit_window",
        dry_run=False,
        run_submit_manifest_path=root / "submit_window" / "manifest.json",
        run_submit_attempts_path=root / "submit_window" / "submission_attempts.csv",
        run_submit_orders_submitted_path=root / "submit_window" / "orders_submitted.jsonl",
        market_state_cache_path=root / "shared" / "market_state.json",
        state_snapshot_path=root / "shared" / "state_snapshot.json",
        nonce_path=root / "shared" / "nonce.json",
        order_ttl_sec=300,
        max_position_per_market_usdc=100.0,
        max_exposure_per_category_usdc=1000.0,
        max_net_exposure_usdc=1000.0,
        online_capacity_wait_timeout_sec=0,
        online_capacity_wait_poll_sec=1,
        runs_root_dir=root / "runs",
        rejections_path=root / "rejections.jsonl",
        events_path=root / "events.jsonl",
        orders_path=root / "orders.jsonl",
        logs_path=root / "logs.jsonl",
        metrics_path=root / "metrics.json",
        balances_path=root / "balances.json",
        max_open_orders=20,
        online_price_cap_safety_buffer=0.0,
    )


class SubmitSelectionRuntimeTest(unittest.TestCase):
    def test_submit_selected_orders_stops_after_region_restriction(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = _cfg(root)
            selection = pd.DataFrame(
                [
                    {
                        "market_id": "m1",
                        "selected_token_id": "t1",
                        "selected_for_submission": True,
                        "stake_usdc": 1.0,
                        "direction_model": 1,
                        "batch_id": "batch_001",
                        "selected_outcome_label": "Yes",
                        "q_pred": 0.5,
                    },
                    {
                        "market_id": "m2",
                        "selected_token_id": "t2",
                        "selected_for_submission": True,
                        "stake_usdc": 1.0,
                        "direction_model": 1,
                        "batch_id": "batch_001",
                        "selected_outcome_label": "Yes",
                        "q_pred": 0.5,
                    },
                ]
            )
            token_state = pd.DataFrame(
                [
                    {"token_id": "t1"},
                    {"token_id": "t2"},
                ]
            )
            state = SimpleNamespace(
                get_market_exposure=lambda *args, **kwargs: 0.0,
                get_category_exposure=lambda *args, **kwargs: 0.0,
                net_exposure_usdc=0.0,
            )

            submit_calls: list[str] = []

            def fake_submit_order(*args, **kwargs):
                submit_calls.append(str(kwargs.get("token_id") or ""))
                raise RuntimeError(
                    "PolyApiException[status_code=403, error_message={'error': 'Trading restricted in your region'}]"
                )

            with (
                patch.object(submission, "build_clob_client", return_value=object()),
                patch.object(submission, "sweep_expired_orders", return_value=None),
                patch.object(submission, "reconcile", return_value=None),
                patch.object(submission, "build_balance_provider", return_value=SimpleNamespace(get_available_usdc=lambda: 100.0)),
                patch.object(submission, "load_fee_rate", return_value=0.0),
                patch.object(submission, "_wait_for_capacity", return_value=(state, 0, None)),
                patch.object(
                    submission,
                    "get_live_quote",
                    side_effect=[
                        {"best_bid": 0.49, "best_ask": 0.50, "mid": 0.495, "tick_size": 0.01, "quote_source": "clob"},
                    ],
                ),
                patch.object(
                    submission,
                    "build_submission_signal",
                    return_value=(
                        {
                            "decision_id": "d1",
                            "order_attempt_id": "oa1",
                            "reference_mid_price": 0.495,
                            "price_limit": 0.50,
                            "amount_usdc": 1.0,
                        },
                        "",
                    ),
                ),
                patch.object(submission, "check_price_and_liquidity", return_value=(True, "")),
                patch.object(submission, "check_basic_risk", return_value=(True, "")),
                patch.object(submission, "build_decision_from_signal", return_value=({"decision_id": "d1"}, "")),
                patch.object(submission, "record_decision_created", return_value=None),
                patch.object(submission, "record_rejection", return_value=None),
                patch.object(submission, "record_order_submitted", return_value=None),
                patch.object(submission, "refresh_state_snapshot", return_value={}),
                patch.object(submission, "refresh_market_state_cache", return_value={}),
                patch.object(submission, "submit_order", side_effect=fake_submit_order),
            ):
                result = submission.submit_selected_orders(cfg, selection, token_state)

            self.assertEqual(result.abort_reason, "REGION_RESTRICTED")
            self.assertEqual(result.attempted_count, 1)
            self.assertEqual(result.status_counts, {"REGION_RESTRICTED": 1})
            self.assertEqual(submit_calls, ["t1"])

    def test_capacity_reason_ignores_net_exposure_limit_when_config_disabled(self) -> None:
        cfg = SimpleNamespace(
            max_position_per_market_usdc=100.0,
            max_exposure_per_category_usdc=100.0,
            max_net_exposure_usdc=0.0,
        )
        row = {
            "market_id": "m1",
            "stake_usdc": 10.0,
            "direction_model": 1,
            "category": "SPORTS",
        }
        state = SimpleNamespace(
            get_market_exposure=lambda *args, **kwargs: 0.0,
            get_category_exposure=lambda *args, **kwargs: 0.0,
            net_exposure_usdc=9999.0,
        )
        balance_provider = SimpleNamespace(get_available_usdc=lambda: 100000.0)

        reason = submission._capacity_reason(row, state, cfg, balance_provider)

        self.assertIsNone(reason)


if __name__ == "__main__":
    unittest.main()
