import unittest
from types import SimpleNamespace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd

from execution_engine.online.execution.live_quote import quote_from_clob
from execution_engine.online.execution.pricing import build_submission_signal
from execution_engine.online.scoring.live import run_live_inference
from execution_engine.online.scoring.rules import ServingFeatureBundle
from execution_engine.online.scoring.selection import allocate_candidates, build_selection_decisions
from execution_engine.online.pipeline.eligibility import apply_live_price_filter, apply_structural_coarse_filter
from execution_engine.integrations.trading.state_machine import can_transition
from execution_engine.runtime.validation import check_basic_risk
from execution_engine.shared.io import read_jsonl, write_jsonl
from execution_engine.shared.time import to_iso, utc_now


class SubmitPricingGuardsTest(unittest.TestCase):
    def test_quote_from_clob_uses_economic_best_levels_not_first_levels(self) -> None:
        class FakeClobClient:
            def get_order_book(self, token_id: str) -> dict[str, object]:
                return {
                    "bids": [{"price": "0.01"}, {"price": "0.47"}, {"price": "0.30"}],
                    "asks": [{"price": "0.99"}, {"price": "0.53"}, {"price": "0.70"}],
                    "min_order_size": "5",
                }

            def get_midpoint(self, token_id: str) -> float:
                return 0.5

        quote = quote_from_clob(FakeClobClient(), "token-1")

        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual(quote["best_bid"], 0.47)
        self.assertEqual(quote["best_ask"], 0.53)
        self.assertAlmostEqual(quote["spread"], 0.06, places=6)

    def test_build_submission_signal_rejects_boundary_top_of_book(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=1,
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.5,
        }
        quote = {
            "best_bid": 0.01,
            "best_ask": 0.99,
            "tick_size": 0.01,
            "min_order_size": 5.0,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertIsNone(signal)
        self.assertEqual(reason, "ABNORMAL_TOP_OF_BOOK")

    def test_build_submission_signal_accepts_non_boundary_quote(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=1,
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "selected_outcome_label": "Under",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.4,
            "direction_model": -1,
            "category": "SPORTS",
            "domain": "example.com",
            "market_type": "over_under",
            "source_host": "feed.example.com",
            "position_side": "OUTCOME_1",
            "rule_group_key": "example.com|SPORTS|over_under",
            "rule_leaf_id": 1,
            "growth_score": 1.0,
            "f_exec": 0.02,
            "settlement_key": "2026-03-25",
            "cluster_key": "example.com|SPORTS|2026-03-25",
        }
        quote = {
            "best_bid": 0.29,
            "best_ask": 0.51,
            "tick_size": 0.01,
            "min_order_size": 5.0,
            "mid": 0.4,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertEqual(reason, "OK")
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal["price_limit"], 0.3)
        self.assertEqual(signal["best_bid_at_submit"], 0.29)
        self.assertEqual(signal["best_ask_at_submit"], 0.51)
        self.assertEqual(signal["source_host"], "feed.example.com")

    def test_build_submission_signal_uses_two_ticks_from_best_bid(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=2,
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.4,
        }
        quote = {
            "best_bid": 0.29,
            "best_ask": 0.51,
            "tick_size": 0.01,
            "min_order_size": 5.0,
            "mid": 0.4,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertEqual(reason, "OK")
        self.assertIsNotNone(signal)
        assert signal is not None
        self.assertEqual(signal["price_limit"], 0.31)

    def test_build_submission_signal_allows_exact_half_point_spread(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=1,
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "selected_outcome_label": "Under",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.4,
            "direction_model": -1,
        }
        quote = {
            "best_bid": 0.3,
            "best_ask": 0.8,
            "tick_size": 0.01,
            "min_order_size": 5.0,
            "mid": 0.55,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertEqual(reason, "OK")
        self.assertIsNotNone(signal)

    def test_build_submission_signal_rejects_limit_price_below_rule_min(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=0,
            online_limit_ticks_below_best_bid=0,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.21,
        }
        quote = {
            "best_bid": 0.19,
            "best_ask": 0.23,
            "tick_size": 0.01,
            "min_order_size": 5.0,
            "mid": 0.21,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertIsNone(signal)
        self.assertEqual(reason, "LIMIT_PRICE_OUTSIDE_RULE_RANGE")

    def test_build_submission_signal_rejects_limit_price_above_rule_max(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_from_best_bid=0,
            online_limit_ticks_below_best_bid=0,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        row = {
            "selected_token_id": "token-1",
            "market_id": "market-1",
            "stake_usdc": 2.0,
            "q_pred": 0.95,
            "price": 0.79,
        }
        quote = {
            "best_bid": 0.81,
            "best_ask": 0.83,
            "tick_size": 0.01,
            "min_order_size": 5.0,
            "mid": 0.82,
        }

        signal, reason = build_submission_signal(row, quote, cfg, fee_rate=0.001)

        self.assertIsNone(signal)
        self.assertEqual(reason, "LIMIT_PRICE_OUTSIDE_RULE_RANGE")


class StructuralFilterTest(unittest.TestCase):
    def test_structural_filter_rejects_uma_resolution_statuses(self) -> None:
        cfg = SimpleNamespace(
            online_coarse_horizon_slack_hours=0.1,
            online_universe_window_hours=24.0,
        )
        markets = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "accepting_orders": True,
                    "remaining_hours": 4.0,
                    "end_time_utc": "2026-03-20T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "uma_resolution_statuses": "[\"proposed\"]",
                }
            ]
        )
        rules = pd.DataFrame(
            [
                {
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "h_min": 1.0,
                    "h_max": 12.0,
                }
            ]
        )

        result = apply_structural_coarse_filter(cfg, markets, rules)

        self.assertTrue(result.direct_candidates.empty)
        self.assertEqual(result.rejected.iloc[0]["coarse_filter_reason"], "uma_resolution_status_filtered")

    def test_structural_filter_enforces_hard_online_horizon_cap(self) -> None:
        cfg = SimpleNamespace(
            online_coarse_horizon_slack_hours=0.1,
            online_universe_window_hours=12.0,
        )
        markets = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "accepting_orders": True,
                    "remaining_hours": 36.0,
                    "end_time_utc": "2026-03-20T00:00:00Z",
                    "domain": "example.com",
                    "category": "POLITICS",
                    "market_type": "no_yes",
                    "uma_resolution_statuses": "[]",
                }
            ]
        )
        rules = pd.DataFrame(
            [
                {
                    "domain": "example.com",
                    "category": "POLITICS",
                    "market_type": "no_yes",
                    "h_min": 24.0,
                    "h_max": 1000.0,
                }
            ]
        )

        result = apply_structural_coarse_filter(cfg, markets, rules)

        self.assertTrue(result.direct_candidates.empty)
        self.assertEqual(result.rejected.iloc[0]["coarse_filter_reason"], "outside_trading_horizon")


class LiveFilterCoverageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = SimpleNamespace(
            online_token_state_max_age_sec=300,
            rule_engine_min_price=0.2,
            rule_engine_max_price=0.8,
        )
        self.rules = pd.DataFrame(
            [
                {
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "h_min": 1.0,
                    "h_max": 12.0,
                    "price_min": 0.45,
                    "price_max": 0.55,
                }
            ]
        )

    def test_live_filter_does_not_reject_when_group_family_matches_but_live_mid_is_outside_fine_band(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "remaining_hours": 4.0,
                    "selected_reference_token_id": "ref-token",
                    "selected_token_id": "other-token",
                    "best_bid": 0.2,
                    "best_ask": 0.3,
                    "last_trade_price": 0.0,
                    "order_price_min_tick_size": 0.001,
                }
            ]
        )
        token_state = pd.DataFrame(
            [
                {
                    "token_id": "ref-token",
                    "latest_event_at_utc": to_iso(utc_now()),
                    "best_bid": 0.2,
                    "best_ask": 0.3,
                    "mid_price": 0.25,
                    "raw_event_count": 3,
                    "tick_size": 0.001,
                }
            ]
        )

        result = apply_live_price_filter(self.cfg, candidates, self.rules, token_state)

        self.assertEqual(len(result.eligible), 1)
        self.assertEqual(int(result.state_counts.get("LIVE_ELIGIBLE", 0)), 1)
        self.assertEqual(result.eligible.iloc[0]["live_filter_reason"], "live_state_ok")

    def test_live_filter_accepts_when_reference_token_live_mid_is_in_band(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "remaining_hours": 4.0,
                    "selected_reference_token_id": "ref-token",
                    "selected_token_id": "other-token",
                    "best_bid": 0.49,
                    "best_ask": 0.51,
                    "last_trade_price": 0.0,
                    "order_price_min_tick_size": 0.001,
                }
            ]
        )
        token_state = pd.DataFrame(
            [
                {
                    "token_id": "ref-token",
                    "latest_event_at_utc": to_iso(utc_now()),
                    "best_bid": 0.49,
                    "best_ask": 0.51,
                    "mid_price": 0.5,
                    "raw_event_count": 3,
                    "tick_size": 0.001,
                }
            ]
        )

        result = apply_live_price_filter(self.cfg, candidates, self.rules, token_state)

        self.assertEqual(len(result.eligible), 1)
        self.assertEqual(int(result.state_counts.get("LIVE_ELIGIBLE", 0)), 1)

    def test_live_filter_keeps_candidate_when_live_mid_is_outside_exact_rule_band(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "remaining_hours": 4.0,
                    "selected_reference_token_id": "ref-token",
                    "selected_token_id": "other-token",
                    "best_bid": 0.39,
                    "best_ask": 0.41,
                    "last_trade_price": 0.0,
                    "order_price_min_tick_size": 0.001,
                }
            ]
        )
        token_state = pd.DataFrame(
            [
                {
                    "token_id": "ref-token",
                    "latest_event_at_utc": to_iso(utc_now()),
                    "best_bid": 0.39,
                    "best_ask": 0.41,
                    "mid_price": 0.4,
                    "raw_event_count": 3,
                    "tick_size": 0.001,
                }
            ]
        )

        result = apply_live_price_filter(self.cfg, candidates, self.rules, token_state)

        self.assertEqual(len(result.eligible), 1)
        self.assertEqual(int(result.state_counts.get("LIVE_ELIGIBLE", 0)), 1)
        self.assertEqual(result.eligible.iloc[0]["live_filter_reason"], "live_state_ok")

    def test_live_filter_rejects_wide_websocket_spread_before_rule_match(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "remaining_hours": 4.0,
                    "selected_reference_token_id": "ref-token",
                    "selected_token_id": "other-token",
                    "best_bid": 0.01,
                    "best_ask": 0.99,
                    "last_trade_price": 0.0,
                    "order_price_min_tick_size": 0.001,
                }
            ]
        )
        token_state = pd.DataFrame(
            [
                {
                    "token_id": "ref-token",
                    "latest_event_at_utc": to_iso(utc_now()),
                    "best_bid": 0.01,
                    "best_ask": 0.99,
                    "mid_price": 0.5,
                    "raw_event_count": 3,
                    "tick_size": 0.001,
                }
            ]
        )

        result = apply_live_price_filter(self.cfg, candidates, self.rules, token_state)

        self.assertTrue(result.eligible.empty)
        self.assertEqual(int(result.state_counts.get("LIVE_SPREAD_TOO_WIDE", 0)), 1)
        self.assertEqual(result.rejected.iloc[0]["live_filter_reason"], "live_spread_above_threshold")

    def test_live_filter_allows_price_on_closed_interval_boundary(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "remaining_hours": 4.0,
                    "selected_reference_token_id": "ref-token",
                    "selected_token_id": "other-token",
                    "best_bid": 0.19,
                    "best_ask": 0.21,
                    "last_trade_price": 0.0,
                    "order_price_min_tick_size": 0.001,
                }
            ]
        )
        token_state = pd.DataFrame(
            [
                {
                    "token_id": "ref-token",
                    "latest_event_at_utc": to_iso(utc_now()),
                    "best_bid": 0.19,
                    "best_ask": 0.21,
                    "mid_price": 0.2,
                    "raw_event_count": 3,
                    "tick_size": 0.001,
                }
            ]
        )
        rules = pd.DataFrame(
            [
                {
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "over_under",
                    "h_min": 1.0,
                    "h_max": 12.0,
                    "price_min": 0.2,
                    "price_max": 0.3,
                }
            ]
        )

        result = apply_live_price_filter(self.cfg, candidates, rules, token_state)

        self.assertEqual(len(result.eligible), 1)
        self.assertEqual(int(result.state_counts.get("LIVE_ELIGIBLE", 0)), 1)


class OrderManagerSweepTest(unittest.TestCase):
    def test_sweep_expired_orders_skips_error_orders(self) -> None:
        from execution_engine.integrations.trading.order_manager import sweep_expired_orders

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runs_root = root / "runs"
            run_dir = runs_root / "2026-03-31" / "RUN_001"
            orders_file = run_dir / "orders.jsonl"
            original_order = {
                "order_attempt_id": "order-1",
                "status": "ERROR",
                "created_at_utc": "2026-03-31T00:00:00Z",
                "updated_at_utc": "2026-03-31T00:05:00Z",
                "expiration_seconds": 60,
                "clob_order_id": "clob-1",
            }
            write_jsonl(orders_file, [original_order])

            cfg = SimpleNamespace(
                runs_root_dir=runs_root,
                order_ttl_sec=60,
                dry_run=False,
                orders_path=root / "active_orders.jsonl",
                logs_path=root / "logs.jsonl",
                metrics_path=root / "metrics.json",
            )

            sweep_expired_orders(cfg)

            self.assertEqual(read_jsonl(cfg.orders_path), [])
            self.assertEqual(read_jsonl(cfg.logs_path), [])

    def test_sweep_expired_orders_dry_run_records_cancel_lifecycle(self) -> None:
        from execution_engine.integrations.trading.order_manager import sweep_expired_orders

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runs_root = root / "runs"
            run_dir = runs_root / "2026-03-31" / "RUN_001"
            orders_file = run_dir / "orders.jsonl"
            orders_file.parent.mkdir(parents=True, exist_ok=True)
            original_order = {
                "order_attempt_id": "order-1",
                "status": "DRY_RUN_SUBMITTED",
                "created_at_utc": "2026-03-31T00:00:00Z",
                "updated_at_utc": "2026-03-31T00:00:00Z",
                "expiration_seconds": 60,
                "action": "BUY",
                "amount_usdc": 5.0,
            }
            write_jsonl(orders_file, [original_order])

            cfg = SimpleNamespace(
                runs_root_dir=runs_root,
                order_ttl_sec=60,
                dry_run=True,
                orders_path=root / "active_orders.jsonl",
                logs_path=root / "logs.jsonl",
                metrics_path=root / "metrics.json",
            )

            sweep_expired_orders(cfg)

            order_states = read_jsonl(cfg.orders_path)
            self.assertEqual([row["status"] for row in order_states], ["CANCEL_REQUESTED", "CANCELED"])
            self.assertEqual([row["status_reason"] for row in order_states], ["TTL_EXPIRED", "TTL_EXPIRED"])

    def test_request_cancel_persists_dry_run_cancel(self) -> None:
        from execution_engine.integrations.trading.order_manager import request_cancel

        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = SimpleNamespace(
                dry_run=True,
                orders_path=root / "orders.jsonl",
                logs_path=root / "logs.jsonl",
                metrics_path=root / "metrics.json",
            )
            order = {
                "order_attempt_id": "order-1",
                "status": "ACKED",
                "created_at_utc": "2026-03-31T00:00:00Z",
                "updated_at_utc": "2026-03-31T00:00:00Z",
            }

            terminal = request_cancel(order, cfg, reason="MANUAL_CANCEL")

            self.assertEqual(terminal["status"], "CANCELED")
            self.assertEqual(read_jsonl(cfg.orders_path)[-1]["status"], "CANCELED")
            self.assertEqual(
                [row["status"] for row in read_jsonl(cfg.orders_path)],
                ["CANCEL_REQUESTED", "CANCELED"],
            )


class AllocationBalanceSourceTest(unittest.TestCase):
    def test_live_clob_client_scales_micro_usdc_balance_to_usdc(self) -> None:
        from execution_engine.integrations.trading.clob_client import LiveClobClient

        cfg = SimpleNamespace(
            clob_signature_type=1,
        )
        client = LiveClobClient(cfg)
        client._client = SimpleNamespace(
            get_balance_allowance=lambda params: {
                "balance": "99947134",
                "allowance": "99947134",
            }
        )
        client._types = {
            "AssetType": SimpleNamespace(COLLATERAL="COLLATERAL"),
            "BalanceAllowanceParams": lambda **kwargs: kwargs,
        }

        self.assertAlmostEqual(client.get_balance_usdc(), 99.947134, places=6)

    def test_build_balance_provider_uses_live_clob_client_when_enabled(self) -> None:
        from execution_engine.integrations.providers.balance_provider import ClobBalanceProvider, build_balance_provider

        cfg = SimpleNamespace(
            dry_run=False,
            clob_enabled=True,
            balances_path=Path("unused.json"),
        )
        sentinel_client = object()

        with patch(
            "execution_engine.integrations.trading.clob_client.build_clob_client",
            return_value=sentinel_client,
        ):
            provider = build_balance_provider(cfg)

        self.assertIsInstance(provider, ClobBalanceProvider)
        self.assertIs(provider.clob_client, sentinel_client)

    def test_allocate_candidates_uses_live_available_balance_instead_of_initial_bankroll(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "edge_final": 0.8,
                    "f_exec": 0.5,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                }
            ]
        )
        cfg = SimpleNamespace(
            dry_run=False,
            clob_enabled=True,
            initial_bankroll_usdc=10.0,
            max_trade_amount_usdc=50.0,
            online_min_growth_score=0.2,
        )
        state = SimpleNamespace(net_exposure_usdc=0.0, seen_held_event=lambda event_id: False)
        bt_cfg = SimpleNamespace(max_position_f=1.0)

        class StubBalanceProvider:
            def get_available_usdc(self) -> float:
                return 40.0

        with patch("execution_engine.online.scoring.selection.build_balance_provider", return_value=StubBalanceProvider()):
            result = allocate_candidates(candidates, cfg, state, bt_cfg)

        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(float(result.iloc[0]["stake_usdc"]), 20.0, places=6)

    def test_allocate_candidates_filters_growth_score_at_threshold(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "edge_final": 0.8,
                    "f_exec": 0.5,
                    "growth_score": 0.2,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "market-2",
                    "snapshot_time": "2026-03-31T00:01:00Z",
                    "edge_final": 0.7,
                    "f_exec": 0.5,
                    "growth_score": 0.21,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
            ]
        )
        cfg = SimpleNamespace(
            dry_run=False,
            clob_enabled=True,
            initial_bankroll_usdc=10.0,
            max_trade_amount_usdc=50.0,
            online_min_growth_score=0.2,
        )
        state = SimpleNamespace(net_exposure_usdc=0.0, seen_held_event=lambda event_id: False)
        bt_cfg = SimpleNamespace(max_position_f=1.0)

        class StubBalanceProvider:
            def get_available_usdc(self) -> float:
                return 40.0

        with patch("execution_engine.online.scoring.selection.build_balance_provider", return_value=StubBalanceProvider()):
            result = allocate_candidates(candidates, cfg, state, bt_cfg)

        self.assertEqual(list(result["market_id"]), ["market-2"])

    def test_allocate_candidates_allows_positive_growth_when_threshold_is_zero(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "edge_final": 0.8,
                    "f_exec": 0.5,
                    "growth_score": 0.0,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "market-2",
                    "snapshot_time": "2026-03-31T00:01:00Z",
                    "edge_final": 0.7,
                    "f_exec": 0.5,
                    "growth_score": 0.01,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
            ]
        )
        cfg = SimpleNamespace(
            dry_run=False,
            clob_enabled=True,
            initial_bankroll_usdc=10.0,
            max_trade_amount_usdc=50.0,
            online_min_growth_score=0.0,
        )
        state = SimpleNamespace(net_exposure_usdc=0.0, seen_held_event=lambda event_id: False)
        bt_cfg = SimpleNamespace(max_position_f=1.0)

        class StubBalanceProvider:
            def get_available_usdc(self) -> float:
                return 40.0

        with patch("execution_engine.online.scoring.selection.build_balance_provider", return_value=StubBalanceProvider()):
            result = allocate_candidates(candidates, cfg, state, bt_cfg)

        self.assertEqual(list(result["market_id"]), ["market-2"])

    def test_allocate_candidates_skips_held_and_duplicate_event_ids(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-held",
                    "event_id": "event-held",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "edge_final": 0.9,
                    "f_exec": 0.4,
                    "growth_score": 0.5,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "market-1",
                    "event_id": "event-1",
                    "snapshot_time": "2026-03-31T00:01:00Z",
                    "edge_final": 0.8,
                    "f_exec": 0.4,
                    "growth_score": 0.5,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
                {
                    "market_id": "market-2",
                    "event_id": "event-1",
                    "snapshot_time": "2026-03-31T00:02:00Z",
                    "edge_final": 0.7,
                    "f_exec": 0.4,
                    "growth_score": 0.5,
                    "source_host": "example.com",
                    "category": "SPORTS",
                    "closedTime": "2026-04-01T00:00:00Z",
                },
            ]
        )
        cfg = SimpleNamespace(
            dry_run=False,
            clob_enabled=True,
            initial_bankroll_usdc=10.0,
            max_trade_amount_usdc=50.0,
            online_min_growth_score=0.0,
        )
        state = SimpleNamespace(
            net_exposure_usdc=0.0,
            seen_held_event=lambda event_id: event_id == "event-held",
        )
        bt_cfg = SimpleNamespace(max_position_f=1.0)

        class StubBalanceProvider:
            def get_available_usdc(self) -> float:
                return 40.0

        with patch("execution_engine.online.scoring.selection.build_balance_provider", return_value=StubBalanceProvider()):
            result = allocate_candidates(candidates, cfg, state, bt_cfg)

        self.assertEqual(list(result["market_id"]), ["market-1"])

    def test_build_selection_decisions_marks_event_position_and_duplicate_reasons(self) -> None:
        model_outputs = pd.DataFrame(
            [
                {
                    "market_id": "market-held",
                    "event_id": "event-held",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "rule_group_key": "group",
                    "rule_leaf_id": 1,
                    "growth_score": 0.4,
                },
                {
                    "market_id": "market-picked",
                    "event_id": "event-picked",
                    "snapshot_time": "2026-03-31T00:01:00Z",
                    "rule_group_key": "group",
                    "rule_leaf_id": 2,
                    "growth_score": 0.4,
                },
                {
                    "market_id": "market-dup",
                    "event_id": "event-picked",
                    "snapshot_time": "2026-03-31T00:02:00Z",
                    "rule_group_key": "group",
                    "rule_leaf_id": 3,
                    "growth_score": 0.4,
                },
            ]
        )
        selected = pd.DataFrame(
            [
                {
                    "market_id": "market-picked",
                    "event_id": "event-picked",
                    "snapshot_time": "2026-03-31T00:01:00Z",
                    "rule_group_key": "group",
                    "rule_leaf_id": 2,
                    "selected_token_id": "token-picked",
                    "selected_outcome_label": "Yes",
                    "stake_usdc": 1.0,
                    "growth_score": 0.4,
                }
            ]
        )
        cfg = SimpleNamespace(run_id="test-run")

        decisions = build_selection_decisions(
            model_outputs,
            selected,
            cfg,
            min_growth_score=0.0,
            held_event_ids={"event-held"},
        )

        reasons = dict(zip(decisions["market_id"], decisions["selection_reason"]))
        self.assertEqual(reasons["market-held"], "event_position_exists")
        self.assertEqual(reasons["market-picked"], "allocated")
        self.assertEqual(reasons["market-dup"], "event_already_selected")


class LiveInferenceGrowthColumnsTest(unittest.TestCase):
    def test_run_live_inference_merges_growth_columns_into_model_outputs(self) -> None:
        cfg = SimpleNamespace()
        eligible = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "first_seen_at_utc": "2026-03-31T00:00:00Z",
                    "live_mid_price": 0.45,
                    "remaining_hours": 2.0,
                    "end_time_utc": "2026-03-31T02:00:00Z",
                    "domain": "example.com",
                    "domain_parsed": "example.com",
                    "sub_domain": "sports",
                    "category": "SPORTS",
                    "category_raw": "SPORTS",
                    "category_parsed": "SPORTS",
                    "category_override_flag": False,
                    "market_type": "no_yes",
                    "outcome_pattern": "binary",
                    "source_url": "https://example.com",
                    "resolution_source": "example.com",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                    "token_0_id": "token-0",
                    "token_1_id": "token-1",
                    "selected_reference_token_id": "token-0",
                    "selected_reference_outcome_label": "Yes",
                    "selected_reference_side_index": 0,
                    "best_bid": 0.44,
                    "best_ask": 0.46,
                    "last_trade_price": 0.45,
                    "tick_size": 0.01,
                    "order_price_min_tick_size": 0.01,
                    "liquidity": 1000.0,
                    "volume24hr": 500.0,
                    "token_state_age_sec": 1.0,
                    "question": "Will team A win?",
                    "description": "Test market",
                    "volume": 100.0,
                    "volume1wk": 1000.0,
                    "volume24hr_clob": 200.0,
                    "volume1wk_clob": 300.0,
                    "neg_risk": False,
                    "rewards_min_size": 10.0,
                    "rewards_max_spread": 0.1,
                    "one_hour_price_change": 0.0,
                    "one_day_price_change": 0.0,
                    "one_week_price_change": 0.0,
                    "liquidity_amm": 0.0,
                    "liquidity_clob": 1000.0,
                    "group_item_title": "Group",
                    "game_id": "game-1",
                    "market_maker_address": "maker-1",
                    "start_time_utc": "2026-03-31T00:00:00Z",
                }
            ]
        )
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "closedTime": "2026-03-31T02:00:00Z",
                    "price": 0.45,
                    "horizon_hours": 2.0,
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "no_yes",
                    "source_host": "example.com",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                    "token_0_id": "token-0",
                    "token_1_id": "token-1",
                }
            ]
        )
        rule_hits = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "no_yes",
                    "horizon_hours": 2.0,
                    "rule_group_key": "example.com|SPORTS|no_yes",
                    "rule_leaf_id": 1,
                    "rule_direction": 1,
                    "rule_score": 0.25,
                    "price": 0.45,
                    "price_min": 0.4,
                    "price_max": 0.5,
                    "h_min": 1.0,
                    "h_max": 4.0,
                }
            ]
        )
        feature_inputs = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "rule_group_key": "example.com|SPORTS|no_yes",
                    "rule_leaf_id": 1,
                    "feature_a": 1.0,
                }
            ]
        )
        viable_candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "snapshot_time": "2026-03-31T00:00:00Z",
                    "rule_group_key": "example.com|SPORTS|no_yes",
                    "rule_leaf_id": 1,
                    "rule_direction": 1,
                    "price": 0.45,
                    "edge_prob": 0.2,
                    "edge_final": 0.2,
                    "direction_model": 1,
                    "f_star": 0.3,
                    "f_exec": 0.02,
                    "g_net": 0.01,
                    "growth_score": 0.5,
                }
            ]
        )
        runtime = SimpleNamespace(
            cfg=cfg,
            rules_frame=pd.DataFrame(),
            serving_feature_bundle=ServingFeatureBundle(
                fine_features=pd.DataFrame(
                    columns=[
                        "group_key",
                        "price_bin",
                        "horizon_hours",
                        "leaf_id",
                        "direction",
                        "q_full",
                        "p_full",
                        "edge_full",
                        "edge_std_full",
                        "edge_lower_bound_full",
                        "rule_score",
                        "n_full",
                    ]
                ),
                group_features=pd.DataFrame(columns=["group_key"]),
                defaults_manifest={"fine_feature_defaults": {}},
            ),
            feature_contract=SimpleNamespace(feature_columns=("feature_a",), categorical_columns=tuple()),
            model_payload=SimpleNamespace(
                predict_q=lambda frame: [0.65],
                predict_trade_value=lambda candidates, frame: [1.2],
            ),
            rule_runtime=SimpleNamespace(
                build_market_feature_cache=lambda market_context, annotations: pd.DataFrame(),
                match_rules=lambda snapshots_arg, rules_arg: rule_hits.copy(),
                preprocess_features=lambda matched_arg, cache_arg: feature_inputs.copy(),
                compute_growth_and_direction=lambda predicted_arg, cfg_arg: viable_candidates.copy(),
                apply_earliest_market_dedup=lambda frame, score_column: frame.copy(),
                backtest_config=SimpleNamespace(),
            ),
        )

        with patch("execution_engine.online.scoring.live.apply_live_price_filter", return_value=SimpleNamespace(eligible=eligible, state_counts={})):
            with patch("execution_engine.online.scoring.live._build_live_snapshot_rows", return_value=snapshots):
                result = run_live_inference(runtime, pd.DataFrame([{"market_id": "market-1"}]), pd.DataFrame())

        model_outputs = result.rule_model.model_outputs
        self.assertEqual(len(model_outputs), 1)
        self.assertAlmostEqual(float(model_outputs.iloc[0]["q_pred"]), 0.65, places=6)
        self.assertAlmostEqual(float(model_outputs.iloc[0]["f_exec"]), 0.02, places=6)
        self.assertAlmostEqual(float(model_outputs.iloc[0]["growth_score"]), 0.5, places=6)


class RiskGuardBehaviorTest(unittest.TestCase):
    def test_check_basic_risk_does_not_block_on_open_orders_limit(self) -> None:
        signal = {
            "order_type": "LIMIT",
            "amount_usdc": 5.0,
            "price_limit": 0.5,
            "decision_id": "decision-1",
            "market_id": "market-1",
            "outcome_index": 0,
            "action": "BUY",
            "category": "SPORTS",
        }
        state = SimpleNamespace(
            current_daily_pnl=lambda: 0.0,
            daily_order_count=0,
            open_orders_count=999,
            net_exposure_usdc=0.0,
            seen_market_action=lambda *args: False,
            seen_recent_decision=lambda *args: False,
            get_market_exposure=lambda *args: 0.0,
            get_category_exposure=lambda *args: 0.0,
        )
        cfg = SimpleNamespace(
            max_trade_amount_usdc=10.0,
            max_notional=10.0,
            fat_finger_high=0.99,
            fat_finger_low=0.01,
            daily_loss_limit=-500.0,
            max_daily_orders=0,
            enforce_one_order_per_market=False,
            dup_window_sec=5,
            max_open_orders=1,
            max_position_per_market_usdc=10.0,
            max_exposure_per_category_usdc=0.0,
            max_net_exposure_usdc=100.0,
            balance_strict=False,
        )

        ok, reason = check_basic_risk(signal, state, cfg, balance_provider=None)

        self.assertTrue(ok)
        self.assertEqual(reason, "OK")


class OrderStateMachineTest(unittest.TestCase):
    def test_delayed_can_transition_to_cancel_requested(self) -> None:
        self.assertTrue(can_transition("DELAYED", "CANCEL_REQUESTED"))


if __name__ == "__main__":
    unittest.main()
