import unittest
from types import SimpleNamespace

import pandas as pd

from execution_engine.online.execution.live_quote import quote_from_clob
from execution_engine.online.execution.pricing import build_submission_signal
from execution_engine.online.pipeline.eligibility import apply_live_price_filter, apply_structural_coarse_filter
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
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
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
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
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
        self.assertEqual(signal["price_limit"], 0.27)
        self.assertEqual(signal["best_bid_at_submit"], 0.29)
        self.assertEqual(signal["best_ask_at_submit"], 0.51)

    def test_build_submission_signal_allows_exact_half_point_spread(self) -> None:
        cfg = SimpleNamespace(
            run_id="test-run",
            online_limit_ticks_below_best_bid=1,
            online_price_cap_safety_buffer=0.01,
            max_trade_amount_usdc=5.0,
            order_ttl_sec=300,
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


class LiveFilterCoverageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = SimpleNamespace(
            online_token_state_max_age_sec=300,
            rule_engine_min_price=0.01,
            rule_engine_max_price=0.99,
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

    def test_live_filter_compares_reference_token_live_mid_to_rule_band(self) -> None:
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

        self.assertTrue(result.eligible.empty)
        self.assertEqual(int(result.state_counts.get("LIVE_PRICE_MISS", 0)), 1)
        self.assertEqual(result.rejected.iloc[0]["live_filter_reason"], "live_price_outside_rule_band")

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

    def test_live_filter_accepts_when_live_mid_is_within_midpoint_tolerance(self) -> None:
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


if __name__ == "__main__":
    unittest.main()
