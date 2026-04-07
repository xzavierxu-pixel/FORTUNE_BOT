import math
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from execution_engine.online.scoring.live import CriticalFeatureContractError, _build_live_snapshot_rows, _ensure_feature_contract
from execution_engine.online.scoring.rule_runtime import FeatureContract
from execution_engine.online.scoring.price_history import PricePoint

REPO_ROOT = Path(__file__).resolve().parents[2]


class LiveSnapshotSemanticsTest(unittest.TestCase):
    def test_ensure_feature_contract_prunes_to_contract_and_logs_missing_columns(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-04-05T00:00:00Z",
                    "rule_group_key": "g1",
                    "rule_leaf_id": 1,
                    "price": 0.5,
                    "remaining_hours": 4.0,
                }
            ]
        )
        contract = FeatureContract(
            feature_columns=("price", "domain"),
            numeric_columns=("price",),
            categorical_columns=("domain",),
            required_noncritical_columns=("price", "domain"),
        )

        with self.assertLogs("execution_engine.online.scoring.live", level="WARNING") as captured:
            aligned = _ensure_feature_contract(frame, contract)

        self.assertEqual(
            list(aligned.columns),
            ["market_id", "snapshot_time", "rule_group_key", "rule_leaf_id", "price", "domain"],
        )
        self.assertNotIn("remaining_hours", aligned.columns)
        self.assertEqual(aligned.iloc[0]["domain"], "UNKNOWN")
        self.assertIn("domain", captured.output[0])
        self.assertEqual(aligned.attrs["feature_contract_summary"]["defaulted_noncritical_columns"], ["domain"])
        self.assertEqual(aligned.attrs["feature_contract_summary"]["available_feature_column_count"], 1)

    def test_ensure_feature_contract_raises_for_missing_critical_columns(self) -> None:
        frame = pd.DataFrame([{"market_id": "market-1", "snapshot_time": "2026-04-05T00:00:00Z"}])
        contract = FeatureContract(
            feature_columns=("price", "domain"),
            numeric_columns=("price",),
            categorical_columns=("domain",),
            required_critical_columns=("price",),
            required_noncritical_columns=("domain",),
        )

        with self.assertRaises(CriticalFeatureContractError) as captured:
            _ensure_feature_contract(frame, contract)

        self.assertEqual(captured.exception.summary["missing_critical_columns"], ["price"])

    def test_build_live_snapshot_rows_uses_canonical_quote_window_and_source_host(self) -> None:
        cfg = SimpleNamespace(
            online_token_state_max_age_sec=3600,
            rule_engine_dir=REPO_ROOT / "polymarket_rule_engine",
        )
        frame = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "first_seen_at_utc": "2026-04-05T00:00:00Z",
                    "live_mid_price": 0.5,
                    "token_state_age_sec": 12.0,
                    "selected_reference_token_id": "token-1",
                    "remaining_hours": 4.0,
                    "end_time_utc": "2026-04-05T04:00:00Z",
                    "start_time_utc": "2026-04-04T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "source_url": "https://feed.example.com/post",
                    "resolution_source": "https://resolve.example.com/fallback",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                    "token_0_id": "token-1",
                    "token_1_id": "token-2",
                    "selected_reference_outcome_label": "Yes",
                    "selected_reference_side_index": 0,
                    "best_bid": 0.49,
                    "best_ask": 0.51,
                    "last_trade_price": 0.5,
                    "order_price_min_tick_size": 0.001,
                    "liquidity": 1000.0,
                    "volume24hr": 500.0,
                }
            ]
        )

        class StubHistoryClient:
            def __init__(self, _cfg: object) -> None:
                pass

            def fetch_history(self, token_id: str, *, start_ts: int, end_ts: int, fidelity_minutes: int = 1):
                return [
                    PricePoint(ts=end_ts - 60, price=0.49, source="clob_prices_history"),
                    PricePoint(ts=end_ts - 3600, price=0.45, source="clob_prices_history"),
                ]

        with patch("execution_engine.online.scoring.live.ClobPriceHistoryClient", StubHistoryClient):
            snapshots = _build_live_snapshot_rows(cfg, frame)

        row = snapshots.iloc[0]
        self.assertNotIn("delta_hours_bucket", snapshots.columns)
        self.assertAlmostEqual(float(row["price"]), 0.5, places=6)
        self.assertAlmostEqual(float(row["horizon_hours"]), 4.0, places=6)
        self.assertEqual(row["closedTime"], "2026-04-05T04:00:00Z")
        self.assertEqual(row["source_host"], "feed.example.com")
        self.assertEqual(row["selected_quote_side"], "right")
        self.assertAlmostEqual(float(row["selected_quote_offset_sec"]), 0.0, places=6)
        self.assertAlmostEqual(float(row["selected_quote_points_in_window"]), 1.0, places=6)
        self.assertGreater(float(row["selected_quote_left_gap_sec"]), 1000.0)
        self.assertAlmostEqual(float(row["selected_quote_right_gap_sec"]), 0.0, places=6)
        self.assertGreater(float(row["selected_quote_local_gap_sec"]), 1000.0)
        self.assertTrue(bool(row["stale_quote_flag"]))
        self.assertAlmostEqual(float(row["snapshot_quality_score"]), 1.0 + math.log1p(1.0), places=6)

    def test_build_live_snapshot_rows_aligns_history_fetch_window_to_end_time(self) -> None:
        fixed_now = datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc)
        cfg = SimpleNamespace(
            online_token_state_max_age_sec=3600,
            rule_engine_dir=REPO_ROOT / "polymarket_rule_engine",
        )
        frame = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "batch_id": "batch-001",
                    "first_seen_at_utc": "2026-04-05T00:00:00Z",
                    "live_mid_price": 0.5,
                    "token_state_age_sec": 12.0,
                    "selected_reference_token_id": "token-1",
                    "remaining_hours": 1.0,
                    "end_time_utc": "2026-04-05T01:00:00Z",
                    "start_time_utc": "2026-04-04T00:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "source_url": "https://feed.example.com/post",
                    "resolution_source": "https://resolve.example.com/fallback",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                    "token_0_id": "token-1",
                    "token_1_id": "token-2",
                    "selected_reference_outcome_label": "Yes",
                    "selected_reference_side_index": 0,
                    "best_bid": 0.49,
                    "best_ask": 0.51,
                    "last_trade_price": 0.5,
                    "order_price_min_tick_size": 0.001,
                    "liquidity": 1000.0,
                    "volume24hr": 500.0,
                }
            ]
        )
        fetch_calls: list[tuple[int, int, int]] = []

        class StubHistoryClient:
            def __init__(self, _cfg: object) -> None:
                pass

            def fetch_history(self, token_id: str, *, start_ts: int, end_ts: int, fidelity_minutes: int = 1):
                fetch_calls.append((start_ts, end_ts, fidelity_minutes))
                return [
                    PricePoint(ts=end_ts - 7200, price=0.45, source="clob_prices_history"),
                ]

        with patch("execution_engine.online.scoring.live._utc_now", return_value=fixed_now):
            with patch("execution_engine.online.scoring.live.ClobPriceHistoryClient", StubHistoryClient):
                snapshots = _build_live_snapshot_rows(cfg, frame)

        expected_end_ts = int(pd.Timestamp("2026-04-05T01:00:00Z").timestamp())
        expected_start_ts = int(expected_end_ts - (24.1 * 3600))
        self.assertEqual(fetch_calls, [(expected_start_ts, expected_end_ts, 1)])
        self.assertTrue(pd.isna(snapshots.iloc[0]["p_1h"]))


if __name__ == "__main__":
    unittest.main()
