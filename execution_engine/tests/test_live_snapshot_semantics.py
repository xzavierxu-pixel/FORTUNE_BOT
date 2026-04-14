import math
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd

from execution_engine.online.pipeline.eligibility import LiveFilterResult
from execution_engine.online.scoring.live import (
    CriticalFeatureContractError,
    _build_live_snapshot_rows,
    _ensure_feature_contract,
    run_live_inference,
)
from execution_engine.online.scoring.rules import ServingFeatureBundle
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

    def test_run_live_inference_uses_group_default_fallback_when_no_fine_rule_matches(self) -> None:
        candidates = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "selected_reference_token_id": "token-1",
                }
            ]
        )
        live_filter = LiveFilterResult(
            eligible=candidates.copy(),
            rejected=pd.DataFrame(),
            state_counts={"LIVE_ELIGIBLE": 1},
        )
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "snapshot_time": "2026-04-05T00:00:00Z",
                    "closedTime": "2026-04-05T04:00:00Z",
                    "scheduled_end": "2026-04-05T04:00:00Z",
                    "domain": "example.com",
                    "category": "SPORTS",
                    "market_type": "moneyline",
                    "price": 0.91,
                    "horizon_hours": 48.0,
                    "batch_id": "batch-001",
                    "source_host": "example.com",
                    "selected_quote_offset_sec": 0.0,
                    "snapshot_quality_score": 1.0,
                    "token_0_id": "yes-token",
                    "token_1_id": "no-token",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                }
            ]
        )
        bundle = ServingFeatureBundle(
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
            group_features=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|moneyline",
                        "group_decision": "keep",
                        "group_default_leaf_id": "__GROUP_DEFAULT__|example.com|SPORTS|moneyline",
                        "group_default_direction": 1,
                        "group_default_q_full": 0.61,
                        "group_default_p_full": 0.44,
                        "group_default_edge_full": 0.17,
                        "group_default_edge_std_full": 0.08,
                        "group_default_edge_lower_bound_full": 0.12,
                        "group_default_rule_score": 0.12,
                        "group_default_n_full": 140.0,
                    }
                ]
            ),
            defaults_manifest={
                "fine_feature_defaults": {
                    "leaf_id": {"group_column": "group_default_leaf_id"},
                    "direction": {"group_column": "group_default_direction"},
                    "q_full": {"group_column": "group_default_q_full"},
                    "p_full": {"group_column": "group_default_p_full"},
                    "edge_full": {"group_column": "group_default_edge_full"},
                    "edge_std_full": {"group_column": "group_default_edge_std_full"},
                    "edge_lower_bound_full": {"group_column": "group_default_edge_lower_bound_full"},
                    "rule_score": {"group_column": "group_default_rule_score"},
                    "n_full": {"group_column": "group_default_n_full"},
                }
            },
        )
        rule_runtime = SimpleNamespace(
            build_market_feature_cache=lambda market_context, market_annotations: pd.DataFrame(),
            match_rules=lambda snapshots_frame, rules_frame: pd.DataFrame(),
            preprocess_features=lambda model_input, market_feature_cache: model_input.copy(),
            compute_growth_and_direction=lambda predicted, cfg: predicted.assign(
                edge_final=predicted["q_pred"] - predicted["price"],
                direction_model=predicted["rule_direction"],
                f_star=0.1,
                f_exec=0.02,
                g_net=0.01,
                growth_score=1.0,
            ),
            apply_earliest_market_dedup=lambda frame, score_column: frame.copy(),
            backtest_config=SimpleNamespace(),
        )
        runtime = SimpleNamespace(
            cfg=SimpleNamespace(rule_engine_dir=REPO_ROOT / "polymarket_rule_engine"),
            rule_runtime=rule_runtime,
            rules_frame=pd.DataFrame(
                [
                    {
                        "group_key": "example.com|SPORTS|moneyline",
                        "domain": "example.com",
                        "category": "SPORTS",
                        "market_type": "moneyline",
                    }
                ]
            ),
            serving_feature_bundle=bundle,
            model_payload=SimpleNamespace(
                predict_q=lambda feature_inputs: np.asarray([0.63] * len(feature_inputs), dtype=float),
                predict_trade_value=lambda predicted, feature_inputs: np.asarray([0.08] * len(feature_inputs), dtype=float),
            ),
            feature_contract=FeatureContract(
                feature_columns=("price", "domain", "category", "market_type", "horizon_hours", "fine_feature_q_full"),
                numeric_columns=("price", "horizon_hours", "fine_feature_q_full"),
                categorical_columns=("domain", "category", "market_type"),
                required_noncritical_columns=("price", "domain", "category", "market_type", "horizon_hours", "fine_feature_q_full"),
            ),
        )

        with patch("execution_engine.online.scoring.live.apply_live_price_filter", return_value=live_filter):
            with patch("execution_engine.online.scoring.live._build_live_snapshot_rows", return_value=snapshots):
                with patch("execution_engine.online.scoring.live._build_market_feature_context", return_value=pd.DataFrame()):
                    with patch("execution_engine.online.scoring.live._build_market_annotations", return_value=pd.DataFrame()):
                        result = run_live_inference(runtime, candidates, token_state=pd.DataFrame())

        self.assertEqual(len(result.rule_model.rule_hits), 1)
        self.assertEqual(int(result.rule_model.rule_hits.iloc[0]["rule_leaf_id"]), -1)
        self.assertEqual(result.rule_model.rule_hits.iloc[0]["rule_match_reason"], "group_default_fallback")
        self.assertEqual(len(result.rule_model.feature_inputs), 1)
        self.assertTrue(bool(result.rule_model.feature_inputs.iloc[0]["group_match_found"]))
        self.assertFalse(bool(result.rule_model.feature_inputs.iloc[0]["fine_match_found"]))
        self.assertTrue(bool(result.rule_model.feature_inputs.iloc[0]["used_group_fallback_only"]))
        self.assertEqual(len(result.rule_model.model_outputs), 1)
        self.assertEqual(len(result.rule_model.viable_candidates), 1)


if __name__ == "__main__":
    unittest.main()
