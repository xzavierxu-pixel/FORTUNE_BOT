import json
import math
import os
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from execution_engine.online.scoring.price_history import PricePoint
from execution_engine.online.pipeline.lifecycle import record_candidate_state
from execution_engine.online.pipeline.submit_window import _persist_batch_training_artifacts
from execution_engine.online.execution.monitor import _build_batch_lifecycle_exports, _export_shared_orders_live
from execution_engine.online.reporting.candidate_audit import build_candidate_audit
from execution_engine.online.reporting.artifact_retention import compact_run_artifacts
from execution_engine.online.scoring.snapshot_builder import build_snapshot_inputs
from execution_engine.online.streaming.io import RawEventBuffer
from execution_engine.runtime.config import load_config
from execution_engine.shared.io import read_jsonl
from execution_engine.shared.time import to_iso, utc_now

REPO_ROOT = Path(__file__).resolve().parents[2]


class StorageConfigTest(unittest.TestCase):
    def test_load_config_disables_ws_raw_by_default(self) -> None:
        with TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "data"
            with patch.dict(
                os.environ,
                {
                    "PEG_BASE_DATA_DIR": str(base_dir),
                    "PEG_RUN_DATE": "2026-04-04",
                    "PEG_RUN_ID": "storage-test",
                },
                clear=False,
            ):
                cfg = load_config()

        self.assertFalse(cfg.online_market_ws_raw_enabled)
        self.assertFalse(cfg.shared_ws_raw_dir.exists())


class RawEventBufferTest(unittest.TestCase):
    def test_raw_event_buffer_disabled_does_not_write_files(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root_dir = Path(tmpdir) / "ws_raw"
            buffer = RawEventBuffer(root_dir, flush_events=1, enabled=False)

            buffer.append(0, utc_now(), {"event_type": "book", "asset_id": "token-1"})
            buffer.flush_all()

            self.assertEqual(buffer.raw_event_count, 0)
            self.assertFalse(root_dir.exists())


class SnapshotBuilderWithoutWsRawTest(unittest.TestCase):
    def test_build_snapshot_inputs_uses_token_state_without_ws_raw(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            token_state_path = root / "shared" / "token_state" / "current_token_state.csv"
            token_state_path.parent.mkdir(parents=True, exist_ok=True)
            now = utc_now()
            token_state = pd.DataFrame(
                [
                    {
                        "token_id": "token-1",
                        "latest_event_at_utc": to_iso(now),
                        "latest_event_timestamp_ms": int(pd.Timestamp(now).timestamp() * 1000),
                        "latest_event_type": "book",
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "mid_price": 0.5,
                        "last_trade_price": 0.5,
                        "raw_event_count": 4,
                        "tick_size": 0.001,
                        "subscription_source": "universe_reference",
                    }
                ]
            )
            token_state.to_csv(token_state_path, index=False)

            market_state_cache_path = root / "shared" / "positions" / "market_state.json"
            market_state_cache_path.parent.mkdir(parents=True, exist_ok=True)
            market_state_cache_path.write_text(
                '{"open_market_ids":[],"pending_market_ids":[]}',
                encoding="utf-8",
            )

            cfg = SimpleNamespace(
                run_id="run-1",
                token_state_current_path=token_state_path,
                market_state_cache_path=market_state_cache_path,
                open_positions_path=root / "shared" / "positions" / "open_positions.jsonl",
                runs_root_dir=root / "runs",
                online_market_batch_size=20,
                online_token_state_max_age_sec=3600,
                rule_engine_min_price=0.2,
                rule_engine_max_price=0.8,
                shared_ws_raw_dir=root / "shared" / "ws_raw",
                rule_engine_dir=REPO_ROOT / "polymarket_rule_engine",
            )
            universe = pd.DataFrame(
                [
                    {
                        "market_id": "market-1",
                        "selected_reference_token_id": "token-1",
                        "selected_reference_outcome_label": "Yes",
                        "selected_reference_side_index": 0,
                        "remaining_hours": 4.0,
                        "end_time_utc": "2026-04-05T00:00:00Z",
                        "start_time_utc": "2026-04-04T00:00:00Z",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "outcome_0_label": "Yes",
                        "outcome_1_label": "No",
                        "token_0_id": "token-1",
                        "token_1_id": "token-2",
                        "order_price_min_tick_size": 0.001,
                        "best_bid": 0.48,
                        "best_ask": 0.52,
                        "last_trade_price": 0.5,
                        "liquidity": 1000,
                        "volume24hr": 500,
                    }
                ]
            )

            class StubHistoryClient:
                def __init__(self, _cfg: object) -> None:
                    pass

                def fetch_history(self, token_id: str, *, start_ts: int, end_ts: int, fidelity_minutes: int = 1):
                    return [
                        PricePoint(ts=end_ts - 3600, price=0.45, source="clob_prices_history"),
                        PricePoint(ts=end_ts - 24 * 3600, price=0.4, source="clob_prices_history"),
                    ]

            with patch("execution_engine.online.scoring.snapshot_builder.ClobPriceHistoryClient", StubHistoryClient):
                result = build_snapshot_inputs(cfg, universe, market_limit=None, market_offset=0)

            self.assertFalse(cfg.shared_ws_raw_dir.exists())
            self.assertEqual(len(result.snapshots), 1)
            self.assertAlmostEqual(float(result.snapshots.iloc[0]["price"]), 0.5, places=6)
            self.assertEqual(result.raw_inputs[0]["latest_ws_price"]["source_event_type"], "book")
            self.assertEqual(result.processing_counts.get("snapshot_built"), 1)

    def test_build_snapshot_inputs_uses_canonical_quote_window_and_source_host(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            token_state_path = root / "shared" / "token_state" / "current_token_state.csv"
            token_state_path.parent.mkdir(parents=True, exist_ok=True)
            now = utc_now()
            token_state = pd.DataFrame(
                [
                    {
                        "token_id": "token-1",
                        "latest_event_at_utc": to_iso(now),
                        "latest_event_timestamp_ms": int(pd.Timestamp(now).timestamp() * 1000),
                        "latest_event_type": "book",
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "mid_price": 0.5,
                        "last_trade_price": 0.5,
                        "raw_event_count": 4,
                        "tick_size": 0.001,
                        "subscription_source": "universe_reference",
                    }
                ]
            )
            token_state.to_csv(token_state_path, index=False)

            market_state_cache_path = root / "shared" / "positions" / "market_state.json"
            market_state_cache_path.parent.mkdir(parents=True, exist_ok=True)
            market_state_cache_path.write_text(
                '{"open_market_ids":[],"pending_market_ids":[]}',
                encoding="utf-8",
            )

            cfg = SimpleNamespace(
                run_id="run-1",
                token_state_current_path=token_state_path,
                market_state_cache_path=market_state_cache_path,
                open_positions_path=root / "shared" / "positions" / "open_positions.jsonl",
                runs_root_dir=root / "runs",
                online_market_batch_size=20,
                online_token_state_max_age_sec=3600,
                rule_engine_min_price=0.2,
                rule_engine_max_price=0.8,
                shared_ws_raw_dir=root / "shared" / "ws_raw",
                rule_engine_dir=REPO_ROOT / "polymarket_rule_engine",
            )
            universe = pd.DataFrame(
                [
                    {
                        "market_id": "market-1",
                        "selected_reference_token_id": "token-1",
                        "selected_reference_outcome_label": "Yes",
                        "selected_reference_side_index": 0,
                        "remaining_hours": 4.0,
                        "end_time_utc": "2026-04-05T00:00:00Z",
                        "start_time_utc": "2026-04-04T00:00:00Z",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "source_url": "https://news.example.com/story",
                        "resolution_source": "https://resolve.example.com/fallback",
                        "outcome_0_label": "Yes",
                        "outcome_1_label": "No",
                        "token_0_id": "token-1",
                        "token_1_id": "token-2",
                        "order_price_min_tick_size": 0.001,
                        "best_bid": 0.48,
                        "best_ask": 0.52,
                        "last_trade_price": 0.5,
                        "liquidity": 1000,
                        "volume24hr": 500,
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

            with patch("execution_engine.online.scoring.snapshot_builder.ClobPriceHistoryClient", StubHistoryClient):
                result = build_snapshot_inputs(cfg, universe, market_limit=None, market_offset=0)

            row = result.snapshots.iloc[0]
            self.assertEqual(row["source_host"], "news.example.com")
            self.assertEqual(row["selected_quote_side"], "right")
            self.assertAlmostEqual(float(row["selected_quote_offset_sec"]), 0.0, places=6)
            self.assertAlmostEqual(float(row["selected_quote_points_in_window"]), 1.0, places=6)
            self.assertGreater(float(row["selected_quote_left_gap_sec"]), 1000.0)
            self.assertAlmostEqual(float(row["selected_quote_right_gap_sec"]), 0.0, places=6)
            self.assertGreater(float(row["selected_quote_local_gap_sec"]), 1000.0)
            self.assertTrue(bool(row["stale_quote_flag"]))
            self.assertAlmostEqual(float(row["snapshot_quality_score"]), 1.0 + math.log1p(1.0), places=6)

    def test_build_snapshot_inputs_aligns_history_fetch_window_to_end_time(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            token_state_path = root / "shared" / "token_state" / "current_token_state.csv"
            token_state_path.parent.mkdir(parents=True, exist_ok=True)
            fixed_now = datetime(2026, 4, 5, 0, 0, tzinfo=timezone.utc)
            token_state = pd.DataFrame(
                [
                    {
                        "token_id": "token-1",
                        "latest_event_at_utc": to_iso(fixed_now),
                        "latest_event_timestamp_ms": int(pd.Timestamp(fixed_now).timestamp() * 1000),
                        "latest_event_type": "book",
                        "best_bid": 0.49,
                        "best_ask": 0.51,
                        "mid_price": 0.5,
                        "last_trade_price": 0.5,
                        "raw_event_count": 1,
                        "tick_size": 0.001,
                        "subscription_source": "universe_reference",
                    }
                ]
            )
            token_state.to_csv(token_state_path, index=False)

            market_state_cache_path = root / "shared" / "positions" / "market_state.json"
            market_state_cache_path.parent.mkdir(parents=True, exist_ok=True)
            market_state_cache_path.write_text(
                '{"open_market_ids":[],"pending_market_ids":[]}',
                encoding="utf-8",
            )

            cfg = SimpleNamespace(
                run_id="run-1",
                token_state_current_path=token_state_path,
                market_state_cache_path=market_state_cache_path,
                open_positions_path=root / "shared" / "positions" / "open_positions.jsonl",
                runs_root_dir=root / "runs",
                online_market_batch_size=20,
                online_token_state_max_age_sec=3600,
                rule_engine_min_price=0.2,
                rule_engine_max_price=0.8,
                shared_ws_raw_dir=root / "shared" / "ws_raw",
                rule_engine_dir=REPO_ROOT / "polymarket_rule_engine",
            )
            universe = pd.DataFrame(
                [
                    {
                        "market_id": "market-1",
                        "selected_reference_token_id": "token-1",
                        "selected_reference_outcome_label": "Yes",
                        "selected_reference_side_index": 0,
                        "remaining_hours": 1.0,
                        "end_time_utc": "2026-04-05T01:00:00Z",
                        "start_time_utc": "2026-04-04T00:00:00Z",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "source_url": "https://news.example.com/story",
                        "resolution_source": "https://resolve.example.com/fallback",
                        "outcome_0_label": "Yes",
                        "outcome_1_label": "No",
                        "token_0_id": "token-1",
                        "token_1_id": "token-2",
                        "order_price_min_tick_size": 0.001,
                        "best_bid": 0.48,
                        "best_ask": 0.52,
                        "last_trade_price": 0.5,
                        "liquidity": 1000,
                        "volume24hr": 500,
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

            with patch("execution_engine.online.scoring.snapshot_builder._utc_now", return_value=fixed_now):
                with patch("execution_engine.online.scoring.snapshot_builder.ClobPriceHistoryClient", StubHistoryClient):
                    result = build_snapshot_inputs(cfg, universe, market_limit=None, market_offset=0)

            expected_end_ts = int(pd.Timestamp("2026-04-05T01:00:00Z").timestamp())
            expected_start_ts = int(expected_end_ts - (24.1 * 3600))
            self.assertEqual(fetch_calls, [(expected_start_ts, expected_end_ts, 1)])
            self.assertTrue(pd.isna(result.snapshots.iloc[0]["p_1h"]))


class MinimalArtifactPolicyTest(unittest.TestCase):
    def test_candidate_lifecycle_suppresses_non_submission_events_in_minimal_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = SimpleNamespace(
                artifact_policy="minimal",
                run_id="run-1",
                run_mode="submit_window",
                events_path=root / "events.jsonl",
            )

            record_candidate_state(cfg, market_id="m-skip", state="DIRECT_CANDIDATE", reason="rule_match")
            record_candidate_state(cfg, market_id="m-reject", state="SUBMISSION_REJECTED", reason="BEST_BID_MISSING")
            record_candidate_state(cfg, market_id="m-submit", state="SUBMITTED", reason="ACKED")

            rows = read_jsonl(cfg.events_path)
            self.assertEqual([row["candidate_state"] for row in rows], ["SUBMISSION_REJECTED", "SUBMITTED"])
            self.assertEqual([row["market_id"] for row in rows], ["m-reject", "m-submit"])

    def test_persist_batch_training_artifacts_keeps_selection_only_in_minimal_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            cfg = SimpleNamespace(
                run_id="run-1",
                run_mode="submit_window",
                artifact_policy="minimal",
                run_snapshot_processed_markets_path=run_dir / "snapshot_score" / "processed_markets.csv",
                run_snapshot_raw_inputs_path=run_dir / "snapshot_score" / "raw_snapshot_inputs.jsonl",
                run_snapshot_normalized_path=run_dir / "snapshot_score" / "normalized_snapshots.csv",
                run_snapshot_feature_inputs_path=run_dir / "snapshot_score" / "feature_inputs.csv",
                run_snapshot_rule_hits_path=run_dir / "snapshot_score" / "rule_hits.csv",
                run_snapshot_model_outputs_path=run_dir / "snapshot_score" / "model_outputs.csv",
                run_snapshot_selection_path=run_dir / "snapshot_score" / "selection_decisions.csv",
                run_snapshot_score_manifest_path=run_dir / "snapshot_score" / "manifest.json",
            )
            runtime = SimpleNamespace(
                cfg=cfg,
                feature_contract=SimpleNamespace(
                    feature_columns=("price", "domain"),
                    numeric_columns=("price",),
                    categorical_columns=("domain",),
                    required_critical_columns=("price",),
                    required_noncritical_columns=("domain",),
                    optional_debug_columns=(),
                ),
                model_payload=SimpleNamespace(
                    runtime_manifest={
                        "feature_semantics_version": "decision_time_v1",
                        "normalization_manifest": {
                            "manifest_version": 1,
                            "annotation_pipeline_version": "shared_v1",
                            "domain_policy": {"allowed_domains": ["example.com"]},
                        },
                    }
                ),
            )
            batch = SimpleNamespace(
                batch_id="batch-a",
                frame=pd.DataFrame(
                    [
                        {
                            "market_id": "market-1",
                            "selected_reference_token_id": "ref-1",
                            "domain": "OTHER",
                            "domain_parsed": "newsite.com",
                            "category_override_flag": True,
                        }
                    ]
                ),
            )
            inference_result = SimpleNamespace(
                live_filter=SimpleNamespace(
                    eligible=pd.DataFrame([{"market_id": "market-1", "selected_reference_token_id": "ref-1"}])
                ),
                snapshots=pd.DataFrame([{"market_id": "market-1", "snapshot_time": "2026-03-19T00:00:00Z"}]),
                rule_model=SimpleNamespace(
                    rule_hits=pd.DataFrame([{"market_id": "market-1", "snapshot_time": "2026-03-19T00:00:00Z"}]),
                    feature_inputs=pd.DataFrame([{"market_id": "market-1", "snapshot_time": "2026-03-19T00:00:00Z"}]),
                    model_outputs=pd.DataFrame(
                        [
                            {
                                "market_id": "market-1",
                                "snapshot_time": "2026-03-19T00:00:00Z",
                                "token_0_id": "token-0",
                                "token_1_id": "token-1",
                                "outcome_0_label": "YES",
                                "outcome_1_label": "NO",
                                "direction_model": 1,
                                "price": 0.4,
                                "q_pred": 0.6,
                            }
                        ]
                    ),
                ),
                feature_contract_summary={
                    "expected_feature_column_count": 2,
                    "available_feature_column_count": 1,
                    "missing_critical_columns": [],
                    "defaulted_noncritical_columns": ["domain"],
                    "defaulted_noncritical_count": 1,
                },
            )
            selection = pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "batch_id": "batch-a",
                        "market_id": "market-1",
                        "selected_token_id": "token-0",
                        "selected_for_submission": True,
                        "selection_reason": "allocated",
                    }
                ]
            )

            _persist_batch_training_artifacts(runtime, batch, inference_result, selection)

            self.assertTrue(cfg.run_snapshot_selection_path.exists())
            self.assertFalse(cfg.run_snapshot_processed_markets_path.exists())
            self.assertFalse(cfg.run_snapshot_raw_inputs_path.exists())
            self.assertFalse(cfg.run_snapshot_normalized_path.exists())
            self.assertFalse(cfg.run_snapshot_feature_inputs_path.exists())
            self.assertFalse(cfg.run_snapshot_rule_hits_path.exists())
            self.assertFalse(cfg.run_snapshot_model_outputs_path.exists())
            self.assertTrue((cfg.run_snapshot_score_manifest_path.parent / "feature_default_summary.json").exists())
            self.assertTrue((cfg.run_snapshot_score_manifest_path.parent / "annotation_normalization_summary.json").exists())
            self.assertTrue((cfg.run_snapshot_score_manifest_path.parent / "feature_semantics_manifest.json").exists())

    def test_monitor_skips_per_submit_lifecycle_exports_in_minimal_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            submit_dir = root / "runs" / "2026-04-04" / "RUN_1" / "submit_hourly"
            submit_dir.mkdir(parents=True, exist_ok=True)
            (submit_dir / "orders_submitted.jsonl").write_text(
                '{"order_attempt_id":"order-1","run_id":"RUN_1","batch_id":"batch-1","market_id":"m1","token_id":"t1"}\n',
                encoding="utf-8",
            )
            cfg = SimpleNamespace(
                artifact_policy="minimal",
                runs_root_dir=root / "runs",
            )

            export_counts = _build_batch_lifecycle_exports(
                cfg,
                latest_orders={"order-1": {"order_attempt_id": "order-1", "status": "FILLED"}},
                fills=[{"order_attempt_id": "order-1", "fill_id": "fill-1"}],
                open_positions=[{"entry_order_attempt_id": "order-1"}],
                opened_position_events=[{"order_attempt_id": "order-1"}],
            )

            self.assertEqual(export_counts["exported_submit_dirs"], 0)
            self.assertFalse((submit_dir / "fills.jsonl").exists())
            self.assertFalse((submit_dir / "cancels.jsonl").exists())
            self.assertFalse((submit_dir / "opened_positions.jsonl").exists())
            self.assertFalse((submit_dir / "opened_position_events.jsonl").exists())

    def test_monitor_shared_exports_keep_latest_orders_only_in_minimal_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = SimpleNamespace(
                artifact_policy="minimal",
                orders_live_latest_orders_path=root / "shared" / "orders_live" / "latest_orders.jsonl",
                orders_live_fills_path=root / "shared" / "orders_live" / "fills.jsonl",
                orders_live_cancels_path=root / "shared" / "orders_live" / "cancels.jsonl",
                orders_live_opened_positions_path=root / "shared" / "orders_live" / "opened_positions.jsonl",
                orders_live_opened_position_events_path=root / "shared" / "orders_live" / "opened_position_events.jsonl",
            )

            counts = _export_shared_orders_live(
                cfg,
                latest_orders={"order-1": {"order_attempt_id": "order-1", "updated_at_utc": "2026-04-04T00:00:00Z"}},
                fills=[{"order_attempt_id": "order-1", "filled_at_utc": "2026-04-04T00:01:00Z"}],
                open_positions=[{"entry_order_attempt_id": "order-1", "opened_at_utc": "2026-04-04T00:02:00Z"}],
                opened_position_events=[{"order_attempt_id": "order-1"}],
            )

            self.assertEqual(counts["shared_latest_order_count"], 1)
            self.assertEqual(counts["shared_fill_count"], 0)
            self.assertEqual(counts["shared_cancel_count"], 0)
            self.assertEqual(counts["shared_open_position_count"], 0)
            self.assertEqual(counts["shared_opened_position_event_count"], 1)
            self.assertTrue(cfg.orders_live_latest_orders_path.exists())
            self.assertTrue(cfg.orders_live_opened_position_events_path.exists())
            self.assertFalse(cfg.orders_live_fills_path.exists())
            self.assertFalse(cfg.orders_live_cancels_path.exists())
            self.assertFalse(cfg.orders_live_opened_positions_path.exists())


class ArtifactRetentionTest(unittest.TestCase):
    def test_compact_run_artifacts_deletes_old_debug_files_but_keeps_core_audit_files(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            old_run = root / "runs" / "2026-03-20" / "RUN_OLD"
            recent_run = root / "runs" / "2026-04-03" / "RUN_RECENT"
            old_run.mkdir(parents=True, exist_ok=True)
            recent_run.mkdir(parents=True, exist_ok=True)

            (old_run / "snapshot_score").mkdir(parents=True, exist_ok=True)
            (old_run / "submit_hourly").mkdir(parents=True, exist_ok=True)
            (old_run / "audit").mkdir(parents=True, exist_ok=True)
            (recent_run / "snapshot_score").mkdir(parents=True, exist_ok=True)
            (recent_run / "submit_hourly").mkdir(parents=True, exist_ok=True)
            (recent_run / "audit").mkdir(parents=True, exist_ok=True)

            (old_run / "snapshot_score" / "selection_decisions.csv").write_text("market_id\nm1\n", encoding="utf-8")
            (old_run / "submit_hourly" / "orders_submitted.jsonl").write_text('{"market_id":"m1"}\n', encoding="utf-8")
            (old_run / "run_summary.json").write_text("{}", encoding="utf-8")
            (old_run / "snapshot_score" / "processed_markets.csv").write_text("market_id\nm1\n", encoding="utf-8")
            (old_run / "snapshot_score" / "feature_inputs.csv").write_text("market_id\nm1\n", encoding="utf-8")
            (old_run / "submit_hourly" / "submission_attempts.csv").write_text("market_id\nm1\n", encoding="utf-8")
            (old_run / "events.jsonl").write_text('{"event_type":"CANDIDATE_STATE"}\n', encoding="utf-8")
            (old_run / "audit" / "market_audit.csv").write_text("market_id\nm1\n", encoding="utf-8")
            (old_run / "audit" / "funnel_summary.json").write_text("{}", encoding="utf-8")

            (recent_run / "snapshot_score" / "processed_markets.csv").write_text("market_id\nm2\n", encoding="utf-8")
            (recent_run / "snapshot_score" / "selection_decisions.csv").write_text("market_id\nm2\n", encoding="utf-8")
            (recent_run / "events.jsonl").write_text('{"event_type":"CANDIDATE_STATE"}\n', encoding="utf-8")
            (recent_run / "audit" / "market_audit.csv").write_text("market_id\nm2\n", encoding="utf-8")

            cfg = SimpleNamespace(
                run_id="RUN_CURRENT",
                run_mode="artifact_retention",
                artifact_policy="minimal",
                runs_root_dir=root / "runs",
                data_dir=root / "runs" / "2026-04-04" / "RUN_CURRENT",
                artifact_retention_full_days=7,
                artifact_retention_debug_days=2,
            )

            result = compact_run_artifacts(cfg, today=date(2026, 4, 4))

            self.assertEqual(result.scanned_run_count, 2)
            self.assertEqual(result.compacted_run_count, 1)
            self.assertTrue((old_run / "snapshot_score" / "selection_decisions.csv").exists())
            self.assertTrue((old_run / "submit_hourly" / "orders_submitted.jsonl").exists())
            self.assertTrue((old_run / "run_summary.json").exists())
            self.assertFalse((old_run / "events.jsonl").exists())
            self.assertFalse((old_run / "audit" / "market_audit.csv").exists())
            self.assertTrue((old_run / "audit" / "funnel_summary.json").exists())
            self.assertFalse((old_run / "snapshot_score" / "processed_markets.csv").exists())
            self.assertFalse((old_run / "snapshot_score" / "feature_inputs.csv").exists())
            self.assertFalse((old_run / "submit_hourly" / "submission_attempts.csv").exists())
            self.assertTrue((recent_run / "snapshot_score" / "processed_markets.csv").exists())
            self.assertTrue((recent_run / "events.jsonl").exists())
            self.assertTrue((recent_run / "audit" / "market_audit.csv").exists())
            self.assertTrue(result.manifest_path.exists())

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["deleted_file_count"], 5)


class CandidateAuditTest(unittest.TestCase):
    def test_build_candidate_audit_writes_market_and_funnel_outputs(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "events.jsonl"
            run_events = [
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-struct",
                    "candidate_state": "STRUCTURAL_REJECT",
                    "reason": "rule_family_miss",
                    "event_time_utc": "2026-04-04T00:00:01Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-live",
                    "candidate_state": "DIRECT_CANDIDATE",
                    "reason": "rule_family_horizon_match",
                    "event_time_utc": "2026-04-04T00:00:02Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-live",
                    "candidate_state": "LIVE_PRICE_MISS",
                    "reason": "live_price_outside_rule_band",
                    "event_time_utc": "2026-04-04T00:00:03Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-submit",
                    "candidate_state": "DIRECT_CANDIDATE",
                    "reason": "rule_family_horizon_match",
                    "event_time_utc": "2026-04-04T00:00:04Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-submit",
                    "candidate_state": "INFERRED",
                    "reason": "allocated",
                    "event_time_utc": "2026-04-04T00:00:05Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-submit",
                    "candidate_state": "SELECTED_FOR_SUBMISSION",
                    "reason": "allocated",
                    "event_time_utc": "2026-04-04T00:00:06Z",
                },
                {
                    "event_type": "CANDIDATE_STATE",
                    "run_id": "RUN_1",
                    "run_mode": "submit_window",
                    "market_id": "m-submit",
                    "candidate_state": "SUBMITTED",
                    "reason": "ACKED",
                    "event_time_utc": "2026-04-04T00:00:07Z",
                },
            ]
            events_path.write_text(
                "\n".join(json.dumps(row, ensure_ascii=True) for row in run_events) + "\n",
                encoding="utf-8",
            )
            cfg = SimpleNamespace(
                run_id="RUN_1",
                run_mode="submit_window",
                events_path=events_path,
                run_audit_market_path=root / "audit" / "market_audit.csv",
                run_audit_funnel_summary_path=root / "audit" / "funnel_summary.json",
            )

            result = build_candidate_audit(cfg)

            market_audit = pd.read_csv(cfg.run_audit_market_path, dtype=str)
            funnel = json.loads(cfg.run_audit_funnel_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(result.market_count, 3)
            self.assertEqual(result.candidate_event_count, 7)
            self.assertEqual(len(market_audit), 3)
            self.assertEqual(
                market_audit.loc[market_audit["market_id"] == "m-struct", "terminal_reason"].iloc[0],
                "rule_family_miss",
            )
            self.assertEqual(
                market_audit.loc[market_audit["market_id"] == "m-live", "terminal_state"].iloc[0],
                "LIVE_PRICE_MISS",
            )
            self.assertEqual(
                market_audit.loc[market_audit["market_id"] == "m-submit", "submitted"].iloc[0].lower(),
                "true",
            )
            self.assertEqual(funnel["final_state_counts"]["SUBMITTED"], 1)
            stage_names = {row["stage"] for row in funnel["market_funnel"]}
            self.assertIn("structural_reject", stage_names)
            self.assertIn("live_price_miss", stage_names)
            self.assertIn("submitted", stage_names)

    def test_build_candidate_audit_uses_submit_window_funnel_counts_in_minimal_mode(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            events_path = root / "events.jsonl"
            events_path.write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "event_type": "CANDIDATE_STATE",
                                "run_id": "RUN_1",
                                "run_mode": "submit_window",
                                "market_id": "m-reject",
                                "candidate_state": "SUBMISSION_REJECTED",
                                "reason": "BEST_BID_MISSING",
                                "event_time_utc": "2026-04-04T00:00:01Z",
                            },
                            ensure_ascii=True,
                        ),
                        json.dumps(
                            {
                                "event_type": "CANDIDATE_STATE",
                                "run_id": "RUN_1",
                                "run_mode": "submit_window",
                                "market_id": "m-submit",
                                "candidate_state": "SUBMITTED",
                                "reason": "ACKED",
                                "event_time_utc": "2026-04-04T00:00:02Z",
                            },
                            ensure_ascii=True,
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            cfg = SimpleNamespace(
                run_id="RUN_1",
                run_mode="submit_window",
                artifact_policy="minimal",
                events_path=events_path,
                run_audit_market_path=root / "audit" / "market_audit.csv",
                run_audit_funnel_summary_path=root / "audit" / "funnel_summary.json",
            )
            funnel_payload = {
                "stage0_expanded_market_count": 10,
                "stage1_structural_reject_count": 3,
                "stage1_state_reject_count": 1,
                "stage1_direct_candidate_count": 6,
                "stage2_live_eligible_count": 4,
                "stage2_live_price_miss_count": 1,
                "stage2_live_spread_too_wide_count": 1,
                "stage2_live_state_missing_count": 0,
                "stage2_live_state_stale_count": 0,
                "stage2_invalid_price_count": 0,
                "stage2_unaccounted_count": 0,
                "stage3_growth_filtered_count": 2,
                "stage3_selected_count": 2,
                "stage3_live_eligible_not_selected_count": 2,
                "stage4_submit_attempted_count": 2,
                "stage4_selected_not_attempted_count": 0,
                "stage4_submit_success_count": 1,
                "stage4_submit_rejection_count": 1,
                "stage4_submit_status_counts": {"ACKED": 1, "BEST_BID_MISSING": 1},
            }

            result = build_candidate_audit(cfg, funnel_payload=funnel_payload)

            market_audit = pd.read_csv(cfg.run_audit_market_path, dtype=str)
            funnel = json.loads(cfg.run_audit_funnel_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(result.market_count, 2)
            self.assertEqual(result.candidate_event_count, 2)
            self.assertEqual(set(market_audit["market_id"]), {"m-reject", "m-submit"})
            self.assertEqual(funnel["candidate_event_count"], 2)
            self.assertEqual(funnel["market_count"], 2)
            self.assertEqual(funnel["funnel_source"], "submit_window_manifest")
            stage_map = {row["stage"]: row for row in funnel["market_funnel"]}
            self.assertEqual(stage_map["expanded_market"]["unique_markets"], 10)
            self.assertEqual(stage_map["structural_reject"]["unique_markets"], 3)
            self.assertEqual(stage_map["submit_success"]["unique_markets"], 1)
            self.assertEqual(stage_map["submit_rejection"]["unique_markets"], 1)


if __name__ == "__main__":
    unittest.main()
