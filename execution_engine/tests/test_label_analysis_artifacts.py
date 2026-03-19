import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from execution_engine.online.analysis.label_history import load_scanned_market_ids, load_selection_history
from execution_engine.online.pipeline.submit_window import _persist_batch_training_artifacts, _write_selection_snapshot


class LabelAnalysisArtifactsTest(unittest.TestCase):
    def test_selection_snapshot_persists_rows_for_label_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            selection_path = run_dir / "snapshot_score" / "selection_decisions.csv"
            cfg = SimpleNamespace(
                data_dir=run_dir,
                runs_root_dir=run_dir.parent,
                run_id="run-1",
                run_date="2026-03-19",
            )

            first_batch = pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "batch_id": "batch-a",
                        "market_id": "market-1",
                        "selected_token_id": "token-1",
                        "selected_outcome_label": "YES",
                        "selected_for_submission": True,
                        "selection_reason": "allocated",
                        "growth_score": 1.0,
                        "f_exec": 0.02,
                        "q_pred": 0.62,
                        "trade_value_pred": 1.3,
                        "price": 0.41,
                        "horizon_hours": 4.0,
                        "direction_model": 1,
                        "position_side": "OUTCOME_0",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "rule_group_key": "example.com|SPORTS|moneyline",
                        "rule_leaf_id": 10,
                        "settlement_key": "2026-03-20",
                        "cluster_key": "example.com|SPORTS|2026-03-20",
                    }
                ]
            )
            second_batch = pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "batch_id": "batch-a",
                        "market_id": "market-1",
                        "selected_token_id": "token-1",
                        "selected_outcome_label": "YES",
                        "selected_for_submission": False,
                        "selection_reason": "not_allocated",
                        "growth_score": 0.8,
                        "f_exec": 0.01,
                        "q_pred": 0.58,
                        "trade_value_pred": 1.0,
                        "price": 0.41,
                        "horizon_hours": 4.0,
                        "direction_model": 1,
                        "position_side": "OUTCOME_0",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "rule_group_key": "example.com|SPORTS|moneyline",
                        "rule_leaf_id": 10,
                        "settlement_key": "2026-03-20",
                        "cluster_key": "example.com|SPORTS|2026-03-20",
                    },
                    {
                        "run_id": "run-1",
                        "batch_id": "batch-b",
                        "market_id": "market-2",
                        "selected_token_id": "token-2",
                        "selected_outcome_label": "NO",
                        "selected_for_submission": True,
                        "selection_reason": "allocated",
                        "growth_score": 1.2,
                        "f_exec": 0.03,
                        "q_pred": 0.67,
                        "trade_value_pred": 1.5,
                        "price": 0.33,
                        "horizon_hours": 2.0,
                        "direction_model": -1,
                        "position_side": "OUTCOME_1",
                        "category": "SPORTS",
                        "domain": "example.com",
                        "market_type": "moneyline",
                        "rule_group_key": "example.com|SPORTS|moneyline",
                        "rule_leaf_id": 11,
                        "settlement_key": "2026-03-21",
                        "cluster_key": "example.com|SPORTS|2026-03-21",
                    },
                ]
            )

            _write_selection_snapshot(selection_path, first_batch)
            _write_selection_snapshot(selection_path, second_batch)

            loaded = load_selection_history(cfg, scope="run")

            self.assertEqual(len(loaded), 2)
            market_1 = loaded[loaded["market_id"] == "market-1"].iloc[0]
            self.assertEqual(market_1["selection_reason"], "not_allocated")
            self.assertEqual(str(market_1["selected_for_submission"]).lower(), "false")
            self.assertEqual(set(loaded["market_id"].tolist()), {"market-1", "market-2"})

    def test_load_scanned_market_ids_uses_submit_window_artifacts_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            cfg = SimpleNamespace(
                data_dir=run_dir,
                runs_root_dir=run_dir.parent,
            )

            snapshot_dir = run_dir / "snapshot_score"
            submit_dir = run_dir / "submit_hourly"
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            submit_dir.mkdir(parents=True, exist_ok=True)

            pd.DataFrame([{"market_id": "market-selection"}]).to_csv(snapshot_dir / "selection_decisions.csv", index=False)
            (run_dir / "decisions.jsonl").write_text(
                json.dumps({"market_id": "market-decision"}) + "\n",
                encoding="utf-8",
            )
            (run_dir / "events.jsonl").write_text(
                json.dumps({"market_id": "market-event", "event_type": "CANDIDATE_STATE"}) + "\n",
                encoding="utf-8",
            )
            (submit_dir / "orders_submitted.jsonl").write_text(
                json.dumps({"market_id": "market-submitted"}) + "\n",
                encoding="utf-8",
            )
            pd.DataFrame([{"market_id": "legacy-universe"}]).to_csv(run_dir / "universe.csv", index=False)
            pd.DataFrame([{"market_id": "legacy-processed"}]).to_csv(run_dir / "processed_markets.csv", index=False)

            market_ids = load_scanned_market_ids(cfg, scope="run")

            self.assertEqual(
                market_ids,
                {"market-selection", "market-decision", "market-event", "market-submitted"},
            )

    def test_batch_training_artifacts_persist_inputs_and_predictions_per_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            cfg = SimpleNamespace(
                run_id="run-1",
                run_mode="submit_window",
                run_snapshot_processed_markets_path=run_dir / "snapshot_score" / "processed_markets.csv",
                run_snapshot_raw_inputs_path=run_dir / "snapshot_score" / "raw_snapshot_inputs.jsonl",
                run_snapshot_normalized_path=run_dir / "snapshot_score" / "normalized_snapshots.csv",
                run_snapshot_feature_inputs_path=run_dir / "snapshot_score" / "feature_inputs.csv",
                run_snapshot_rule_hits_path=run_dir / "snapshot_score" / "rule_hits.csv",
                run_snapshot_model_outputs_path=run_dir / "snapshot_score" / "model_outputs.csv",
                run_snapshot_selection_path=run_dir / "snapshot_score" / "selection_decisions.csv",
                run_snapshot_score_manifest_path=run_dir / "snapshot_score" / "manifest.json",
            )
            runtime = SimpleNamespace(cfg=cfg)
            batch = SimpleNamespace(
                batch_id="batch-a",
                frame=pd.DataFrame([{"market_id": "market-1", "selected_reference_token_id": "ref-1"}]),
            )
            inference_result = SimpleNamespace(
                live_filter=SimpleNamespace(
                    eligible=pd.DataFrame([{"market_id": "market-1", "selected_reference_token_id": "ref-1"}])
                ),
                snapshots=pd.DataFrame([{"market_id": "market-1", "snapshot_time": "2026-03-19T00:00:00Z"}]),
                rule_model=SimpleNamespace(
                    rule_hits=pd.DataFrame(
                        [
                            {
                                "market_id": "market-1",
                                "snapshot_time": "2026-03-19T00:00:00Z",
                                "rule_group_key": "g1",
                                "rule_leaf_id": 1,
                            }
                        ]
                    ),
                    feature_inputs=pd.DataFrame(
                        [
                            {
                                "market_id": "market-1",
                                "snapshot_time": "2026-03-19T00:00:00Z",
                                "rule_group_key": "g1",
                                "rule_leaf_id": 1,
                                "feature_a": 1.23,
                            }
                        ]
                    ),
                    model_outputs=pd.DataFrame(
                        [
                            {
                                "market_id": "market-1",
                                "snapshot_time": "2026-03-19T00:00:00Z",
                                "rule_group_key": "g1",
                                "rule_leaf_id": 1,
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
            )
            selection = pd.DataFrame(
                [
                    {
                        "run_id": "run-1",
                        "batch_id": "batch-a",
                        "market_id": "market-1",
                        "selected_token_id": "token-0",
                        "rule_group_key": "g1",
                        "rule_leaf_id": 1,
                        "selected_for_submission": True,
                        "selection_reason": "allocated",
                    }
                ]
            )

            _persist_batch_training_artifacts(runtime, batch, inference_result, selection)

            processed = pd.read_csv(cfg.run_snapshot_processed_markets_path, dtype=str)
            normalized = pd.read_csv(cfg.run_snapshot_normalized_path, dtype=str)
            rule_hits = pd.read_csv(cfg.run_snapshot_rule_hits_path, dtype=str)
            features = pd.read_csv(cfg.run_snapshot_feature_inputs_path, dtype=str)
            outputs = pd.read_csv(cfg.run_snapshot_model_outputs_path, dtype=str)
            selections = pd.read_csv(cfg.run_snapshot_selection_path, dtype=str)
            raw_lines = cfg.run_snapshot_raw_inputs_path.read_text(encoding="utf-8").strip().splitlines()
            manifest = json.loads(cfg.run_snapshot_score_manifest_path.read_text(encoding="utf-8"))

            self.assertEqual(len(processed), 1)
            self.assertEqual(len(normalized), 1)
            self.assertEqual(len(rule_hits), 1)
            self.assertEqual(len(features), 1)
            self.assertEqual(len(outputs), 1)
            self.assertEqual(len(selections), 1)
            self.assertEqual(len(raw_lines), 1)
            self.assertEqual(outputs.iloc[0]["selected_token_id"], "token-0")
            self.assertEqual(manifest["model_output_count"], 1)
            self.assertEqual(manifest["selection_decision_count"], 1)


if __name__ == "__main__":
    unittest.main()
