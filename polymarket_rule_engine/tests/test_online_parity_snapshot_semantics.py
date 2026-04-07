import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

sys.path.append(os.path.abspath("."))
sys.path.append(os.path.abspath("polymarket_rule_engine"))

from execution_engine.online.scoring.price_history import PricePoint, build_historical_price_features, build_quote_window_features
from rule_baseline.data_collection.build_snapshots import generate_snapshots
from rule_baseline.datasets.snapshots import add_term_structure_features, project_online_contract_snapshot_rows


class OnlineParitySnapshotSemanticsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cfg = SimpleNamespace(rule_engine_dir=Path.cwd() / "polymarket_rule_engine")
        self.t_start = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        self.t_res = datetime(2026, 4, 2, 0, 0, tzinfo=timezone.utc)
        self.end_ts = int(self.t_res.timestamp())
        self.row = {
            "id": "m1",
            "market_id": "m1",
            "category": "SPORTS",
            "resolutionSource": "https://example.com/story",
        }
        self.token_meta = {
            "primary_token_id": "tok_yes",
            "secondary_token_id": "tok_no",
            "primary_outcome": "Yes",
            "secondary_outcome": "No",
        }
        self.history = [
            {"t": self.end_ts - 24 * 3600, "p": 0.25},
            {"t": self.end_ts - 12 * 3600, "p": 0.35},
            {"t": self.end_ts - 6 * 3600 - 180, "p": 0.41},
            {"t": self.end_ts - 6 * 3600 + 60, "p": 0.42},
            {"t": self.end_ts - 4 * 3600, "p": 0.44},
            {"t": self.end_ts - 2 * 3600, "p": 0.52},
            {"t": self.end_ts - 1 * 3600, "p": 0.61},
        ]
        self.points = [PricePoint(ts=item["t"], price=float(item["p"]), source="history") for item in self.history]

    def test_offline_snapshot_quote_window_matches_live_builder(self) -> None:
        snapshots, _ = generate_snapshots(
            row=self.row,
            token_meta=self.token_meta,
            winner_index=0,
            t_start=self.t_start,
            t_sched=self.t_res,
            t_res=self.t_res,
            delta_hours=0.0,
            y_ref=1,
            history=self.history,
        )
        snapshot_row = next(item for item in snapshots if int(item["horizon_hours"]) == 4)

        live_quote = build_quote_window_features(
            self.cfg,
            merged_points=self.points,
            target_ts=int(snapshot_row["snapshot_target_ts"]),
        )

        self.assertEqual(snapshot_row["selected_quote_ts"], live_quote["selected_quote_ts"])
        self.assertEqual(snapshot_row["selected_quote_side"], live_quote["selected_quote_side"])
        self.assertEqual(snapshot_row["selected_quote_offset_sec"], live_quote["selected_quote_offset_sec"])
        self.assertEqual(snapshot_row["selected_quote_points_in_window"], live_quote["selected_quote_points_in_window"])
        self.assertEqual(snapshot_row["selected_quote_left_gap_sec"], live_quote["selected_quote_left_gap_sec"])
        self.assertEqual(snapshot_row["selected_quote_right_gap_sec"], live_quote["selected_quote_right_gap_sec"])
        self.assertEqual(snapshot_row["selected_quote_local_gap_sec"], live_quote["selected_quote_local_gap_sec"])
        self.assertEqual(snapshot_row["stale_quote_flag"], live_quote["stale_quote_flag"])

    def test_offline_term_structure_matches_live_builder(self) -> None:
        snapshots, _ = generate_snapshots(
            row=self.row,
            token_meta=self.token_meta,
            winner_index=0,
            t_start=self.t_start,
            t_sched=self.t_res,
            t_res=self.t_res,
            delta_hours=0.0,
            y_ref=1,
            history=self.history,
        )
        offline_frame = add_term_structure_features(pd.DataFrame(snapshots))
        offline_row = offline_frame[offline_frame["horizon_hours"] == 4].iloc[0]

        live_history = build_historical_price_features(
            current_price=float(offline_row["price"]),
            now_ts=int(offline_row["snapshot_target_ts"]),
            end_ts=self.end_ts,
            merged_points=self.points,
        )

        for column in [
            "p_1h",
            "p_2h",
            "p_4h",
            "p_6h",
            "p_12h",
            "p_24h",
            "delta_p_1_2",
            "delta_p_2_4",
            "delta_p_4_12",
            "delta_p_12_24",
            "term_structure_slope",
            "path_price_mean",
            "path_price_std",
            "path_price_min",
            "path_price_max",
            "path_price_range",
            "price_reversal_flag",
            "price_acceleration",
            "closing_drift",
        ]:
            offline_value = offline_row[column]
            live_value = live_history[column]
            if pd.isna(offline_value):
                self.assertIsNone(live_value, column)
            else:
                self.assertAlmostEqual(float(offline_value), float(live_value), places=6, msg=column)

    def test_project_online_contract_snapshot_rows_uses_shared_projection(self) -> None:
        snapshots = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "category": "SPORTS",
                    "price": 0.44,
                    "horizon_hours": 4,
                    "y": 1,
                    "r_std": 0.2,
                    "scheduled_end": self.t_res.isoformat(),
                    "closedTime": self.t_res.isoformat(),
                    "delta_hours": 0.0,
                    "source_host": "example.com",
                    "primary_token_id": "tok_yes",
                    "secondary_token_id": "tok_no",
                    "primary_outcome": "Yes",
                    "secondary_outcome": "No",
                    "winning_outcome_index": 0,
                    "winning_outcome_label": "Yes",
                    "snapshot_target_ts": self.end_ts - 4 * 3600,
                    "selected_quote_ts": self.end_ts - 4 * 3600,
                    "selected_quote_side": "right",
                    "selected_quote_offset_sec": 0,
                    "selected_quote_points_in_window": 1,
                    "selected_quote_left_gap_sec": 100,
                    "selected_quote_right_gap_sec": 0,
                    "selected_quote_local_gap_sec": 100,
                    "stale_quote_flag": False,
                }
            ]
        )

        projected = project_online_contract_snapshot_rows(snapshots)

        self.assertEqual(projected.iloc[0]["token_0_id"], "tok_yes")
        self.assertEqual(projected.iloc[0]["token_1_id"], "tok_no")
        self.assertEqual(projected.iloc[0]["selected_reference_token_id"], "tok_yes")
        self.assertEqual(projected.iloc[0]["selected_reference_outcome_label"], "Yes")
        self.assertEqual(projected.iloc[0]["category_source"], "snapshot")


if __name__ == "__main__":
    unittest.main()
