import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.datasets.splits import compute_artifact_split, select_preferred_split
from rule_baseline.training.train_snapshot_model import DEFAULT_OFFLINE_TRAIN_SAMPLE_ROWS, resolve_train_sample_rows


class TrainingSplitDefaultsTest(unittest.TestCase):
    def test_offline_artifact_split_uses_last_month_as_validation(self) -> None:
        df = pd.DataFrame(
            {
                "closedTime": [
                    "2026-01-15T00:00:00Z",
                    "2026-02-15T00:00:00Z",
                    "2026-03-20T00:00:00Z",
                    "2026-04-16T00:00:00Z",
                ]
            }
        )

        split = compute_artifact_split(
            df,
            artifact_mode="offline",
            reference_end="2026-04-16T00:00:00Z",
            history_start_override="2026-01-01T00:00:00Z",
        )

        self.assertEqual(split.train_start, pd.Timestamp("2026-01-01T00:00:00Z"))
        self.assertEqual(split.train_end, pd.Timestamp("2026-03-16T23:59:59Z"))
        self.assertEqual(split.valid_start, pd.Timestamp("2026-03-17T00:00:00Z"))
        self.assertEqual(split.valid_end, pd.Timestamp("2026-04-16T00:00:00Z"))
        self.assertIsNone(split.test_start)
        self.assertIsNone(split.test_end)

    def test_select_preferred_split_falls_back_to_valid(self) -> None:
        df = pd.DataFrame(
            [
                {"dataset_split": "train", "value": 1},
                {"dataset_split": "valid", "value": 2},
            ]
        )

        split_name, selected = select_preferred_split(df)

        self.assertEqual(split_name, "valid")
        self.assertEqual(selected["value"].tolist(), [2])

    def test_resolve_train_sample_rows_defaults_to_200k_for_offline(self) -> None:
        self.assertEqual(resolve_train_sample_rows("offline", None), DEFAULT_OFFLINE_TRAIN_SAMPLE_ROWS)
        self.assertIsNone(resolve_train_sample_rows("online", None))
        self.assertEqual(resolve_train_sample_rows("offline", 12345), 12345)


if __name__ == "__main__":
    unittest.main()
