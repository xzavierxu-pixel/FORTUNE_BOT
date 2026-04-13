import os
import sys
import unittest

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.training.train_rules_naive_output_rule import (
    MIN_GROUP_UNIQUE_MARKETS,
    build_group_decisions,
    build_rules,
)


def _make_group_rows(
    domain: str,
    *,
    category: str = "SPORTS",
    market_type: str = "other",
    unique_markets: int = 20,
    price: float = 0.5,
    y: int = 0,
    horizon_hours: int = 6,
    price_bin: str = "0.40-0.50",
    rows_per_market: int = 1,
) -> list[dict]:
    rows: list[dict] = []
    for market_idx in range(unique_markets):
        market_id = f"{domain}-m{market_idx}"
        for row_idx in range(rows_per_market):
            rows.append(
                {
                    "market_id": market_id,
                    "domain": domain,
                    "category": category,
                    "market_type": market_type,
                    "price": price,
                    "y": y,
                    "r_std": 0.1,
                    "dataset_split": "train",
                    "price_bin": price_bin,
                    "horizon_hours": horizon_hours,
                }
            )
    return rows


class GroupKeyRuleGenerationTest(unittest.TestCase):
    def test_build_group_decisions_uses_q25_threshold_and_market_floor(self) -> None:
        rows = []
        rows += _make_group_rows("drop.example", unique_markets=20, price=0.95, y=1)
        rows += _make_group_rows("keep-a.example", unique_markets=20, price=0.5, y=1)
        rows += _make_group_rows("keep-b.example", unique_markets=20, price=0.5, y=0)
        rows += _make_group_rows("keep-c.example", unique_markets=20, price=0.6, y=0)
        rows += _make_group_rows("insufficient.example", unique_markets=MIN_GROUP_UNIQUE_MARKETS - 1, price=0.99, y=1)
        df = pd.DataFrame(rows)

        group_stats, thresholds = build_group_decisions(df)
        decisions = dict(zip(group_stats["group_key"], group_stats["selection_status"]))

        self.assertEqual(decisions["drop.example|SPORTS|other"], "drop")
        self.assertEqual(decisions["keep-a.example|SPORTS|other"], "keep")
        self.assertEqual(decisions["keep-b.example|SPORTS|other"], "keep")
        self.assertEqual(decisions["keep-c.example|SPORTS|other"], "keep")
        self.assertEqual(decisions["insufficient.example|SPORTS|other"], "insufficient_data")
        self.assertLess(thresholds["global_group_logloss_q25"], 0.69314718056)
        self.assertLess(thresholds["global_group_brier_q25"], 0.25)

    def test_build_rules_preserves_price_bins_and_exact_horizon_hours(self) -> None:
        rows = []
        rows += _make_group_rows(
            "keep.example",
            unique_markets=20,
            price=0.5,
            y=1,
            horizon_hours=6,
            price_bin="0.40-0.50",
        )
        rows += _make_group_rows(
            "keep.example",
            unique_markets=20,
            price=0.7,
            y=0,
            horizon_hours=12,
            price_bin="0.70-0.80",
        )
        rows += _make_group_rows(
            "drop.example",
            unique_markets=20,
            price=0.99,
            y=1,
            horizon_hours=6,
            price_bin="0.90-1.00",
        )
        df = pd.DataFrame(rows)

        rules_df, report_df = build_rules(df, "offline")

        self.assertEqual(set(report_df["selection_status"]), {"keep", "drop"})
        self.assertEqual(set(rules_df["group_key"]), {"keep.example|SPORTS|other"})
        self.assertEqual(set(rules_df["price_min"]), {0.4, 0.7})
        self.assertEqual(set(rules_df["price_max"]), {0.5, 0.8})
        self.assertEqual(set(rules_df["horizon_hours"]), {6, 12})

        horizon_6 = rules_df[rules_df["horizon_hours"] == 6].iloc[0]
        horizon_12 = rules_df[rules_df["horizon_hours"] == 12].iloc[0]
        self.assertEqual((horizon_6["h_min"], horizon_6["h_max"]), (5.0, 9.0))
        self.assertEqual((horizon_12["h_min"], horizon_12["h_max"]), (9.0, 18.0))
        self.assertTrue((rules_df["group_decision"] == "keep").all())


if __name__ == "__main__":
    unittest.main()
