import unittest
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from execution_engine.online.scoring.annotations import apply_online_market_annotations


class OnlineAnnotationsTest(unittest.TestCase):
    def test_apply_online_market_annotations_overrides_refresh_placeholders(self) -> None:
        markets = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "resolution_source": "https://example.com/story",
                    "description": "Example market",
                    "game_id": "",
                    "category": "UNKNOWN",
                    "category_raw": "POLITICS",
                    "category_parsed": "UNKNOWN",
                    "category_override_flag": False,
                    "domain": "UNKNOWN",
                    "domain_parsed": "UNKNOWN",
                    "sub_domain": "",
                    "source_url": "UNKNOWN",
                    "market_type": "UNKNOWN",
                    "outcome_pattern": "UNKNOWN",
                    "outcome_0_label": "Yes",
                    "outcome_1_label": "No",
                }
            ]
        )
        cfg = SimpleNamespace()
        online_annotations = pd.DataFrame(
            [
                {
                    "market_id": "m1",
                    "domain": "example.com.special",
                    "domain_parsed": "example.com",
                    "sub_domain": "/story",
                    "source_url": "https://example.com/story",
                    "category": "FINANCE",
                    "category_raw": "POLITICS",
                    "category_parsed": "FINANCE",
                    "category_override_flag": True,
                    "market_type": "moneyline",
                    "outcome_pattern": "no_yes",
                }
            ]
        )

        with patch("execution_engine.online.scoring.annotations.build_online_annotations", return_value=online_annotations):
            annotated = apply_online_market_annotations(cfg, markets)

        row = annotated.iloc[0]
        self.assertEqual(row["domain"], "example.com.special")
        self.assertEqual(row["category"], "FINANCE")
        self.assertEqual(row["category_raw"], "POLITICS")
        self.assertEqual(row["category_parsed"], "FINANCE")
        self.assertTrue(bool(row["category_override_flag"]))
        self.assertEqual(row["market_type"], "moneyline")


if __name__ == "__main__":
    unittest.main()
