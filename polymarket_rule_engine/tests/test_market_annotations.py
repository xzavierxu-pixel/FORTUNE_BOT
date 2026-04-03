from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.domain_extractor.market_annotations import (
    MarketSourceParser,
    build_domain_candidate,
    build_market_annotations,
    build_domain_summary,
    build_domain_summary_aggregated,
    extract_coarse_market_family,
    normalize_annotation_buckets,
    resolve_source_url,
    save_market_annotations,
)
from rule_baseline.utils import config


def _build_raw_frame(rows: list[dict[str, str]]) -> pd.DataFrame:
    defaults = {
        "resolutionSource": "",
        "description": "",
        "outcomes": '["Yes","No"]',
        "gameId": "",
        "category": "",
    }
    normalized = []
    for row in rows:
        item = defaults.copy()
        item.update(row)
        normalized.append(item)
    return pd.DataFrame(normalized)


def test_build_market_annotations_domain_breakdown(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_DOMAIN_COUNT", 2)
    raw = _build_raw_frame(
        [
            {
                "id": "1",
                "resolutionSource": "https://www.ncaa.com/news/basketball-men/article",
                "description": "NCAA Men's Basketball Championship odds",
            },
            {
                "id": "2",
                "resolutionSource": "https://www.ncaa.com/news/basketball-men/article-two",
                "description": "March Madness futures market",
            },
            {
                "id": "3",
                "resolutionSource": "https://www.ncaa.com/news/swimming/article",
                "description": "NCAA swimming title odds",
            },
            {
                "id": "4",
                "resolutionSource": "https://www.binance.com/en/trade/BTC_USDT",
            },
            {
                "id": "5",
                "resolutionSource": "https://www.binance.com/price/ETHUSDT",
            },
            {
                "id": "6",
                "resolutionSource": "https://www.binance.com/en/trade/ETH_BTC",
            },
            {
                "id": "7",
                "resolutionSource": "https://liquipedia.net/counterstrike/Some_Page",
            },
            {
                "id": "8",
                "resolutionSource": "https://liquipedia.net/counterstrike/Another_Page",
            },
            {
                "id": "9",
                "resolutionSource": "https://example.com/markets/abc",
            },
            {
                "id": "10",
                "resolutionSource": "https://example.com/markets/def",
            },
        ]
    )

    annotations = build_market_annotations(raw, include_domain_candidate=True)
    by_market = annotations.set_index("market_id")

    assert by_market.loc["1", "domain_candidate"] == "ncaa.com.basketball"
    assert by_market.loc["2", "domain_candidate"] == "ncaa.com.basketball"
    assert by_market.loc["1", "domain"] == "ncaa.com.basketball"
    assert by_market.loc["3", "domain_candidate"] == "ncaa.com.other"
    assert by_market.loc["3", "domain"] == "ncaa.com"

    assert by_market.loc["4", "domain_candidate"] == "binance.com/en/trade/BTC_USDT"
    assert by_market.loc["5", "domain_candidate"] == "binance.com/price/ETHUSDT"
    assert by_market.loc["6", "domain_candidate"] == "binance.com"
    assert by_market.loc["6", "domain"] == "OTHER"

    assert by_market.loc["7", "domain_candidate"] == "liquipedia.net/counterstrike"
    assert by_market.loc["8", "domain"] == "liquipedia.net/counterstrike"

    assert by_market.loc["9", "domain_candidate"] == "example.com"
    assert by_market.loc["10", "domain"] == "example.com"


def test_build_market_annotations_default_schema_unchanged(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_DOMAIN_COUNT", 1)
    raw = _build_raw_frame(
        [
            {
                "id": "1",
                "resolutionSource": "https://www.ncaa.com/news/football/article",
                "description": "College football title market",
            }
        ]
    )

    annotations = build_market_annotations(raw)
    assert "domain_candidate" not in annotations.columns
    assert annotations.loc[0, "domain_parsed"] == "ncaa.com"
    assert annotations.loc[0, "domain"] == "ncaa.com.football"


def test_resolve_source_url_skips_amazonaws_links_in_description() -> None:
    row = pd.Series(
        {
            "resolutionSource": "",
            "description": (
                "Mirror https://polymarket-prod.s3.amazonaws.com/some/file first, "
                "canonical source https://www.nba.com/game/123 after."
            ),
        }
    )

    assert resolve_source_url(row) == "https://www.nba.com/game/123"


def test_build_market_annotations_low_frequency_child_falls_back_to_parent(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_DOMAIN_COUNT", 2)
    raw = _build_raw_frame(
        [
            {
                "id": "1",
                "resolutionSource": "https://www.binance.com/en/trade/BTC_USDT",
            },
            {
                "id": "2",
                "resolutionSource": "https://www.binance.com/en/trade/ETH_USDT",
            },
            {
                "id": "3",
                "resolutionSource": "https://www.binance.com/en/trade/SOL_USDT",
            },
            {
                "id": "4",
                "resolutionSource": "https://example.com/path/a",
            },
        ]
    )

    annotations = build_market_annotations(raw, include_domain_candidate=True).set_index("market_id")

    assert annotations.loc["1", "domain_candidate"] == "binance.com/en/trade/BTC_USDT"
    assert annotations.loc["1", "domain"] == "binance.com"
    assert annotations.loc["2", "domain"] == "binance.com"
    assert annotations.loc["3", "domain"] == "binance.com"
    assert annotations.loc["4", "domain"] == "OTHER"


def test_build_market_annotations_applies_bucket_rollups(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_DOMAIN_COUNT", 1)
    monkeypatch.setattr(config, "LOW_FREQUENCY_BUCKET_COUNT", 3)
    raw = _build_raw_frame(
        [
            {"id": "1", "resolutionSource": "https://example.com/a", "category": "SPORTS", "outcomes": '["Yes","No"]'},
            {"id": "2", "resolutionSource": "https://example.com/b", "category": "SPORTS", "outcomes": '["Yes","No"]'},
            {"id": "3", "resolutionSource": "https://example.com/c", "category": "SPORTS", "outcomes": '["Over","Under"]'},
            {"id": "4", "resolutionSource": "https://example.com/d", "category": "TECH", "outcomes": '["Yes","No"]'},
            {"id": "5", "resolutionSource": "https://example.com/e", "category": "POLITICS", "outcomes": '["Yes","No"]'},
            {"id": "6", "resolutionSource": "https://example.com/f", "category": "POLITICS", "outcomes": '["Yes","No"]'},
            {"id": "7", "resolutionSource": "https://example.com/g", "category": "POLITICS", "outcomes": '["Yes","No"]'},
        ]
    )

    annotations = build_market_annotations(raw).set_index("market_id")

    assert annotations.loc["3", "category"] == "POLITICS"
    assert annotations.loc["3", "market_type"] == "over_under"
    assert annotations.loc["4", "category"] == "POLITICS"
    assert annotations.loc["4", "market_type"] == "no_yes"


def test_normalize_annotation_buckets_limits_domain_to_three_categories(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_BUCKET_COUNT", 1)
    annotations = pd.DataFrame(
        [
            {"market_id": "1", "domain": "example.com", "category": "POLITICS", "market_type": "no_yes"},
            {"market_id": "2", "domain": "example.com", "category": "POLITICS", "market_type": "other"},
            {"market_id": "3", "domain": "example.com", "category": "SPORTS", "market_type": "no_yes"},
            {"market_id": "4", "domain": "example.com", "category": "SPORTS", "market_type": "other"},
            {"market_id": "5", "domain": "example.com", "category": "TECH", "market_type": "no_yes"},
            {"market_id": "6", "domain": "example.com", "category": "FINANCE", "market_type": "no_yes"},
            {"market_id": "7", "domain": "example.com", "category": "CULTURE", "market_type": "no_yes"},
        ]
    )

    normalized = normalize_annotation_buckets(annotations)
    example_categories = set(normalized.loc[normalized["domain"] == "example.com", "category"])

    assert len(example_categories) == 3
    assert example_categories == {"POLITICS", "SPORTS", "OTHER"}


def test_normalize_annotation_buckets_rolls_small_buckets_up(monkeypatch) -> None:
    monkeypatch.setattr(config, "LOW_FREQUENCY_BUCKET_COUNT", 3)
    annotations = pd.DataFrame(
        [
            {"market_id": "1", "domain": "example.com", "category": "SPORTS", "market_type": "no_yes"},
            {"market_id": "2", "domain": "example.com", "category": "SPORTS", "market_type": "no_yes"},
            {"market_id": "3", "domain": "example.com", "category": "SPORTS", "market_type": "over_under"},
            {"market_id": "4", "domain": "example.com", "category": "TECH", "market_type": "no_yes"},
            {"market_id": "5", "domain": "example.com", "category": "POLITICS", "market_type": "no_yes"},
            {"market_id": "6", "domain": "example.com", "category": "POLITICS", "market_type": "no_yes"},
            {"market_id": "7", "domain": "example.com", "category": "POLITICS", "market_type": "no_yes"},
        ]
    )

    normalized = normalize_annotation_buckets(annotations).set_index("market_id")

    assert normalized.loc["3", "category"] == "POLITICS"
    assert normalized.loc["3", "market_type"] == "over_under"
    assert normalized.loc["4", "category"] == "POLITICS"
    assert normalized.loc["4", "market_type"] == "no_yes"


def test_build_domain_summary_aggregated_sums_market_count() -> None:
    summary = pd.DataFrame(
        [
            {
                "domain": "example.com",
                "category": "SPORTS",
                "market_type": "no_yes",
                "sub_domain": "/a",
                "market_count": 3,
            },
            {
                "domain": "example.com",
                "category": "SPORTS",
                "market_type": "no_yes",
                "sub_domain": "/b",
                "market_count": 5,
            },
            {
                "domain": "example.com",
                "category": "SPORTS",
                "market_type": "other",
                "sub_domain": "/b",
                "market_count": 2,
            },
        ]
    )

    aggregated = build_domain_summary_aggregated(summary)
    by_key = aggregated.set_index(["domain", "category", "market_type"])

    assert list(aggregated.columns) == ["domain", "category", "market_type", "market_count"]
    assert by_key.loc[("example.com", "SPORTS", "no_yes"), "market_count"] == 8
    assert by_key.loc[("example.com", "SPORTS", "other"), "market_count"] == 2


def test_save_market_annotations_writes_aggregated_summary(monkeypatch, tmp_path) -> None:
    annotations = pd.DataFrame(
        [
            {
                "market_id": "1",
                "domain": "example.com",
                "domain_parsed": "example.com",
                "sub_domain": "/a",
                "source_url": "https://example.com/a",
                "category": "SPORTS",
                "category_raw": "SPORTS",
                "category_parsed": "SPORTS",
                "category_override_flag": False,
                "market_type": "no_yes",
                "outcome_pattern": "no_yes",
            },
            {
                "market_id": "2",
                "domain": "example.com",
                "domain_parsed": "example.com",
                "sub_domain": "/b",
                "source_url": "https://example.com/b",
                "category": "SPORTS",
                "category_raw": "SPORTS",
                "category_parsed": "SPORTS",
                "category_override_flag": False,
                "market_type": "no_yes",
                "outcome_pattern": "no_yes",
            },
        ]
    )

    market_path = tmp_path / "market_domain_features.csv"
    summary_path = tmp_path / "domain_summary.csv"
    aggregated_path = tmp_path / "domain_summary_aggregated.csv"
    other_patterns_path = tmp_path / "other_outcome_patterns_by_url.csv"

    monkeypatch.setattr(config, "MARKET_DOMAIN_FEATURES_PATH", market_path)
    monkeypatch.setattr(config, "DOMAIN_SUMMARY_PATH", summary_path)
    monkeypatch.setattr(config, "DOMAIN_SUMMARY_AGGREGATED_PATH", aggregated_path)
    monkeypatch.setattr(config, "OTHER_OUTCOME_PATTERNS_BY_URL_PATH", other_patterns_path)

    save_market_annotations(annotations)

    aggregated = pd.read_csv(aggregated_path)

    assert aggregated_path.exists()
    assert list(aggregated.columns) == ["domain", "category", "market_type", "market_count"]
    assert aggregated.loc[0, "market_count"] == 2


def test_extract_coarse_market_family_priority() -> None:
    assert extract_coarse_market_family(
        "Spread: Yankees (-1.5)",
        "This market also references total runs over/under 8.5.",
    ) == "spread"
    assert extract_coarse_market_family(
        "Total Kills Over/Under 50.5 in Game 1?",
        "This is about the total kills in Game 1.",
    ) == "total"
    assert extract_coarse_market_family(
        "Scorigami in NFL Week 10?",
        "This market resolves yes if a new scoreline occurs.",
    ) == "prop"
    assert extract_coarse_market_family(
        "Bruins vs. Blackhawks",
        "If the Boston Bruins win, the market resolves to Bruins.",
    ) == "moneyline"


def test_normalize_sub_domain_exclusions() -> None:
    excluded_domains = [
        "atptour.com",
        "gol.gg",
        "mlb.com",
        "nba.com",
        "nfl.com",
        "nhl.com",
        "sofascore.com",
        "wtatennis.com",
    ]

    for domain in excluded_domains:
        assert MarketSourceParser.normalize_sub_domain(domain, "/news/example-path") == ""


def test_normalize_sub_domain_twitch() -> None:
    assert MarketSourceParser.normalize_sub_domain("twitch.tv", "/BLASTDota/videos") == "/BLASTDota"


def test_build_domain_candidate_twitch() -> None:
    row = pd.Series({"domain_parsed": "twitch.tv", "sub_domain": "/lck", "description": ""})

    assert build_domain_candidate(row) == "twitch.tv/lck"


def test_build_domain_candidate_twitch_non_whitelisted_channel() -> None:
    row = pd.Series({"domain_parsed": "twitch.tv", "sub_domain": "/randomchannel", "description": ""})

    assert build_domain_candidate(row) == "twitch.tv"


def test_build_domain_candidate_twitch_new_whitelisted_channels() -> None:
    eslcs_row = pd.Series({"domain_parsed": "twitch.tv", "sub_domain": "/ESLCS", "description": ""})
    cblol_row = pd.Series({"domain_parsed": "twitch.tv", "sub_domain": "/cblol", "description": ""})

    assert build_domain_candidate(eslcs_row) == "twitch.tv/ESLCS"
    assert build_domain_candidate(cblol_row) == "twitch.tv/cblol"


def test_build_domain_candidate_chainlink() -> None:
    row = pd.Series({"domain_parsed": "chain.link", "sub_domain": "/ecosystem", "description": ""})

    assert build_domain_candidate(row) == "chain.link/ecosystem"


def test_build_domain_candidate_ncaa_other_bucket() -> None:
    row = pd.Series(
        {
            "domain_parsed": "ncaa.com",
            "sub_domain": "",
            "description": "NCAA swimming championship odds",
            "question": "Swimming title market",
        }
    )

    assert build_domain_candidate(row) == "ncaa.com.other"


def test_build_domain_candidate_ncaa_championship_bucket() -> None:
    row = pd.Series(
        {
            "domain_parsed": "ncaa.com",
            "sub_domain": "",
            "description": "NCAA championship title odds",
            "question": "Who wins the NCAA national championship?",
        }
    )

    assert build_domain_candidate(row) == "ncaa.com.championship"


def test_build_domain_candidate_nba_total() -> None:
    row = pd.Series(
        {
            "domain_parsed": "nba.com",
            "sub_domain": "",
            "question": "Hornets vs. Magic: O/U 220.5",
            "description": "This market resolves based on the total combined points.",
        }
    )

    assert build_domain_candidate(row) == "nba.com.total"


def test_build_domain_candidate_nfl_prop() -> None:
    row = pd.Series(
        {
            "domain_parsed": "nfl.com",
            "sub_domain": "",
            "question": "Scorigami in NFL Week 10?",
            "description": "This market resolves yes if a new scoring combination occurs.",
        }
    )

    assert build_domain_candidate(row) == "nfl.com.prop"


def test_build_domain_candidate_gol_total() -> None:
    row = pd.Series(
        {
            "domain_parsed": "gol.gg",
            "sub_domain": "",
            "question": "Total Kills Over/Under 50.5 in Game 1?",
            "description": "This market is about the total kills in Game 1.",
        }
    )

    assert build_domain_candidate(row) == "gol.gg.total"


def test_build_domain_candidate_gol_spread() -> None:
    row = pd.Series(
        {
            "domain_parsed": "gol.gg",
            "sub_domain": "",
            "question": "Game Handicap: DRX (-2.5) vs Dplus KIA (+2.5)",
            "description": "This market resolves based on game handicap.",
        }
    )

    assert build_domain_candidate(row) == "gol.gg.spread"
