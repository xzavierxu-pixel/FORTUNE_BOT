from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged

UNKNOWN = "UNKNOWN"
OTHER = "other"
STRUCTURED_OUTCOME_TYPES = {"down_up", "negative_positive", "no_yes", "over_under"}
SPORTS_DOMAINS = {
    "dotabuff.com",
    "gol.gg",
    "hltv.org",
    "liquipedia.net",
    "nba.com",
    "ncaa.com",
    "nfl.com",
    "nhl.com",
    "sofascore.com",
    "twitch.tv",
    "vlr.gg",
}
CRYPTO_DOMAINS = {"binance.com", "chain.link", "dupe.fi"}
FINANCE_DOMAINS = {"nasdaq.com", "seekingalpha.com", "yahoo.com"}


class MarketSourceParser:
    @staticmethod
    def extract_url_from_text(text: str) -> str | None:
        if not text:
            return None
        urls = re.findall(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s,"]*)?', str(text))
        return urls[0] if urls else None

    @staticmethod
    def normalize_sub_domain(domain: str, path: str) -> str:
        if not path:
            return ""
        if domain == "hltv.org":
            return ""
        if domain in {"liquipedia.net", "x.com"}:
            match = re.match(r"^(/[^/]+)", path)
            if match:
                return match.group(1)
        for prefix in ["/article", "/news", "/homenews", "/scores"]:
            if path.startswith(prefix):
                return prefix
        cleaned = re.sub(r"[^a-zA-Z0-9]+$", "", path)
        return cleaned if re.search(r"[a-zA-Z0-9]", cleaned) else ""

    @staticmethod
    def parse_domain_parts(raw_source: str) -> tuple[str, str, str]:
        if not raw_source or str(raw_source).lower() in {"nan", "unknown", ""}:
            return UNKNOWN, "", ""
        try:
            url = str(raw_source).strip()
            if not url.startswith("http"):
                url = "http://" + url
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            if not host:
                return UNKNOWN, "", url

            parts = host.split(".")
            if len(parts) >= 3 and len(parts[-1]) == 2 and len(parts[-2]) <= 3:
                domain = ".".join(parts[-3:])
            elif len(parts) >= 2:
                domain = ".".join(parts[-2:])
            else:
                domain = host
            return domain, MarketSourceParser.normalize_sub_domain(domain, parsed.path), url
        except Exception:
            return UNKNOWN, "", str(raw_source)


def normalize_outcomes(outcomes_str: str) -> tuple[str, str]:
    if not outcomes_str:
        return OTHER, OTHER
    try:
        value = str(outcomes_str).strip().replace('\\"', '"').replace('""', '"')
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.startswith("[") and value.endswith("]"):
            choices = [match.lower().strip() for match in re.findall(r'"([^"]*)"', value)]
        else:
            choices = [value.lower().strip()]
        normalized = "_".join(sorted(choice.replace(" ", "_") for choice in choices if choice))
        normalized = normalized or OTHER
        simple_market_type = normalized if normalized in STRUCTURED_OUTCOME_TYPES else OTHER
        return simple_market_type, normalized
    except Exception:
        return OTHER, OTHER


def resolve_source_url(row: pd.Series) -> str:
    direct_source = row.get("resolutionSource", "")
    if direct_source and str(direct_source).lower() not in {"nan", "unknown", ""}:
        return str(direct_source)
    description = row.get("description", "")
    extracted = MarketSourceParser.extract_url_from_text(description)
    return extracted or UNKNOWN


def _normalize_category(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized else UNKNOWN


def infer_category_from_source(
    domain_parsed: pd.Series,
    game_id: pd.Series,
) -> pd.Series:
    parsed = pd.Series(UNKNOWN, index=domain_parsed.index, dtype="string")
    sports_mask = game_id.astype(str).str.strip().ne("")
    parsed.loc[sports_mask] = "SPORTS"
    parsed.loc[domain_parsed.isin(SPORTS_DOMAINS)] = "SPORTS"
    parsed.loc[domain_parsed.isin(CRYPTO_DOMAINS)] = "CRYPTO"
    parsed.loc[domain_parsed.isin(FINANCE_DOMAINS)] = "FINANCE"
    return parsed.astype(str)


def build_market_annotations(raw_markets: pd.DataFrame | None = None) -> pd.DataFrame:
    if raw_markets is None:
        if not config.RAW_MERGED_PATH.exists():
            rebuild_canonical_merged()
        if not config.RAW_MERGED_PATH.exists():
            raise FileNotFoundError(f"Merged raw markets not found at {config.RAW_MERGED_PATH}")
        print(f"[INFO] Loading merged raw markets from {config.RAW_MERGED_PATH}...")
        raw_markets = pd.read_csv(config.RAW_MERGED_PATH, dtype=str, low_memory=False).fillna("")

    df = raw_markets.copy()
    if "id" not in df.columns:
        raise ValueError("Merged raw markets are missing 'id'.")

    df["market_id"] = df["id"].astype(str)
    df["source_url"] = df.apply(resolve_source_url, axis=1)

    parsed = df["source_url"].apply(MarketSourceParser.parse_domain_parts)
    df["domain_parsed"] = parsed.apply(lambda value: value[0])
    df["sub_domain"] = parsed.apply(lambda value: value[1])
    df["source_url"] = parsed.apply(lambda value: value[2] or UNKNOWN)

    outcomes_series = df["outcomes"] if "outcomes" in df.columns else pd.Series([""] * len(df), index=df.index)
    outcome_info = outcomes_series.apply(normalize_outcomes)
    df["market_type_parsed"] = outcome_info.apply(lambda value: value[0])
    df["outcome_pattern"] = outcome_info.apply(lambda value: value[1])

    game_id = df["gameId"] if "gameId" in df.columns else pd.Series([""] * len(df), index=df.index)
    raw_category = df["category"] if "category" in df.columns else pd.Series([UNKNOWN] * len(df), index=df.index)

    df["market_type"] = df["market_type_parsed"]

    df["category_raw"] = raw_category.apply(_normalize_category)
    df["category_parsed"] = infer_category_from_source(
        domain_parsed=df["domain_parsed"].astype(str),
        game_id=game_id.astype(str),
    )
    df["category"] = df["category_parsed"].where(df["category_parsed"] != UNKNOWN, df["category_raw"])

    domain_counts = Counter(domain for domain in df["domain_parsed"] if domain not in {"", UNKNOWN})

    def normalize_domain(domain: str) -> str:
        if domain in {"", UNKNOWN}:
            return UNKNOWN
        if domain_counts.get(domain, 0) < config.LOW_FREQUENCY_DOMAIN_COUNT:
            return "OTHER"
        return domain

    df["domain"] = df["domain_parsed"].apply(normalize_domain)
    df["category_override_flag"] = (
        (df["category_parsed"] != UNKNOWN)
        & (df["category_raw"] != UNKNOWN)
        & (df["category_parsed"] != df["category_raw"])
    )

    columns = [
        "market_id",
        "domain",
        "domain_parsed",
        "sub_domain",
        "source_url",
        "category",
        "category_raw",
        "category_parsed",
        "category_override_flag",
        "market_type",
        "outcome_pattern",
    ]
    return df[columns].drop_duplicates(subset=["market_id"]).reset_index(drop=True)


def build_other_outcome_patterns_by_url(annotations: pd.DataFrame) -> pd.DataFrame:
    if annotations.empty:
        return pd.DataFrame(
            columns=[
                "domain",
                "source_url",
                "outcome_pattern",
                "market_count",
                "url_market_count",
                "share_within_url",
            ]
        )

    other_patterns = annotations[annotations["market_type"] == OTHER].copy()
    if other_patterns.empty:
        return pd.DataFrame(
            columns=[
                "domain",
                "source_url",
                "outcome_pattern",
                "market_count",
                "url_market_count",
                "share_within_url",
            ]
        )

    grouped = (
        other_patterns.groupby(["domain", "source_url", "outcome_pattern"], dropna=False)
        .agg(market_count=("market_id", "count"))
        .reset_index()
    )
    url_totals = (
        other_patterns.groupby(["domain", "source_url"], dropna=False)
        .agg(url_market_count=("market_id", "count"))
        .reset_index()
    )
    grouped = grouped.merge(url_totals, on=["domain", "source_url"], how="left")
    grouped["share_within_url"] = grouped["market_count"] / grouped["url_market_count"].clip(lower=1)
    return grouped.sort_values(
        ["domain", "source_url", "market_count", "outcome_pattern"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)


def save_market_annotations(annotations: pd.DataFrame, path: Path | None = None) -> pd.DataFrame:
    target = path or config.MARKET_DOMAIN_FEATURES_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    annotations.to_csv(target, index=False)
    print(f"[INFO] Saved market annotations to {target}")

    summary = (
        annotations.groupby(["domain", "category", "market_type", "sub_domain"], dropna=False)
        .agg(market_count=("market_id", "count"))
        .reset_index()
        .sort_values(["market_count", "domain"], ascending=[False, True])
    )
    try:
        summary.to_csv(config.DOMAIN_SUMMARY_PATH, index=False)
        print(f"[INFO] Saved domain summary to {config.DOMAIN_SUMMARY_PATH}")
    except PermissionError as exc:
        print(f"[WARN] Could not write domain summary to {config.DOMAIN_SUMMARY_PATH}: {exc}")

    other_patterns = build_other_outcome_patterns_by_url(annotations)
    try:
        other_patterns.to_csv(config.OTHER_OUTCOME_PATTERNS_BY_URL_PATH, index=False)
        print(f"[INFO] Saved other outcome patterns by URL to {config.OTHER_OUTCOME_PATTERNS_BY_URL_PATH}")
    except PermissionError as exc:
        print(f"[WARN] Could not write other outcome pattern audit to {config.OTHER_OUTCOME_PATTERNS_BY_URL_PATH}: {exc}")
    return annotations


def build_and_save_market_annotations() -> pd.DataFrame:
    return save_market_annotations(build_market_annotations())


def load_market_annotations(path: Path | None = None, rebuild_if_missing: bool = True) -> pd.DataFrame:
    target = path or config.MARKET_DOMAIN_FEATURES_PATH
    required_columns = {
        "market_id",
        "domain",
        "category",
        "market_type",
        "category_raw",
        "category_parsed",
        "outcome_pattern",
    }
    if not target.exists():
        if not rebuild_if_missing:
            print(f"[WARN] Market annotations not found at {target}.")
            return pd.DataFrame(columns=sorted(required_columns))
        return build_and_save_market_annotations()

    print(f"[INFO] Loading market annotations from {target}...")
    annotations = pd.read_csv(target, low_memory=False)
    if required_columns.difference(annotations.columns):
        print(f"[WARN] Market annotations at {target} are missing required columns. Rebuilding.")
        return build_and_save_market_annotations()
    annotations["market_id"] = annotations["market_id"].astype(str)
    return annotations
