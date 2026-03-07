import os
import re
import sys
from collections import Counter
from urllib.parse import urlparse

import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.raw_batches import rebuild_canonical_merged


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
            return "UNKNOWN", "", ""

        try:
            url = str(raw_source).strip()
            if not url.startswith("http"):
                url = "http://" + url
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            if not host:
                return "UNKNOWN", "", url

            parts = host.split(".")
            if len(parts) >= 3 and len(parts[-1]) == 2 and len(parts[-2]) <= 3:
                domain = ".".join(parts[-3:])
            elif len(parts) >= 2:
                domain = ".".join(parts[-2:])
            else:
                domain = host
            return domain, MarketSourceParser.normalize_sub_domain(domain, parsed.path), url
        except Exception:
            return "UNKNOWN", "", str(raw_source)


def normalize_outcomes(outcomes_str: str) -> tuple[str, str]:
    if not outcomes_str:
        return "other", "other"
    try:
        value = str(outcomes_str).strip().replace('\\"', '"').replace('""', '"')
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if value.startswith("[") and value.endswith("]"):
            choices = [match.lower().strip() for match in re.findall(r'"([^"]*)"', value)]
        else:
            choices = [value.lower().strip()]
        normalized = "_".join(sorted(choice.replace(" ", "_") for choice in choices if choice))
        simple_market_type = normalized if normalized in {"down_up", "no_yes", "over_under"} else "other"
        return simple_market_type, normalized or "other"
    except Exception:
        return "other", "other"


def resolve_source_url(row: pd.Series) -> str:
    direct_source = row.get("resolutionSource", "")
    if direct_source and str(direct_source).lower() not in {"nan", "unknown", ""}:
        return str(direct_source)
    description = row.get("description", "")
    extracted = MarketSourceParser.extract_url_from_text(description)
    return extracted or "UNKNOWN"


def build_market_domain_features() -> pd.DataFrame:
    if not config.RAW_MERGED_PATH.exists():
        rebuild_canonical_merged()

    if not config.RAW_MERGED_PATH.exists():
        raise FileNotFoundError(f"Merged raw markets not found at {config.RAW_MERGED_PATH}")

    print(f"[INFO] Loading merged raw markets from {config.RAW_MERGED_PATH}...")
    df = pd.read_csv(config.RAW_MERGED_PATH, dtype=str, low_memory=False).fillna("")
    if "id" not in df.columns:
        raise ValueError("Merged raw markets are missing 'id'.")

    df["market_id"] = df["id"].astype(str)
    df["source_url"] = df.apply(resolve_source_url, axis=1)

    parsed = df["source_url"].apply(MarketSourceParser.parse_domain_parts)
    df["domain_raw"] = parsed.apply(lambda value: value[0])
    df["sub_domain"] = parsed.apply(lambda value: value[1])
    df["source_url"] = parsed.apply(lambda value: value[2] or "UNKNOWN")
    df["source_host"] = df["domain_raw"]

    outcomes_series = df["outcomes"] if "outcomes" in df.columns else pd.Series([""] * len(df), index=df.index)
    outcome_info = outcomes_series.apply(normalize_outcomes)
    df["market_type_normalized"] = outcome_info.apply(lambda value: value[0])
    df["outcome_pattern"] = outcome_info.apply(lambda value: value[1])

    raw_market_type = df["marketType"] if "marketType" in df.columns else pd.Series([""] * len(df), index=df.index)
    sports_market_type = (
        df["sportsMarketType"] if "sportsMarketType" in df.columns else pd.Series([""] * len(df), index=df.index)
    )
    df["market_type"] = raw_market_type.where(raw_market_type != "", sports_market_type)
    df["market_type"] = df["market_type"].where(df["market_type"] != "", df["market_type_normalized"])
    df["market_type"] = df["market_type"].replace("", "UNKNOWN")
    df["category"] = df.get("category", "").replace("", "UNKNOWN")

    domain_counts = Counter(domain for domain in df["domain_raw"] if domain not in {"", "UNKNOWN"})

    def normalize_domain(domain: str) -> str:
        if domain in {"", "UNKNOWN"}:
            return "UNKNOWN"
        if domain_counts.get(domain, 0) < config.LOW_FREQUENCY_DOMAIN_COUNT:
            return "OTHER"
        return domain

    df["domain"] = df["domain_raw"].apply(normalize_domain)
    df["sub_domain"] = df["sub_domain"].fillna("")
    df["source_host"] = df["domain"]

    market_features = df[
        [
            "market_id",
            "domain",
            "sub_domain",
            "market_type",
            "source_url",
            "source_host",
            "category",
            "outcome_pattern",
        ]
    ].copy()
    market_features = market_features.drop_duplicates(subset=["market_id"]).reset_index(drop=True)
    market_features.to_csv(config.MARKET_DOMAIN_FEATURES_PATH, index=False)
    print(f"[INFO] Saved market-level domain features to {config.MARKET_DOMAIN_FEATURES_PATH}")

    summary = (
        market_features.groupby(["domain", "category", "market_type", "sub_domain"], dropna=False)
        .agg(market_count=("market_id", "count"))
        .reset_index()
        .sort_values(["market_count", "domain"], ascending=[False, True])
    )
    summary.to_csv(config.DOMAIN_SUMMARY_PATH, index=False)
    print(f"[INFO] Saved domain summary to {config.DOMAIN_SUMMARY_PATH}")

    return market_features


def main():
    build_market_domain_features()


if __name__ == "__main__":
    main()
