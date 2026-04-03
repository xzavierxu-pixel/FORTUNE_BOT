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
    "atptour.com",
    "dotabuff.com",
    "gol.gg",
    "hltv.org",
    "liquipedia.net",
    "mlb.com",
    "nba.com",
    "ncaa.com",
    "nfl.com",
    "nhl.com",
    "sofascore.com",
    "twitch.tv",
    "vlr.gg",
    "wtatennis.com",
}
CRYPTO_DOMAINS = {"binance.com", "chain.link", "dupe.fi"}
FINANCE_DOMAINS = {"nasdaq.com", "seekingalpha.com", "yahoo.com"}
COARSE_MARKET_FAMILY_DOMAINS = {
    "atptour.com",
    "gol.gg",
    "mlb.com",
    "nba.com",
    "nfl.com",
    "nhl.com",
    "sofascore.com",
    "wtatennis.com",
}
TWITCH_ALLOWED_SUB_DOMAINS = {
    "/BLASTDota",
    "/ESLCS",
    "/cblol",
    "/esl_dota2",
    "/lck",
    "/lcs",
    "/lec",
    "/lplenglish",
}
NCAA_SPORT_KEYWORDS = {
    "basketball": [
        "basketball",
        "march madness",
        "final four",
        "sweet 16",
        "elite eight",
        "cbb",
        "wbb",
        "ncaam",
        "ncaaw",
    ],
    "football": [
        "football",
        "cfb",
        "college football",
        "heisman",
        "bowl",
        "playoff",
    ],
}
NCAA_CHAMPIONSHIP_KEYWORDS = [
    "championship",
    "title",
    "tournament winner",
    "national champion",
]
NCAA_OTHER_SPORT_KEYWORDS = [
    "baseball",
    "golf",
    "hockey",
    "lacrosse",
    "soccer",
    "softball",
    "swimming",
    "tennis",
    "volleyball",
    "wrestling",
]
SPREAD_PATTERNS = [
    re.compile(r"\bspread\b", re.IGNORECASE),
    re.compile(r"\bhandicap\b", re.IGNORECASE),
    re.compile(r"\bpuck line\b", re.IGNORECASE),
    re.compile(r"\brun line\b", re.IGNORECASE),
    re.compile(r"[(-+]\d+(?:\.\d+)?[)]"),
]
TOTAL_PATTERNS = [
    re.compile(r"\bo/u\b", re.IGNORECASE),
    re.compile(r"\bover/under\b", re.IGNORECASE),
    re.compile(r"\btotal kills\b", re.IGNORECASE),
    re.compile(r"\btotal goals\b", re.IGNORECASE),
    re.compile(r"\btotal runs\b", re.IGNORECASE),
    re.compile(r"\btotal points\b", re.IGNORECASE),
    re.compile(r"\bgames total\b", re.IGNORECASE),
]
PROP_PATTERNS = [
    re.compile(r"\bscorigami\b", re.IGNORECASE),
    re.compile(r"\btouchdown\b", re.IGNORECASE),
    re.compile(r"\bto score\b", re.IGNORECASE),
    re.compile(r"\bexactly \d+ maps\b", re.IGNORECASE),
    re.compile(r"\bto win \d+ maps\b", re.IGNORECASE),
    re.compile(r"\bgrand slam tournament\b", re.IGNORECASE),
]
MONEYLINE_PATTERNS = [
    re.compile(r"\bvs\.\b", re.IGNORECASE),
    re.compile(r"\bvs\b", re.IGNORECASE),
    re.compile(r"\bmatch winner\b", re.IGNORECASE),
    re.compile(r"\bgame \d+ winner\b", re.IGNORECASE),
    re.compile(r"\bmap \d+ winner\b", re.IGNORECASE),
    re.compile(r"\bbo[135]\b", re.IGNORECASE),
]
GOL_GG_FAMILY_SUFFIXES = {
    "spread": "gol.gg.spread",
    "total": "gol.gg.total",
    "prop": "gol.gg.prop",
    "moneyline": "gol.gg.moneyline",
}
COARSE_FAMILY_SUFFIXES = {
    "spread": "{domain}.spread",
    "total": "{domain}.total",
    "prop": "{domain}.prop",
    "moneyline": "{domain}.moneyline",
}


class MarketSourceParser:
    @staticmethod
    def extract_urls_from_text(text: str) -> list[str]:
        if not text:
            return []
        return re.findall(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(?:/[^\s,"]*)?', str(text))

    @staticmethod
    def extract_url_from_text(text: str) -> str | None:
        urls = MarketSourceParser.extract_urls_from_text(text)
        return urls[0] if urls else None

    @staticmethod
    def normalize_sub_domain(domain: str, path: str) -> str:
        if not path:
            return ""
        if domain in {
            "atptour.com",
            "gol.gg",
            "hltv.org",
            "mlb.com",
            "nba.com",
            "nfl.com",
            "nhl.com",
            "sofascore.com",
            "wtatennis.com",
        }:
            return ""
        if domain in {"liquipedia.net", "twitch.tv", "x.com"}:
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
    for extracted in MarketSourceParser.extract_urls_from_text(description):
        parsed = urlparse(extracted)
        host = parsed.netloc.lower()
        if "amazonaws" in host:
            continue
        return extracted
    return UNKNOWN


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


def extract_ncaa_sport_from_description(description: str) -> str | None:
    text = str(description or "").strip().lower()
    if not text:
        return None
    for sport, keywords in NCAA_SPORT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return sport
    for keyword in NCAA_OTHER_SPORT_KEYWORDS:
        if keyword in text:
            return "other"
    for keyword in NCAA_CHAMPIONSHIP_KEYWORDS:
        if keyword in text:
            return "championship"
    return "other"


def _match_any_pattern(text: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(text) for pattern in patterns)


def extract_coarse_market_family(question: str, description: str) -> str | None:
    text = " ".join(part for part in [question, description] if part).strip()
    if not text:
        return None

    if _match_any_pattern(text, SPREAD_PATTERNS):
        return "spread"
    if _match_any_pattern(text, TOTAL_PATTERNS):
        return "total"
    if _match_any_pattern(text, PROP_PATTERNS):
        return "prop"
    if _match_any_pattern(text, MONEYLINE_PATTERNS):
        return "moneyline"
    return None


def build_domain_candidate(row: pd.Series) -> str:
    domain_parsed = str(row.get("domain_parsed") or UNKNOWN)
    sub_domain = str(row.get("sub_domain") or "")
    question = str(row.get("question") or "")
    description = str(row.get("description") or "")

    if domain_parsed == "ncaa.com":
        sport = extract_ncaa_sport_from_description(description)
        return f"ncaa.com.{sport}" if sport else "ncaa.com"

    if domain_parsed == "gol.gg":
        family = extract_coarse_market_family(question, description)
        if family:
            return GOL_GG_FAMILY_SUFFIXES[family]
        return "gol.gg"

    if domain_parsed == "binance.com":
        if sub_domain and sub_domain.upper().endswith("USDT"):
            return f"binance.com{sub_domain}"
        return "binance.com"

    if domain_parsed == "chain.link":
        return f"chain.link{sub_domain}" if sub_domain else "chain.link"

    if domain_parsed == "liquipedia.net":
        return f"{domain_parsed}{sub_domain}" if sub_domain else domain_parsed

    if domain_parsed == "twitch.tv":
        if sub_domain in TWITCH_ALLOWED_SUB_DOMAINS:
            return f"twitch.tv{sub_domain}"
        return "twitch.tv"

    if domain_parsed in COARSE_MARKET_FAMILY_DOMAINS:
        family = extract_coarse_market_family(question, description)
        if family:
            return COARSE_FAMILY_SUFFIXES[family].format(domain=domain_parsed)

    return domain_parsed


def build_parent_domain_candidate(row: pd.Series) -> str:
    domain_candidate = str(row.get("domain_candidate") or UNKNOWN)
    domain_parsed = str(row.get("domain_parsed") or UNKNOWN)
    if domain_candidate in {"", UNKNOWN}:
        return UNKNOWN
    if domain_candidate == domain_parsed:
        return domain_candidate
    return domain_parsed or UNKNOWN


def build_domain_category_mapping(category_totals: pd.DataFrame) -> dict[tuple[str, str], str]:
    category_mapping: dict[tuple[str, str], str] = {}
    for domain, domain_totals in category_totals.groupby("domain", sort=False):
        categories = domain_totals["category"].tolist()
        if len(categories) <= 3:
            keep_categories = set(categories)
        else:
            provisional_keep = categories[:3]
            if "OTHER" in provisional_keep:
                keep_categories = set(provisional_keep)
            else:
                keep_categories = set(categories[:2])

        for category in categories:
            mapped_category = category if category in keep_categories else "OTHER"
            category_mapping[(str(domain), str(category))] = mapped_category
    return category_mapping


def normalize_annotation_buckets(annotations: pd.DataFrame) -> pd.DataFrame:
    if annotations.empty:
        return annotations.copy()

    normalized = annotations.copy()
    category_totals = (
        normalized.groupby(["domain", "category"], dropna=False)
        .agg(category_market_count=("market_id", "count"))
        .reset_index()
        .sort_values(
            ["domain", "category_market_count", "category"],
            ascending=[True, False, True],
            kind="stable",
        )
    )
    category_mapping = build_domain_category_mapping(category_totals)
    normalized["category"] = normalized.apply(
        lambda row: category_mapping.get((str(row["domain"]), str(row["category"])), str(row["category"])),
        axis=1,
    )

    category_totals_after_mapping = (
        normalized.groupby(["domain", "category"], dropna=False)
        .agg(category_market_count=("market_id", "count"))
        .reset_index()
        .sort_values(
            ["domain", "category_market_count", "category"],
            ascending=[True, False, True],
            kind="stable",
        )
    )
    primary_category_by_domain: dict[str, str] = {}
    for domain, domain_totals in category_totals_after_mapping.groupby("domain", sort=False):
        non_other_categories = domain_totals[domain_totals["category"] != "OTHER"]
        candidate_rows = non_other_categories if not non_other_categories.empty else domain_totals
        if candidate_rows.empty:
            continue
        primary_category_by_domain[str(domain)] = str(candidate_rows.iloc[0]["category"])

    bucket_counts = (
        normalized.groupby(["domain", "category", "market_type"], dropna=False)
        .agg(bucket_market_count=("market_id", "count"))
        .reset_index()
    )
    low_bucket_keys = {
        (str(row["domain"]), str(row["category"]), str(row["market_type"]))
        for _, row in bucket_counts.iterrows()
        if int(row["bucket_market_count"]) < config.LOW_FREQUENCY_BUCKET_COUNT
    }
    if low_bucket_keys:
        low_bucket_mask = normalized.apply(
            lambda row: (
                str(row["domain"]),
                str(row["category"]),
                str(row["market_type"]),
            )
            in low_bucket_keys,
            axis=1,
        )
        normalized.loc[low_bucket_mask, "category"] = normalized.loc[low_bucket_mask, "domain"].map(
            lambda domain: primary_category_by_domain.get(str(domain), "OTHER")
        )

    return normalized


def build_domain_summary(annotations: pd.DataFrame) -> pd.DataFrame:
    if annotations.empty:
        return pd.DataFrame(columns=["domain", "category", "market_type", "sub_domain", "market_count"])

    return (
        annotations.groupby(["domain", "category", "market_type", "sub_domain"], dropna=False)
        .agg(market_count=("market_id", "count"))
        .reset_index()
        .sort_values(["market_count", "domain"], ascending=[False, True])
    )


def build_domain_summary_aggregated(summary: pd.DataFrame) -> pd.DataFrame:
    if summary.empty:
        return pd.DataFrame(columns=["domain", "category", "market_type", "market_count"])

    return (
        summary.groupby(["domain", "category", "market_type"], dropna=False)
        .agg(market_count=("market_count", "sum"))
        .reset_index()
        .sort_values(["market_count", "domain"], ascending=[False, True])
    )


def build_market_annotations(
    raw_markets: pd.DataFrame | None = None,
    *,
    include_domain_candidate: bool = False,
) -> pd.DataFrame:
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
    df["domain_candidate"] = df.apply(build_domain_candidate, axis=1)

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

    domain_counts = Counter(domain for domain in df["domain_candidate"] if domain not in {"", UNKNOWN})
    df["parent_domain_candidate"] = df.apply(build_parent_domain_candidate, axis=1)
    parent_domain_counts = Counter(
        domain for domain in df["parent_domain_candidate"] if domain not in {"", UNKNOWN}
    )

    def normalize_domain(domain: str, parent_domain: str) -> str:
        if domain in {"", UNKNOWN}:
            return UNKNOWN
        if domain_counts.get(domain, 0) < config.LOW_FREQUENCY_DOMAIN_COUNT:
            if (
                parent_domain not in {"", UNKNOWN}
                and parent_domain != domain
                and parent_domain_counts.get(parent_domain, 0) >= config.LOW_FREQUENCY_DOMAIN_COUNT
            ):
                return parent_domain
            return "OTHER"
        return domain

    df["domain"] = df.apply(
        lambda row: normalize_domain(
            str(row.get("domain_candidate") or UNKNOWN),
            str(row.get("parent_domain_candidate") or UNKNOWN),
        ),
        axis=1,
    )
    df = normalize_annotation_buckets(df)
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
    if include_domain_candidate:
        columns.insert(2, "domain_candidate")
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

    summary = build_domain_summary(annotations)
    try:
        summary.to_csv(config.DOMAIN_SUMMARY_PATH, index=False)
        print(f"[INFO] Saved domain summary to {config.DOMAIN_SUMMARY_PATH}")
    except PermissionError as exc:
        print(f"[WARN] Could not write domain summary to {config.DOMAIN_SUMMARY_PATH}: {exc}")

    aggregated_summary = build_domain_summary_aggregated(summary)
    try:
        aggregated_summary.to_csv(config.DOMAIN_SUMMARY_AGGREGATED_PATH, index=False)
        print(f"[INFO] Saved aggregated domain summary to {config.DOMAIN_SUMMARY_AGGREGATED_PATH}")
    except PermissionError as exc:
        print(
            f"[WARN] Could not write aggregated domain summary to "
            f"{config.DOMAIN_SUMMARY_AGGREGATED_PATH}: {exc}"
        )

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
