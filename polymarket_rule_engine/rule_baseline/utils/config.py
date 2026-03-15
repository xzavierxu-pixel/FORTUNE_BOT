import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ==========================================
# Pipeline Constants
# ==========================================

UTC = timezone.utc

# Time Horizons (Hours)
HORIZONS = [1, 2, 4, 6, 12, 24]

# Historical bootstrap window for the first raw fetch.
DATE_START_STR = "2025-12-07"

# Rolling windows
RAW_FETCH_OVERLAP_HOURS = 72
VALIDATION_DAYS = 30
TEST_DAYS = 30
ONLINE_VALIDATION_DAYS = 20

# Constraints
DELTA_FIXED_HOURS = 24.0
SNAP_WINDOW_SEC = 300
EPSILON = 1e-5
MIN_SAMPLES_LEAF = 200
EDGE_THRESHOLD = 0.05
LOW_FREQUENCY_DOMAIN_COUNT = 25
MAX_ALLOWED_RESOLVE_DELTA_HOURS = 1.0
STALE_QUOTE_MAX_OFFSET_SEC = 120
STALE_QUOTE_MAX_GAP_SEC = 900

# Raw market quality filters
MIN_MARKET_VOLUME = 50.0
MIN_MARKET_LIQUIDITY = 50.0
MAX_MARKET_SPREAD = 0.25
MAX_REWARD_SPREAD = 0.25
DOMAIN_BLACKLIST: set[str] = set()

# Rule selection / statistics
BETA_PRIOR_STRENGTH = 20.0
FDR_ALPHA = 0.10
MIN_EDGE_LOWER_BOUND = 0.0

# Feature engineering
TEXT_EMBED_DIM = 16

# Backtesting risk limits
MAX_DOMAIN_EXPOSURE_F = 0.20
MAX_CATEGORY_EXPOSURE_F = 0.25
MAX_CLUSTER_EXPOSURE_F = 0.15
MAX_SETTLEMENT_EXPOSURE_F = 0.20
MAX_SIDE_EXPOSURE_F = 0.30
MAX_TRADE_LIQUIDITY_F = 0.02
MAX_TRADE_VOLUME24_F = 0.02

# Backtesting
FEE_RATE = 0.001

# API & Rate Limits
PRICE_SCALE_FALLBACK = 10_000.0

# Paths
RULE_BASELINE_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = RULE_BASELINE_DIR.parent
DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
RAW_BATCHES_DIR = RAW_DIR / "batches"
RAW_BATCH_MANIFEST_PATH = RAW_DIR / "batch_manifest.csv"
LEGACY_RAW_MARKETS_PATH = RAW_DIR / "raw_markets.csv"

INTERMEDIATE_DIR = DATA_DIR / "intermediate"
RAW_MERGED_PATH = INTERMEDIATE_DIR / "raw_markets_merged.csv"

PROCESSED_DIR = DATA_DIR / "processed"
SNAPSHOT_BATCHES_DIR = PROCESSED_DIR / "batches"
SNAPSHOT_BATCH_MANIFEST_PATH = PROCESSED_DIR / "batch_manifest.csv"
SNAPSHOT_AUDIT_BATCHES_DIR = PROCESSED_DIR / "audit_batches"
SNAPSHOT_QUARANTINE_BATCHES_DIR = PROCESSED_DIR / "quarantine_batches"
SNAPSHOTS_PATH = PROCESSED_DIR / "snapshots.csv"
SNAPSHOT_QUARANTINE_PATH = PROCESSED_DIR / "snapshots_quarantine.csv"
SNAPSHOT_BUILD_SUMMARY_PATH = PROCESSED_DIR / "snapshot_build_summary.json"
SNAPSHOT_HIT_RATE_PATH = PROCESSED_DIR / "snapshot_horizon_hit_rate.csv"
SNAPSHOT_MISSINGNESS_PATH = PROCESSED_DIR / "snapshot_missingness_by_domain.csv"
SNAPSHOT_MARKET_AUDIT_PATH = PROCESSED_DIR / "snapshot_market_audit.csv"
RAW_MARKET_QUARANTINE_PATH = INTERMEDIATE_DIR / "raw_market_quarantine.csv"

DOMAIN_DIR = DATA_DIR / "domain"
MARKET_DOMAIN_FEATURES_PATH = DOMAIN_DIR / "market_domain_features.csv"
DOMAIN_SUMMARY_PATH = DOMAIN_DIR / "domain_summary.csv"
OTHER_OUTCOME_PATTERNS_BY_URL_PATH = DOMAIN_DIR / "other_outcome_patterns_by_url.csv"

NAIVE_RULES_DIR = DATA_DIR / "naive_rules"
EDGE_DIR = DATA_DIR / "edge"
RULES_OUTPUT_PATH = EDGE_DIR / "trading_rules.csv"
NAIVE_RULES_OUTPUT_PATH = NAIVE_RULES_DIR / "naive_trading_rules.csv"
NAIVE_RULES_JSON_PATH = NAIVE_RULES_DIR / "naive_trading_rules.json"
NAIVE_RULES_REPORT_PATH = NAIVE_RULES_DIR / "naive_all_leaves_report.csv"

MODELS_DIR = DATA_DIR / "models"
MODEL_PATH = MODELS_DIR / "ensemble_snapshot_q.pkl"

PREDICTIONS_DIR = DATA_DIR / "predictions"
PREDICTIONS_PATH = PREDICTIONS_DIR / "snapshots_with_predictions.csv"

BACKTEST_DIR = DATA_DIR / "backtesting"
ANALYSIS_DIR = DATA_DIR / "analysis"

OFFLINE_DIR = DATA_DIR / "offline"
ONLINE_DIR = DATA_DIR / "online"


def ensure_data_dirs() -> None:
    for path in [
        RAW_DIR,
        RAW_BATCHES_DIR,
        INTERMEDIATE_DIR,
        PROCESSED_DIR,
        SNAPSHOT_BATCHES_DIR,
        SNAPSHOT_AUDIT_BATCHES_DIR,
        SNAPSHOT_QUARANTINE_BATCHES_DIR,
        DOMAIN_DIR,
        OFFLINE_DIR,
        ONLINE_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


ensure_data_dirs()


def parse_utc_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def history_start() -> datetime:
    return parse_utc_datetime(DATE_START_STR)


def current_utc() -> datetime:
    return datetime.now(tz=UTC)


def resolve_history_start(value: str | datetime | None = None) -> datetime:
    return parse_utc_datetime(value) if value is not None else history_start()


def get_fetch_window(
    max_closed_time: datetime | None,
    now: datetime | None = None,
    history_start_override: str | datetime | None = None,
) -> tuple[datetime, datetime]:
    window_end = parse_utc_datetime(now) if now is not None else current_utc()
    effective_history_start = resolve_history_start(history_start_override)
    if max_closed_time is None:
        window_start = effective_history_start
    else:
        window_start = max(effective_history_start, max_closed_time - timedelta(hours=RAW_FETCH_OVERLAP_HOURS))
    return window_start, window_end


def compute_split_boundaries(
    reference_end: datetime | None = None,
    validation_days: int = VALIDATION_DAYS,
    history_start_override: str | datetime | None = None,
) -> tuple[datetime, datetime, datetime]:
    end_ref = parse_utc_datetime(reference_end) if reference_end is not None else current_utc()
    valid_start = end_ref - timedelta(days=validation_days)
    train_end = valid_start - timedelta(seconds=1)
    return resolve_history_start(history_start_override), train_end, valid_start


def compute_three_way_split_boundaries(
    reference_end: datetime | None = None,
    validation_days: int = VALIDATION_DAYS,
    test_days: int = TEST_DAYS,
    history_start_override: str | datetime | None = None,
) -> tuple[datetime, datetime, datetime, datetime, datetime, datetime]:
    end_ref = parse_utc_datetime(reference_end) if reference_end is not None else current_utc()
    test_start = end_ref - timedelta(days=test_days)
    valid_start = test_start - timedelta(days=validation_days)
    train_start = resolve_history_start(history_start_override)
    train_end = valid_start - timedelta(seconds=1)
    valid_end = test_start - timedelta(seconds=1)
    test_end = end_ref
    return train_start, train_end, valid_start, valid_end, test_start, test_end


def get_dates() -> tuple[datetime, datetime, datetime]:
    return compute_split_boundaries()


def get_lgbm_dates() -> tuple[datetime, datetime, datetime]:
    return compute_split_boundaries()


# Category Mapping & Taxonomy
BROAD_CATEGORIES = [
    "CRYPTO",
    "SPORTS",
    "GAMES",
    "FINANCE",
    "CULTURE",
    "TECH",
    "POLITICS",
    "EARNINGS",
    "GEOPOLITICS",
    "WORLD",
    "ECONOMY",
    "ELECTION",
    "MENTIONS",
]

# Keyword Mapping (Tag -> Broad Category)
TAG_MAPPING = {
    "nba": "SPORTS",
    "nfl": "SPORTS",
    "soccer": "SPORTS",
    "football": "SPORTS",
    "ufc": "SPORTS",
    "mma": "SPORTS",
    "baseball": "SPORTS",
    "hockey": "SPORTS",
    "tennis": "SPORTS",
    "golf": "SPORTS",
    "f1": "SPORTS",
    "athletics": "SPORTS",
    "counter strike 2": "SPORTS",
    "bitcoin": "CRYPTO",
    "ethereum": "CRYPTO",
    "solana": "CRYPTO",
    "nft": "CRYPTO",
    "defi": "CRYPTO",
    "memecoins": "CRYPTO",
    "crypto": "CRYPTO",
    "crypto prices": "CRYPTO",
    "xrp": "CRYPTO",
    "ripple": "CRYPTO",
    "dogecoin": "CRYPTO",
    "games": "GAMES",
    "video games": "GAMES",
    "chess": "GAMES",
    "esports": "GAMES",
    "dota": "GAMES",
    "leagueoflegends": "GAMES",
    "mouz": "GAMES",
    "legends": "GAMES",
    "us politics": "POLITICS",
    "white house": "POLITICS",
    "congress": "POLITICS",
    "senate": "POLITICS",
    "trump": "POLITICS",
    "biden": "POLITICS",
    "democrats": "POLITICS",
    "republicans": "POLITICS",
    "elections": "ELECTION",
    "presidential debate": "ELECTION",
    "midterms": "ELECTION",
    "war": "GEOPOLITICS",
    "military": "GEOPOLITICS",
    "foreign policy": "GEOPOLITICS",
    "middle east": "GEOPOLITICS",
    "ukraine": "GEOPOLITICS",
    "finance": "FINANCE",
    "stocks": "FINANCE",
    "market": "FINANCE",
    "business": "FINANCE",
    "investing": "FINANCE",
    "equities": "FINANCE",
    "indicies": "FINANCE",
    "rates": "ECONOMY",
    "inflation": "ECONOMY",
    "fed": "ECONOMY",
    "recession": "ECONOMY",
    "gdp": "ECONOMY",
    "economics": "ECONOMY",
    "earnings": "EARNINGS",
    "quarterly earnings": "EARNINGS",
    "revenue": "EARNINGS",
    "tech": "TECH",
    "technology": "TECH",
    "ai": "TECH",
    "openai": "TECH",
    "space": "TECH",
    "science": "TECH",
    "apple": "TECH",
    "nvidia": "TECH",
    "google": "TECH",
    "pop culture": "CULTURE",
    "movies": "CULTURE",
    "music": "CULTURE",
    "celebrities": "CULTURE",
    "awards": "CULTURE",
    "mentions": "MENTIONS",
    "twitter": "MENTIONS",
    "tweets": "MENTIONS",
    "social media": "MENTIONS",
}

CATEGORY_MAP = TAG_MAPPING

DEFAULT_API_PARAMS = {
    "closed": True,
    "uma_resolution_status": "resolved",
    "neg_risk": False,
    "order": "closedTime",
    "ascending": False,
}

COLS_TO_DROP = [
    "archived",
    "restricted",
    "enableOrderBook",
    "orderMinSize",
    "negRiskRequestID",
    "ready",
    "pagerDutyNotificationEnabled",
    "approved",
    "automaticallyResolved",
    "automaticallyActive",
    "manualActivation",
    "rfqEnabled",
    "holdingRewardsEnabled",
    "feesEnabled",
    "seriesColor",
    "submitted_by",
    "competitive",
    "secondsDelay",
    "wideFormat",
    "sentDiscord",
    "readyForCron",
    "fpmmLive",
    "notificationsEnabled",
]
