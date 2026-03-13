"""Centralized configuration for PEG.

All parameters live here to make tuning easy.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent.parent / "data" / "execution_engine"


def _get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def _get_bool(name: str, default: bool) -> bool:
    raw = _get_env(name, "1" if default else "0").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    return int(_get_env(name, str(default)))


def _get_float(name: str, default: float) -> float:
    return float(_get_env(name, str(default)))


@dataclass(frozen=True)
class PegConfig:
    # Runtime
    dry_run: bool
    run_id: str

    # Order sizing and TTL
    order_usdc: float
    order_ttl_sec: int

    # Signal TTL (5-10 min window)
    signal_ttl_sec_min: int
    signal_ttl_sec_max: int

    # Price validation
    price_dev_abs: float
    price_dev_rel: float
    price_dev_spread_k: float
    max_spread: float
    min_depth_usdc: float
    price_refresh_retries: int
    price_refresh_backoff_sec: int

    # Risk limits
    max_notional: float
    daily_loss_limit: float
    dup_window_sec: int
    min_time_to_close_sec: int
    max_open_orders: int
    max_position_per_market_usdc: float
    max_net_exposure_usdc: float
    max_exposure_per_category_usdc: float
    fat_finger_high: float
    fat_finger_low: float

    # File-based IO
    data_dir: Path
    rule_signals_path: Path
    llm_signals_path: Path
    mid_prices_path: Path
    decisions_path: Path
    orders_path: Path
    events_path: Path
    fills_path: Path
    rejections_path: Path
    logs_path: Path
    metrics_path: Path
    alerts_path: Path
    balances_path: Path
    nonce_path: Path

    # Behavior flags
    enforce_one_order_per_market: bool
    llm_signal_source: str  # "file" only for now

    # Adapter inputs
    rule_candidates_path: Path
    llm_snapshot_dir: Path
    llm_results_path: Path
    llm_attempts_dir: Path

    # Balance check behavior
    balance_strict: bool
    balance_source: str

    # CLOB / Gamma integration
    clob_enabled: bool
    clob_host: str
    clob_chain_id: int
    clob_private_key: str
    clob_funder: str
    clob_api_key: str
    clob_api_secret: str
    clob_api_passphrase: str
    clob_signature_type: int
    clob_request_timeout_sec: int
    clob_derive_api_key: bool
    gamma_base_url: str
    token_cache_path: Path
    token_cache_ttl_sec: int
    price_source: str

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.llm_attempts_dir.mkdir(parents=True, exist_ok=True)
        self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = PegConfig(
    dry_run=_get_bool("PEG_DRY_RUN", True),
    run_id=_get_env("PEG_RUN_ID", "manual"),

    order_usdc=_get_float("PEG_ORDER_USDC", 10.0),
    order_ttl_sec=_get_int("PEG_ORDER_TTL_SEC", 300),

    signal_ttl_sec_min=_get_int("PEG_SIGNAL_TTL_SEC_MIN", 300),
    signal_ttl_sec_max=_get_int("PEG_SIGNAL_TTL_SEC_MAX", 600),

    price_dev_abs=_get_float("PEG_PRICE_DEV_ABS", 0.05),
    price_dev_rel=_get_float("PEG_PRICE_DEV_REL", 0.0),
    price_dev_spread_k=_get_float("PEG_PRICE_DEV_SPREAD_K", 0.0),
    max_spread=_get_float("PEG_MAX_SPREAD", 0.0),
    min_depth_usdc=_get_float("PEG_MIN_DEPTH_USDC", 0.0),
    price_refresh_retries=_get_int("PEG_PRICE_REFRESH_RETRIES", 2),
    price_refresh_backoff_sec=_get_int("PEG_PRICE_REFRESH_BACKOFF_SEC", 2),

    max_notional=_get_float("PEG_MAX_NOTIONAL", 100.0),
    daily_loss_limit=_get_float("PEG_DAILY_LOSS_LIMIT", -500.0),
    dup_window_sec=_get_int("PEG_DUP_WINDOW_SEC", 5),
    min_time_to_close_sec=_get_int("PEG_MIN_TIME_TO_CLOSE_SEC", 900),
    max_open_orders=_get_int("PEG_MAX_OPEN_ORDERS", 20),
    max_position_per_market_usdc=_get_float("PEG_MAX_POSITION_PER_MARKET_USDC", 200.0),
    max_net_exposure_usdc=_get_float("PEG_MAX_NET_EXPOSURE_USDC", 1000.0),
    max_exposure_per_category_usdc=_get_float("PEG_MAX_EXPOSURE_PER_CATEGORY_USDC", 0.0),
    fat_finger_high=_get_float("PEG_FAT_FINGER_HIGH", 0.99),
    fat_finger_low=_get_float("PEG_FAT_FINGER_LOW", 0.01),

    data_dir=DATA_DIR,
    rule_signals_path=DATA_DIR / "rule_signals.jsonl",
    llm_signals_path=DATA_DIR / "llm_signals.jsonl",
    mid_prices_path=DATA_DIR / "mid_prices.json",
    decisions_path=DATA_DIR / "decisions.jsonl",
    orders_path=DATA_DIR / "orders.jsonl",
    events_path=DATA_DIR / "events.jsonl",
    fills_path=DATA_DIR / "fills.jsonl",
    rejections_path=DATA_DIR / "rejections.jsonl",
    logs_path=DATA_DIR / "logs.jsonl",
    metrics_path=DATA_DIR / "metrics.json",
    alerts_path=DATA_DIR / "alerts.jsonl",
    balances_path=DATA_DIR / "balances.json",
    nonce_path=DATA_DIR / "nonce.json",

    enforce_one_order_per_market=_get_bool("PEG_ONE_ORDER_PER_MARKET", True),
    llm_signal_source=_get_env("PEG_LLM_SIGNAL_SOURCE", "file"),

    rule_candidates_path=Path(_get_env("PEG_RULE_CANDIDATES_PATH", str(DATA_DIR / "rule_candidates.csv"))),
    llm_snapshot_dir=Path(_get_env("PEG_LLM_SNAPSHOT_DIR", "polymarket_llm_miroflow/tasks/snapshots")),
    llm_results_path=Path(_get_env("PEG_LLM_RESULTS_PATH", str(DATA_DIR / "llm_results.jsonl"))),
    llm_attempts_dir=Path(_get_env("PEG_LLM_ATTEMPTS_DIR", str(DATA_DIR / "llm_attempts"))),

    balance_strict=_get_bool("PEG_BALANCE_STRICT", False),
    balance_source=_get_env("PEG_BALANCE_SOURCE", "file"),

    clob_enabled=_get_bool("PEG_CLOB_ENABLED", False),
    clob_host=_get_env("PEG_CLOB_HOST", "https://clob.polymarket.com"),
    clob_chain_id=_get_int("PEG_CLOB_CHAIN_ID", 137),
    clob_private_key=_get_env("PEG_CLOB_PRIVATE_KEY", ""),
    clob_funder=_get_env("PEG_CLOB_FUNDER", ""),
    clob_api_key=_get_env("PEG_CLOB_API_KEY", ""),
    clob_api_secret=_get_env("PEG_CLOB_API_SECRET", ""),
    clob_api_passphrase=_get_env("PEG_CLOB_API_PASSPHRASE", ""),
    clob_signature_type=_get_int("PEG_CLOB_SIGNATURE_TYPE", 1),
    clob_request_timeout_sec=_get_int("PEG_CLOB_TIMEOUT_SEC", 20),
    clob_derive_api_key=_get_bool("PEG_CLOB_DERIVE_API_KEY", False),
    gamma_base_url=_get_env("PEG_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"),
    token_cache_path=Path(_get_env("PEG_TOKEN_CACHE_PATH", str(DATA_DIR / "token_cache.json"))),
    token_cache_ttl_sec=_get_int("PEG_TOKEN_CACHE_TTL_SEC", 3600),
    price_source=_get_env("PEG_PRICE_SOURCE", "file"),
)


def load_config() -> PegConfig:
    """Return config with ensured data directory."""
    DEFAULT_CONFIG.ensure_dirs()
    return DEFAULT_CONFIG
