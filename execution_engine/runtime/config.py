"""Centralized configuration for PEG."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
import os
import re

BASE_DIR = Path(__file__).resolve().parent
ENGINE_DIR = BASE_DIR.parent
BASE_DATA_DIR = ENGINE_DIR / "data"
REPO_DIR = ENGINE_DIR.parent
RULE_ENGINE_DIR = REPO_DIR / "polymarket_rule_engine"
RULE_BASELINE_DIR = RULE_ENGINE_DIR / "rule_baseline"
RULE_BASELINE_DATASETS_DIR = RULE_BASELINE_DIR / "datasets"
RULE_ENGINE_OFFLINE_DIR = RULE_ENGINE_DIR / "data" / "offline"


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


def _sanitize_run_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    cleaned = cleaned.strip("._-")
    return cleaned or fallback


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _first_existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _build_runtime_context() -> dict[str, object]:
    run_id = _sanitize_run_component(_get_env("PEG_RUN_ID", "manual"), "manual")
    run_date = _sanitize_run_component(_get_env("PEG_RUN_DATE", _utc_today_str()), _utc_today_str())
    return {
        "dry_run": _get_bool("PEG_DRY_RUN", True),
        "run_id": run_id,
        "run_date": run_date,
        "run_mode": _sanitize_run_component(_get_env("PEG_RUN_MODE", "manual"), "manual"),
    }


def _build_directory_context(run_id: str, run_date: str) -> dict[str, Path]:
    base_data_dir = Path(_get_env("PEG_BASE_DATA_DIR", str(BASE_DATA_DIR)))
    shared_data_dir = Path(_get_env("PEG_SHARED_DATA_DIR", str(base_data_dir / "shared")))
    runs_root_dir = Path(_get_env("PEG_RUNS_ROOT_DIR", str(base_data_dir / "runs")))
    return {
        "base_data_dir": base_data_dir,
        "shared_data_dir": shared_data_dir,
        "runs_root_dir": runs_root_dir,
        "run_day_dir": runs_root_dir / run_date,
        "data_dir": runs_root_dir / run_date / run_id,
        "summary_dir": Path(_get_env("PEG_SUMMARY_DIR", str(base_data_dir / "summary"))),
        "shared_universe_dir": Path(_get_env("PEG_SHARED_UNIVERSE_DIR", str(shared_data_dir / "universe"))),
        "shared_positions_dir": Path(_get_env("PEG_SHARED_POSITIONS_DIR", str(shared_data_dir / "positions"))),
        "shared_orders_live_dir": Path(_get_env("PEG_SHARED_ORDERS_LIVE_DIR", str(shared_data_dir / "orders_live"))),
        "shared_state_dir": Path(_get_env("PEG_SHARED_STATE_DIR", str(shared_data_dir / "state"))),
        "shared_token_state_dir": Path(_get_env("PEG_SHARED_TOKEN_STATE_DIR", str(shared_data_dir / "token_state"))),
        "shared_ws_raw_dir": Path(_get_env("PEG_SHARED_WS_RAW_DIR", str(shared_data_dir / "ws_raw"))),
        "shared_labels_dir": Path(_get_env("PEG_SHARED_LABELS_DIR", str(shared_data_dir / "labels"))),
    }


def _resolve_rule_engine_defaults() -> tuple[Path, Path]:
    default_rules_path = _first_existing_path(
        [
            RULE_BASELINE_DATASETS_DIR / "trading_rules.csv",
            RULE_BASELINE_DATASETS_DIR / "edge" / "trading_rules.csv",
            RULE_ENGINE_OFFLINE_DIR / "edge" / "trading_rules.csv",
        ]
    )
    default_model_path = _first_existing_path(
        [
            RULE_BASELINE_DATASETS_DIR / "ensemble_snapshot_q.pkl",
            RULE_BASELINE_DATASETS_DIR / "models" / "ensemble_snapshot_q.pkl",
            RULE_ENGINE_OFFLINE_DIR / "models" / "ensemble_snapshot_q.pkl",
        ]
    )
    return default_rules_path, default_model_path


@dataclass(frozen=True)
class PegConfig:
    # Runtime
    dry_run: bool
    run_id: str
    run_date: str
    run_mode: str

    # Directories
    base_data_dir: Path
    shared_data_dir: Path
    runs_root_dir: Path
    run_day_dir: Path
    data_dir: Path
    summary_dir: Path
    shared_universe_dir: Path
    shared_positions_dir: Path
    shared_orders_live_dir: Path
    shared_state_dir: Path
    shared_token_state_dir: Path
    shared_ws_raw_dir: Path
    shared_labels_dir: Path

    # Summary artifacts
    run_summary_path: Path
    summary_index_path: Path
    summary_dashboard_path: Path
    universe_current_path: Path
    universe_current_manifest_path: Path
    run_universe_path: Path
    run_universe_manifest_path: Path
    open_positions_path: Path
    market_state_cache_path: Path
    state_snapshot_path: Path
    orders_live_latest_orders_path: Path
    orders_live_fills_path: Path
    orders_live_cancels_path: Path
    orders_live_opened_positions_path: Path
    orders_live_opened_position_events_path: Path
    resolved_labels_path: Path
    token_state_current_path: Path
    token_state_current_json_path: Path
    run_stream_manifest_path: Path
    run_stream_token_state_path: Path
    run_snapshot_score_manifest_path: Path
    run_snapshot_processed_markets_path: Path
    run_snapshot_raw_inputs_path: Path
    run_snapshot_normalized_path: Path
    run_snapshot_feature_inputs_path: Path
    run_snapshot_rule_hits_path: Path
    run_snapshot_model_outputs_path: Path
    run_snapshot_selection_path: Path
    run_submit_manifest_path: Path
    run_submit_attempts_path: Path
    run_submit_orders_submitted_path: Path
    run_submit_post_submit_features_path: Path
    run_submit_window_manifest_path: Path
    run_deferred_reports_path: Path
    run_monitor_manifest_path: Path
    run_label_manifest_path: Path
    run_label_resolved_labels_path: Path
    run_label_order_lifecycle_path: Path
    run_label_executed_analysis_path: Path
    run_label_opportunity_analysis_path: Path
    run_label_summary_path: Path

    # Order sizing and TTL
    initial_bankroll_usdc: float
    max_trade_amount_usdc: float
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
    max_daily_orders: int
    dup_window_sec: int
    min_time_to_close_sec: int
    max_open_orders: int
    max_position_per_market_usdc: float
    max_net_exposure_usdc: float
    max_exposure_per_category_usdc: float
    fat_finger_high: float
    fat_finger_low: float

    # File-based IO
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

    # Balance check behavior
    balance_strict: bool

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

    # Rule engine integration
    rule_engine_dir: Path
    rule_engine_rules_path: Path
    rule_engine_model_path: Path
    rule_engine_raw_markets_path: Path
    rule_engine_max_markets: int
    rule_engine_page_size: int
    rule_engine_order_buffer: float
    rule_engine_min_price: float
    rule_engine_max_price: float
    rule_engine_max_horizon_hours: float

    # Online pipeline settings
    online_universe_window_hours: float
    online_market_batch_size: int
    online_gamma_event_page_size: int
    online_require_two_token_markets: bool
    online_require_rule_coverage: bool
    online_coarse_horizon_slack_hours: float
    online_limit_ticks_below_best_bid: int
    online_stream_duration_sec: int
    online_market_ws_url: str
    online_market_ws_custom_features: bool
    online_market_ws_ping_interval_sec: int
    online_market_ws_idle_timeout_sec: int
    online_market_ws_connect_timeout_sec: int
    online_market_ws_reconnect_backoff_sec: int
    online_market_ws_max_tokens_per_connection: int
    online_market_ws_raw_flush_events: int
    online_market_ws_state_flush_sec: int
    online_token_state_max_age_sec: int
    online_capacity_wait_poll_sec: int
    online_price_cap_safety_buffer: float
    online_deferred_artifacts_enabled: bool
    submit_window_run_monitor_after: bool
    submit_window_monitor_sleep_sec: int
    submit_window_fail_on_monitor_error: bool

    def ensure_dirs(self) -> None:
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        self.shared_data_dir.mkdir(parents=True, exist_ok=True)
        self.runs_root_dir.mkdir(parents=True, exist_ok=True)
        self.run_day_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.summary_dir.mkdir(parents=True, exist_ok=True)
        self.shared_universe_dir.mkdir(parents=True, exist_ok=True)
        self.shared_positions_dir.mkdir(parents=True, exist_ok=True)
        self.shared_orders_live_dir.mkdir(parents=True, exist_ok=True)
        self.shared_state_dir.mkdir(parents=True, exist_ok=True)
        self.shared_token_state_dir.mkdir(parents=True, exist_ok=True)
        self.shared_ws_raw_dir.mkdir(parents=True, exist_ok=True)
        self.shared_labels_dir.mkdir(parents=True, exist_ok=True)
        self.token_cache_path.parent.mkdir(parents=True, exist_ok=True)


def load_config() -> PegConfig:
    """Build a fresh config from current environment variables."""
    runtime = _build_runtime_context()
    run_id = str(runtime["run_id"])
    run_date = str(runtime["run_date"])
    run_mode = str(runtime["run_mode"])
    dry_run = bool(runtime["dry_run"])

    directories = _build_directory_context(run_id, run_date)
    base_data_dir = directories["base_data_dir"]
    shared_data_dir = directories["shared_data_dir"]
    runs_root_dir = directories["runs_root_dir"]
    run_day_dir = directories["run_day_dir"]
    data_dir = directories["data_dir"]
    summary_dir = directories["summary_dir"]
    shared_universe_dir = directories["shared_universe_dir"]
    shared_positions_dir = directories["shared_positions_dir"]
    shared_orders_live_dir = directories["shared_orders_live_dir"]
    shared_state_dir = directories["shared_state_dir"]
    shared_token_state_dir = directories["shared_token_state_dir"]
    shared_ws_raw_dir = directories["shared_ws_raw_dir"]
    shared_labels_dir = directories["shared_labels_dir"]

    default_rules_path, default_model_path = _resolve_rule_engine_defaults()

    cfg = PegConfig(
        dry_run=dry_run,
        run_id=run_id,
        run_date=run_date,
        run_mode=run_mode,
        base_data_dir=base_data_dir,
        shared_data_dir=shared_data_dir,
        runs_root_dir=runs_root_dir,
        run_day_dir=run_day_dir,
        data_dir=data_dir,
        summary_dir=summary_dir,
        shared_universe_dir=shared_universe_dir,
        shared_positions_dir=shared_positions_dir,
        shared_orders_live_dir=shared_orders_live_dir,
        shared_state_dir=shared_state_dir,
        shared_token_state_dir=shared_token_state_dir,
        shared_ws_raw_dir=shared_ws_raw_dir,
        shared_labels_dir=shared_labels_dir,
        run_summary_path=data_dir / "run_summary.json",
        summary_index_path=summary_dir / "runs_index.jsonl",
        summary_dashboard_path=summary_dir / "dashboard.html",
        universe_current_path=Path(_get_env("PEG_UNIVERSE_CURRENT_PATH", str(shared_universe_dir / "current_universe.csv"))),
        universe_current_manifest_path=Path(
            _get_env("PEG_UNIVERSE_CURRENT_MANIFEST_PATH", str(shared_universe_dir / "current_universe_manifest.json"))
        ),
        run_universe_path=Path(_get_env("PEG_RUN_UNIVERSE_PATH", str(data_dir / "universe_refresh" / "current_universe.csv"))),
        run_universe_manifest_path=Path(
            _get_env("PEG_RUN_UNIVERSE_MANIFEST_PATH", str(data_dir / "universe_refresh" / "manifest.json"))
        ),
        open_positions_path=Path(_get_env("PEG_OPEN_POSITIONS_PATH", str(shared_positions_dir / "open_positions.jsonl"))),
        market_state_cache_path=Path(
            _get_env("PEG_MARKET_STATE_CACHE_PATH", str(shared_positions_dir / "market_state.json"))
        ),
        state_snapshot_path=Path(
            _get_env("PEG_STATE_SNAPSHOT_PATH", str(shared_state_dir / "state_snapshot.json"))
        ),
        orders_live_latest_orders_path=Path(
            _get_env("PEG_ORDERS_LIVE_LATEST_ORDERS_PATH", str(shared_orders_live_dir / "latest_orders.jsonl"))
        ),
        orders_live_fills_path=Path(
            _get_env("PEG_ORDERS_LIVE_FILLS_PATH", str(shared_orders_live_dir / "fills.jsonl"))
        ),
        orders_live_cancels_path=Path(
            _get_env("PEG_ORDERS_LIVE_CANCELS_PATH", str(shared_orders_live_dir / "cancels.jsonl"))
        ),
        orders_live_opened_positions_path=Path(
            _get_env(
                "PEG_ORDERS_LIVE_OPENED_POSITIONS_PATH",
                str(shared_orders_live_dir / "opened_positions.jsonl"),
            )
        ),
        orders_live_opened_position_events_path=Path(
            _get_env(
                "PEG_ORDERS_LIVE_OPENED_POSITION_EVENTS_PATH",
                str(shared_orders_live_dir / "opened_position_events.jsonl"),
            )
        ),
        resolved_labels_path=Path(_get_env("PEG_RESOLVED_LABELS_PATH", str(shared_labels_dir / "resolved_labels.csv"))),
        token_state_current_path=Path(
            _get_env("PEG_TOKEN_STATE_CURRENT_PATH", str(shared_token_state_dir / "current_token_state.csv"))
        ),
        token_state_current_json_path=Path(
            _get_env("PEG_TOKEN_STATE_CURRENT_JSON_PATH", str(shared_token_state_dir / "current_token_state.json"))
        ),
        run_stream_manifest_path=Path(
            _get_env("PEG_RUN_STREAM_MANIFEST_PATH", str(data_dir / "market_stream" / "manifest.json"))
        ),
        run_stream_token_state_path=Path(
            _get_env("PEG_RUN_STREAM_TOKEN_STATE_PATH", str(data_dir / "market_stream" / "token_state.csv"))
        ),
        run_snapshot_score_manifest_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_SCORE_MANIFEST_PATH", str(data_dir / "snapshot_score" / "manifest.json"))
        ),
        run_snapshot_processed_markets_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_PROCESSED_MARKETS_PATH", str(data_dir / "snapshot_score" / "processed_markets.csv"))
        ),
        run_snapshot_raw_inputs_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_RAW_INPUTS_PATH", str(data_dir / "snapshot_score" / "raw_snapshot_inputs.jsonl"))
        ),
        run_snapshot_normalized_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_NORMALIZED_PATH", str(data_dir / "snapshot_score" / "normalized_snapshots.csv"))
        ),
        run_snapshot_feature_inputs_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_FEATURE_INPUTS_PATH", str(data_dir / "snapshot_score" / "feature_inputs.csv"))
        ),
        run_snapshot_rule_hits_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_RULE_HITS_PATH", str(data_dir / "snapshot_score" / "rule_hits.csv"))
        ),
        run_snapshot_model_outputs_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_MODEL_OUTPUTS_PATH", str(data_dir / "snapshot_score" / "model_outputs.csv"))
        ),
        run_snapshot_selection_path=Path(
            _get_env("PEG_RUN_SNAPSHOT_SELECTION_PATH", str(data_dir / "snapshot_score" / "selection_decisions.csv"))
        ),
        run_submit_manifest_path=Path(
            _get_env("PEG_RUN_SUBMIT_MANIFEST_PATH", str(data_dir / "submit_hourly" / "manifest.json"))
        ),
        run_submit_attempts_path=Path(
            _get_env("PEG_RUN_SUBMIT_ATTEMPTS_PATH", str(data_dir / "submit_hourly" / "submission_attempts.csv"))
        ),
        run_submit_orders_submitted_path=Path(
            _get_env("PEG_RUN_SUBMIT_ORDERS_SUBMITTED_PATH", str(data_dir / "submit_hourly" / "orders_submitted.jsonl"))
        ),
        run_submit_post_submit_features_path=Path(
            _get_env(
                "PEG_RUN_SUBMIT_POST_SUBMIT_FEATURES_PATH",
                str(data_dir / "submit_hourly" / "post_submit_model_features.csv"),
            )
        ),
        run_submit_window_manifest_path=Path(
            _get_env("PEG_RUN_SUBMIT_WINDOW_MANIFEST_PATH", str(data_dir / "submit_window" / "manifest.json"))
        ),
        run_deferred_reports_path=Path(
            _get_env("PEG_RUN_DEFERRED_REPORTS_PATH", str(data_dir / "deferred" / "reports.jsonl"))
        ),
        run_monitor_manifest_path=Path(
            _get_env("PEG_RUN_MONITOR_MANIFEST_PATH", str(data_dir / "order_monitor" / "manifest.json"))
        ),
        run_label_manifest_path=Path(
            _get_env("PEG_RUN_LABEL_MANIFEST_PATH", str(data_dir / "label_analysis" / "manifest.json"))
        ),
        run_label_resolved_labels_path=Path(
            _get_env("PEG_RUN_LABEL_RESOLVED_LABELS_PATH", str(data_dir / "label_analysis" / "resolved_labels.csv"))
        ),
        run_label_order_lifecycle_path=Path(
            _get_env("PEG_RUN_LABEL_ORDER_LIFECYCLE_PATH", str(data_dir / "label_analysis" / "order_lifecycle.csv"))
        ),
        run_label_executed_analysis_path=Path(
            _get_env("PEG_RUN_LABEL_EXECUTED_ANALYSIS_PATH", str(data_dir / "label_analysis" / "executed_analysis.csv"))
        ),
        run_label_opportunity_analysis_path=Path(
            _get_env(
                "PEG_RUN_LABEL_OPPORTUNITY_ANALYSIS_PATH",
                str(data_dir / "label_analysis" / "opportunity_analysis.csv"),
            )
        ),
        run_label_summary_path=Path(
            _get_env("PEG_RUN_LABEL_SUMMARY_PATH", str(data_dir / "label_analysis" / "summary.json"))
        ),
        initial_bankroll_usdc=_get_float("PEG_INITIAL_BANKROLL_USDC", 100.0),
        max_trade_amount_usdc=_get_float("PEG_MAX_TRADE_AMOUNT_USDC", 5.0),
        order_usdc=_get_float("PEG_ORDER_USDC", 5.0),
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
        max_notional=_get_float("PEG_MAX_NOTIONAL", 5.0),
        daily_loss_limit=_get_float("PEG_DAILY_LOSS_LIMIT", -500.0),
        max_daily_orders=_get_int("PEG_MAX_DAILY_ORDERS", 0),
        dup_window_sec=_get_int("PEG_DUP_WINDOW_SEC", 5),
        min_time_to_close_sec=_get_int("PEG_MIN_TIME_TO_CLOSE_SEC", 900),
        max_open_orders=_get_int("PEG_MAX_OPEN_ORDERS", 20),
        max_position_per_market_usdc=_get_float("PEG_MAX_POSITION_PER_MARKET_USDC", 5.0),
        max_net_exposure_usdc=_get_float("PEG_MAX_NET_EXPOSURE_USDC", 100.0),
        max_exposure_per_category_usdc=_get_float("PEG_MAX_EXPOSURE_PER_CATEGORY_USDC", 0.0),
        fat_finger_high=_get_float("PEG_FAT_FINGER_HIGH", 0.99),
        fat_finger_low=_get_float("PEG_FAT_FINGER_LOW", 0.01),
        decisions_path=Path(_get_env("PEG_DECISIONS_PATH", str(data_dir / "decisions.jsonl"))),
        orders_path=Path(_get_env("PEG_ORDERS_PATH", str(data_dir / "orders.jsonl"))),
        events_path=Path(_get_env("PEG_EVENTS_PATH", str(data_dir / "events.jsonl"))),
        fills_path=Path(_get_env("PEG_FILLS_PATH", str(data_dir / "fills.jsonl"))),
        rejections_path=Path(_get_env("PEG_REJECTIONS_PATH", str(data_dir / "rejections.jsonl"))),
        logs_path=Path(_get_env("PEG_LOGS_PATH", str(data_dir / "logs.jsonl"))),
        metrics_path=Path(_get_env("PEG_METRICS_PATH", str(data_dir / "metrics.json"))),
        alerts_path=Path(_get_env("PEG_ALERTS_PATH", str(data_dir / "alerts.jsonl"))),
        balances_path=Path(_get_env("PEG_BALANCES_PATH", str(data_dir / "balances.json"))),
        nonce_path=Path(_get_env("PEG_NONCE_PATH", str(shared_data_dir / "nonce.json"))),
        enforce_one_order_per_market=_get_bool("PEG_ONE_ORDER_PER_MARKET", True),
        balance_strict=_get_bool("PEG_BALANCE_STRICT", False),
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
        token_cache_path=Path(_get_env("PEG_TOKEN_CACHE_PATH", str(shared_data_dir / "token_cache.json"))),
        token_cache_ttl_sec=_get_int("PEG_TOKEN_CACHE_TTL_SEC", 3600),
        rule_engine_dir=Path(_get_env("PEG_RULE_ENGINE_DIR", str(RULE_ENGINE_DIR))),
        rule_engine_rules_path=Path(
            _get_env("PEG_RULE_ENGINE_RULES_PATH", str(default_rules_path))
        ),
        rule_engine_model_path=Path(
            _get_env("PEG_RULE_ENGINE_MODEL_PATH", str(default_model_path))
        ),
        rule_engine_raw_markets_path=Path(
            _get_env(
                "PEG_RULE_ENGINE_RAW_MARKETS_PATH",
                str(RULE_ENGINE_DIR / "data" / "intermediate" / "raw_markets_merged.csv"),
            )
        ),
        rule_engine_max_markets=_get_int("PEG_RULE_ENGINE_MAX_MARKETS", 5000),
        rule_engine_page_size=_get_int("PEG_RULE_ENGINE_PAGE_SIZE", 500),
        rule_engine_order_buffer=_get_float("PEG_RULE_ENGINE_ORDER_BUFFER", 0.0),
        rule_engine_min_price=_get_float("PEG_RULE_ENGINE_MIN_PRICE", 0.01),
        rule_engine_max_price=_get_float("PEG_RULE_ENGINE_MAX_PRICE", 0.99),
        rule_engine_max_horizon_hours=_get_float("PEG_RULE_ENGINE_MAX_HORIZON_HOURS", 1000.0),
        online_universe_window_hours=_get_float("PEG_ONLINE_UNIVERSE_WINDOW_HOURS", 24.0),
        online_market_batch_size=_get_int("PEG_ONLINE_MARKET_BATCH_SIZE", 20),
        online_gamma_event_page_size=_get_int("PEG_ONLINE_GAMMA_EVENT_PAGE_SIZE", 50),
        online_require_two_token_markets=_get_bool("PEG_ONLINE_REQUIRE_TWO_TOKEN_MARKETS", True),
        online_require_rule_coverage=_get_bool("PEG_ONLINE_REQUIRE_RULE_COVERAGE", True),
        online_coarse_horizon_slack_hours=_get_float("PEG_ONLINE_COARSE_HORIZON_SLACK_HOURS", 0.1),
        online_limit_ticks_below_best_bid=_get_int("PEG_ONLINE_LIMIT_TICKS_BELOW_BEST_BID", 1),
        online_stream_duration_sec=_get_int("PEG_ONLINE_STREAM_DURATION_SEC", 5),
        online_market_ws_url=_get_env(
            "PEG_ONLINE_MARKET_WS_URL",
            "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        ),
        online_market_ws_custom_features=_get_bool("PEG_ONLINE_MARKET_WS_CUSTOM_FEATURES", True),
        online_market_ws_ping_interval_sec=_get_int("PEG_ONLINE_MARKET_WS_PING_INTERVAL_SEC", 10),
        online_market_ws_idle_timeout_sec=_get_int("PEG_ONLINE_MARKET_WS_IDLE_TIMEOUT_SEC", 30),
        online_market_ws_connect_timeout_sec=_get_int("PEG_ONLINE_MARKET_WS_CONNECT_TIMEOUT_SEC", 20),
        online_market_ws_reconnect_backoff_sec=_get_int("PEG_ONLINE_MARKET_WS_RECONNECT_BACKOFF_SEC", 5),
        online_market_ws_max_tokens_per_connection=_get_int("PEG_ONLINE_MARKET_WS_MAX_TOKENS_PER_CONNECTION", 20),
        online_market_ws_raw_flush_events=_get_int("PEG_ONLINE_MARKET_WS_RAW_FLUSH_EVENTS", 100),
        online_market_ws_state_flush_sec=_get_int("PEG_ONLINE_MARKET_WS_STATE_FLUSH_SEC", 5),
        online_token_state_max_age_sec=_get_int("PEG_ONLINE_TOKEN_STATE_MAX_AGE_SEC", 7200),
        online_capacity_wait_poll_sec=_get_int("PEG_ONLINE_CAPACITY_WAIT_POLL_SEC", 30),
        online_price_cap_safety_buffer=_get_float("PEG_ONLINE_PRICE_CAP_SAFETY_BUFFER", 0.01),
        online_deferred_artifacts_enabled=_get_bool("PEG_ONLINE_DEFERRED_ARTIFACTS_ENABLED", False),
        submit_window_run_monitor_after=_get_bool("PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER", True),
        submit_window_monitor_sleep_sec=_get_int("PEG_SUBMIT_WINDOW_MONITOR_SLEEP_SEC", 0),
        submit_window_fail_on_monitor_error=_get_bool("PEG_SUBMIT_WINDOW_FAIL_ON_MONITOR_ERROR", False),
    )
    cfg.ensure_dirs()
    return cfg
