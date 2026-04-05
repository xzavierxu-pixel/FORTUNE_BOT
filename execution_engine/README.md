# Polymarket Execution Gateway

`execution_engine` is the live execution layer for Polymarket.

Target-state design for the production submit-window path:

- [SUBMIT_WINDOW_MAIN_PATH_IMPLEMENTATION_DESIGN.md](C:\Users\ROG\Desktop\fortune_bot\execution_engine\SUBMIT_WINDOW_MAIN_PATH_IMPLEMENTATION_DESIGN.md)

Use the dedicated environment at `C:\Users\ROG\Desktop\fortune_bot\.venv-execution` for live trading and dry-runs. This keeps the official `py-clob-client` isolated from unrelated tools in the global Python environment. The current deployment model expects Python `3.13` for live model compatibility.

Run artifacts live under [execution_engine/data](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data):

- `runs/YYYY-MM-DD/<run_id>/...`: one directory per daily run
- `shared/`: shared cache and nonce state
- `summary/runs_index.jsonl`: run-level summary index
- `summary/dashboard.html`: local monitoring dashboard

The repository now exposes a single production trading model centered on `run_submit_window`.

## Top-level layout

- `execution_engine/app/`: user-facing entrypoints
- `execution_engine/runtime/`: config, state, decisions, validation, and exposure logic
- `execution_engine/integrations/`: Gamma/CLOB/balance integrations
- `execution_engine/online/`: online trading pipeline by job responsibility
- `execution_engine/shared/`: shared I/O, time, logging, metrics, and alert helpers

## Online package layout

The online pipeline is now organized by job responsibility instead of a single flat module list:

- `execution_engine/online/universe/`: Gamma page expansion and shared market helpers
- `execution_engine/online/streaming/`: WebSocket market ingestion and token-state persistence
- `execution_engine/online/scoring/`: live snapshot building, rule matching, model scoring, and selection
- `execution_engine/online/execution/`: passive order submission, monitoring, and position state
- `execution_engine/online/analysis/`: resolved labels, lifecycle joins, executed/opportunity analysis
- `execution_engine/online/pipeline/`: cross-job orchestration and eligibility flow
- `execution_engine/online/reporting/`: run summaries and local dashboard generation

This keeps the implementation aligned with the jobs in the design document and removes the previous flat `execution_engine/online/*.py` layout.

## Main commands

Bootstrap the isolated execution environment:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\env\bootstrap_venv.ps1
```

Stream reference-token market data into the shared token-state store:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\stream_market_data.ps1 -RunId STREAM_001 -DurationSec 60 -MarketLimit 20
```

Run the direct page-based submit window:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\run_submit_window.ps1 -RunId SUBMIT_WINDOW_001
```

Run standalone order lifecycle monitoring and reconciliation:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\monitor_orders.ps1 -RunId MONITOR_001
```

Build resolved-label sync plus executed/opportunity analysis:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\label_analysis_daily.ps1 -RunId LABEL_001
```

## Online pipeline flow

1. Run the submit window directly against Gamma event pages
2. Expand markets into a narrow execution schema and apply Stage 1 structural filtering
3. Stream one reference token per market and apply Stage 2 live-price filtering
4. Reuse `rule_baseline` rules, feature preprocessing, model scoring, and stake sizing
5. Submit passive limit orders per selected market as `final quote -> execution gate -> submit`
6. Run post-submit order lifecycle monitoring, reconciliation, and exit handling
7. Sync resolved labels and run executed/opportunity analysis

## Environment layout

- `execution_engine/requirements-live.txt`: live runtime dependencies for rule-engine execution
- `execution_engine/app/scripts/env/`: environment/bootstrap scripts
- `execution_engine/app/scripts/online/`: online pipeline scripts

`bootstrap_venv.ps1` and the Linux bootstrap script install `py-clob-client` from the official GitHub repository by default: [Polymarket/py-clob-client](https://github.com/Polymarket/py-clob-client). If a local [py-clob-client](C:\Users\ROG\Desktop\fortune_bot\py-clob-client) project exists with `setup.py` or `pyproject.toml`, the bootstrap scripts prefer that local checkout instead. You can also override the Git source with `FORTUNE_BOT_PY_CLOB_CLIENT_GIT_URL` and `FORTUNE_BOT_PY_CLOB_CLIENT_REF`. On Linux, the bootstrap script now defaults to `python3.13` and can be overridden with `FORTUNE_BOT_PYTHON_BIN`.

## Important environment variables

Core:

- `PEG_DRY_RUN=1|0`
- `PEG_RUN_ID=<id>`
- `PEG_ORDER_TTL_SEC` default `900` seconds

Rule engine integration:

- `PEG_RULE_ENGINE_DIR`
- `PEG_RULE_ENGINE_RULES_PATH`
- `PEG_RULE_ENGINE_MODEL_PATH`
- `PEG_RULE_ENGINE_MAX_MARKETS`
- `PEG_RULE_ENGINE_PAGE_SIZE`
- `PEG_RULE_ENGINE_ORDER_BUFFER`

Online pipeline:

- `PEG_ONLINE_UNIVERSE_WINDOW_HOURS`
- `PEG_ONLINE_MARKET_BATCH_SIZE`
- `PEG_ONLINE_GAMMA_EVENT_PAGE_SIZE`
- `PEG_ONLINE_REQUIRE_TWO_TOKEN_MARKETS`
- `PEG_ONLINE_REQUIRE_RULE_COVERAGE`
- `PEG_ONLINE_COARSE_HORIZON_SLACK_HOURS`
- `PEG_ONLINE_LIMIT_TICKS_BELOW_BEST_BID`
- `PEG_ONLINE_STREAM_DURATION_SEC`
- `PEG_ONLINE_TOKEN_STATE_MAX_AGE_SEC`
- `PEG_ONLINE_CAPACITY_WAIT_POLL_SEC`
- `PEG_MARKET_STATE_CACHE_PATH`
- `PEG_SHARED_STATE_DIR`
- `PEG_STATE_SNAPSHOT_PATH`

CLOB:

- `PEG_CLOB_ENABLED=1`
- `PEG_CLOB_PRIVATE_KEY`
- `PEG_CLOB_API_KEY`
- `PEG_CLOB_API_SECRET`
- `PEG_CLOB_API_PASSPHRASE`

## Output files

Core execution outputs under [execution_engine/data/runs](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data\runs):

- `decisions.jsonl`
- `orders.jsonl`
- `fills.jsonl`
- `rejections.jsonl`
- `logs.jsonl`
- `metrics.json`

Each run directory also gets a `run_summary.json`, and the aggregate dashboard is rebuilt at [dashboard.html](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data\summary\dashboard.html).

Online shared artifacts under [execution_engine/data/shared](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data\shared):

- `positions/market_state.json`
- `positions/open_positions.jsonl`
- `orders_live/latest_orders.jsonl`
- `orders_live/fills.jsonl`
- `orders_live/cancels.jsonl`
- `orders_live/opened_positions.jsonl`
- `orders_live/opened_position_events.jsonl`
- `state/state_snapshot.json`
- `token_state/current_token_state.csv`
- `token_state/current_token_state.json`
- `ws_raw/YYYY-MM-DD/HH/shard_XX.jsonl`

Per-run direct submission artifacts:

- `submit_window/manifest.json`
- `submit_window/submission_attempts.csv`
- `submit_window/orders_submitted.jsonl`
- `submit_window/post_submit_model_features.csv`

Per-run order monitoring artifacts:

- `order_monitor/manifest.json`

Per-run label analysis artifacts:

- `label_analysis/manifest.json`
- `label_analysis/resolved_labels.csv`
- `label_analysis/order_lifecycle.csv`
- `label_analysis/executed_analysis.csv`
- `label_analysis/opportunity_analysis.csv`
- `label_analysis/summary.json`

`label_analysis/order_lifecycle.csv` is the canonical daily lifecycle table for submitted orders. It drives fill/cancel/reject rates, average order lifetime, and average fill-latency metrics in the daily summary.

The submit window recomputes `remaining_hours` at page expansion time before Stage 1 filtering. This keeps the live submit path tied to direct page fetches instead of cached universe output.

## Current behavior note

The live path is wired end-to-end and dry-run verified. Whether it emits signals at a given moment depends entirely on the current market set passing the offline rule filters, model scoring thresholds, and exposure constraints.

