# Polymarket Execution Gateway

`execution_engine` is the live execution layer for Polymarket.

Target-state design for the next online pipeline iteration:

- [ONLINE_EXECUTION_PIPELINE_DESIGN.md](C:\Users\ROG\Desktop\fortune_bot\execution_engine\ONLINE_EXECUTION_PIPELINE_DESIGN.md)

Use the dedicated environment at `C:\Users\ROG\Desktop\fortune_bot\.venv-execution` for live trading and dry-runs. This keeps the official `py-clob-client` isolated from unrelated tools in the global Python environment.

Run artifacts live under [execution_engine/data](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data):

- `runs/YYYY-MM-DD/<run_id>/...`: one directory per daily run
- `shared/`: shared cache and nonce state
- `summary/runs_index.jsonl`: run-level summary index
- `summary/dashboard.html`: local monitoring dashboard

The repository now exposes a single execution model: the online pipeline described in [ONLINE_EXECUTION_PIPELINE_DESIGN.md](C:\Users\ROG\Desktop\fortune_bot\execution_engine\ONLINE_EXECUTION_PIPELINE_DESIGN.md).

## Top-level layout

- `execution_engine/app/`: user-facing entrypoints
- `execution_engine/runtime/`: config, state, decisions, validation, and exposure logic
- `execution_engine/integrations/`: Gamma/CLOB/balance integrations
- `execution_engine/online/`: online trading pipeline by job responsibility
- `execution_engine/shared/`: shared I/O, time, logging, metrics, and alert helpers

## Online package layout

The online pipeline is now organized by job responsibility instead of a single flat module list:

- `execution_engine/online/universe/`: rolling 24h market universe refresh
- `execution_engine/online/streaming/`: WebSocket market ingestion and token-state persistence
- `execution_engine/online/scoring/`: hourly snapshot building, rule matching, model scoring, and selection
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

Refresh the shared 24h online universe:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\refresh_universe.ps1 -RunId UNIVERSE_001 -MaxMarkets 1000
```

Stream reference-token market data into the shared token-state store:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\stream_market_data.ps1 -RunId STREAM_001 -DurationSec 60 -MarketLimit 20
```

Run the hourly snapshot scoring job:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\score_hourly.ps1 -RunId SCORE_001
```

Submit hourly selections as passive limit orders:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\submit_hourly.ps1 -RunId SUBMIT_001
```

Run the full hourly online cycle in batch order:

```powershell
powershell -ExecutionPolicy Bypass -File execution_engine\app\scripts\online\run_hourly_cycle.ps1 -RunId CYCLE_001
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

1. Refresh a rolling 24h market universe
2. Stream reference-token market data into shared token state
3. Build hourly market-level snapshots
4. Reuse `rule_baseline` rules, feature preprocessing, model scoring, and stake sizing
5. Submit passive limit orders at `best_bid - 1 tick`
6. Monitor 5-minute TTL outcomes and rebuild shared open-position state
7. Sync resolved labels and run executed/opportunity analysis

## Environment layout

- `execution_engine/requirements-live.txt`: live runtime dependencies for rule-engine execution
- `execution_engine/app/scripts/env/`: environment/bootstrap scripts
- `execution_engine/app/scripts/online/`: online pipeline scripts

`bootstrap_venv.ps1` installs the local [py-clob-client](C:\Users\ROG\Desktop\fortune_bot\py-clob-client) clone in editable mode. That uses the official package metadata instead of the `py-clob-client` repo's pinned `requirements.txt`, so the online runtime stays isolated without forcing old versions into your global Python.

## Important environment variables

Core:

- `PEG_DRY_RUN=1|0`
- `PEG_RUN_ID=<id>`

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
- `PEG_ONLINE_REQUIRE_TWO_TOKEN_MARKETS`
- `PEG_ONLINE_REQUIRE_RULE_COVERAGE`
- `PEG_ONLINE_LIMIT_TICKS_BELOW_BEST_BID`
- `PEG_ONLINE_TOKEN_STATE_MAX_AGE_SEC`
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

Online streaming shared artifacts under [execution_engine/data/shared](C:\Users\ROG\Desktop\fortune_bot\execution_engine\data\shared):

- `universe/current_universe.csv`
- `universe/current_universe_manifest.json`
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

Per-run hourly scoring artifacts:

- `snapshot_score/processed_markets.csv`
- `snapshot_score/raw_snapshot_inputs.jsonl`
- `snapshot_score/normalized_snapshots.csv`
- `snapshot_score/feature_inputs.csv`
- `snapshot_score/rule_hits.csv`
- `snapshot_score/model_outputs.csv`
- `snapshot_score/selection_decisions.csv`
- `snapshot_score/manifest.json`

Per-run hourly submission artifacts:

- `submit_hourly/submission_attempts.csv`
- `submit_hourly/orders_submitted.jsonl`
- `submit_hourly/fills.jsonl`
- `submit_hourly/cancels.jsonl`
- `submit_hourly/opened_positions.jsonl`
- `submit_hourly/opened_position_events.jsonl`
- `submit_hourly/manifest.json`

Per-run order monitoring artifacts:

- `order_monitor/manifest.json`

Per-run hourly cycle artifacts:

- `hourly_cycle/manifest.json`
- `hourly_cycle/batches/batch_XXX/universe.csv`
- `hourly_cycle/batches/batch_XXX/market_stream/*`
- `hourly_cycle/batches/batch_XXX/snapshot_score/*`
- `hourly_cycle/batches/batch_XXX/submit_hourly/*`

Per-run label analysis artifacts:

- `label_analysis/manifest.json`
- `label_analysis/resolved_labels.csv`
- `label_analysis/order_lifecycle.csv`
- `label_analysis/executed_analysis.csv`
- `label_analysis/opportunity_analysis.csv`
- `label_analysis/summary.json`

`label_analysis/order_lifecycle.csv` is the canonical daily lifecycle table for submitted orders. It drives fill/cancel/reject rates, average order lifetime, and average fill-latency metrics in the daily summary.

Hourly scoring also recomputes `remaining_hours` from `end_time_utc` at run time before horizon filtering. This prevents stale 6-hour universe snapshots from leaking expired markets into hourly scoring between universe refreshes.

## Current behavior note

The live path is wired end-to-end and dry-run verified. Whether it emits signals at a given moment depends entirely on the current market set passing the offline rule filters, model scoring thresholds, and exposure constraints.

