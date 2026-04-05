# Execution Engine Storage Optimization Plan

## 1. Document Status

- Status: Draft for implementation
- Scope: `execution_engine` runtime artifacts, shared artifacts, run artifacts, and retention policy
- Primary audience: engineering and operations
- Primary decision: reduce artifact duplication, stop persisting `ws_raw`, and establish a single-source-of-truth storage model

## 2. Executive Summary

`execution_engine` currently writes too many overlapping artifacts. The same business facts are often persisted in multiple places:

1. global ledgers
2. per-run snapshots
3. shared mirrors
4. post-submit exports
5. raw websocket archives

This increases disk usage, slows diagnosis, and makes it harder to know which file is authoritative.

This plan changes the storage model to:

1. keep one authoritative ledger for orders, fills, events, rejections, alerts, and metrics
2. keep a small set of shared current-state snapshots for live execution
3. keep one compact per-run audit table for submit-window decisions
4. stop persisting `ws_raw`
5. demote heavy intermediate artifacts to debug-only outputs
6. add retention so old runs are compacted automatically

After this change, the system will still preserve trading correctness and operator visibility, but artifact volume will be materially lower and data ownership will be much clearer.

## 3. Problem Statement

The current system stores redundant data in four main ways.

### 3.1 Lifecycle duplication

The order lifecycle is already captured by the global ledgers:

1. `orders.jsonl`
2. `fills.jsonl`
3. `events.jsonl`
4. `rejections.jsonl`

But `monitor_order_lifecycle` also writes overlapping lifecycle mirrors:

1. `shared/orders_live/latest_orders.jsonl`
2. `shared/orders_live/fills.jsonl`
3. `shared/orders_live/cancels.jsonl`
4. `shared/orders_live/opened_positions.jsonl`
5. `shared/orders_live/opened_position_events.jsonl`
6. per-submit-dir `fills.jsonl`
7. per-submit-dir `cancels.jsonl`
8. per-submit-dir `opened_positions.jsonl`

These files are useful for convenience, but many rows are re-exports of facts already present in the ledgers.

### 3.2 Snapshot and scoring duplication

The submit-window scoring path writes many wide tables that are all derived from the same batch:

1. `processed_markets.csv`
2. `raw_snapshot_inputs.jsonl`
3. `normalized_snapshots.csv`
4. `feature_inputs.csv`
5. `rule_hits.csv`
6. `model_outputs.csv`
7. `selection_decisions.csv`
8. `post_submit_model_features.csv`

This is excessive for production. Most of these are training or debugging intermediates, not operational state.

### 3.3 State and summary duplication

The runtime state layer persists several overlapping summaries:

1. `state_snapshot.json`
2. `market_state.json`
3. `open_positions.jsonl`
4. run manifests
5. run summary
6. summary index
7. dashboard inputs

Some of this overlap is justified, but too much of the same state is copied into multiple artifacts.

### 3.4 Raw websocket archival

`online.streaming.manager` and `online.streaming.io` currently persist every raw websocket payload into `shared/ws_raw/.../shard_XX.jsonl`.

This is not a primary trading ledger. It is mostly a diagnostics archive. It also grows linearly with message volume and has weak operational value compared with its storage cost.

## 4. Design Principles

The optimization must follow these principles.

### 4.1 One fact, one authoritative file

Each business fact should have one primary persisted source. Other files should be:

1. a compact current-state index
2. a run-level audit view
3. a debug-only derivative

### 4.2 Production artifacts must support operations first

The default storage shape must optimize for:

1. trading correctness
2. restart safety
3. operator diagnosis
4. post-trade analysis

It must not optimize for full forensic replay by default.

### 4.3 Debug data must be opt-in

Heavy intermediates should exist only when explicitly enabled for:

1. incident debugging
2. model validation
3. feature inspection
4. manual quote comparison

### 4.4 Shared state must be current-state oriented

Shared directories should represent the latest system state, not a second historical archive.

### 4.5 Retention must be automatic

Old runs should not remain unbounded. The system should automatically compact or delete low-value artifacts after a configured age.

## 5. Current Artifact Inventory

### 5.1 Authoritative ledgers that should remain

These should remain as primary history sources:

1. `data/decisions.jsonl`
2. `data/orders.jsonl`
3. `data/fills.jsonl`
4. `data/events.jsonl`
5. `data/rejections.jsonl`
6. `data/alerts.jsonl`
7. `data/metrics.json`
8. `data/balances.json`

### 5.2 Shared current-state artifacts that should remain

These support the active runtime and can remain as compact indexes:

1. `shared/token_state/current.csv`
2. `shared/token_state/current.json`
3. `shared/state/state_snapshot.json`
4. `shared/state/market_state.json`
5. `shared/positions/open_positions.jsonl`
6. `shared/orders_live/latest_orders.jsonl`

### 5.3 Artifacts that should be reduced or removed

These are the main targets for optimization:

1. `shared/ws_raw/**/*`
2. per-run `raw_snapshot_inputs.jsonl`
3. per-run `normalized_snapshots.csv`
4. per-run `feature_inputs.csv`
5. per-run `rule_hits.csv`
6. per-run `model_outputs.csv`
7. per-run `post_submit_model_features.csv`
8. per-submit-dir lifecycle mirrors
9. shared `fills.jsonl`, `cancels.jsonl`, and `opened_positions.jsonl` if they duplicate global ledgers without serving a strong live-state purpose

## 6. Target Artifact Model

The target model has three storage layers.

### 6.1 Layer A: authoritative ledgers

These remain append-only and authoritative:

1. `orders.jsonl`
2. `fills.jsonl`
3. `events.jsonl`
4. `rejections.jsonl`
5. `decisions.jsonl`

All historical analysis should prefer these files over mirrored exports.

### 6.2 Layer B: shared current-state indexes

These remain, but only as compact snapshots of current state:

1. `token_state/current.csv`
2. `token_state/current.json`
3. `state/state_snapshot.json`
4. `state/market_state.json`
5. `positions/open_positions.jsonl`
6. `orders_live/latest_orders.jsonl`

These files are not historical ledgers. They exist to support:

1. live execution decisions
2. restart recovery
3. current operational inspection

### 6.3 Layer C: run-level audit artifacts

Each run should keep only a minimal set of audit outputs by default:

1. `selection_decisions.csv`
2. `orders_submitted.jsonl`
3. `submit_window_manifest.json`
4. `run_summary.json`
5. `monitor_manifest.json` when monitor ran

This is enough to answer:

1. what markets were considered
2. which candidates were selected
3. what was submitted
4. what happened after submission

### 6.4 Debug-only artifacts

The following become debug-only:

1. `processed_markets.csv`
2. `raw_snapshot_inputs.jsonl`
3. `normalized_snapshots.csv`
4. `feature_inputs.csv`
5. `rule_hits.csv`
6. `model_outputs.csv`
7. `post_submit_model_features.csv`
8. any lifecycle mirror written only for convenience

## 7. Decision on ws_raw

### 7.1 Final decision

`ws_raw` will not be persisted in normal operation.

### 7.2 Reasoning

This is the correct decision because:

1. `ws_raw` is not the system of record for trading
2. order submission and monitoring do not require historical raw websocket payload files
3. live trading uses token state snapshots, not raw frame replay
4. the few current uses of `ws_raw` are diagnostic or feature-enrichment convenience paths, not correctness-critical dependencies
5. raw frame archives are expensive relative to their production value

### 7.3 Replacement model

After removing `ws_raw`, the system should rely on:

1. `shared/token_state/current.csv` and `current.json` for the latest market quote state
2. CLOB `prices-history` for historical price backfill
3. optional debug-only live capture if manual investigation is explicitly requested

### 7.4 Feature impact

Some historical price features currently merge CLOB history with the latest `ws_raw` quote. After this change:

1. default production behavior should use token-state current price as the latest point when available
2. if token-state is missing, the system should fall back to pure CLOB history
3. no production path should scan `shared/ws_raw`

### 7.5 Debug fallback

If raw websocket diagnostics are needed later, they should be reintroduced only behind an explicit debug flag and short retention.

## 8. Configuration Strategy

Introduce an artifact policy with three levels.

### 8.1 `minimal`

Production default.

Write only:

1. authoritative ledgers
2. current-state shared indexes
3. compact run manifests
4. `selection_decisions.csv`
5. `orders_submitted.jsonl`

Do not write:

1. `ws_raw`
2. wide intermediate scoring tables
3. duplicate lifecycle mirrors in submit directories

### 8.2 `standard`

Operational troubleshooting mode.

Write everything in `minimal`, plus:

1. selected extra run diagnostics that are cheap and frequently useful
2. limited monitor detail artifacts if they support operator workflows

### 8.3 `debug`

Investigation mode.

Write everything in `standard`, plus:

1. heavy feature tables
2. raw snapshot inputs
3. optional raw websocket capture
4. any one-off experiment artifacts

## 9. File-by-File Optimization Plan

### 9.1 Streaming layer

Affected modules:

1. `online/streaming/io.py`
2. `online/streaming/manager.py`
3. `online/scoring/price_history.py`
4. `online/scoring/snapshot_builder.py`

Planned changes:

1. disable `RawEventBuffer` by default
2. stop writing `shared/ws_raw`
3. remove production reads of `shared/ws_raw`
4. switch latest-point feature enrichment to token-state derived price
5. keep stream manifest, but remove dependency on `raw_event_root_dir` as an operational artifact

### 9.2 Submit-window scoring artifacts

Affected module:

1. `online/pipeline/submit_window.py`

Planned changes:

1. keep `selection_decisions.csv` as the default run-level audit table
2. make `processed_markets.csv` optional
3. make `raw_snapshot_inputs.jsonl` debug-only
4. make `normalized_snapshots.csv` debug-only
5. make `feature_inputs.csv` debug-only
6. make `rule_hits.csv` debug-only
7. make `model_outputs.csv` debug-only
8. remove or debug-gate `post_submit_model_features.csv`

### 9.3 Lifecycle exports

Affected module:

1. `online/execution/monitor.py`

Planned changes:

1. keep one global source of historical truth in `orders.jsonl`, `fills.jsonl`, and `events.jsonl`
2. keep `shared/orders_live/latest_orders.jsonl` because it is a current-state index
3. remove per-submit-dir lifecycle mirror exports by default
4. review whether shared `fills.jsonl`, `cancels.jsonl`, and `opened_positions.jsonl` are still needed as independent files
5. if they remain, narrow them to current-state or last-known-open views only

### 9.4 State and summary artifacts

Affected modules:

1. `runtime/state.py`
2. `online/execution/positions.py`
3. `online/reporting/run_summary.py`
4. `online/reporting/summary_metrics.py`

Planned changes:

1. keep `state_snapshot.json` as the canonical aggregate runtime snapshot
2. narrow `market_state.json` to exclusion and gating fields only
3. avoid copying the same counts and path lists into multiple summary files
4. keep dashboard generation based on compact summary inputs rather than duplicated raw exports

## 10. Retention Policy

Retention must apply to both shared and run-scoped artifacts.

### 10.1 Shared artifacts

Shared current-state artifacts should be overwritten in place when possible.

No long-term historical accumulation should happen in:

1. `shared/token_state`
2. `shared/state`
3. `shared/orders_live`
4. `shared/positions`

### 10.2 Run artifacts

Retention policy should support two windows:

1. short-term full retention for recent runs
2. long-term compact retention for historical runs

Recommended defaults:

1. keep full run artifacts for the most recent 7 days
2. compact runs older than 7 days to summary-plus-audit only
3. delete debug-only files older than 2 days

### 10.3 Compaction rules

For an old run, retain only:

1. `run_summary.json`
2. `submit_window_manifest.json`
3. `selection_decisions.csv`
4. `orders_submitted.jsonl`
5. `monitor_manifest.json` if present

Delete old debug and intermediate files after compaction.

## 11. Migration Plan

Implementation should happen in four phases.

### Phase 1: stop `ws_raw`

1. add artifact policy configuration
2. disable raw websocket persistence in production
3. remove production reads of `shared/ws_raw`
4. use token-state latest price instead of `ws_raw` latest price

### Phase 2: remove duplicated scoring outputs

1. make wide scoring tables debug-only
2. keep `selection_decisions.csv` as the default run audit artifact
3. simplify snapshot manifests to reference only active outputs

### Phase 3: shrink lifecycle mirrors

1. remove per-submit-dir lifecycle re-exports
2. narrow shared lifecycle files to current-state needs only
3. ensure label analysis reads from authoritative ledgers or retained run audit files

### Phase 4: retention and cleanup

1. implement automatic run compaction
2. implement age-based cleanup for debug artifacts
3. document operator cleanup behavior

## 12. Compatibility and Analysis Impact

This optimization must preserve the following workflows.

### 12.1 Trading correctness

Must remain unchanged:

1. selection
2. submission
3. reconciliation
4. exit logic
5. settlement handling

### 12.2 Label analysis

`label_history.py` and `order_lifecycle.py` should continue to read from:

1. `selection_decisions.csv`
2. `orders_submitted.jsonl`
3. `orders.jsonl`
4. `fills.jsonl`
5. `events.jsonl`

They should not depend on `ws_raw`.

### 12.3 Manual diagnostics

Manual scripts that currently assume `ws_raw` exists must be updated to:

1. use token-state snapshots
2. use live CLOB queries
3. optionally run with explicit debug capture enabled

## 13. Validation Plan

Each implementation phase must be validated with focused tests.

### 13.1 Streaming validation

1. stream run still produces token-state outputs when raw capture is disabled
2. snapshot builder still computes historical features without `ws_raw`
3. manifests remain readable and accurate

### 13.2 Submit-window validation

1. submit-window still completes with `minimal` artifact policy
2. `selection_decisions.csv` still contains enough audit context
3. no production code path requires debug-only files

### 13.3 Monitor validation

1. reconciliation still updates shared state correctly
2. exit submission and settlement cancellation still work
3. label-analysis inputs remain available after mirror reduction

### 13.4 Retention validation

1. old runs compact without deleting required audit files
2. dashboard and summaries still load after compaction
3. cleanup does not touch current shared state

## 14. Risks and Mitigations

### Risk 1: analysis scripts still expect removed files

Mitigation:

1. identify consumers before deleting artifacts
2. update readers to use ledgers or `selection_decisions.csv`
3. add focused regression tests

### Risk 2: less forensic visibility during incidents

Mitigation:

1. keep a debug artifact policy
2. allow temporary raw capture for targeted investigations
3. document the operational procedure

### Risk 3: summary files lose useful operational context

Mitigation:

1. define a required summary schema before deleting fields
2. keep only high-value counts and pointers
3. validate against operator workflows

## 15. Final Storage Decisions

The final decisions from this document are:

1. `ws_raw` is not stored in normal operation
2. `selection_decisions.csv` is the default per-run audit table
3. wide scoring intermediates are debug-only
4. global ledgers remain the historical source of truth
5. shared directories are current-state indexes, not secondary history ledgers
6. per-submit lifecycle mirrors are removed or heavily reduced
7. retention and compaction are mandatory parts of the design

## 16. Recommended Immediate Next Step

The next implementation step should be:

1. add artifact policy config
2. disable `ws_raw`
3. remove `ws_raw` reads from scoring
4. keep the rest of the optimization behind staged changes

This yields the best first-step payoff with the lowest behavioral risk.
