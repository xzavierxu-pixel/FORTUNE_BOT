# Submit Window Main-Path Implementation Design

## 1. Document Status

- Status: Draft for implementation
- Scope: `execution_engine` online trading path, deployment path, and monitoring path
- Primary audience: engineering
- Primary decision: `run_submit_window` becomes the single trading entrypoint

## 2. Executive Summary

This design retires the legacy `hourly-cycle` trading path and removes `refresh-universe` from the trading workflow entirely.

The system will adopt a single production trading path:

1. `run_submit_window` performs all pre-submit work and order submission.
2. A post-submit lifecycle stage runs immediately after submission.
3. The post-submit lifecycle stage is implemented with `monitor_order_lifecycle`.
4. Deployment, health checks, and run summaries are rewritten around this single path.

After this change:

- `refresh-universe` is not part of trading and is removed from production scheduling.
- `hourly-cycle` pre-submit logic is deleted, not preserved behind a flag.
- `submit-window` is the only supported production trading workflow.
- Post-submit reconciliation, exit handling, and shared state rebuild happen under the `submit-window` run.

## 3. Problem Statement

The repository currently contains overlapping online execution paths:

- `submit-window` is the reviewed and approved pre-submit workflow.
- `hourly-cycle` is a legacy orchestration path that duplicates pre-submit work.
- `monitor_order_lifecycle` contains valuable post-submit behavior, but it is not directly integrated into the `submit-window` deployment path.
- `refresh-universe` still exists as a separate scheduled job even though `submit-window` fetches Gamma event pages directly and does not depend on cached universe output.

This causes four problems:

1. Production ownership is ambiguous because two trading entrypoints exist.
2. Legacy `hourly-cycle` logic increases maintenance and review cost.
3. Post-submit lifecycle handling is not guaranteed when only `submit-window` is scheduled.
4. Deployment and healthcheck configuration still reflect retired workflows.

## 4. Product Decision

The product decision is:

1. `run_submit_window` is the only production trading workflow.
2. `refresh-universe` is fully removed from the production trading system.
3. Legacy `hourly-cycle` logic before order submission is removed from the codebase.
4. Order-lifecycle processing after submission is attached directly to `run_submit_window`.
5. Production scheduling, service definitions, health checks, manifests, and summaries must align to this single-path model.

## 5. Goals

The implementation must achieve the following:

1. Preserve the currently reviewed `run_submit_window` pre-submit semantics.
2. Remove all legacy `hourly-cycle` logic that happens before order submission.
3. Eliminate all production dependence on `refresh-universe`.
4. Execute post-submit lifecycle handling immediately after `submit-window` completes submission work.
5. Present a single run-level manifest and summary model centered on `submit-window`.
6. Simplify deployment so the only production trading timer is `fortune-bot-submit-window.timer`.
7. Ensure post-submit lifecycle work does not block the next run's submit phase once the current run's submit phase has completed.
8. Standardize execution-engine logs, manifests, summaries, and operator-facing timestamps on Beijing time.

## 6. Non-Goals

This design does not aim to:

1. Change the market discovery, filtering, scoring, selection, or submission behavior already approved in `run_submit_window`.
2. Keep `hourly-cycle` as a fallback trading mode.
3. Remove helper functions from shared library modules if they are still used by `submit-window` internals.
4. Redesign the internal business semantics of `monitor_order_lifecycle`.
5. Change label analysis behavior beyond updating dependencies and health checks.
6. Preserve the old whole-run serial execution model where post-submit lifecycle blocks the next run's submit phase.

## 7. Current State

### 7.1 Current trading path split

Current repository behavior is split across these paths:

1. `submit-window`
   - Direct Gamma event paging
   - Structural filtering
   - Live filtering
   - inference
   - selection
   - order submission

2. `hourly-cycle`
   - optional universe refresh
   - cached universe loading
   - legacy eligibility flow
   - legacy streaming/scoring/submission orchestration
   - optional pre-monitor and post-monitor calls

3. `monitor_order_lifecycle`
   - order reconciliation
   - exit lifecycle handling
   - shared state rebuild
   - order lifecycle exports

4. `refresh-universe`
   - periodic shared universe generation
   - not required by `submit-window`

### 7.2 Current deployment shape

Production deployment currently includes these timers:

1. `fortune-bot-submit-window.timer`
2. `fortune-bot-hourly-cycle.timer`
3. `fortune-bot-refresh-universe.timer`
4. `fortune-bot-healthcheck.timer`
5. `fortune-bot-label-analysis.timer`

This does not reflect the desired single-path ownership model.

## 8. Target Architecture

### 8.1 Single production trading path

The target production path is:

1. `fortune-bot-submit-window.timer`
2. `fortune-bot-submit-window.service`
3. `execution_engine/app/scripts/linux/run_submit_window.sh`
4. `execution_engine.app.cli.online.main run-submit-window`
5. `run_submit_window`
6. post-submit lifecycle stage
7. run completion

### 8.2 Post-submit lifecycle stage

The post-submit lifecycle stage is part of the `submit-window` run and executes after all submission attempts for the current run are finished.

Its implementation uses `monitor_order_lifecycle` with summary publishing disabled for the nested call.

The post-submit lifecycle stage is logically downstream of the current run, but it is not allowed to own the scheduling gate for the next run's submit phase.

The post-submit stage must handle:

1. TTL cleanup
2. order reconciliation
3. exit lifecycle management
4. state snapshot refresh
5. market state cache refresh
6. open-position refresh
7. lifecycle export generation
8. shared `orders_live/*` rebuild
9. opened-position event generation

### 8.3 Concurrency boundary between runs

The implementation must distinguish two kinds of run activity:

1. active `submit phase`
2. active `post-submit lifecycle`

Only active `submit phase` is allowed to block the next scheduled submit run.

This means:

1. if run `N` is still fetching pages, evaluating candidates, or submitting orders, run `N+1` submit must not start
2. if run `N` has finished submission work and is only canceling orders, reconciling state, or handling exits, run `N+1` submit must still be allowed to start
3. overlapping `post-submit lifecycle` from run `N` with `submit phase` from run `N+1` is an intended and valid state

### 8.4 Timezone ownership

The production execution-engine timezone for operator-facing timestamps is `Asia/Shanghai`.

This timezone requirement applies to:

1. application logs
2. manifest timestamps
3. summary timestamps
4. job heartbeat timestamps
5. any operator-facing human-readable diagnostic output

### 8.5 Removed components from production trading

The following components are removed from the production trading path:

1. `refresh_current_universe`
2. `fortune-bot-refresh-universe.timer`
3. `fortune-bot-refresh-universe.service`
4. `run_hourly_cycle`
5. `fortune-bot-hourly-cycle.timer`
6. `fortune-bot-hourly-cycle.service`
7. any CLI or shell wrapper whose only purpose is legacy `hourly-cycle` trading

## 9. Functional Requirements

### 9.1 Submit-window remains the only pre-submit path

The system must preserve `run_submit_window` as the sole implementation of:

1. Gamma page fetching
2. market expansion
3. structural filtering
4. direct candidate batching
5. reference-token streaming
6. live filtering
7. inference
8. selection
9. order submission

No legacy `hourly-cycle` pre-submit code may remain active in production.

### 9.2 Post-submit lifecycle is mandatory

After `run_submit_window` completes submission work, the system must run a post-submit lifecycle phase by default.

This phase must:

1. run in the same top-level `submit-window` workflow
2. use the same `cfg` and run context
3. write its own detailed manifest if needed
4. contribute summary fields back to the main `submit-window` manifest

### 9.3 Scheduling must be gated by submit phase only

The next scheduled submit run must be blocked only by an active submit phase from a previous run.

The implementation must enforce all of the following:

1. an active page scan or submission loop from run `N` blocks run `N+1`
2. an active cancel/reconcile/exit stage from run `N` does not block run `N+1`
3. skip/cancel/defer decisions for a scheduled run are based on submit-phase state only
4. the system must not treat the whole top-level workflow as one indivisible scheduling lock

Examples:

1. if the previous run is still processing the configured `--max-pages 300`, the next run may be skipped or deferred
2. if the previous run has finished all submission attempts and is only canceling or reconciling orders, the next run must still proceed

### 9.4 Final run status must be phase-aware

The final `submit-window` run result must distinguish:

1. submit phase success or failure
2. post-submit lifecycle phase success or failure
3. final workflow outcome

Required final status examples:

1. `completed`
2. `submit_failed`
3. `completed_with_post_submit_failure`

### 9.5 Operator-facing time fields must use Beijing time

The system must emit Beijing-time timestamps by default for all operator-facing outputs.

Required rules:

1. any human-readable log timestamp emitted by execution-engine must use `Asia/Shanghai`
2. any manifest or summary field intended for direct operator inspection must use Beijing time by default
3. if UTC fields are retained for machine compatibility, they must be explicitly suffixed with `_utc`
4. any Beijing-time field must be explicitly named so its timezone is unambiguous, for example `_bj`, `_cst`, or `_local`
5. documentation and runbook examples must interpret runtime timestamps in Beijing time

### 9.6 Deployment must expose one trading timer

The production trading schedule must be centered on:

1. `fortune-bot-submit-window.timer`

No additional trading timer is allowed for:

1. `refresh-universe`
2. `hourly-cycle`

### 9.7 Health checks must monitor the new owner path

Health check configuration must track:

1. `submit_window`
2. `healthcheck`
3. `label_analysis_daily`

Optional non-trading checks may be added later, but `hourly_cycle` and `refresh_universe` must not remain required production checks.

## 10. Detailed Behavior

### 10.1 New top-level flow

The new top-level `submit-window` flow is:

1. initialize runtime container
2. execute existing submit-window page and batch loop
3. perform submission attempts
4. collect submit metrics and artifacts
5. release submit-phase exclusivity as soon as submission work is complete
6. run post-submit lifecycle handling
7. merge post-submit lifecycle summary into the main workflow manifest
8. publish one final workflow summary

The critical requirement is that step 5 happens before step 6 begins any long-running cancel or reconciliation work.

### 10.2 Post-submit lifecycle call contract

`run_submit_window` must call `monitor_order_lifecycle` with:

1. `publish_summary_enabled=False`
2. configurable `sleep_sec`
3. the same resolved `PegConfig`

The nested call must not create a second top-level summary entry for the same workflow run.

The nested call must also not retain the submit-phase scheduling lock for the duration of the post-submit lifecycle stage.

### 10.3 Shared artifacts after submit-window

After a successful `submit-window` run, the following shared artifacts must be refreshed by the post-submit stage:

1. `shared/orders_live/latest_orders.jsonl`
2. `shared/orders_live/fills.jsonl`
3. `shared/orders_live/cancels.jsonl`
4. `shared/orders_live/opened_positions.jsonl`
5. `shared/orders_live/opened_position_events.jsonl`
6. `shared/positions/market_state.json`
7. `shared/state/state_snapshot.json`

### 10.4 Exit lifecycle ownership

Exit handling remains part of the post-submit lifecycle stage.

This means `submit-window` becomes responsible for both:

1. opening positions through new submissions
2. maintaining the account lifecycle after submissions, including exit management

The implementation must document that the post-submit stage is account-global, not limited to orders created in the current run.

### 10.5 Required locking and run-state semantics

The runtime implementation must expose separate state for:

1. `submit_phase_active`
2. `post_submit_active`

It must be possible for these states to represent:

1. run `N` with `post_submit_active=True`
2. while run `N+1` with `submit_phase_active=True`

The implementation must not collapse both states into a single whole-run mutex.

### 10.6 Required time semantics

All execution-engine time handling for operator-facing outputs must default to Beijing time.

Recommended implementation rules:

1. define one shared timezone source for `Asia/Shanghai`
2. route log timestamp formatting through that shared timezone
3. route manifest and summary timestamp formatting through that shared timezone
4. retain UTC only where required for interoperability, and name those fields explicitly

## 11. Code Changes

### 11.1 `submit_window.py`

File:

- `execution_engine/online/pipeline/submit_window.py`

Required changes:

1. import `monitor_order_lifecycle`
2. extend `SubmitWindowResult` to include post-submit lifecycle summary fields
3. execute post-submit lifecycle after all page and batch submission work completes
4. release submit-phase exclusivity before post-submit lifecycle begins
5. capture lifecycle success or failure explicitly
6. merge lifecycle counters and manifest paths into `submit_window/manifest.json`
7. publish a single final run summary for the whole workflow
8. emit operator-facing timestamps in Beijing time

Required new manifest fields:

1. `post_submit_monitor_enabled`
2. `post_submit_monitor_status`
3. `post_submit_monitor_manifest_path`
4. `post_submit_latest_order_count`
5. `post_submit_open_order_count`
6. `post_submit_fill_count`
7. `post_submit_open_position_count`
8. `post_submit_exit_candidate_count`
9. `post_submit_exit_submitted_count`
10. `post_submit_settlement_close_count`
11. `post_submit_canceled_exit_order_count`
12. `final_status`
13. `submit_phase_started_at_bj`
14. `submit_phase_finished_at_bj`
15. `post_submit_started_at_bj`
16. `post_submit_finished_at_bj`

### 11.2 `monitor.py`

File:

- `execution_engine/online/execution/monitor.py`

Required changes:

1. preserve business behavior
2. support nested invocation from `submit-window` without publishing a separate top-level run summary
3. keep writing `order_monitor/manifest.json`
4. keep returning structured counts required by the main workflow
5. emit operator-facing timestamps in Beijing time

No pre-submit legacy behavior from `hourly-cycle` should be moved here.

### 11.3 `config.py`

File:

- `execution_engine/runtime/config.py`

Required additions:

1. `PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER`
2. `PEG_SUBMIT_WINDOW_MONITOR_SLEEP_SEC`
3. `PEG_SUBMIT_WINDOW_FAIL_ON_MONITOR_ERROR`
4. shared timezone configuration for execution-engine operator-facing timestamps

Recommended defaults:

1. `RUN_MONITOR_AFTER=1`
2. `MONITOR_SLEEP_SEC=0`
3. `FAIL_ON_MONITOR_ERROR=0` during rollout, then optionally `1` after stabilization
4. `TZ=Asia/Shanghai` or an equivalent execution-engine timezone setting

### 11.4 Remove legacy hourly-cycle implementation path

Files and surfaces to clean:

1. `execution_engine/app/scripts/linux/run_hourly_cycle.sh`
2. any CLI entrypoint exposing `run-hourly-cycle`
3. any PowerShell entrypoint for `hourly-cycle` if still present
4. legacy runtime/config fields used only by `hourly-cycle`
5. scoring or submission adapters used only by `hourly-cycle`
6. docs that still present `hourly-cycle` as a valid production workflow

Required cleanup rule:

If code exists only to support pre-submit behavior of `hourly-cycle`, it must be removed rather than left dormant.

### 11.5 Remove refresh-universe from production path

Files and surfaces to clean:

1. `deploy/systemd/fortune-bot-refresh-universe.service`
2. `deploy/systemd/fortune-bot-refresh-universe.timer`
3. deploy scripts that start or stop the refresh-universe timer
4. env examples that treat refresh-universe as a required unit
5. docs that present refresh-universe as a trading prerequisite

Clarification:

This requirement removes `refresh-universe` from the production trading system. If helper functions inside `online/universe/refresh.py` are still shared by page-based market expansion, they may remain as internal library code, but the standalone refresh workflow is retired.

## 12. Deployment Changes

### 12.1 systemd units

Production trading units after migration:

1. `fortune-bot-submit-window.service`
2. `fortune-bot-submit-window.timer`
3. `fortune-bot-healthcheck.service`
4. `fortune-bot-healthcheck.timer`
5. `fortune-bot-label-analysis.service`
6. `fortune-bot-label-analysis.timer`

Removed production units:

1. `fortune-bot-hourly-cycle.service`
2. `fortune-bot-hourly-cycle.timer`
3. `fortune-bot-refresh-universe.service`
4. `fortune-bot-refresh-universe.timer`

### 12.2 pipeline scripts

Files:

1. `execution_engine/app/scripts/linux/start_pipeline.sh`
2. `execution_engine/app/scripts/linux/restart_pipeline.sh`
3. `execution_engine/app/scripts/linux/stop_pipeline.sh`

Required changes:

1. remove `hourly-cycle` from managed services and timers
2. remove `refresh-universe` from managed services and timers
3. keep `submit-window`, `healthcheck`, and `label-analysis`
4. keep the tmux streaming path only if it is still required by the reviewed production model

### 12.3 environment template

File:

- `deploy/env/fortune_bot.env.example`

Required changes:

1. add submit-window post-submit monitor env vars
2. remove `CHECK_HOURLY_CYCLE_MAX_AGE_SEC`
3. remove `CHECK_REFRESH_UNIVERSE_MAX_AGE_SEC`
4. replace required-unit defaults with submit-window ownership
5. add `CHECK_SUBMIT_WINDOW_MAX_AGE_SEC`

Recommended default:

```env
CHECK_REQUIRED_UNITS=fortune-bot-submit-window.timer,fortune-bot-label-analysis.timer,fortune-bot-healthcheck.timer
CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=5400
CHECK_LABEL_ANALYSIS_DAILY_MAX_AGE_SEC=93600
PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER=1
PEG_SUBMIT_WINDOW_MONITOR_SLEEP_SEC=0
PEG_SUBMIT_WINDOW_FAIL_ON_MONITOR_ERROR=0
```

## 13. Healthcheck Changes

`deploy/monitor/check_jobs.py` already supports job-age checks through `CHECK_*_MAX_AGE_SEC`.

The required production monitoring model is:

1. `submit_window` is the required trading job
2. `label_analysis_daily` remains a scheduled analytics job
3. `healthcheck` remains the watchdog job itself

The job file names and monitoring conventions must be updated so `submit_window` becomes the canonical trading heartbeat.

## 14. Manifest and Summary Model

### 14.1 Top-level ownership

The top-level run owner is `submit-window`.

This means:

1. `submit_window/manifest.json` is the canonical trading-run manifest
2. nested `order_monitor/manifest.json` remains an implementation detail of the post-submit phase
3. the final run summary is published once, from the top-level `submit-window` workflow
4. the top-level manifest must expose phase-separated timestamps in Beijing time

### 14.2 Required top-level summary shape

The final run summary must include both:

1. submit funnel metrics
2. post-submit lifecycle metrics

It must be possible to answer these questions from one summary:

1. how many direct candidates were evaluated
2. how many orders were submitted
3. how many submissions were rejected
4. what the latest order state looks like after reconciliation
5. how many fills and open positions exist after the lifecycle phase
6. whether exits were considered or submitted
7. whether the full workflow completed successfully
8. when submit phase started and ended in Beijing time
9. when post-submit lifecycle started and ended in Beijing time

## 15. Migration Plan

### Phase 1: Functional integration

1. integrate `monitor_order_lifecycle` into `run_submit_window`
2. add config flags
3. merge lifecycle results into submit-window manifest
4. keep standalone `monitor-orders` CLI for debugging and manual recovery

### Phase 2: Legacy path removal

1. remove `hourly-cycle` scripts, CLI path, and deploy units
2. remove `refresh-universe` deploy units and env defaults
3. delete or isolate any code used only by legacy hourly-cycle pre-submit processing

### Phase 3: Deployment convergence

1. update start/restart/stop scripts
2. update env example
3. update healthcheck expectations
4. update README and operational documentation

## 16. Acceptance Criteria

### 16.1 Functional

1. `run_submit_window` remains behaviorally unchanged before order submission.
2. After submission, `monitor_order_lifecycle` is automatically executed.
3. A next scheduled run is blocked only when the previous run's submit phase is still active.
4. A next scheduled run is not blocked merely because the previous run is still canceling or reconciling orders.
5. Shared order and position artifacts are rebuilt during the same top-level workflow run.
6. Exit lifecycle handling is executed through the post-submit stage.
7. `submit_window/manifest.json` contains both submit and post-submit results.
8. Operator-facing timestamps are emitted in Beijing time.

### 16.2 Legacy removal

1. No production deploy script starts `hourly-cycle`.
2. No production deploy script starts `refresh-universe`.
3. No healthcheck configuration requires `hourly-cycle`.
4. No healthcheck configuration requires `refresh-universe`.
5. Legacy hourly-cycle pre-submit code paths are deleted or made unreachable and then removed.

### 16.3 Operational

1. A production operator can identify trading health by looking at the `submit_window` heartbeat only.
2. A production operator does not need to know about `hourly-cycle` or `refresh-universe` to understand live trading.
3. A failed post-submit lifecycle stage is visible without obscuring whether submission itself succeeded.

## 17. Risks and Mitigations

### 17.1 Risk: nested summary duplication

If `monitor_order_lifecycle` continues publishing an independent top-level summary when called by `submit-window`, the summary index will contain duplicate workflow entries.

Mitigation:

1. disable summary publishing for nested monitor calls
2. publish exactly one final summary from `submit-window`

### 17.2 Risk: hidden account-global side effects

`monitor_order_lifecycle` is account-global and may reconcile or manage exits unrelated to the current run.

Mitigation:

1. document this explicitly in code and manifests
2. treat the post-submit phase as account lifecycle maintenance, not per-run local cleanup

### 17.3 Risk: incomplete legacy cleanup

If `hourly-cycle` files remain partially active, operators may restart the wrong path later.

Mitigation:

1. remove deploy references
2. remove README references
3. delete unused entrypoints and config when migration completes

### 17.4 Risk: post-submit and next submit contend for shared state

Allowing run `N` post-submit lifecycle to overlap with run `N+1` submit phase can introduce shared-state races if both paths mutate the same files or caches without phase-aware coordination.

Mitigation:

1. define which artifacts are submit-phase critical versus post-submit derived
2. keep the submit-phase gate narrow, but add artifact-level locking where real shared writes exist
3. document any intentionally account-global side effects

### 17.5 Risk: mixed UTC and Beijing-time timestamps create operator confusion

If some logs stay in UTC while manifests and summaries move to Beijing time, operators will misread sequencing during incidents.

Mitigation:

1. make Beijing time the default for all operator-facing outputs
2. name any retained UTC fields explicitly with `_utc`
3. update runbooks and examples to use Beijing time consistently

## 18. Final Decision Summary

The final design is:

1. `run_submit_window` is the only production trading workflow.
2. `refresh-universe` is removed from the production path.
3. legacy `hourly-cycle` pre-submit behavior is removed from the codebase.
4. post-submit lifecycle handling is attached directly to `submit-window`.
5. the next scheduled submit is blocked only by an active prior submit phase, not by a still-running cancel/reconcile phase.
6. deployment, health checks, manifests, summaries, and logs are rewritten around this single-path ownership model and Beijing-time operator semantics.
