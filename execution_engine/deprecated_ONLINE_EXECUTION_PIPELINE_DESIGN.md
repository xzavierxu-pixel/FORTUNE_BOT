# Low-Latency Submit PRD
## 1. Document Status
- Status: Draft for implementation- Scope: `execution_engine` online execution path- Primary audience: Codex / engineering implementation- Goal: minimize elapsed time from Gamma fetch to final order submission
## 2. Problem Statement
The current online execution workflow is optimized for auditability and replay, not for low-latency submission. Today the pipeline does too much before submission:
1. Fetches a broad Gamma universe.2. Materializes universe artifacts.3. Runs eligibility over stored data.4. Streams only after substantial upstream delay.5. Scores and selects.6. Writes multiple intermediate files.7. Submits only after the above is complete.
This creates a large delay between the first time a market is seen in `_build_binary_market_row()` and the moment the order is finally submitted.
Two additional facts matter for the redesign:
1. The trading input source is Gamma **event data**, not flat market-only data. One event may contain multiple markets.2. Different fields change at very different speeds: - `domain`, `category`, `market_type` are effectively stable for a market. - `remaining_hours` changes continuously but slowly. - `price` changes quickly and must be treated as fast-moving data.
The redesigned workflow must reflect these realities.
## 3. Primary Objective
Reduce the time from Gamma API fetch to order submission as much as possible without changing strategy semantics.
## 4. Strategy Invariants
The redesign must preserve the following invariants:
1. Keep **market-level directional inference**.2. Do not change the current semantics where the model predicts one market-level direction and then maps that direction to one token.3. Each market uses exactly **one reference token** during streaming and market evaluation.4. Final order submission still validates the **selected token** with a fresh live quote.5. Risk checks, price checks, and time-window checks remain mandatory before order placement.6. State reconstruction remains possible from append-only fact logs.
## 5. Core Design Principles
### 5.1 Two-stage rule filtering
Rule filtering must be split into two stages:
1. **Stage 1: Structural coarse filtering** Runs before WebSocket streaming. Uses only stable or slowly changing fields.
2. **Stage 2: Live price filtering** Runs after WebSocket streaming. Uses fast-changing price and live-state freshness.
### 5.2 Stable vs slow vs fast features
Feature classes:
1. Stable features: - `domain` - `category` - `market_type` - outcome labels - token ids - source metadata
2. Slowly changing features: - `remaining_hours`
3. Fast-changing features: - `best_bid` - `best_ask` - `last_trade_price` - live midpoint - spread
Rules for usage:
1. Stable features may be used in Stage 1.2. Slowly changing features may be used in Stage 1.3. Fast-changing features must not be used as a permanent reject in Stage 1.
### 5.3 No warm candidate pool
This PRD explicitly **does not** use a warm candidate pool.
Reason:
1. Some Gamma-derived features also drift over time.2. The priority is direct processing, not long-lived in-memory retention.3. The system should process markets as close as possible to their fetch time.
Therefore the preferred workflow is:
1. Start the runtime loop when the process starts.2. Fetch the next event page.3. Expand markets.4. Structurally filter.5. Batch immediately.6. Stream immediately.7. Infer immediately.8. For each selected order, validate and submit immediately.
If a market fails Stage 2 live-price filtering in the current pass, it is simply not submitted in that pass. It may be seen again later through a later Gamma fetch or later submit window.
### 5.4 Minimal critical-path column set
The low-latency path must minimize columns as aggressively as it minimizes steps.
Design rule:
1. If a field is not required for Stage 1 structural filtering, Stage 2 live filtering, market-level inference, submit-time execution gating, or append-only fact logging, it must not be carried on the critical path.2. If an upstream source returns a wider payload than needed, the ingestion layer may receive that payload but must discard unneeded fields immediately after extracting the required subset.3. The execution path must not construct wide intermediate rows merely because the offline research or replay pipeline once used them.4. The execution path must prefer narrow typed records over wide replay-oriented frames.
### 5.5 Offline feature-contract alignment
The online inference path must align to the offline model feature contract.
Required behavior:
1. The authoritative model-input schema is the serialized model payload feature list together with its numeric and categorical feature lists.2. Fields excluded from offline training input, including fields equivalent to offline `DROP_COLS`, must not be constructed in the latency-critical execution path unless they are separately required for Stage 1, Stage 2, submit-time execution gating, or append-only fact logging.3. The online path must not build replay-only, audit-only, or training-only columns before submission just because downstream tooling can consume them.4. If a field is useful only for offline analysis, calibration review, dashboards, or diagnostics, it belongs in deferred reconstruction, not in pre-submit processing.
## 6. Target Workflow Overview
The target workflow is:
1. Start the process and initialize the runtime immediately.2. Load config, rules, runtime, and model payload once.3. Begin fetching Gamma event pages immediately after runtime initialization.4. Expand markets from the current event page into a narrow market record.5. Immediately discard source fields that are not required for Stage 1, Stage 2, inference, submit-time execution, or append-only fact logs.6. Apply structural filtering.7. Apply Stage 1 structural coarse rule filtering.8. Assemble direct micro-batches from the filtered markets.9. Start processing a batch immediately, even if it has fewer than 20 markets.10. For each batch, stream up to 20 reference tokens.11. Build only the minimal in-memory inference inputs required by the preloaded model payload.12. Run Stage 2 live price filtering and model inference.13. Select direction and map to target token.14. For each selected order, fetch a final execution quote for the selected token.15. For that same selected order, immediately run the submit-time execution gate.16. For that same selected order, immediately submit or reject the order.17. If position capacity is currently full, keep the runtime active and wait for an existing market to resolve or be exited before attempting a new order.18. Only after all submission attempts derived from the current fetched page are complete, fetch the next Gamma event page.19. Defer wide artifacts, replay tables, diagnostics, and noncritical I/O until after submission.20. Use independent reconciliation jobs for TTL handling, order reconciliation, exits, and summaries.
## 7. Batch and Token Policy
### 7.1 Reference token policy
Reference token policy must be fixed and deterministic:
1. Default reference token is `token_0_id`.2. Reference outcome label is `outcome_0_label`.3. Snapshot `price` is interpreted as the reference token price.
### 7.2 Streaming policy
1. One market contributes one reference token to streaming.2. One streaming request can contain at most 20 tokens.3. A micro-batch may contain up to 20 markets.4. A micro-batch may start with fewer than 20 markets if that is what is currently available and immediate processing is preferred over waiting.5. Streaming must not subscribe to both token sides.
### 7.3 Inference policy
Inference remains market-level:
1. One market produces one inference row.2. `direction_model > 0` maps to `token_0_id`.3. `direction_model <= 0` maps to `token_1_id`.
There is no dual-token model inference in this PRD.
### 7.4 Submission semantics
Submission is not batch-atomic.
Required behavior:
1. A batch is streamed and inferred together.2. Selection is produced at the batch level.3. Final execution quote lookup is performed **per selected order**.4. For each selected order, `final quote -> execution gate -> submit` is a contiguous sequence.5. Orders are submitted **one by one**, not as a single atomic batch call.6. The system must not first collect final quotes or gate results for all selected orders and only then begin submission.
This preserves correct risk-state updates, nonce handling, and final quote validation.
## 8. Two-Stage Rule Filtering Design
## 8.1 Stage 1: Structural coarse filter
Purpose:
Reduce the market set before WebSocket usage while avoiding irreversible mistakes caused by fast price movement.
Allowed inputs:
1. `domain`2. `category`3. `market_type`4. `remaining_hours`5. stable market metadata6. open/pending state7. binary / expiry / supported-family structural checks
Disallowed hard gates:
1. Gamma `best_bid`2. Gamma `best_ask`3. Gamma `last_trade_price`4. any fast-price-derived exact rule match requirement5. any field carried only for offline training, replay, or dashboard generation
Structural coarse filter output states:
1. `STRUCTURAL_REJECT` Market is incompatible with the strategy and should not proceed further in the current pass.
2. `DIRECT_CANDIDATE` Market matches rule families on stable dimensions and should proceed to the next available batch in the current pass.
3. `STATE_REJECT` Market is excluded because it is already open or pending.
### 8.1.1 Structural reject conditions
The market is a `STRUCTURAL_REJECT` if any of the following are true:
1. Not binary.2. Missing end time.3. Already expired.4. Outside configured trading horizon.5. Category/domain/market_type do not match any rule family.6. Excluded by static business filters such as unsupported market family.
### 8.1.2 Remaining-hours usage in coarse filter
`remaining_hours` is allowed in Stage 1 because it changes slowly. However, Stage 1 should not be over-tight.
Recommended behavior:
1. Use current `remaining_hours`.2. Allow a small coarse slack near horizon edges when appropriate.3. Do not reject a market in Stage 1 only because a fast-moving price would currently place it outside a narrow live rule band.
Recommended config:
- `coarse_horizon_slack_hours`: small positive tolerance, for example `0.10` to `0.25` hours depending on strategy tolerance.
### 8.1.3 Key Stage 1 rule
If a market matches a rule family on stable fields and slow fields, but may or may not currently match on price, the market must still proceed as a `DIRECT_CANDIDATE` into Stage 2 for the current pass.
It must not be permanently discarded in Stage 1 because of missing fast-price agreement.
## 8.2 Stage 2: Live price filter
Purpose:
Use fresh streamed prices to determine whether a direct candidate is currently tradable.
Allowed inputs:
1. live token state from WebSocket2. token-state freshness3. live midpoint4. live rule price bands5. invalid-price checks
Stage 2 output states:
1. `LIVE_ELIGIBLE` Can proceed to inference and selection.
2. `LIVE_PRICE_MISS` Structurally valid but currently outside live rule price range.
3. `LIVE_STATE_MISSING` No usable live state.
4. `LIVE_STATE_STALE` Live state exists but is too old.
5. `INVALID_PRICE` Price invalid for rule engine domain.
### 8.2.1 Price-miss handling policy
Markets that fail with `LIVE_PRICE_MISS` are not submitted in the current pass.
Required behavior:
1. Mark the market as a live-price miss for the current pass.2. Do not keep it in a long-lived warm pool.3. Allow it to be seen again only through a later Gamma fetch or a later submit window.
This keeps the pipeline direct and low-latency while still preventing Stage 1 from permanently rejecting markets on stale price.
### 8.2.2 Stage 2 decision rule
Stage 2 is a **rule filter**, not an execution-quality filter.
Required behavior:
1. Confirm that live state exists.2. Confirm that live state is fresh enough.3. Confirm that live `mid_price` is valid.4. Confirm that live `mid_price` falls within the rule price band.
Stage 2 must **not** use spread as a rule-match criterion.
Reason:
1. Rule filtering answers: does this market currently satisfy strategy conditions?2. Spread answers: is execution quality acceptable right now?3. Those are different concerns and should live in different stages.
## 9. Candidate Lifecycle State Machine
Each market in the active submit pass must be treated as a short-lived pass candidate.
States:
1. `NEW_PAGE_MARKET`2. `STRUCTURAL_REJECT`3. `STATE_REJECT`4. `DIRECT_CANDIDATE`5. `BATCH_ASSIGNED`6. `LIVE_STATE_MISSING`7. `LIVE_STATE_STALE`8. `LIVE_PRICE_MISS`9. `INFERRED`10. `SELECTED_FOR_SUBMISSION`11. `SUBMISSION_REJECTED`12. `SUBMITTED`13. `PASS_COMPLETE`
State transitions:
1. `NEW_PAGE_MARKET -> STRUCTURAL_REJECT`2. `NEW_PAGE_MARKET -> STATE_REJECT`3. `NEW_PAGE_MARKET -> DIRECT_CANDIDATE`4. `DIRECT_CANDIDATE -> BATCH_ASSIGNED`5. `BATCH_ASSIGNED -> LIVE_PRICE_MISS`6. `BATCH_ASSIGNED -> LIVE_STATE_MISSING`7. `BATCH_ASSIGNED -> LIVE_STATE_STALE`8. `BATCH_ASSIGNED -> INFERRED`9. `INFERRED -> SELECTED_FOR_SUBMISSION`10. `SELECTED_FOR_SUBMISSION -> SUBMISSION_REJECTED`11. `SELECTED_FOR_SUBMISSION -> SUBMITTED`12. any nonterminal active state -> `PASS_COMPLETE` when the current direct-processing pass ends
## 10. Direct Batch Assembly Requirements
The system must assemble batches directly from the markets that pass Stage 1 in the current pass.
Each direct candidate record must contain at least:
1. `market_id`2. `token_0_id`3. `token_1_id`4. `selected_reference_token_id`5. `category`6. `domain`7. `market_type`8. `remaining_hours`9. `first_seen_at_utc`10. `coarse_filter_reason`
The direct candidate record must not be expanded with offline-analysis-only columns.It must not carry wide text, replay, historical-path, calibration, or dashboard fields unless a later noncritical job explicitly reconstructs them.
Direct batch behavior:
1. Markets enter the batch assembler immediately after structural coarse filtering.2. The assembler should emit a batch as soon as one of the following is true: - batch size reaches 20 markets, - current event page expansion is complete and there are pending candidates, - the runtime policy prefers immediate processing over waiting.3. A batch is therefore allowed to contain fewer than 20 markets.4. Once a batch is emitted, its markets are processed immediately and are not retained in a long-lived warm pool.5. The next Gamma page fetch must not begin until all submission attempts derived from the current page have completed.
## 11. Execution Quote and Submit Gate
After batch inference selects a market for submission, the system must fetch a fresh execution quote for the **selected token**.
The quote lookup, execution gate, and order submission must be executed immediately for that selected order as one continuous submit path.The system must not wait for all selected orders in the batch to finish quote lookup or gate evaluation before beginning submission.
The submit path may enrich the selected order with selected-token quote fields required for execution, but it must not rebuild the broader snapshot or replay schema.
### 11.1 Quote source priority
Final execution quote source priority:
1. **CLOB order book / midpoint** for the selected token2. token-state fallback if CLOB quote is unavailable
Gamma must not be used as the final execution quote source.
Reason:
1. Gamma is suitable for discovery and metadata, not final execution truth.2. WebSocket stream is batch-level reference-token state, not necessarily final selected-token state.3. The selected token may differ from the reference token.4. CLOB order book is the closest source to actual executable price.
### 11.2 Execution gate inputs
The submit-time execution gate may use:
1. selected-token `best_bid`2. selected-token `best_ask`3. selected-token `mid`4. selected-token `tick_size`5. selected-token `spread`
The execution gate must not depend on `depth_usdc` in the low-latency design.
### 11.3 Execution gate checks
Required submit-time checks:
1. final execution quote exists2. final `mid` is valid3. final quote can build a valid passive limit signal4. final quote does not violate configured price-deviation limits5. final quote does not violate configured spread limits6. decision timing is valid7. risk checks pass
Spread belongs here as an execution-quality guard, not in Stage 2 rule filtering.
## 12. Runtime Start and Capacity Design
The workflow starts when the process starts. It does not wait for the top of the hour to begin runtime initialization or Gamma processing.
Suggested behavior:
1. Runtime initialization starts immediately when the service starts.2. After runtime initialization completes, Gamma event pages are fetched in direct-processing order.3. The system processes one fetched page at a time from expansion through submission completion.4. The next Gamma event page is fetched only after all submission attempts from the current page are complete.5. If position capacity is already full, the runtime remains active and waits until an existing market resolves or is exited before attempting to submit a new order.6. Per-order decision timing checks remain mandatory before order placement, but they are evaluated inside the per-order submit path rather than as a global startup barrier.
## 13. Scheduling Design
This PRD selects a **continuous runtime** scheduling model.
### 12.1 Services
1. `fortune-bot-runtime.service` - Starts on process launch. - Loads config, rules, model payload, and runtime once. - Keeps the runtime active continuously.
2. `fortune-bot-submit-loop.service` - Executes Gamma event paging, structural coarse filtering, direct batch assembly, live filtering, inference, capacity-aware submission, and sequential page progression.
3. `fortune-bot-reconcile.service` - Handles TTL cleanup, reconcile, exit lifecycle, and state rebuild.
4. `fortune-bot-summary.service` - Rebuilds summaries and dashboards.
5. `fortune-bot-refresh-universe.service` - Optional cache and fallback refresh only.
6. `fortune-bot-label-analysis.service` - Daily analytics only.
### 12.2 Timers
Recommended timers:
1. `fortune-bot-runtime.service` - Start on boot or under the process supervisor.
2. `reconcile.timer` - `OnCalendar=*-*-* *:07,17,37:00`
3. `summary.timer` - `OnCalendar=*-*-* *:12,42:00`
4. `refresh-universe.timer` - Run hourly or every two hours as cache refresh, not as a trading prerequisite.
5. `label-analysis.timer` - Daily, off the critical path.
## 14. I/O Policy
### 13.1 Critical-path writes allowed
These writes may remain synchronous because they are append-only facts:
1. `decisions.jsonl`2. `orders.jsonl`3. `rejections.jsonl`4. minimal execution events
These writes must use narrow append-only payloads.They must not embed full source rows, full token-state rows, raw event dumps, historical price paths, or replay-oriented feature tables.
### 13.2 Deferred writes required
These writes must be moved out of the critical submit path:
1. `processed_markets.csv`2. `raw_snapshot_inputs.jsonl`3. `normalized_snapshots.csv`4. `feature_inputs.csv`5. `rule_hits.csv`6. `model_outputs.csv`7. `selection_decisions.csv`8. `submission_attempts.csv`9. `orders_submitted.jsonl`10. scoring manifests11. submit manifests12. dashboard rebuild artifacts13. any wide diagnostic frame whose columns are not directly required for order submission14. any row that exists only because offline training once consumed it and not because live execution requires it
### 13.3 Design rule
Submission must happen before the system spends time writing large replay or audit files.
Corollary:
1. `raw_snapshot_inputs.jsonl` style payloads must be disabled on the critical path.2. `processed_markets`, `normalized_snapshots`, `feature_inputs`, `rule_hits`, and `model_outputs` style tables must be reconstructed asynchronously only if explicitly needed.3. The absence of those deferred tables must not block live submission.
## 15. Model and Runtime Loading Requirements
1. The rule runtime must be loaded exactly once per runtime process.2. The model payload must be loaded exactly once per runtime process.3. The rules frame must be loaded exactly once per runtime process.4. No batch inference path may call `joblib.load()` directly.5. The loaded model payload feature contract must define the exact inference input schema used online.6. The online path must not construct columns that are absent from the payload feature contract unless they are separately required for Stage 1, Stage 2, submit-time execution, or append-only fact logs.
## 16. Gamma Event Paging Requirements
1. Gamma fetching must be event-page-based.2. Use `offset` and `limit`.3. Suggested default page size: 50 events.4. One event may contain multiple markets.5. The system must be able to start trading from the current page before later pages are fetched.6. The next page fetch must not begin until all submission attempts derived from the current page are complete.7. Fetching must prioritize earliest end-date ordering where available.8. Event expansion must immediately project each market into the minimal execution schema instead of carrying forward the full source payload.9. Fields that are later dropped by offline model training or are otherwise absent from the online payload feature contract must be discarded during expansion rather than preserved into downstream execution tables.
## 17. Submission Requirements
For each selected market:
1. Determine `selected_token_id` from `direction_model`.2. Obtain a fresh live quote for the selected token.3. Build a submission signal.4. Validate submit-time execution quality.5. Validate decision timing.6. Validate full risk checks.7. Submit passive limit order.
These steps must run as one continuous per-order path. The system must not first evaluate steps 2 through 6 for all selected orders and only then begin step 7.
Submission must remain the first-class priority of the system.
Notes:
1. The selected-token live quote is a **per-order** final quote, not a second batch WebSocket pass.2. The preferred quote source is CLOB order book / midpoint.3. Submission is sequential per selected order, even though inference is batch-level.4. Sequential per-order submission does not permit a batch-wide gate barrier ahead of submission.5. Submission-time logging must remain narrow and must not trigger construction of offline feature or replay tables.
## 18. New Module Responsibilities
### 17.1 Prewarm runtime
New module suggestion:
- `execution_engine/online/pipeline/prewarm.py`
Responsibilities:
1. Build `PegConfig`.2. Load rule runtime.3. Load rules frame.4. Load model payload.5. Return a reusable runtime container.
### 17.2 Gamma event page source
New module suggestion:
- `execution_engine/online/universe/page_source.py`
Responsibilities:
1. Fetch Gamma event pages incrementally.2. Expand markets from each event page.3. Project markets into the minimal execution schema and discard nonrequired source fields immediately.4. Yield market candidates without forcing full refresh.
### 17.3 Direct batch assembler
New module suggestion:
- `execution_engine/online/pipeline/candidate_queue.py`
Responsibilities:
1. Accept direct candidates from the current fetch pass.2. Assemble up-to-20-market batches.3. Emit underfilled batches when immediate processing is preferred.4. Avoid long-lived warm-pool retention semantics.
### 17.4 Submit window orchestrator
New module suggestion:
- `execution_engine/online/pipeline/submit_window.py`
Responsibilities:
1. Drive event-page fetch.2. Apply Stage 1 coarse filtering.3. Feed the direct batch assembler.4. Form up-to-20-market batches, including underfilled batches.5. Build only the model payload's required inference inputs.6. Run stream, live filter, inference, and submission.7. Hand results to deferred writer.
### 17.5 Deferred writer
New module suggestion:
- `execution_engine/online/reporting/deferred_writer.py`
Responsibilities:
1. Persist noncritical artifacts after submission.2. Retry or fail independently of order submission.3. Reconstruct wide replay or analytics tables only when explicitly enabled.
## 19. File-Level Implementation Guidance
### 18.1 Files to keep and refactor
1. `execution_engine/online/universe/refresh.py` - Keep `_build_binary_market_row()` and structural logic. - Extract event-page-level structural filter helpers. - Add a minimal execution-row projection that discards source columns not required downstream. - Retain full refresh path for cache/fallback.
2. `execution_engine/online/pipeline/eligibility.py` - Split into Stage 1 structural coarse filter and Stage 2 live price filter. - Remove any Stage 1 dependency on fast-price exact-match rule coverage.
3. `execution_engine/online/streaming/manager.py` - Add a path for reference-token-only streaming. - Support memory-first return values.
4. `execution_engine/online/scoring/snapshot_builder.py` - Support in-memory token state inputs. - Replace wide snapshot construction with a minimal inference-input builder derived from the model payload feature contract. - Do not build historical-path, replay, or diagnostic columns on the submit critical path.
5. `execution_engine/online/scoring/rule_runtime.py` - Remove batch-local model loading. - Support injected preloaded model payload. - Expose the payload feature contract so online code can request only required columns.
6. `execution_engine/online/scoring/hourly.py` - Add batch scoring entrypoint that can run without immediate file output. - Default to in-memory execution and narrow fact outputs only.
7. `execution_engine/online/execution/submission.py` - Add in-memory selection submission entrypoint.
8. `execution_engine/online/execution/monitor.py` - Keep as noncritical reconciliation path.
## 20. Metrics and Observability
The following metrics are required:
1. `gamma_event_page_fetch_latency_ms`2. `expanded_market_count`3. `structural_reject_count`4. `direct_candidate_count`5. `underfilled_batch_count`6. `underfilled_batch_avg_size`7. `stream_latency_ms`8. `token_state_age_sec`9. `live_price_miss_count`10. `inference_latency_ms`11. `selection_to_submit_latency_ms`12. `gamma_to_submit_latency_ms`13. `submit_success_count`14. `submit_rejection_count`15. `deferred_io_latency_ms`16. `submit_quote_lookup_latency_ms`17. `spread_gate_reject_count`18. `critical_path_candidate_column_count`19. `critical_path_snapshot_column_count`20. `deferred_artifact_reconstruction_count`
North-star metric:
- `gamma_to_submit_latency_ms`
Supporting minimization metrics:
- `critical_path_candidate_column_count`- `critical_path_snapshot_column_count`
## 21. Acceptance Criteria
### 20.1 Functional
1. The system preserves market-level directional inference.2. Each market streams one reference token only.3. Each micro-batch streams at most 20 tokens.4. Structural coarse filter does not use fast price as a permanent reject criterion.5. The system does not maintain a long-lived warm candidate pool.6. Underfilled batches are allowed and can be processed immediately.7. Stage 2 uses live-state freshness and valid `mid_price` in rule-band checks, not spread.8. Submission still validates the selected token using a fresh per-order live quote.9. Reconciliation remains capable of rebuilding state from fact logs.10. The workflow starts when the runtime process starts rather than waiting for a top-of-hour submit window.11. The next Gamma page is not fetched until submission handling for the current page is complete.12. The submit path executes `final quote -> execution gate -> submit` per selected order without a batch-wide submission barrier.13. Stage 1 does not use fast-price exact-match rule coverage as a permanent reject.14. Fields excluded from the offline model input contract are not constructed on the live submit critical path unless separately required for filtering, submission, or append-only facts.15. Wide replay and analysis tables are deferred and are not required for successful order submission.
### 20.2 Performance
1. Model payload loads once per runtime process.2. Submission occurs before noncritical artifact writing.3. The system can begin trading before all Gamma event pages are fetched.4. The number of synchronous pre-submit file writes is materially lower than in the current design.5. The low-latency path does not compute or depend on `depth_usdc`.6. The critical-path column count is materially lower than in the current design.7. The online path does not construct offline `DROP_COLS`-equivalent fields unless separately required outside model inference.
### 20.3 Operational
1. Continuous runtime scheduling works with independent reconcile and summary timers.2. Reconcile runs independently of the critical path.3. Summary rebuild and label analysis are not allowed to block order submission.4. Deferred replay reconstruction can be disabled without impacting live submission correctness.
## 22. Rollout Plan
### Phase A
Introduce prewarm runtime and preloaded model payload.
### Phase B
Introduce Gamma event-page fetch and structural coarse filtering.
### Phase C
Introduce direct batch assembly and up-to-20-market batching.
### Phase D
Introduce reference-token-only streaming and live price filtering.
### Phase E
Introduce in-memory submission path and deferred writer.
### Phase F
Switch systemd from legacy hourly-cycle-first scheduling to prewarm + submit-window scheduling.
## 23. Final Design Summary
The redesigned system should behave as follows:
1. Start the runtime when the process starts and load the model and rule runtime once.2. Fetch Gamma event data page by page instead of waiting for full universe construction.3. Expand markets from the current event page into a minimal execution schema and discard nonrequired source fields immediately.4. Use stable and slow features for Stage 1 structural coarse filtering.5. Do not let Stage 1 permanently reject markets because of fast price.6. Use live streamed prices for Stage 2 freshness and `mid_price` rule-band filtering.7. Stream only one reference token per market and at most 20 tokens per batch.8. Allow underfilled batches to process immediately.9. Build only the minimal execution and inference columns required by the preloaded model payload and submit path.10. Do not construct offline `DROP_COLS`-equivalent fields on the submit critical path unless separately required for filtering, submission, or append-only facts.11. Perform market-level batch inference.12. For each selected order, run `final quote -> execution gate -> submit` immediately and sequentially.13. Fetch the next Gamma page only after submission handling for the current page is complete.14. If capacity is full, keep the runtime active and wait for an existing market to resolve or be exited before opening a new order.15. Use spread only in the submit-time execution gate.16. Write most artifacts later, and reconstruct wide analytics tables only off the critical path.
This is the required target architecture for minimizing Gamma-to-submit latency while preserving current strategy semantics.