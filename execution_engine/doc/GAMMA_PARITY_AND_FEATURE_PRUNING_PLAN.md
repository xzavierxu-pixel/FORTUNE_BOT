# Gamma Parity And Feature Pruning Plan
## 1. Goal
This document is the consolidated overview for the execution-side alignment work.
It now combines the previous plan document, the final decision log, and the one-page implementation policy.
It defines the execution plan for reducing unnecessary inconsistency between `execution_engine` and the offline rule-engine pipeline, with one explicit constraint:
- if a field comes from Gamma and offline uses the same Gamma source, the execution path should keep the same semantics unless there is a hard live-only reason not to.
It also defines a second constraint:
- execution should not compute Gamma-derived features or intermediate fields that are not actually consumed by rules, the model feature contract, or downstream live execution logic.
This is a plan document only. It does not imply that the implementation has already been changed.
Detailed field-by-field alignment rules are tracked in:
- `execution_engine/doc/FIELD_AND_FEATURE_ALIGNMENT_MATRIX.md`- `execution_engine/doc/MIGRATION_BATCH_PLAN.md`
These three documents are now the complete documentation set for this workstream.
## 2. Current Source Inventory
The current execution path uses multiple source types.
### 2.1 Gamma market metadata
Current entrypoints:
- `execution_engine/online/universe/refresh.py`- `execution_engine/online/universe/page_source.py`- `execution_engine/integrations/providers/gamma_provider.py`
Gamma currently supplies the market metadata and many market-structure fields used to build the execution candidate frame, including:
- `question`- `description`- `resolution_source`- `game_id`- `category` and related parsed fields- `volume`, `liquidity`- `best_bid`, `best_ask`, `spread`, `last_trade_price`- `volume24hr`, `volume1wk`- `volume24hr_clob`, `volume1wk_clob`- `order_price_min_tick_size`- `neg_risk`- `rewards_min_size`, `rewards_max_spread`- `line`- `one_hour_price_change`, `one_day_price_change`, `one_week_price_change`- `liquidity_amm`, `liquidity_clob`- `group_item_title`, `market_maker_address`- token labels and token ids- start/end/update timestamps
For this source family, parity with offline should be the default target.
### 2.2 WebSocket token state
Current entrypoints:
- `execution_engine/online/streaming/`- `execution_engine/online/pipeline/eligibility.py`- `execution_engine/online/scoring/live.py`
This source is live-only and is used for:
- real-time best bid / ask- current mid price- token-state freshness- live price eligibility
This source cannot be made identical to offline, but naming and field semantics should stay as close as possible.
### 2.3 CLOB price history and order book
Current entrypoints:
- `execution_engine/online/scoring/price_history.py`- `execution_engine/online/execution/live_quote.py`- `execution_engine/integrations/trading/clob_client.py`
This source is also live-only and is used for:
- historical price feature reconstruction for live scoring- final quote validation before submission- order submission
### 2.4 Offline rule and model artifacts
Current entrypoints:
- `execution_engine/runtime/config.py`- `execution_engine/online/scoring/rule_runtime.py`
This is intentionally the default serving dependency and should remain so.
## 3. Current Waste And Parity Problems
### 3.1 Gamma-derived fields are processed too broadly before actual consumption is known
Current execution flow:
1. fetch Gamma market rows2. build a broad execution source frame3. build market annotations4. build live snapshots5. build market feature cache through offline feature helpers6. later align to the model feature contract
The waste in the current shape is:
- Gamma-derived raw fields are projected broadly up front- `build_market_feature_cache()` computes the full market feature set- many derived fields are later dropped by training defaults, not present in the deployed feature contract, or not used by downstream live execution
That means execution currently does more work than necessary for Gamma-backed inputs.
### 3.2 Silent contract fallback hides parity drift
Current runtime behavior fills missing model features with defaults.
Implication:
- if execution stops building a feature that the model contract expects, inference still runs- parity can drift silently instead of failing loudly
This is especially risky when pruning fields and derived features.
### 3.3 Annotation is partially shared but not fully canonicalized
Execution already reuses offline annotation logic, which is good.
However:
- execution still constructs its own reduced annotation input frame- execution then performs extra normalization against offline annotations
This is only partial parity, not full canonical parity.
### 3.4 Live snapshot field names are sometimes reused with changed semantics
Some live fields use the same names as offline snapshot fields but are currently populated from different live approximations.
That creates two problems:
- semantic drift is harder to see- downstream code cannot reliably assume that the same field name means the same statistic
## 4. Planning Principles
This work should follow four strict rules.
### 4.1 Same source, same semantics
If both offline and execution are using Gamma-origin market metadata, then:
- the normalization layer should be shared- field names should match- typing and default rules should match- the same annotation and market-feature inputs should be used whenever possible
### 4.2 Compute only what is actually consumed
Execution should not build features only to delete them later.
The allowed reasons to compute a field are:
- required for rule matching or rule coverage- required by the deployed model feature contract- required for post-model selection, submission, monitoring, or auditing
If a Gamma-derived field does not satisfy one of those conditions, it should not be processed in the hot path.
### 4.3 Prune at the earliest safe boundary
A field should be removed as early as possible, but not before its last real consumer.
That means:
- do not keep wide raw Gamma frames alive longer than necessary- do not keep a full derived market-feature frame alive after model input assembly- keep only minimal keys and downstream-required columns after feature assembly
### 4.4 Live-only data may differ, but names must stay honest
If a live-only approximation cannot exactly match offline semantics, then one of two things should happen:
- compute a closer equivalent with the same meaning- or rename the field so that the difference is explicit
The same field name should not hide a materially different definition.
## 5. Final Decisions And Implementation Boundary
This section replaces the previous standalone decision log.
### 5.1 Explicit non-goals for this workstream
- execution should continue to consume offline rule and model artifacts by default- selection ordering does not need to match the offline execution-parity backtest exactly- the shared rule and growth core is not the primary target; the main target is input construction, semantic alignment, and feature pruning
### 5.2 Canonical semantic rules
- Gamma-origin overlapping fields must be aligned before they are pruned- `domain`, `category`, and `market_type` must remain canonical offline-annotation outputs- if a live field cannot match the offline meaning, it must either be renamed as live-only or removed from the model hot path- the same field name must not hide an execution-only approximation
### 5.3 Default field-retention policy
Must keep:
- rule-path keys and filters such as `market_id`, `end_time_utc`, `remaining_hours`, `accepting_orders`, `uma_resolution_statuses`, `domain`, `category`, `market_type`, `selected_reference_token_id`- selection / submission / lifecycle fields such as `price`, `q_pred`, `direction_model`, `edge_final`, `f_exec`, `selected_token_id`, `selected_outcome_label`, `stake_usdc`, `first_seen_at_utc`, `run_id`, `batch_id`, `rule_group_key`, `rule_leaf_id`, `position_side`, `settlement_key`, `cluster_key`- canonical snapshot fields such as `price`, `horizon_hours`, `snapshot_time`, `closedTime`
Conditional keep only when there is a real consumer:
- annotation extensions such as `domain_parsed`, `sub_domain`, `source_url`, `category_raw`, `category_parsed`, `category_override_flag`, `outcome_pattern`- extended market-structure fields such as `game_id`, `group_item_title`, `market_maker_address`, `neg_risk`, `rewards_*`, `line`, `one_*_price_change`, `liquidity_*`- historical-price features- quote-window statistics- snapshot extensions such as `snapshot_date`, `scheduled_end`, `primary_outcome`, `secondary_outcome`, `source_host`
Default stop-compute unless a real contract or downstream consumer proves otherwise:
- `text_embed_*`- text length, keyword, category-word, and sentiment feature families- duration feature families- second-order interaction families with no real consumer- quote-window fields that cannot be constructed with offline-equivalent semantics
### 5.4 Special semantic decisions
- `question` and `description` are not default long-lived fields; they should survive only as long as annotation or real text-feature consumers require them- `remaining_hours` and `horizon_hours` should not remain duplicated across later stages; one canonical horizon field should survive into the model path- `source_host` cannot continue to exist as a weak alias for `domain`; either restore the canonical meaning or rename it as a live-only field- if execution keeps `p_1h` through `closing_drift`, they must remain same-name same-meaning relative to offline
### 5.5 Structural end-state
The target end-state remains a narrow staged pipeline:
1. `gamma_context_table`2. `model_input_table`3. `execution_decision_table`
Missing model-contract columns should be surfaced explicitly rather than silently masked by default fills.
### 5.6 Normative implementation specs
The three main problem classes in this workstream are considered document-complete only when implementation follows three normative specs:
1. the canonical annotation input and merge spec in `FIELD_AND_FEATURE_ALIGNMENT_MATRIX.md`2. the canonical live snapshot construction spec in `FIELD_AND_FEATURE_ALIGNMENT_MATRIX.md`3. the parity acceptance gate in `MIGRATION_BATCH_PLAN.md`
These sections are not descriptive background. They are intended to be the implementation contract for agent-driven changes.
## 6. Execution Plan
## Phase 1: Build A Real Consumption Inventory
Objective:
- identify the exact minimal field set that execution actually needs.
Tasks:
1. extract the deployed `feature_contract.feature_columns` from the actual serving bundle used by execution2. classify live-path fields into four buckets: - rule-only - model-only - downstream-only - unused3. trace which raw Gamma columns are prerequisites for each actually-used derived feature4. trace which annotation fields are actually required by: - structural coarse filter - rule matching - model feature preprocessing5. trace which snapshot fields are actually required by: - `prepare_feature_inputs()` - selection - submission - monitoring
Deliverable:
- a field-level inventory table with `source -> derived field -> actual consumer -> can prune?`
Notes:
- this phase is required before changing any feature-building logic- without the real bundle contract, pruning remains partly speculative
## Phase 2: Canonicalize Gamma Projection
Objective:
- make Gamma-origin execution fields match offline semantics at the normalization boundary.
Tasks:
1. define one canonical Gamma market projection shared by offline and execution for overlapping fields2. align naming between execution and offline raw-market feature builders3. align default handling for missing Gamma values4. remove execution-only ad hoc remapping where the offline shape can be reused directly
Expected result:
- execution no longer carries a separate semantic interpretation of the same Gamma market fields
Priority fields:
- `question`- `description`- `resolution_source`- `game_id`- `volume`, `liquidity`- `volume24hr`, `volume1wk`- `volume24hr_clob`, `volume1wk_clob`- `best_bid`, `best_ask`, `spread`, `last_trade_price`- `line`- `one_hour_price_change`, `one_day_price_change`, `one_week_price_change`- `liquidity_amm`, `liquidity_clob`- `group_item_title`, `market_maker_address`- `start_time_utc`, `created_at_utc`, `end_time_utc`
## Phase 3: Canonicalize Annotation Input
Objective:
- make execution use the same annotation input contract as offline when the source fields exist.
Tasks:
1. define the canonical annotation input shape once2. make execution build that same shape from Gamma rows instead of a reduced custom shape3. keep the offline annotation engine as the single source of truth for annotation semantics4. keep execution-specific post-processing only where live-only constraints require it
Expected result:
- annotation behavior is shared by construction, not merely shared at the last classification step
## Phase 4: Introduce Consumption-Driven Market Feature Building
Objective:
- stop computing full Gamma-derived market features when only a subset is actually needed.
Tasks:
1. split market feature generation into composable groups based on prerequisite raw fields2. allow the feature layer to accept an explicit requested-derived-feature set3. compute only the derived market features required by: - actual model contract columns - rule/runtime post-processing columns4. keep offline training on the current broad path unless and until its own pipeline is also migrated to a requested-feature build mode
Required design constraint:
- the execution pruning path must not change offline training semantics by default
Expected result:
- execution no longer builds text, duration, liquidity, or interaction-derived market features that are not actually consumed
## Phase 5: Split Snapshot Fields Into Semantic Layers
Objective:
- avoid carrying one wide live snapshot table across the entire live path.
Tasks:
1. separate the execution path into three logical tables: - annotation and raw Gamma context table - model input table - downstream execution table2. ensure each stage keeps only the columns needed by the next stage3. drop broad Gamma raw context after market-feature and annotation consumption is complete4. drop unused model-prep fields after feature alignment is complete
Expected result:
- lower memory footprint- clearer ownership of fields- easier parity auditing
## Phase 6: Tighten Contract Safety
Objective:
- make pruning safe by exposing missing required features explicitly.
Tasks:
1. add a parity/audit mode that reports missing feature-contract columns before inference2. distinguish between: - intentionally defaultable fields - required-but-missing fields that indicate a pipeline bug3. add explicit logging or test assertions for missing contract inputs during execution validation
Expected result:
- feature pruning cannot silently break model semantics
## Phase 7: Add Parity Tests
Objective:
- lock in the new Gamma parity and consumption-driven pruning behavior.
Tasks:
1. add tests ensuring shared Gamma fields map identically into the offline feature helpers2. add tests ensuring execution annotation input matches the canonical offline annotation input shape3. add tests ensuring the execution path can build the exact required model input table from a minimal Gamma row4. add tests ensuring unused Gamma-only fields can be removed without affecting rule matching, model inference, or submission
Expected result:
- future live-path edits cannot quietly reintroduce wide unused Gamma processing
## 7. Scope Classification
### 6.1 Must be aligned
- Gamma-backed market metadata normalization- annotation input contract- market feature prerequisites derived from Gamma fields- rule matching inputs- model feature input semantics for Gamma-derived fields
### 6.2 Can only be approximately aligned
- token freshness fields- live price-window statistics derived from WebSocket state- CLOB price history feature reconstruction
For these fields, the goal is semantic honesty, not artificial identity.
### 6.3 Intentionally live-only
- live eligibility rejection reasons- quote freshness and submission-time checks- order-book validation- capacity waiting- submission, reconcile, monitoring, and exit lifecycle
These should remain separate from offline semantics.
## 8. Recommended Implementation Order
The recommended order is:
1. inventory actual consumption from the deployed feature contract2. canonicalize Gamma normalization3. canonicalize annotation input4. introduce consumption-driven market feature building5. split snapshot data into stage-specific narrow tables6. tighten contract safety checks7. add parity and pruning tests
Reason:
- pruning before a real consumption inventory is unsafe- Gamma normalization and annotation parity should be fixed before optimizing the feature layer- contract safety must be added before aggressive pruning is enabled
## 9. Acceptance Criteria
This plan is complete only when all of the following are true:
1. execution and offline use the same semantics for overlapping Gamma-origin fields2. execution annotation input is canonicalized against the offline annotation path3. execution computes only those Gamma-derived features that are actually consumed by rules, model inputs, or downstream execution4. live snapshot tables are narrowed after each stage instead of remaining globally wide5. missing required model features are surfaced explicitly instead of only being silently default-filled6. parity tests cover Gamma normalization, annotation input parity, and minimal required feature construction
## 10. Non-Goals
This plan does not propose changing:
- default use of offline rule/model artifacts in execution- intentional live selection-order differences- live-only submission and monitoring logic- offline training objectives or model-family choices