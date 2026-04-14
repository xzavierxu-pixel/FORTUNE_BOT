# GroupKey Open Requirements Implementation Plan

## 1. Goal

This document translates `groupkey_open_requirements_and_current_issues.md` into an implementation-first plan tied to the current repository state.

The plan is based on the following verified code and artifact entry points:

- history artifact path contract:
  - `polymarket_rule_engine/rule_baseline/datasets/artifacts.py`
- history feature build and parquet persistence utilities:
  - `polymarket_rule_engine/rule_baseline/training/history_features.py`
- rule-generation main path that already calls history build and serving asset generation:
  - `polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py`
- snapshot training serving-asset loading and audit generation:
  - `polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py`
- inventory generation:
  - `polymarket_rule_engine/rule_baseline/training/build_groupkey_feature_inventory.py`
- migration and consistency markdown generation:
  - `polymarket_rule_engine/rule_baseline/training/groupkey_reports.py`

## 2. Verified Current Baseline

### 2.1 Code-level baseline

- `ArtifactPaths` already defines all expected `history_features_*.parquet` targets.
- `history_features.py` already supports:
  - multi-level history aggregation
  - parquet persistence
  - parquet reload validation
- `train_rules_naive_output_rule.py` already does the following in the default main flow:
  - builds rules
  - builds history feature frames
  - writes history parquet artifacts
  - reloads those artifacts
  - builds `group_serving_features.parquet`
  - builds `fine_serving_features.parquet`
  - writes `serving_feature_defaults.json`
  - writes `rule_funnel_summary.json`
- `train_snapshot_model.py` already consumes the serving assets through `load_serving_feature_bundle(...)` and writes snapshot training audit outputs.
- `build_groupkey_feature_inventory.py` already parses the 500-feature blueprint and merges it with current asset columns, but its status model is still too coarse for release governance.

### 2.2 Artifact baseline on disk

Current files under `polymarket_rule_engine/data/offline/edge/`:

- `trading_rules.csv`
- `group_serving_features.parquet`
- `fine_serving_features.parquet`
- `serving_feature_defaults.json`

Currently missing from disk:

- `history_features_global.parquet`
- `history_features_domain.parquet`
- `history_features_category.parquet`
- `history_features_market_type.parquet`
- `history_features_domain_x_category.parquet`
- `history_features_domain_x_market_type.parquet`
- `history_features_category_x_market_type.parquet`
- `history_features_full_group.parquet`

### 2.3 Inventory baseline

Current `groupkey_feature_inventory.csv` summary:

- total rows: `1258`
- `implemented`: `880`
- `pending`: `378`

This means the blueprint audit is not blocked by missing parsing; it is blocked by missing final disposition governance.

## 3. Implementation Principles

- Reuse the existing rule-generation and history-generation code paths instead of creating a second parallel builder unless a clean CLI wrapper is needed.
- Treat artifact materialization and audit refresh as first-class outputs, not side effects.
- Separate three concerns explicitly:
  - physical artifact generation
  - schema and audit governance
  - full-data operational strategy
- For every deliverable, require both:
  - code/path integration
  - refreshed audit evidence under `data/offline/audit/` or docs

## 4. Workstreams

### Workstream A. Materialize standalone history artifacts

#### Objective

Make `history_features_*.parquet` reliably appear in `data/offline/edge/` after the supported build flow and prove that downstream code uses the materialized artifacts.

#### Why this is first

- The code already exists.
- The requirement is clearly open at the artifact level.
- This is the highest-leverage gap for reproducibility and auditability.

#### Implementation steps

1. Trace the supported rule-generation invocation path.
2. Confirm whether the normal offline command currently reaches `train_rules_naive_output_rule.py:889-891`.
3. If the default user-facing build entry point bypasses that flow, add one explicit offline command or wrapper that guarantees:
   - rule generation
   - history parquet persistence
   - history parquet reload verification
   - serving asset refresh
4. Add a small artifact inventory writer for the history outputs, including at minimum:
   - file path
   - existence
   - row count
   - column count
   - modified time
5. Fail the build clearly when any expected history parquet file is missing after generation.
6. Add or extend tests around the supported entry point, not only the helper functions.

#### Expected code areas

- `polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py`
- `polymarket_rule_engine/rule_baseline/datasets/artifacts.py`
- possibly a new audit helper under `polymarket_rule_engine/rule_baseline/training/`
- tests adjacent to rule-generation and artifact validation

#### Acceptance criteria

- all eight `history_features_*.parquet` files exist after the standard offline build
- build log and audit outputs make the artifact creation explicit
- missing-file cases fail fast instead of silently passing

### Workstream B. Close the blueprint semantic inventory

#### Objective

Convert the current inventory from a parsed-and-partially-matched table into a final disposition ledger.

#### Gap in current implementation

`build_groupkey_feature_inventory.py` currently only distinguishes:

- already implemented
- alias/duplicate merge
- keep but later

This does not satisfy the required final governance states.

#### Required target states

The inventory generator should produce stable final statuses such as:

- `implemented_exact`
- `implemented_approximate`
- `duplicate_or_merge`
- `intentionally_excluded`
- `pending_implementation`
- `unsupported_now`

#### Implementation steps

1. Refactor the inventory schema so `status` becomes final-state oriented instead of generic `implemented/pending`.
2. Preserve the current exact-match and alias-match logic, but remap them into:
   - `implemented_exact`
   - `implemented_approximate`
3. Add rule-based classification for obviously non-serving or intentionally deferred rows.
4. Add a manual override table checked into the repo for cases that cannot be inferred safely from names alone.
5. Regenerate the CSV and produce a summary markdown or audit JSON with counts by final disposition.
6. Ensure the generator is deterministic and can be rerun after each feature round.

#### Expected code areas

- `polymarket_rule_engine/rule_baseline/training/build_groupkey_feature_inventory.py`
- `polymarket_rule_engine/docs/groupkey_feature_inventory.csv`
- likely a new checked-in override file under `polymarket_rule_engine/docs/` or `.../training/`

#### Acceptance criteria

- no open blueprint row remains in an ambiguous generic `pending` bucket
- each unmatched feature has one explicit disposition and note
- regenerated inventory can be used as the single authoritative blueprint ledger

### Workstream C. Add release-grade audit coverage to rule generation

#### Objective

Bring rule generation to the same audit standard that snapshot training already has.

#### Current baseline

- snapshot training already writes:
  - `snapshot_training_funnel.json`
  - `snapshot_training_funnel.md`
- rule generation currently writes:
  - `rule_funnel_summary.json`

The gap is not zero audit coverage; the gap is incomplete, non-authoritative coverage.

#### Implementation steps

1. Introduce a rule-generation audit payload builder mirroring the snapshot training audit pattern.
2. Add a markdown output for rule generation, not just JSON.
3. Add artifact inventory coverage for:
   - `trading_rules.csv`
   - all history parquet files
   - `group_serving_features.parquet`
   - `fine_serving_features.parquet`
   - `serving_feature_defaults.json`
4. Include counts for:
   - selected rules
   - kept/dropped/insufficient groups
   - group-serving rows
   - fine-serving rows
5. Refresh migration/consistency docs from the same run or clearly separate them as post-build report steps.

#### Expected code areas

- `polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py`
- a new audit helper analogous to `snapshot_training_audit.py`
- `polymarket_rule_engine/rule_baseline/training/groupkey_reports.py`

#### Acceptance criteria

- `offline/audit/` contains rule-generation JSON and markdown that are refreshed automatically
- rule-generation artifact presence and shapes are visible without manual inspection

### Workstream D. Establish formal serving schema governance

#### Objective

Define the generated asset contract as a stable interface instead of implicit column knowledge.

#### Implementation steps

1. Generate or hand-maintain one schema reference document covering:
   - `trading_rules.csv`
   - `group_serving_features.parquet`
   - `fine_serving_features.parquet`
   - `serving_feature_defaults.json`
2. Classify columns into semantic buckets:
   - rule-prior columns
   - group-safe serving columns
   - fine-only columns
   - fallback indicator columns
   - generated interaction columns
3. Tie the schema doc to the inventory terminology so feature names do not drift across documents.
4. Optionally add a machine-readable schema dump in audit output for regression detection.

#### Expected code areas

- docs under `polymarket_rule_engine/docs/`
- possibly `groupkey_reports.py` if schema markdown is partially generated

#### Acceptance criteria

- one markdown document can answer what each serving asset contains and why
- schema changes become diffable and reviewable

### Workstream E. Prove migration completeness

#### Objective

Close the governance gap around the shift from legacy rule-level behavior to group-level keep/drop behavior.

#### Implementation steps

1. Search repository-wide for any remaining legacy rule-level sample-drop logic or old matching assumptions.
2. Enumerate affected training and serving entry points.
3. Write one migration summary with:
   - old behavior
   - current behavior
   - entry points changed
   - rollback assumptions
4. Link this summary from the new audit/report outputs.

#### Expected code areas

- `polymarket_rule_engine/rule_baseline/training/`
- `polymarket_rule_engine/docs/groupkey_migration_validation.md`

#### Acceptance criteria

- there is one current, explicit statement of migration semantics
- hidden legacy dependencies are either removed or documented

### Workstream F. Resolve full-data training operational strategy

#### Objective

Turn the current memory limitation from an implicit machine problem into an explicit supported strategy.

#### Current baseline

- sampled snapshot training works
- full-data snapshot training remains blocked by local memory constraints

#### Implementation options

Option A: document the required higher-memory execution environment.

Option B: implement a low-memory path, for example:

- narrower feature set for full rebuilds
- chunked preprocessing
- lighter AutoGluon profile
- staged artifact generation before model fit

#### Implementation steps

1. Measure the current full-data failure point and record it in docs or audit.
2. Decide whether this repo will support:
   - local full rebuild
   - remote/high-memory full rebuild only
3. If supporting local low-memory mode, add one explicit switch and document tradeoffs.
4. Reflect the chosen strategy in `model_training_summary.json` semantics and docs.

#### Acceptance criteria

- full-data rebuild is either operationally supported or explicitly declared environment-bound
- no engineer has to infer this from failed local runs

## 5. Recommended Execution Order

### Phase 1. Artifact materialization and rule audit

- Workstream A
- Workstream C

Reason:

- these are closest to completion already
- they produce immediate operational evidence
- they remove ambiguity around whether the history feature work is conceptually or physically incomplete

### Phase 2. Blueprint and schema governance

- Workstream B
- Workstream D
- Workstream E

Reason:

- these depend on stable artifact outputs and current asset schemas
- they are mostly governance and documentation work backed by code generation

### Phase 3. Full-data operational resolution

- Workstream F

Reason:

- it is important, but it should be solved against a stable asset and audit contract

## 6. Suggested Concrete Deliverables

- `offline/edge/history_features_*.parquet` generated by the supported build path
- one new rule-generation audit markdown under `polymarket_rule_engine/data/offline/audit/`
- one artifact inventory JSON or markdown under `polymarket_rule_engine/data/offline/audit/`
- updated `groupkey_feature_inventory.csv` with final disposition states
- one schema reference markdown in `polymarket_rule_engine/docs/`
- one migration summary markdown in `polymarket_rule_engine/docs/`
- one explicit full-data training strategy note in docs

## 7. Minimal First Implementation Slice

If this is executed incrementally, the first slice should be:

1. make the standard rule-generation command always materialize and validate all history parquet files
2. add automatic rule-generation audit markdown plus artifact inventory
3. add a failing test for missing history artifacts after build

This first slice is small enough to land safely and large enough to close the highest-priority operational gap.

## 8. Definition Of Done

This plan is complete only when the repo can demonstrate all of the following from standard build outputs and checked-in docs:

- history parquet artifacts are materialized on disk
- rule generation and snapshot training both produce authoritative audits
- blueprint inventory has final dispositions, not generic pending rows
- serving asset schema is documented as a stable contract
- full-data training expectations are explicit and operationally honest
