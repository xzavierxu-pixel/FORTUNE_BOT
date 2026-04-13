# GroupKey Open Requirements And Current Issues

## 1. Purpose

This document consolidates the remaining requirements and current implementation issues by combining:

- `groupkey_serving_and_history_feature_plan.md`
- `phase1.md`
- `phase2.md`
- `phase3.md`
- the current repository state verified from code, generated artifacts, and training runs

The goal of this file is to provide one operational requirements document for the next implementation round.

## 2. Verified Current State

The following are confirmed in the current codebase and artifact set.

### 2.1 Group-level migration status

- training no longer relies on the old coarse `domain/category/market_type` cartesian rule merge
- snapshot-to-rule matching now uses:
  - `domain`
  - `category`
  - `market_type`
  - `price_bin`
  - `rounded_horizon_hours`
- `trading_rules.csv` is still the rule-prior asset
- `group_serving_features.parquet` and `fine_serving_features.parquet` are generated and used by serving lookup
- serving fallback semantics are implemented through:
  - `group_key + price_bin + rounded_horizon_hours` fine lookup
  - `group_key` group fallback
  - manifest-driven fine defaults

### 2.2 Currently existing offline artifacts

These artifacts currently exist under `polymarket_rule_engine/data/offline/`:

- `edge/trading_rules.csv`
- `edge/group_serving_features.parquet`
- `edge/fine_serving_features.parquet`
- `edge/serving_feature_defaults.json`
- `naive_rules/naive_all_leaves_report.csv`
- `audit/rule_funnel_summary.json`
- `audit/snapshot_training_funnel.json`
- `audit/snapshot_training_funnel.md`
- `metadata/model_training_summary.json`
- `models/q_model_bundle_deploy/feature_contract.json`
- `models/q_model_bundle_full/feature_contract.json`

### 2.3 Current verified training-chain behavior

From the latest successful random-sample snapshot training run:

- sample mode:
  - `random_sample_rows=100000`
  - `random_sample_seed=21`
- funnel:
  - `snapshots_loaded=766401`
  - `after_quality_pass=100000`
  - `after_dataset_split=97611`
  - `after_group_selection_keep=91710`
  - `after_rule_bucket_match=91568`
  - `model_feature_frame=91568 x 1026`
  - `model_feature_shape=91568 x 899`
- trained bundle contract now records:
  - `feature_columns=899`
  - `required_critical_columns=12`
  - `required_noncritical_columns=887`

### 2.4 Current verified group selection state

The group selection report currently shows:

- total `group_key` rows in report: `472`
- kept `group_key`: `287`
- dropped `group_key`: `96`
- insufficient-data `group_key`: `89`

This means the current rule generation path is still centered on group-level keep/drop decisions before rule-row retention.

## 3. Requirements Already Partially Or Fully Satisfied

### 3.1 Satisfied or mostly satisfied from the serving/history plan

- `trading_rules.csv` remains the static rule prior asset
- two serving assets exist:
  - `group_serving_features.parquet`
  - `fine_serving_features.parquet`
- shared fine-default manifest exists:
  - `serving_feature_defaults.json`
- fine lookup then group fallback logic exists
- fallback indicator behavior exists
- multiple hierarchical history metric families have already been flattened into group serving features

### 3.2 Satisfied or mostly satisfied from phase2

- old snapshot-to-rule matching explosion was removed
- training now uses exact fine lookup keys for rule attachment
- group-level keep/drop behavior is auditable through rule reports and funnel outputs
- bundle feature contract can now be generated successfully in sampled runs

### 3.3 Satisfied or mostly satisfied from phase3

- normalized drift gap family was added
- tail instability ratio family was added
- expanded rule-gap families were added
- consistency and runtime reporting utilities exist

## 4. Open Requirements

The following items remain unfinished or not fully governed.

### 4.1 History build artifacts are still not physically present

This is the largest gap versus `groupkey_serving_and_history_feature_plan.md`.

Expected artifacts:

- `history_features_global.parquet`
- `history_features_domain.parquet`
- `history_features_category.parquet`
- `history_features_market_type.parquet`
- `history_features_domain_x_category.parquet`
- `history_features_domain_x_market_type.parquet`
- `history_features_category_x_market_type.parquet`
- `history_features_full_group.parquet`

Current issue:

- paths and builder code exist
- these files are still missing from `data/offline/edge/`

Requirement:

- the standalone history builder must actually generate and persist these artifacts
- downstream serving generation must be verified to consume these materialized artifacts rather than silently relying on inline recomputation assumptions

### 4.2 Full phase1 semantic feature audit is not closed

The original phase1 requirement was to audit the 500-feature blueprint and classify what is:

- already in `trading_rules.csv`
- already implemented elsewhere
- redundant
- still missing
- currently unsupported

Current issue:

- there is inventory and partial matching work
- there is not yet a final closed ledger with explicit dispositions for all remaining blueprint rows

Requirement:

- produce a final feature inventory with stable disposition columns such as:
  - `implemented_exact`
  - `implemented_approximate`
  - `duplicate_or_merge`
  - `intentionally_excluded`
  - `pending_implementation`
  - `unsupported_now`

### 4.3 Group-level filtering governance is not release-complete

Phase2 was not only about changing logic. It also required proof that the migration is complete and safe.

Still required:

- explicit repository-wide proof that no hidden training path still depends on the legacy rule-level sample drop behavior
- one migration summary that shows:
  - old rule-level behavior
  - current group-level behavior
  - affected entry points
  - rollback expectations

### 4.4 Serving schema governance is incomplete

The current serving tables are generated and used, but the schema contract is still not documented as a stable interface.

Still required:

- one schema reference for:
  - `trading_rules.csv`
  - `group_serving_features.parquet`
  - `fine_serving_features.parquet`
  - `serving_feature_defaults.json`
- one explicit designation of:
  - rule-prior columns
  - group-safe serving columns
  - fine-only columns
  - fallback indicator columns
  - generated interaction columns

### 4.5 Blueprint-to-serving suitability decision is incomplete

Not every blueprint feature should be landed into serving tables.

Still required:

- explicit marking of which blueprint features are:
  - serving-safe and should be materialized
  - training-only dynamic features
  - redundant with current features
  - intentionally deferred
  - unsupported with current data

### 4.6 Runtime validation is still bounded, not final

Current runtime validation exists, but only on bounded sample runs and local checks.

Still required:

- broader offline coverage validation beyond sampled runs
- real online startup/load validation
- future-growth validation for wider serving assets
- stable runtime SLA documentation for:
  - load latency
  - memory footprint
  - fine hit rate
  - group fallback rate
  - unknown-group fallback rate

### 4.7 Full-data snapshot bundle training is still not operationally resolved

Current state:

- sampled snapshot bundle training now works
- full-data snapshot bundle training still fails in the current environment due to memory constraints

Requirement:

- either provide a production-capable training environment
- or implement an explicit low-memory training strategy with documented tradeoffs

## 5. Current Problems

These are the concrete problems observed in the repository as it exists now.

### 5.1 Missing standalone history parquet outputs

Problem:

- code and artifact paths exist for `history_features_*.parquet`
- files are still absent from the offline artifact directory

Impact:

- the serving/history plan is not actually complete
- auditability is weaker than intended
- it is harder to prove artifact boundaries and reproducibility

### 5.2 Full snapshot training does not complete on current local memory budget

Problem:

- full training uses roughly `701147 x 899` model feature inputs
- AutoGluon can start preprocessing
- model training is skipped for memory safety in the current machine configuration

Impact:

- full offline bundle refresh is not reliably reproducible on this machine
- deployment-grade full-data refresh remains blocked

### 5.3 Audit outputs existed, but were not previously part of the default training contract

Problem:

- before the latest changes, funnel analysis had to be reconstructed manually
- artifact existence and shape could easily be misread from stale files

Current status:

- snapshot audit generation now exists and is automatically wired into `train_snapshot_model.py`

Remaining issue:

- equivalent automatic audit is not yet wired into every related build path, especially rule generation and history generation

### 5.4 Prediction publication behavior remains split-dependent

Problem:

- in offline mode, published predictions only include `dataset_split == test`
- current split boundaries may leave sampled or full runs with no `test` rows

Impact:

- `snapshots_with_predictions.csv` can correctly remain empty even after a successful training run
- this is operationally correct, but easy to misinterpret as a generation failure

Requirement:

- document this behavior explicitly in the training audit and training summary semantics

### 5.5 Artifact naming and interpretation still cause confusion

Problem:

- current paths use several directories:
  - `edge/`
  - `naive_rules/`
  - `audit/`
  - `predictions/`
  - `models/`
- some files are historical carryovers from earlier runs

Impact:

- artifact interpretation is error-prone without a canonical inventory

Requirement:

- maintain one authoritative generated audit inventory under `offline/audit/`
- ensure training and rule-generation paths both refresh that inventory

### 5.6 Phase documents and current repository state are still partially divergent

Problem:

- several requirements described as “planned” now exist in code
- several requirements described as “done” are not materially complete because artifact files are still missing or full-data refresh still fails

Impact:

- roadmap status is ambiguous
- engineering effort can drift toward already-solved or partially-solved work

Requirement:

- keep one continuously updated requirements/status document aligned to code and artifacts

## 6. Recommended Next Requirements In Priority Order

### Priority 1. Materialize standalone history artifacts

Why:

- this is the clearest unresolved requirement from the serving/history plan
- it affects auditability, reproducibility, and future feature work

Deliverables:

- all `history_features_*.parquet` files generated under `data/offline/edge/`
- generation verified from the dedicated builder
- audit inventory updated to show these files as present with shapes

### Priority 2. Close the blueprint semantic inventory

Why:

- further feature work should be guided by an explicit ledger rather than incremental intuition

Deliverables:

- one final inventory CSV or markdown-backed ledger
- stable disposition for all remaining unmatched blueprint rows

### Priority 3. Add automatic audit generation to rule generation path

Why:

- snapshot training now has automatic funnel audit
- rule generation should emit the same level of explicit artifact and funnel accounting

Deliverables:

- `offline/audit/` receives rule-generation audit files automatically
- rule counts, group counts, selection counts, and artifact shapes are refreshed on each build

### Priority 4. Establish formal serving schema reference

Why:

- current generated assets are already wide enough that implicit schema knowledge is no longer safe

Deliverables:

- one markdown schema reference
- one classification of columns by semantic role

### Priority 5. Resolve full-data training operational strategy

Why:

- sampled training is enough for chain validation
- it is not enough for production-grade artifact rebuilds

Deliverables:

- either higher-memory execution environment requirements
- or documented low-memory/reduced-profile training path

## 7. Acceptance Criteria For The Next Round

The next round should be considered complete only when all of the following are true:

- all standalone `history_features_*.parquet` artifacts exist on disk
- `offline/audit/` contains authoritative funnel and artifact inventory outputs for both:
  - rule generation
  - snapshot training
- blueprint inventory has explicit final dispositions for all open rows
- one schema document defines the serving asset contracts
- full-data bundle rebuild is either:
  - operationally runnable
  - or explicitly documented as requiring a larger environment

## 8. Practical Conclusion

The project is no longer blocked on conceptual migration design.

The remaining blockers are now operational and governance-oriented:

- missing materialized history artifacts
- incomplete blueprint closure
- incomplete release-grade audit coverage
- unresolved full-data training memory requirements

That means the next work should prioritize artifact materialization, audit completeness, and schema governance before expanding the feature surface again.
