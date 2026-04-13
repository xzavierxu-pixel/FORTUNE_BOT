# GroupKey Remaining Requirements And Next Plan

## 1. Purpose

This document consolidates:

- the remaining unmet requirements from:
  - `groupkey_feature_implementation_plan.md`
  - `phase2.md`
  - `phase3.md`
  - `groupkey_serving_and_history_feature_plan.md`
- the next concrete implementation plan based on the current repository state

This file is intended to replace cross-reading multiple historical planning documents when deciding what to build next.

## 2. Current State Summary

As of the current implementation, the following are already in place:

- rule-level sample filtering has been removed from the training path
- group-level filtering has replaced rule-level filtering
- `trading_rules.csv` is regenerated and keeps all rows under retained `group_key`
- `group_serving_features.parquet` and `fine_serving_features.parquet` are generated offline
- online serving lookup is designed around:
  - exact `group_key + price_bin + rounded_horizon_hours`
  - fallback to `group_key`
- fine-level fallback defaults are derived from all available rows within the same `group_key`
- hierarchical history features currently exist for:
  - `global`
  - `domain`
  - `category`
  - `market_type`
  - `domain_x_category`
  - `domain_x_market_type`
  - `category_x_market_type`
  - `full_group`
- history windows currently implemented:
  - `expanding`
  - `recent_50`
  - `recent_200`
- currently implemented history metric families include:
  - count / market_count
  - mean
  - p25 / p50 / p75 / p90
  - std
  - min / max
  - tail spread
  - several drift gap and third-batch interaction features
- `groupkey_feature_inventory.csv` now exists and includes:
  - 500 parsed rows from blueprint
  - implemented asset-derived features
  - exact vs approximate vs pending blueprint matching
- standalone history artifacts now exist and are emitted as:
  - `history_features_global.parquet`
  - `history_features_domain.parquet`
  - `history_features_category.parquet`
  - `history_features_market_type.parquet`
  - `history_features_domain_x_category.parquet`
  - `history_features_domain_x_market_type.parquet`
  - `history_features_category_x_market_type.parquet`
  - `history_features_full_group.parquet`
- normalized drift and tail-instability serving families have been added for `full_group`
- expanded rule-gap families now include:
  - domain-relative
  - category-relative
  - market_type-relative
  - pairwise-relative
- migration and consistency markdown reports now exist:
- runtime and validation markdown reports now exist:
  - `groupkey_runtime_report.md`
  - `groupkey_migration_validation.md`
  - `groupkey_consistency_report.md`
- feature-contract preview tooling now exists and currently shows:
  - current code path emits `720` serving-backed columns
  - existing runtime bundle feature contract still registers `0` serving-backed columns
  - bundle refresh is blocked in the current environment by missing `autogluon.tabular`
- runtime report currently shows on a bounded offline sample:
  - `group_match_rate=1.0`
  - `fine_match_rate=1.0`
  - the serving parquet assets and defaults manifest are internally consistent at lookup time

## 3. Remaining Requirements By Document

## 3.1 Remaining From `groupkey_feature_implementation_plan.md`

The following requirements are still not fully complete:

### A. Full 500-feature audit is not yet semantically closed

What exists now:

- `groupkey_feature_inventory.csv` contains parsed blueprint rows
- exact and approximate matches are recorded

What is still missing:

- manual or rule-assisted semantic review of the remaining unmatched blueprint rows
- explicit final classification of all remaining rows into:
  - exact implemented
  - approximate implemented
  - truly unimplemented
  - duplicate/merge
  - intentionally excluded

### B. Price-curve feature family is still mostly missing

Current state:

- no full `p_1h ~ p_12h`, `logit_p_*`, slope, curvature, acceleration family has been landed as a coherent serving feature family

Still needed:

- decide whether price-curve features belong in:
  - training-time dynamic features
  - group serving features
  - fine serving features
- implement a consistent subset rather than one-off columns

### C. Final migration governance is not complete

Current state:

- new group-based path is active

Still needed:

- side-by-side governance summary of old vs new logic
- explicit rollback / release control documentation
- validation summary for sample coverage, score distribution, and model behavior drift

## 3.2 Remaining From `phase2.md`

Most of `phase2` has already been completed.

The remaining work from a phase2 perspective is now mostly governance and validation:

### A. Explicit proof that old rule-level filtering is fully retired from every training entry path

Current state:

- main training path no longer uses rule-level pruning

Still needed:

- repo-wide verification that there is no hidden legacy training path still depending on old rule-level sample dropping
- explicit summary document showing each entry point and whether it is migrated

### B. Final before/after comparison artifact for sample and group changes

Still needed:

- one reproducible report covering:
  - snapshot rows before vs after migration
  - unique markets before vs after
  - retained `group_key` count
  - retained rule row count
  - changed distribution by category / market_type / domain

## 3.3 Remaining From `phase3.md`

`phase3` is only partially complete.

### A. Not all suitable blueprint features have been landed into serving assets

Already done:

- static group quality
- hierarchical history metrics
- quantile / tail / stability metrics
- several interaction and rule-gap features

Still missing:

- more complete rule-gap family
- more complete drift normalization family
- more complete tail-risk normalization family
- more complete cross-level interaction family

### B. No final schema decision document for all serving assets

Still needed:

- finalized schema reference for:
  - `trading_rules.csv`
  - `group_serving_features.parquet`
  - `fine_serving_features.parquet`
  - `serving_feature_defaults.json`
- designation of:
  - static rule columns
  - group fallback columns
  - fine-only columns
  - generated interaction columns

### C. No final statement of which blueprint features are intentionally not suitable for serving tables

Still needed:

- mark blueprint rows that should remain:
  - training-time dynamic only
  - unsupported
  - redundant
  - intentionally deferred

## 3.4 Remaining From `groupkey_serving_and_history_feature_plan.md`

This plan is substantially implemented, but these items remain:

### A. Separate normalized history build artifacts do not yet exist

Status:

- completed

What now exists:

- standalone history build artifacts are emitted per level
- a dedicated history artifact builder script exists
- serving asset generation now consumes the standalone history artifact set instead of recomputing metrics inline

### B. Default manifest is present but not yet fully governed

Current state:

- `serving_feature_defaults.json` exists

Still needed:

- explicit schema contract for the manifest
- versioning policy
- validation that every fine-only serving column has a corresponding fallback entry
- validation that deprecated fine columns are removed from manifest

### C. Startup/runtime performance verification is not complete

Still needed:

- broader online load validation beyond the bounded offline sample
- proof that startup loading remains acceptable after future feature growth
- final runtime validation after the AutoGluon bundle feature contract is rebuilt

Current state:

- migration and consistency reports are implemented
- feature-contract preview tooling is implemented
- runtime report generation is implemented for bounded offline samples

## 4. Highest-Priority Unfinished Requirements

The highest-value remaining items are:

1. Complete semantic closure of blueprint audit
2. Build standalone history feature artifacts
3. Finish the most valuable missing serving feature families
4. Add migration validation reports and runtime performance reports

Updated status:

1. complete
2. complete
3. partially complete
4. partially complete

## 5. Next Implementation Plan

## Phase N1: Blueprint Semantic Closure

### Goal

Turn the current inventory from a useful approximation into a trustworthy implementation ledger.

### Tasks

1. Review the `374` currently unmatched blueprint rows.
2. Reclassify them into:
   - exact implemented
   - approximate implemented
   - pending implementation
   - duplicate / merge
   - intentionally excluded
3. Add a new classification column if needed:
   - `disposition`
4. Add optional note columns:
   - `implementation_reason`
   - `exclusion_reason`

### Deliverable

- updated `groupkey_feature_inventory.csv`
- one markdown review summary for unresolved rows

## Phase N2: Standalone History Artifact Builder

### Goal

Decouple history computation from rule generation.

### Tasks

1. Create a separate build script for hierarchical history artifacts.
2. Emit one artifact per level:
   - global
   - domain
   - category
   - market_type
   - pairwise combinations
   - full_group
3. Ensure each artifact contains:
   - expanding
   - recent_50
   - recent_200
4. Move serving-asset generation to consume these artifacts instead of recomputing inline.

### Deliverable

- dedicated `history_features_*.parquet` assets
- serving build path updated to use them

### Status

- completed

## Phase N3: Missing High-Value Serving Families

### Goal

Finish the most valuable missing feature families before further widening.

### Tasks

1. Implement normalized drift metrics:
   - `*_vs_expanding_*_zscore`
2. Implement normalized tail instability metrics:
   - `*_tail_instability_ratio`
3. Expand rule-gap family:
   - domain-relative
   - category-relative
   - market_type-relative
   - pairwise-relative
4. Expand price-history interaction family where static serving representation still makes sense.

### Deliverable

- widened serving assets
- inventory updated from pending to implemented where appropriate

### Status

- partially complete

Completed in this round:

- normalized drift metrics:
  - `*_vs_expanding_*_zscore`
- normalized tail instability metrics:
  - `*_tail_instability_ratio`
- expanded rule-gap family:
  - domain-relative
  - category-relative
  - market_type-relative
  - pairwise-relative

Still remaining in this phase:

- price-history interaction family expansion beyond the currently landed subset
- inventory reclassification pass to convert pending blueprint rows into implemented/excluded decisions

## Phase N4: Runtime And Migration Validation

### Goal

Convert the current implementation into a release-grade system.

### Tasks

1. Build migration comparison report:
   - old rule-filter behavior vs current group-filter behavior
2. Build serving runtime report:
   - startup load time
   - memory footprint
   - fine hit rate
   - group fallback rate
   - unknown-group fallback rate
3. Build model-input consistency report:
   - training feature columns vs online feature columns
   - missing/defaulted column counts

### Deliverable

- migration report markdown
- runtime report markdown
- consistency report markdown

### Status

- partially complete

Completed in this round:

- migration report markdown
- runtime report markdown
- consistency report markdown
- feature-contract preview markdown/json
- low-memory snapshot-to-rule matching for bundle rebuild path

Remaining blocker:

- refreshing the runtime bundle feature contract still requires rebuilding the snapshot model bundle
- the current environment is missing `autogluon.tabular`, so the final bundle rebuild cannot complete here

## 6. Recommended Execution Order

The recommended next order is:

1. `Phase N1`
2. `Phase N3`
3. `Phase N4`

Reason:

- semantic audit closure avoids implementing the wrong features next
- most history decoupling work is already done
- remaining feature-family work should be focused only on blueprint rows that survive semantic review
- runtime reporting and bundle refresh should happen immediately after the final feature surface is declared stable

## 7. Acceptance Criteria For The Next Round

The next round should be considered complete when:

- all currently unmatched high-priority blueprint rows have explicit disposition
- history features are emitted as standalone build artifacts
- normalized drift and tail-instability families are added
- runtime fallback and load metrics are measured on real data
- documentation clearly states:
  - what is implemented
  - what is intentionally excluded
  - what remains pending

## 8. Suggested Output Files For The Next Round

Recommended new output files:

- `polymarket_rule_engine/docs/groupkey_inventory_review.md`
- `polymarket_rule_engine/docs/groupkey_runtime_report.md`
- `polymarket_rule_engine/docs/groupkey_migration_validation.md`
- `polymarket_rule_engine/docs/groupkey_feature_contract_preview.md`
- `polymarket_rule_engine/data/offline/edge/history_features_global.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_domain.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_category.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_market_type.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_domain_x_category.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_domain_x_market_type.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_category_x_market_type.parquet`
- `polymarket_rule_engine/data/offline/edge/history_features_full_group.parquet`

## 9. Practical Conclusion

The current system is no longer in the "switch filter logic" stage.

The current bottleneck is now:

- semantic audit completeness
- runtime validation
- bundle contract refresh in an environment that can actually train AutoGluon bundles

Feature addition should continue, but only after the remaining blueprint rows and artifact boundaries are made explicit.
