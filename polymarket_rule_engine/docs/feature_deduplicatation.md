# Feature DQC Exact Duplicate Columns Implementation Plan

## 1. Objective

This document defines the implementation plan for the highest-priority Feature DQC issue in the current offline pipeline:

- exact duplicate columns in the exported training feature frame

The target is to stop producing duplicate signals at the source while keeping one shared feature augmentation path for:

- offline training export
- experiment/research export
- online serving fallback behavior

This batch does not cover:

- empty valid split handling
- constant-feature cleanup beyond duplicate-generation paths
- high-cardinality cleanup
- categorical imbalance cleanup

## 2. Current Repository Reality

The current implementation lives under `polymarket_rule_engine/rule_baseline/`, not directly under `polymarket_rule_engine/`.

Key files involved in this batch:

- `polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py`
- `polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py`
- `polymarket_rule_engine/rule_baseline/training/export_features.py`
- `polymarket_rule_engine/rule_baseline/features/serving.py`
- `polymarket_rule_engine/rule_baseline/features/tabular.py`
- `polymarket_rule_engine/rule_baseline/features/snapshot_semantics.py`
- `polymarket_rule_engine/rule_baseline/history/history_features.py`
- `polymarket_rule_engine/rule_baseline/quality_check/data_quality_report.py`

Relevant current behavior:

- `build_feature_table()` in `train_snapshot_model.py` calls `attach_serving_features()`.
- `attach_serving_features()` merges both `group_feature_*` and `fine_feature_*` into the same frame for training and serving.
- `apply_feature_variant()` in `features/tabular.py` re-derives several rule geometry/support features that also exist in fine serving assets.
- `rule_score` is currently assigned equal to `edge_lower_bound_full` at rule-row generation time.
- `build_group_serving_features()` already merges materialized history artifacts, but downstream still reads legacy direct-injection names such as `group_unique_markets` and `group_median_logloss`.

## 3. Confirmed Decisions

The implementation in this document follows the confirmed scope:

1. Training and serving must use the same augmentation path.
2. Serving bundle should be cleaned up together with training export.
3. Online and offline feature behavior must stay aligned.
4. Remove `rule_score` instead of redefining it.
5. Only remove `full_group_key` aggregate duplicates in this batch.
6. Downstream should switch directly to history-backed naming.
7. Feature contract changes are allowed.
8. Validation target is:
   - export succeeds
   - contract remains consistent
   - DQC duplicate groups drop materially
   - training runs with 50k sampled rows

## 4. Problem Summary

The exported training parquet currently mixes four column roles into one flat frame:

- leaf-rule matched columns from exact rule lookup
- fine serving columns generated from the same rules again
- group serving fallback columns
- metadata / monitoring / audit columns

This architecture creates exact duplicate columns, including:

- `q_full`, `fine_feature_q_full`, `q_smooth`
- `p_full`, `fine_feature_p_full`
- `edge_full`, `fine_feature_edge_full`, `fine_feature_rule_full_group_key_max_edge_full`, `fine_feature_rule_full_group_key_mean_edge_full`
- `edge_std_full`, `fine_feature_edge_std_full`
- `edge_lower_bound_full`, `rule_score`, `fine_feature_edge_lower_bound_full`, `fine_feature_rule_full_group_key_max_edge_lower_bound_full`, `fine_feature_rule_full_group_key_mean_edge_lower_bound_full`
- `n_full`, `fine_feature_n_full`, `fine_feature_rule_full_group_key_sum_n_full`
- `group_feature_full_group_expanding_market_count` and legacy group support mirrors
- `group_feature_full_group_expanding_snapshot_count` and legacy group snapshot mirrors
- `group_feature_full_group_expanding_logloss_p50` and legacy logloss mirrors
- `group_feature_full_group_expanding_brier_p50` and legacy brier mirrors

These duplicates come from the feature-building architecture, not from data corruption.

## 5. Design Principles

### 5.1 One canonical source per semantic layer

Each semantic layer should come from exactly one source:

- leaf-rule priors: exact rule match columns
- group fallback and stability context: group serving features
- level/window statistics: history feature artifacts

### 5.2 One augmentation path across training and serving

Training and serving must not diverge at the attachment stage.

That means:

- `attach_serving_features()` remains the shared augmentation path
- duplicate cleanup must happen by changing what the bundle contains
- duplicate cleanup must not rely on training-only post-filtering

### 5.3 Prefer not generating duplicates

If a signal is redundant, the preferred fix is:

- stop generating it in the fine/group assets

instead of:

- generate it
- merge it
- drop it later

### 5.4 Preserve online fallback semantics

Online fallback still depends on:

- `group_key`
- `price_bin`
- `rounded_horizon_hours`
- group defaults when fine-level match is absent

The cleanup must preserve:

- `group_match_found`
- `fine_match_found`
- `used_group_fallback_only`

## 6. Target Architecture

After implementation, the shared feature path should expose three clean signal layers.

### 6.1 Layer A: Leaf rule priors

Canonical source:

- `match_snapshots_to_rules()` in `rule_baseline/training/train_snapshot_model.py`

Canonical columns:

- `q_full`
- `p_full`
- `edge_full`
- `edge_std_full`
- `edge_lower_bound_full`
- `n_full`

Removed:

- `rule_score`

### 6.2 Layer B: Group fallback and group statistics

Canonical source:

- `build_group_serving_features()` in `rule_baseline/training/train_rules_naive_output_rule.py`
- merged via `attach_serving_features()`

Canonical group support/quality statistics should use history-backed names directly:

- `full_group_expanding_market_count`
- `full_group_expanding_snapshot_count`
- `full_group_expanding_logloss_p50`
- `full_group_expanding_brier_p50`

Legacy direct-injection names should not remain in the final feature contract.

### 6.3 Layer C: Derived rule geometry/support features

Canonical source:

- `apply_feature_variant()` in `rule_baseline/features/tabular.py`

Canonical training/serving-visible columns:

- `rule_price_width`
- `rule_horizon_center`
- `rule_horizon_width`
- `rule_edge_buffer`
- `rule_confidence_ratio`
- `rule_support_log1p`
- `rule_snapshot_support_log1p`

Fine-prefixed mirrors of those derivations should not be generated.

## 7. Duplicate Classes and Implementation Strategy

### Class 1. Fine columns that mirror leaf-rule priors

Current duplicate family:

- `fine_feature_q_full`
- `fine_feature_p_full`
- `fine_feature_edge_full`
- `fine_feature_edge_std_full`
- `fine_feature_edge_lower_bound_full`
- `fine_feature_n_full`

Implementation direction:

- stop generating these columns in `build_fine_serving_features()`
- remove them from `FINE_SERVING_COLUMNS`
- remove their defaults from `FINE_DEFAULT_AGGREGATIONS`
- keep bare leaf-rule priors from exact rule matching

### Class 2. Fine columns that mirror deterministic derived rule features

Current duplicate family:

- `fine_feature_rule_price_width`
- `fine_feature_rule_horizon_center`
- `fine_feature_rule_horizon_width`
- `fine_feature_rule_edge_buffer`
- `fine_feature_rule_confidence_ratio`
- `fine_feature_rule_support_log1p`
- `fine_feature_rule_snapshot_support_log1p`

Implementation direction:

- keep formulas only in `features/tabular.py`
- stop generating fine-prefixed versions in `build_fine_serving_features()`

### Class 3. Fine `full_group_key` aggregate columns

Current duplicate family:

- `fine_feature_rule_full_group_key_max_edge_full`
- `fine_feature_rule_full_group_key_mean_edge_full`
- `fine_feature_rule_full_group_key_sum_n_full`
- `fine_feature_rule_full_group_key_max_edge_lower_bound_full`
- `fine_feature_rule_full_group_key_mean_edge_lower_bound_full`

Implementation direction:

- keep domain/category/market_type aggregate columns unchanged in this batch
- stop generating `full_group_key` aggregate columns in `_build_matched_rule_aggregate_features()`
- remove matching defaults and references from the serving bundle contract

### Class 4. `rule_score`

Current behavior:

- rule row generation sets `rule_score = edge_lower_bound_full`

Implementation direction:

- remove `rule_score` from rule generation
- remove it from feature semantics critical columns
- remove dependent fine/group derived columns that use `rule_score`
- replace downstream calculations with `edge_lower_bound_full` where still needed

### Class 5. Legacy group direct-injection statistics

Current legacy fields:

- `group_unique_markets`
- `group_snapshot_rows`
- `group_median_logloss`
- `group_median_brier`
- `group_market_share_global`
- `group_snapshot_share_global`
- `global_group_logloss_q25`
- `global_group_brier_q25`

Implementation direction:

- stop emitting these direct rule-row statistics into the shared feature contract
- switch downstream derived features to history-backed names
- keep group defaults and fallback mechanics, but base them on history-backed group features

## 8. File-Level Change Plan

### 8.1 `rule_baseline/training/train_rules_naive_output_rule.py`

Required changes:

- remove `rule_score` from `RULE_SCHEMA_COLUMNS`
- remove mirrored priors and mirrored deterministic derivations from `FINE_SERVING_COLUMNS`
- remove `full_group_key` aggregate fields from `FINE_SERVING_COLUMNS`
- remove matching fallback entries from `FINE_DEFAULT_AGGREGATIONS`
- update `_build_matched_rule_aggregate_features()` to skip `full_group_key`
- update rule-row generation so `rule_score` is no longer produced
- update `build_group_serving_features()` so direct-injection legacy metrics are no longer the canonical source

### 8.2 `rule_baseline/features/serving.py`

Required changes:

- keep one shared `attach_serving_features()` path
- preserve group/fine matching and fallback indicators
- align fallback defaults with the reduced fine schema
- ensure no missing-column assumptions still reference removed fine fields

### 8.3 `rule_baseline/features/tabular.py`

Required changes:

- switch derived feature inputs from legacy group names to history-backed names
- replace any `rule_score`-based calculations with `edge_lower_bound_full` or remove them
- keep canonical rule geometry/support formulas in this file only

### 8.4 `rule_baseline/features/snapshot_semantics.py`

Required changes:

- remove `rule_score` from `_DEFAULT_CRITICAL_COLUMNS`
- allow the updated feature contract to be the canonical exported contract

### 8.5 `rule_baseline/training/export_features.py`

Required changes:

- continue building manifest from the shared feature frame
- no training-only duplicate filtering path should be introduced
- contract should reflect the cleaned shared augmentation output

### 8.6 Tests

Primary tests to update or add near:

- `polymarket_rule_engine/tests/test_serving_feature_integration.py`
- `polymarket_rule_engine/tests/test_groupkey_serving_assets.py`
- `polymarket_rule_engine/tests/test_feature_semantics_manifest.py`

## 9. Validation Plan

### 9.1 Focused tests

Run targeted tests covering:

- serving attachment and fallback behavior
- serving asset generation
- feature semantics / runtime contract

### 9.2 DQC duplicate-column check

Use:

- `polymarket_rule_engine/rule_baseline/quality_check/data_quality_report.py`

Validation goal:

- duplicate-column groups materially decrease in exported `train.parquet`

### 9.3 Training smoke test

Run a targeted export + training smoke flow with 50k sampled train rows.

Validation goal:

- export succeeds
- feature contract is internally consistent
- training consumes the updated manifest and runs to completion

## 10. Acceptance Criteria

This batch is complete when all of the following are true:

- training and serving use the same feature augmentation path
- `rule_score` is fully removed
- fine mirror columns for leaf priors are no longer generated
- fine mirror columns for deterministic rule derivations are no longer generated
- `full_group_key` aggregate duplicate columns are no longer generated
- downstream code reads history-backed group names directly
- online fallback behavior remains intact
- exported contract is updated and consistent
- DQC duplicate groups drop materially
- training can run with 50k sampled rows
