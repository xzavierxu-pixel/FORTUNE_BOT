# GroupKey History Feature And Serving Plan

## 1. Goal

This document defines the next implementation phase after the group-level rule filter migration.

The target is:

- keep `trading_rules.csv` as a static rule prior asset
- add the unfinished hierarchical history feature system
- make online loading fast by avoiding runtime multi-table joins
- use full historical data for history features
- do **not** use strict `as_of_ts` joins

This plan deliberately optimizes for operational simplicity and online serving speed over strict temporal feature reconstruction.

## 2. Confirmed Constraints

- Online lookup key uses `group_key + price_bin + horizon_hours`.
- `horizon_hours` is rounded before lookup.
- If `group_key + price_bin + horizon_hours` misses, fallback must use `group_key`.
- When fallback happens, fine-grained features must use predefined defaults.
- Group-level features should still be returned during fallback.
- Online features do not update continuously; they are rebuilt offline and shipped as a versioned snapshot.
- Historical features use full historical data rather than strict point-in-time reconstruction.

## 3. High-Level Design

The feature system should be split into three artifact layers.

### 3.1 Rule Prior Table

File:

- `polymarket_rule_engine/data/offline/edge/trading_rules.csv`

Purpose:

- preserve existing rule prior workflow
- keep the current rule schema and current added group-level quality columns
- continue to support rule-level matching and rule prior feature extraction

This table remains a stable rule asset, not the only carrier of all future history features.

### 3.2 Offline History Feature Build Artifacts

Purpose:

- compute the unfinished hierarchical history features from full history
- provide auditable intermediate outputs
- avoid coupling feature generation logic directly to online serving format

Recommended artifacts:

- `history_features_group.parquet`
- `history_features_domain.parquet`
- `history_features_category.parquet`
- `history_features_market_type.parquet`
- `history_features_domain_x_category.parquet`
- `history_features_domain_x_market_type.parquet`
- `history_features_category_x_market_type.parquet`
- `history_features_global.parquet`

Each artifact should contain features for:

- `expanding`
- `recent_50`
- `recent_200`

These are build artifacts, not online lookup assets.

### 3.3 Online Serving Feature Assets

Purpose:

- give the online path the fastest possible lookup
- avoid runtime joins across many history tables
- make fallback behavior explicit and deterministic

Recommended serving assets:

- `group_serving_features.parquet`
- `fine_serving_features.parquet`

`group_serving_features.parquet` key:

- `group_key`

`fine_serving_features.parquet` key:

- `group_key`
- `price_bin`
- `horizon_hours`

Online flow:

1. round `horizon_hours`
2. lookup `fine_serving_features` by `group_key + price_bin + horizon_hours`
3. if hit, use fine features and group features
4. if miss, lookup `group_serving_features` by `group_key`
5. fill all fine-only features with defaults
6. set explicit fallback indicator features

## 4. What Goes Into Each Asset

### 4.1 Keep In `trading_rules.csv`

Keep these categories in `trading_rules.csv`:

- existing rule columns
- rule-level prior metrics
- current group-level quality columns already landed
- static structure columns already needed for rule generation and matching

Examples:

- `group_key`
- `domain`
- `category`
- `market_type`
- `leaf_id`
- `price_min`
- `price_max`
- `h_min`
- `h_max`
- `direction`
- `q_full`
- `p_full`
- `edge_full`
- `edge_std_full`
- `edge_lower_bound_full`
- `rule_score`
- `n_full`
- `horizon_hours`
- `group_unique_markets`
- `group_snapshot_rows`
- `group_median_logloss`
- `group_median_brier`
- `global_group_logloss_q25`
- `global_group_brier_q25`
- `group_decision`

### 4.2 Put Into `group_serving_features`

This table should carry all features that are valid at `group_key` granularity and remain useful when fine matching fails.

Recommended categories:

- static group metadata
- pooled group quality metrics
- hierarchical history metrics aggregated to the group row
- fallback-safe structure features
- model-visible fallback indicators

Examples:

- `group_key`
- `domain`
- `category`
- `market_type`
- `group_unique_markets`
- `group_snapshot_rows`
- `group_median_logloss`
- `group_median_brier`
- `group_decision`
- `domain_is_unknown`
- `domain_category_key`
- `domain_market_type_key`
- `category_market_type_key`
- `global_expanding_bias`
- `global_expanding_abs_bias`
- `global_expanding_logloss`
- `global_expanding_brier`
- `global_recent_50_bias`
- `global_recent_200_bias`
- `domain_expanding_bias`
- `domain_recent_50_bias`
- `domain_recent_200_bias`
- `category_expanding_bias`
- `market_type_expanding_bias`
- `domain_x_category_expanding_bias`
- `domain_x_market_type_expanding_bias`
- `category_x_market_type_expanding_bias`
- `full_group_expanding_bias`
- `full_group_recent_50_bias`
- `full_group_recent_200_bias`
- the corresponding count, abs-bias, brier, logloss, quantile, and stability columns for each level/window
- `fine_match_found_default`
- `group_match_found_default`

Note:

- this table can be wide
- that is acceptable because `group_key` cardinality is limited and online speed is the priority

### 4.3 Put Into `fine_serving_features`

This table should carry features that truly depend on `group_key + price_bin + horizon_hours`.

Recommended categories:

- current rule-level priors
- rule-level support metrics
- any future fine-grained statistics tied to price bin and rounded horizon
- explicit fine-level lookup metadata

Examples:

- `group_key`
- `price_bin`
- `horizon_hours`
- `leaf_id`
- `direction`
- `q_full`
- `p_full`
- `edge_full`
- `edge_std_full`
- `edge_lower_bound_full`
- `rule_score`
- `n_full`
- `rule_price_center`
- `rule_price_width`
- `rule_horizon_center`
- `rule_horizon_width`
- `rule_edge_buffer`
- `rule_confidence_ratio`
- `rule_support_log1p`
- `rule_snapshot_support_log1p`
- any future fine-level windowed summary columns if they are explicitly keyed by the same fine lookup grain

## 5. Default Value Policy

This is the most important serving rule in this phase.

When `group_key + price_bin + horizon_hours` misses:

- group-level features should still load from `group_serving_features`
- fine-only features should not be imputed from neighboring bins
- fine-only features should use stable predefined defaults
- the model must be told that fallback happened

### 5.1 Why Explicit Defaults Are Required

Without explicit defaults:

- online behavior becomes ambiguous
- missing fine matches silently distort model inputs
- future feature additions become hard to reason about
- training and online behavior can drift

Therefore every fine-only column must have a documented default.

### 5.2 Default Principles

Defaults should follow these rules:

1. Use semantically neutral values whenever possible.
2. Do not use ad-hoc nearest-bin or nearest-horizon interpolation in this phase.
3. Add explicit indicator flags so the model knows the feature came from fallback.
4. Use the same defaults in training whenever a fine feature is unavailable.
5. Keep defaults stable across model versions unless intentionally re-baselined.

### 5.3 Recommended Defaults By Feature Type

#### Indicator Columns

Recommended defaults:

- `fine_match_found = 0`
- `group_match_found = 1` if `group_key` exists, otherwise `0`
- `used_group_fallback_only = 1` when fine miss occurs

If fine match succeeds:

- `fine_match_found = 1`
- `used_group_fallback_only = 0`

#### Count / Support Columns

Recommended defaults:

- counts use `0`
- support logs use `0`

Examples:

- `n_full = 0`
- `rule_support_log1p = 0`
- `rule_snapshot_support_log1p = 0`

Reason:

- zero support is the cleanest meaning for a missing fine rule bucket

#### Score / Edge / Quality Magnitude Columns

Recommended defaults:

- use `0.0`

Examples:

- `q_full = 0.0`
- `p_full = 0.0`
- `edge_full = 0.0`
- `edge_std_full = 0.0`
- `edge_lower_bound_full = 0.0`
- `rule_score = 0.0`
- `rule_edge_buffer = 0.0`
- `rule_confidence_ratio = 0.0`

Reason:

- zero keeps the fallback semantics conservative
- the separate indicator columns tell the model this is a miss, not a true measured zero-quality rule

#### Range / Width / Center Columns

Recommended defaults:

- centers use `0.0`
- widths use `0.0`

Examples:

- `rule_price_center = 0.0`
- `rule_price_width = 0.0`
- `rule_horizon_center = 0.0`
- `rule_horizon_width = 0.0`

Reason:

- these fields only have meaning when a fine rule exists
- zero plus miss indicators is simpler than synthetic bin approximation

#### Categorical Fine Fields

Recommended defaults:

- use stable sentinel values

Examples:

- `leaf_id = "__MISSING_FINE_RULE__"`
- `direction = 0`

Reason:

- sentinel values keep missingness explicit

### 5.4 Group-Level Default Policy

If `group_key` itself is missing from `group_serving_features`, use a second fallback layer.

Recommended group defaults:

- categorical keys use `"UNKNOWN"`
- count-like values use `0`
- metric values use `0.0`
- decision-like fields use stable sentinel categories

Examples:

- `group_decision = "unknown_group"`
- `domain = "UNKNOWN"`
- `category = "UNKNOWN"`
- `market_type = "UNKNOWN"`
- `group_unique_markets = 0`
- `group_snapshot_rows = 0`
- all history metric columns = `0.0`
- `group_match_found = 0`

This gives a deterministic final fallback path:

1. fine hit
2. group-only hit
3. unknown-group fallback

### 5.5 Default Manifest

Defaults should not be hardcoded in multiple places.

Create a single manifest, for example:

- `polymarket_rule_engine/data/offline/edge/serving_feature_defaults.json`

This manifest should store:

- feature name
- feature type
- default value
- whether it is fine-only or group-level
- whether a miss indicator is required

This prevents training and online fallback logic from diverging.

## 6. Historical Feature Families To Implement Next

The unfinished feature work should be implemented in batches.

### Batch 1: Hierarchical Core Metrics

For each level:

- `global`
- `domain`
- `category`
- `market_type`
- `domain_x_category`
- `domain_x_market_type`
- `category_x_market_type`
- `full_group_key`

Implement for each window:

- `expanding`
- `recent_50`
- `recent_200`

Minimum metric set:

- count
- market_count
- bias
- abs_bias
- brier
- logloss

### Batch 2: Quantile And Stability Metrics

For the same levels/windows, add:

- p25
- p50
- p75
- tail metrics
- dispersion
- instability

### Batch 3: Interaction Features

After the base families are stable, add:

- history metric x current price interactions
- recent vs expanding gaps
- rule prior gap features
- tail-risk interaction features

## 7. A/B/C/D/E Audit Output

The 500-feature audit should not live inside `trading_rules.csv`.

Create a separate auditable inventory artifact, for example:

- `polymarket_rule_engine/docs/groupkey_feature_inventory.csv`

Required columns:

- `feature_name`
- `feature_family`
- `grain`
- `window`
- `source_table`
- `status`
- `audit_class`
- `implemented_in`
- `serving_asset`
- `notes`

Suggested `audit_class` values:

- `A_keep_and_implement_now`
- `B_keep_but_later`
- `C_already_implemented`
- `D_duplicate_or_merge`
- `E_not_supported_now`

This artifact is the authoritative source for tracking the unfinished 500-feature blueprint.

## 8. Implementation Steps

### Step 1

Create the feature inventory file for the blueprint and classify all candidate features into A/B/C/D/E.

### Step 2

Build offline history feature generation code for:

- `global`
- `domain`
- `category`
- `market_type`
- pairwise combinations
- `full_group_key`

using:

- `expanding`
- `recent_50`
- `recent_200`

with full historical data.

### Step 3

Build `group_serving_features.parquet`.

This step should flatten all group-safe hierarchical history features into one wide group-level serving asset.

### Step 4

Build `fine_serving_features.parquet`.

This step should flatten all fine-grained rule-aligned features into one wide fine-level serving asset.

### Step 5

Create the shared default manifest and wire both training and online logic to the same fallback defaults.

### Step 6

Update online loading code:

- load both serving assets at startup
- lookup fine first
- fallback to group second
- apply manifest defaults for missing fine columns
- expose fallback indicators to the model input pipeline

### Step 7

Verify:

- fine hit rate
- group fallback rate
- unknown-group fallback rate
- online load latency
- memory footprint
- training and online feature column consistency

## 9. Acceptance Criteria

This phase is complete when all of the following are true:

- hierarchical history features exist for all required levels
- `recent_50 / recent_200 / expanding` are all implemented
- `trading_rules.csv` remains stable and readable by downstream code
- online serving uses prebuilt assets instead of runtime multi-table joins
- fine miss behavior is deterministic and documented
- all fine-only columns have explicit defaults
- the 500-feature audit has a traceable inventory artifact
- training and online use the same fallback defaults and indicator semantics

## 10. Final Recommendation

Do not try to force the full unfinished history feature system into `trading_rules.csv`.

Use:

- `trading_rules.csv` for rule prior data
- separate offline history build artifacts for feature generation
- two online serving assets for fast inference
- one shared default manifest for deterministic fallback behavior

Given the current scale of `7541` fine-grained rows, a wide serving table is operationally acceptable and likely the fastest online solution.
