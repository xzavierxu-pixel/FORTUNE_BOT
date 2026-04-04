# AutoGluon Q-Model Next-Round Optimization Plan

## 1. Goal

This document defines the next optimization round for the production `q` model.

Primary objective:

1. minimize offline strict-test `logloss_model`

Secondary objectives:

1. improve or at least preserve `brier_model`
2. make the best result reproducible rather than dependent on a lucky single run
3. keep the resulting model deployable through the current runtime-bundle contract

The optimization target is not raw backtest ROI in this phase. The first filter is still predictive quality, with `logloss_model` as the primary ranking metric.

## 2. Current State

Current best stable production configuration:

1. `predictor_presets=medium_quality`
2. `calibration_mode=grouped_isotonic`
3. `grouped_calibration_column=horizon_hours`
4. `grouped_calibration_min_rows=20`
5. `predictor_time_limit=120`

Observed status from recent experiments:

1. `medium_quality` consistently beats heavier presets such as `good_quality` and `high_quality`
2. narrower model-family subsets did not beat the default medium-quality ensemble
3. grouped calibration by `horizon_hours` is still the best measured default
4. repeated identical runs show non-trivial stochastic variance
5. the previously observed best single result could not be stably reproduced

Implication:

1. the next round should focus on reproducibility, feature quality, and controlled AutoGluon search design rather than brute-force preset escalation

## 3. Optimization Principles

The next round should follow these rules:

1. change one optimization axis at a time
2. evaluate all experiments on the same strict offline `test` split
3. rank results by `logloss_model`, with `brier_model` as the tie-breaker
4. prefer configurations that are repeatable across multiple seeds
5. keep runtime compatibility with the existing `q_model_bundle` contract
6. avoid changes that only improve backtest metrics while degrading probability quality

## 4. Priority Order

Execution priority for the next round:

1. training stability and seed control
2. controlled AutoGluon search configuration
3. feature ablation to remove noisy inputs
4. calibration-method expansion beyond isotonic
5. targeted feature additions
6. optional runtime dependency expansion such as `torch`

## 5. Workstream A: Training Stability and Seed Sweep

### 5.1 Hypothesis

The gap between the best historical single run and current reruns is partly caused by training randomness and finite time-budget effects.

### 5.2 Required changes

Add explicit seed support to the AutoGluon training path:

1. add `random_seed` to `train_snapshot_model.py`
2. pass `random_seed` into the AutoGluon predictor fit path
3. record the seed in `model_training_summary.json`
4. record the seed in the bundle runtime manifest

### 5.3 Experiments

Run seed sweeps on the current best configuration:

1. seeds: `7, 13, 21, 42, 77, 101, 202, 314`
2. fixed config:
   - `medium_quality`
   - `grouped_isotonic`
   - `horizon_hours`
   - `min_rows=20`
   - `time_limit=120`

### 5.4 Success criteria

This workstream succeeds if it finds one of the following:

1. a seed that reliably improves `logloss_model` versus the current best-of-5 bundle
2. a narrow variance band that establishes the realistic expected performance floor and ceiling

### 5.5 Deliverables

1. `analysis/autogluon_seed_sweep_results.csv`
2. `analysis/autogluon_seed_sweep_summary.json`
3. best seed written into the canonical production training config

## 6. Workstream B: Controlled AutoGluon Search Design

### 6.1 Hypothesis

The current use of `medium_quality` default hyperparameters is too generic. Better `logloss_model` may come from explicitly controlling bagging, stacking, and model families instead of moving to heavier presets.

### 6.2 Observations driving this work

Current logs show:

1. `use_bag_holdout=True, but bagged mode is not enabled`

This means part of the current training path is not truly using bagging behavior.

### 6.3 Required changes

Expose additional AutoGluon fit controls:

1. `num_bag_folds`
2. `num_bag_sets`
3. `num_stack_levels`
4. `auto_stack`
5. optional manual hyperparameter sets per model family

### 6.4 Experiment matrix

Recommended order:

1. `GBM + CAT`, `num_bag_folds=5`, `num_stack_levels=1`, `time_limit=300`
2. `GBM + CAT + XGB`, `num_bag_folds=5`, `num_stack_levels=1`, `time_limit=300`
3. current default family mix, `num_bag_folds=5`, `num_stack_levels=1`, `time_limit=300`
4. `GBM + CAT`, `num_bag_folds=8`, `num_stack_levels=1`, `time_limit=600`
5. `GBM + CAT`, `num_bag_folds=5`, `num_stack_levels=2`, `time_limit=600`

### 6.5 Important guardrails

1. do not reintroduce `good_quality` or `high_quality` as the default search path
2. compare each experiment against the same baseline seed set when possible
3. do not judge a configuration by a single lucky run

### 6.6 Success criteria

This workstream succeeds if a controlled bagged configuration beats the current best stable baseline on:

1. lower `logloss_model`
2. no material deterioration in `brier_model`

### 6.7 Deliverables

1. `analysis/autogluon_search_design_results.csv`
2. `analysis/autogluon_search_design_summary.json`

## 7. Workstream C: Feature Ablation for Noise Reduction

### 7.1 Hypothesis

Several current features are likely noisy, high-cardinality, or close to identifiers, which may improve in-sample fit while harming strict-test `logloss_model`.

### 7.2 High-risk feature groups to test for removal

The first ablation round should target:

1. raw text-like columns:
   - `question_market`
   - `description_market`
2. identifier-like columns:
   - `gameId_market`
   - `marketMakerAddress_market`
   - `source_url_market`
3. duplicated descriptive variants with weak structural signal:
   - `groupItemTitle_market`
   - `source_host_market`
   - `domain_parsed_market`
   - `sub_domain_market`

### 7.3 Experiment matrix

Run at least three feature-set variants:

1. baseline feature set
2. no raw text / no ID-like fields
3. strict structured-only set:
   - rule features
   - horizon features
   - price features
   - liquidity / quote freshness features
   - domain / category / market_type

### 7.4 Success criteria

This workstream succeeds if removing noisy fields lowers `logloss_model` without a meaningful collapse in `auc_model`.

### 7.5 Deliverables

1. `analysis/feature_ablation_results.csv`
2. `analysis/feature_ablation_summary.json`
3. updated default `DROP_COLS` or feature whitelist if an ablation wins

## 8. Workstream D: Calibration Expansion Beyond Isotonic

### 8.1 Hypothesis

Grouped isotonic by horizon is currently best among tested options, but the remaining gap may be limited by the calibration method rather than the grouping key.

### 8.2 Methods to add

Add support for:

1. `none`
2. `global_isotonic`
3. `grouped_isotonic`
4. `sigmoid` or Platt scaling
5. `beta_calibration`
6. convex blend between raw and calibrated probabilities

### 8.3 Recommended evaluation order

1. test global sigmoid against current grouped isotonic baseline
2. test grouped sigmoid by `horizon_hours`
3. test global beta calibration
4. test grouped beta calibration by `horizon_hours`
5. test calibrated/raw blending weights such as `0.25, 0.5, 0.75`

### 8.4 Success criteria

This workstream succeeds if a calibration method beats the current grouped isotonic baseline on:

1. lower `logloss_model`
2. same or better `brier_model`

### 8.5 Deliverables

1. `analysis/calibration_method_expansion_results.csv`
2. `analysis/calibration_method_expansion_summary.json`

## 9. Workstream E: Targeted Feature Additions

### 9.1 Hypothesis

The next useful gains are more likely to come from low-dimensional interaction features that explicitly capture tradability and mispricing structure, rather than adding more raw market metadata.

### 9.2 Candidate features

Priority additions:

1. `abs_price_q_gap = abs(price - q_full)`
2. `abs_price_center_gap = abs(price - 0.5)`
3. `horizon_q_gap = horizon_hours * abs(price - q_full)`
4. `log_horizon_x_liquidity = log_horizon * log1p(liquidity)`
5. `spread_over_liquidity`
6. `quote_staleness_x_horizon`
7. `rule_score_x_q_full`
8. `edge_lower_bound_over_std`

### 9.3 Text-derived low-dimensional features

If structured features plateau, add compressed text signals rather than raw-text categories:

1. question length
2. contains year
3. contains date-like phrase
4. contains percentage
5. contains currency amount
6. contains temporal deadline words such as `before`, `after`, `by`, `end of`

### 9.4 Success criteria

This workstream succeeds if one or more additions improve `logloss_model` after the noisy-feature ablation step has been completed.

### 9.5 Deliverables

1. `analysis/feature_addition_results.csv`
2. `analysis/feature_addition_summary.json`

## 10. Workstream F: Optional Torch-Enabled AutoGluon Pass

### 10.1 Hypothesis

The current environment does not have `torch`, so the neural candidates in the medium-quality preset are not actually participating. Adding that dependency may improve `logloss_model`, though this is lower priority than stability and feature quality.

### 10.2 Scope

Only run this after Workstreams A through D are complete.

### 10.3 Experiments

1. install `torch` in the training environment
2. rerun the best structured configuration from prior workstreams
3. compare with and without torch-enabled candidate models

### 10.4 Success criteria

Keep this change only if:

1. `logloss_model` improves materially
2. runtime and artifact size remain acceptable

## 11. Recommended Execution Sequence

The next round should be executed in this order:

1. implement seed control and run seed sweep
2. implement controlled bagging/stacking support
3. run noisy-feature ablation experiments
4. expand calibration methods
5. add targeted interaction features
6. optionally test torch-enabled AutoGluon

Reason:

1. this order isolates variance first
2. then improves training design
3. then removes harmful inputs
4. then optimizes probability mapping
5. only after that adds new feature complexity

## 12. Evaluation Standard

Every experiment should produce:

1. `logloss_model`
2. `brier_model`
3. `auc_model`
4. fit time
5. predictor name
6. calibration mode
7. feature-set identifier
8. random seed

Ranking rule:

1. primary sort: lowest `logloss_model`
2. tie-breaker: lowest `brier_model`
3. secondary sanity check: no catastrophic deterioration in `auc_model`

## 13. Exit Criteria

This next optimization round is complete when:

1. the team has identified a best reproducible configuration rather than a one-off lucky run
2. the winning configuration is documented with seed, feature set, calibration mode, and AutoGluon fit controls
3. the winning bundle has been regenerated and archived
4. offline probability gains have been verified against the previous canonical bundle

## 14. Implementation Notes

Likely code surfaces to touch:

1. `rule_baseline/training/train_snapshot_model.py`
2. `rule_baseline/models/autogluon_qmodel.py`
3. `rule_baseline/features/tabular.py`
4. `rule_baseline/analysis/compare_calibration_methods.py`
5. optional new experiment scripts under `rule_baseline/analysis/`

Artifacts to keep from this round:

1. one CSV per experiment family
2. one JSON summary per experiment family
3. one canonical best-bundle archive
4. one markdown summary of final conclusions
