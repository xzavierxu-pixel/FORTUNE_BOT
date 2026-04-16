# Training Folder Refactor Implementation Plan
## 1. Goal
This document defines a unified staged implementation plan for the current `rule_baseline` cleanup work.
It has two layers:
- a structural refactor plan for `polymarket_rule_engine/rule_baseline/training`- a companion set of functional cleanup workstreams that should be implemented in controlled phases by the coding agent
Target outcome:
- `training/` keeps only the three offline pipeline top-level entry scripts.- Non-entry helpers are moved into directories whose names match their real responsibility.- `quality_check/` is explicitly left unchanged in this refactor.
The first layer is structural and focuses on folder responsibility.
The second layer is functional and covers the rule-training, feature, artifact, and experimental-code cleanup requirements that must be executed after or alongside the folder refactor.
## 2. Scope
### In scope
- Refactor `rule_baseline/training/` into a clearer entrypoint-only directory.- Move reusable modules and standalone helper CLIs into better-aligned packages.- Update imports, tests, docs, and workflow references after file moves.- Remove the obsolete strict rule trainer path.- Add staged functional cleanup workstreams for rule binning parity, history-window changes, serving lookup alignment, artifact path cleanup, leakage cleanup, and experiment retirement.
### Out of scope
- No feature semantics redesign.- No model-quality re-tuning.- No `quality_check/` directory changes.- No unrelated execution engine redesign beyond the explicitly requested feature-pruning follow-up.
## 2.1 Confirmed Operator Decisions
- Implementation starts with `Batch 1` only. Later batches remain planned work, not part of the first execution pass.
- `train_rules_naive_output_rule_strict.py` is already deleted in the repository and should be treated as a removed legacy path. Cleanup work should only remove stale references and cached residue expectations; do not recreate or archive the source file.
- For active-document updates, only the following docs should be edited in later functional batches: `FEATURE_INVENTORY.md`, `OFFLINE_PIPELINE_STEP_BY_STEP_GUIDE.md`, and `OFFLINE_PIPELINE_WORKFLOW_AND_USAGE_MAP.md`.
- Other historical planning or audit docs should not be content-edited; if they must be retained, move them into a legacy-docs area without rewriting their contents.
- No compatibility bridge should be kept for removed `naive_rules/` artifacts.
- Final implementation validation is expected to include a full rerun rather than only a targeted smoke check.
- `execution_engine/tests/test_autogluon_remaining_work.py` is considered removable as part of the later `market_structure_v2` retirement work.
## 3. Verified Current Baseline
Current files under `rule_baseline/training/`:
- `build_groupkey_feature_contract_preview.py`- `build_groupkey_feature_inventory.py`- `build_groupkey_runtime_report.py`- `build_groupkey_validation_reports.py`- `build_history_feature_artifacts.py`- `build_snapshot_training_audit.py`- `groupkey_reports.py`- `history_features.py`- `rule_generation_audit.py`- `snapshot_training_audit.py`- `train_rules_naive_output_rule.py`- `train_snapshot_model.py`
Verified pipeline entry usage:
- `run_pipeline.py` directly invokes: - `training/train_rules_naive_output_rule.py` - `training/train_snapshot_model.py` - `training/build_groupkey_validation_reports.py`
Verified non-entry classifications from current code and docs:
- `history_features.py` is a reusable aggregation library.- `rule_generation_audit.py` and `snapshot_training_audit.py` are reusable audit libraries.- `groupkey_reports.py` is a reusable report-generation library.- `build_groupkey_feature_contract_preview.py`, `build_groupkey_feature_inventory.py`, `build_groupkey_runtime_report.py`, `build_history_feature_artifacts.py`, and `build_snapshot_training_audit.py` are standalone or on-demand helper CLIs.- `train_rules_naive_output_rule_strict.py` has no active pipeline role and is already described in repository planning docs as an obsolete path.
Constraint confirmed for this refactor:
- `quality_check/feature_dqc.py` remains where it is and is not moved in this plan.
Verified repository nuance:
- `train_rules_naive_output_rule_strict.py` is already absent as a live source file in the current repository state; only stale references or cache artifacts may remain.
## 4. Target Directory Model
After the refactor, `rule_baseline/` should expose four clear responsibility zones:
- `training/`: only supported top-level offline pipeline entry scripts- `history/`: reusable history-statistics builders and related helper CLIs- `audits/`: reusable audit payload writers and audit CLIs- `reports/`: report-generation libraries and report helper CLIs
Recommended target tree:
```textrule_baseline/ training/ train_rules_naive_output_rule.py train_snapshot_model.py build_groupkey_validation_reports.py
 history/ __init__.py history_features.py build_history_feature_artifacts.py
 audits/ __init__.py rule_generation_audit.py snapshot_training_audit.py build_snapshot_training_audit.py
 reports/ __init__.py groupkey_reports.py build_groupkey_feature_contract_preview.py build_groupkey_feature_inventory.py build_groupkey_runtime_report.py
 quality_check/ feature_dqc.py
 legacy_docs/ ... historical planning and audit docs moved without content edits```
Notes:
- `build_groupkey_validation_reports.py` remains in `training/` even though it is report-oriented because it is a supported Step 6 pipeline entry.- `quality_check/` stays untouched to avoid mixing feature DQC work with this folder cleanup.- `train_rules_naive_output_rule_strict.py` should not remain in active packages.
## 5. File Migration Mapping
### 5.1 Keep in `training/`
- `train_rules_naive_output_rule.py`- `train_snapshot_model.py`- `build_groupkey_validation_reports.py`
### 5.2 Move to `history/`
- `history_features.py`- `build_history_feature_artifacts.py`
Reason:
- Both are specific to history-feature construction and artifact materialization.- They are not training entrypoints.
### 5.3 Move to `audits/`
- `rule_generation_audit.py`- `snapshot_training_audit.py`- `build_snapshot_training_audit.py`
Reason:
- All three files are audit-specific.- Two are reusable libraries, one is a standalone audit CLI.
### 5.4 Move to `reports/`
- `groupkey_reports.py`- `build_groupkey_feature_contract_preview.py`- `build_groupkey_feature_inventory.py`- `build_groupkey_runtime_report.py`
Reason:
- All four are report, preview, inventory, or governance outputs.- They are not part of the required train-model execution path.
### 5.5 Archive or delete
- `train_rules_naive_output_rule_strict.py`
Recommended handling:
- Treat it as already deleted.- Remove stale references, expectations, and cache residue assumptions only.- Do not recreate the file just to archive it.
## 6. Refactor Principles
### 6.1 Preserve behavior first
This refactor should not change pipeline semantics. It is a module relocation and import cleanup, not a training-logic rewrite.
### 6.2 Keep entrypoint names stable
The three supported pipeline entry scripts should keep their existing filenames so `run_pipeline.py`, README commands, and operator muscle memory stay stable.
### 6.3 Move libraries before moving references in docs
The correct sequence is:
1. create target packages2. move code files3. fix Python imports4. fix tests5. fix docs and examples
This avoids docs drifting ahead of executable code.
### 6.4 Treat helper CLIs differently from reusable modules
Library modules and helper CLI scripts should not be mixed in the same folder unless they share the same operational purpose.
## 7. Implementation Phases
## Phase A. Create target packages
### Objective
Establish the new package layout without changing behavior yet.
### Tasks
1. Create: - `rule_baseline/history/` - `rule_baseline/audits/` - `rule_baseline/reports/` - optionally `rule_baseline/_archive/`2. Add `__init__.py` files where appropriate.3. Do not move entry scripts in this phase.
### Acceptance criteria
- New directories exist.- Python imports can target them cleanly.
## Phase B. Move reusable library modules
### Objective
Move non-entry reusable modules out of `training/` first.
### Tasks
1. Move `history_features.py` to `history/`.2. Move `rule_generation_audit.py` and `snapshot_training_audit.py` to `audits/`.3. Move `groupkey_reports.py` to `reports/`.4. Update imports in: - `train_rules_naive_output_rule.py` - `train_snapshot_model.py` - `build_groupkey_validation_reports.py` - tests and any helper scripts
### Acceptance criteria
- Main pipeline still runs with unchanged CLI entrypoints.- Tests importing these modules pass under the new paths.
## Phase C. Move standalone helper CLIs
### Objective
Move on-demand tools out of `training/`.
### Tasks
1. Move `build_history_feature_artifacts.py` to `history/`.2. Move `build_snapshot_training_audit.py` to `audits/`.3. Move `build_groupkey_feature_contract_preview.py`, `build_groupkey_feature_inventory.py`, and `build_groupkey_runtime_report.py` to `reports/`.4. Update CLI examples in docs and README.5. Update tests that import these scripts directly.
### Acceptance criteria
- `training/` contains only the three intended entry scripts plus no extra helpers.- All moved helper CLIs remain runnable from their new paths.
## Phase D. Archive obsolete strict trainer
### Objective
Remove obsolete rule-training entrypoints from active surface area.
### Tasks
1. Confirm no live code imports `train_rules_naive_output_rule_strict.py`.2. Remove stale references and any assumptions that the source file still exists.3. Remove or update stale doc mentions.
### Acceptance criteria
- No active documentation presents `train_rules_naive_output_rule_strict.py` as a supported path.- No code imports remain.
## Phase E. Documentation and reference cleanup
### Objective
Make the repository layout self-describing again.
### Required updates
- `docs/OFFLINE_PIPELINE_WORKFLOW_AND_USAGE_MAP.md`- `docs/OFFLINE_PIPELINE_STEP_BY_STEP_GUIDE.md`- `README.md`
Historical planning or audit docs outside this active set should not be rewritten; if they must remain in-tree, move them into a legacy-docs location without changing their content.
### Required doc changes
- redefine `training/` as an entrypoint-only directory- explain new `history/`, `audits/`, and `reports/` responsibilities- mark `quality_check/` as intentionally unchanged- mark `train_rules_naive_output_rule_strict.py` as archived or removed
## 8. Import and Test Impact
Expected import changes:
- `rule_baseline.training.history_features` -> `rule_baseline.history.history_features`- `rule_baseline.training.rule_generation_audit` -> `rule_baseline.audits.rule_generation_audit`- `rule_baseline.training.snapshot_training_audit` -> `rule_baseline.audits.snapshot_training_audit`- `rule_baseline.training.groupkey_reports` -> `rule_baseline.reports.groupkey_reports`
Tests likely requiring updates:
- history feature tests- snapshot training audit tests- groupkey validation report tests- groupkey feature inventory tests- any tests importing helper CLI modules directly
Recommended validation set after the refactor:
1. tests covering `train_rules_naive_output_rule.py`2. tests covering `history_features.py`3. tests covering `train_snapshot_model.py`4. tests covering `snapshot_training_audit.py`5. tests covering `build_groupkey_validation_reports.py`6. tests covering `build_groupkey_feature_inventory.py`7. one smoke run of `workflow/run_pipeline.py` with heavy steps skipped where practical
Operator override:
- After implementation batches are complete, prefer a full rerun over a smoke-only validation pass.
## 9. Risks
### Risk 1. Doc path drift
This repo has many planning and audit docs with hardcoded file paths. A code-only move will leave a large amount of stale documentation unless doc cleanup is included in the same change set.
### Risk 2. Test imports pinned to old package names
Several tests import modules from `rule_baseline.training.*`. These will fail immediately after file moves unless updated atomically.
### Risk 3. Helper CLI discoverability drop
Moving helper scripts out of `training/` is correct structurally, but operators may lose discoverability if README and workflow docs are not updated at the same time.
### Risk 4. Partial archive of strict trainer
If the strict trainer is documented as removed but still remains in active code paths or docs, the cleanup will be incomplete and confusing.
## 10. Acceptance Criteria
This refactor is complete when all of the following are true:
- `training/` contains only: - `train_rules_naive_output_rule.py` - `train_snapshot_model.py` - `build_groupkey_validation_reports.py`- all moved files live under directories whose names reflect their responsibility- all Python imports and tests pass under the new module paths- docs consistently describe the new layout- `quality_check/feature_dqc.py` is unchanged and remains in `quality_check/`- `train_rules_naive_output_rule_strict.py` is archived or deleted from active paths
## 11. Recommended Execution Order
Use this order for the actual implementation PR:
1. add new directories and `__init__.py`2. move library modules3. fix imports in active entrypoints4. move helper CLIs5. update tests6. update docs7. archive or delete the strict trainer8. run targeted test suite
This order minimizes broken intermediate states and makes review easier.
## 12. Companion Functional Cleanup Workstreams
The following requirements are intentionally kept in the same document so a coding agent can execute them in controlled batches instead of treating them as unrelated one-off edits.
These workstreams are broader than the folder refactor. They touch training logic, feature semantics, artifact contracts, docs, tests, and selected execution-engine parity paths.
## Workstream F. Rule Training and Binning Parity Cleanup
### Objective
Remove stale rule-quality constraints, eliminate baseline-vs-Step-4 drift, redefine history windows, and make online fine-feature lookup use the same horizon-bucket semantics as training.
### Required changes
#### F1. Remove `MIN_RULE_EDGE_LOWER_BOUND_FULL >= 0.04`
Tasks:
1. Remove the `MIN_RULE_EDGE_LOWER_BOUND_FULL` constant definition.2. Remove all code paths that reference it.3. Update tests and docs that still describe this threshold as active.
Acceptance criteria:
- no active constant definition remains- no filtering logic still depends on this threshold- docs do not describe it as a supported rule gate
#### F2. Make `compare_baseline_families.py` reuse the exact Step 4 binning contract
Problem to fix:
- baseline flow should no longer rely on `build_rule_bins()` defaults- baseline flow currently risks drifting from Step 4 because defaults in `datasets/snapshots.py` still allow smaller steps and auto-selected bin widths
Required behavioral target:
- one single source of truth for: - `TRAIN_PRICE_MIN = 0.2` - `TRAIN_PRICE_MAX = 0.8` - `RULE_PRICE_BIN_STEP = 0.1`- same pre-binning filtering order as Step 4: - snapshot base cleaning - `quality_pass` - tradable price-range filter - split assignment - rule bin build
Implementation direction:
1. Do not leave `compare_baseline_families.py` calling `build_rule_bins(split_snapshots)` with bare defaults.2. Extract a shared helper for the Step 4 pre-binning path between `prepare_rule_training_frame()` and `build_rule_bins()`.3. Make both `train_rules_naive_output_rule.py` and `compare_baseline_families.py` call that shared helper.4. Ensure explicit passing of: - `min_price=0.2` - `max_price=0.8` - `price_bin_step=0.1`
Acceptance criteria:
- baseline rule bins and Step 4 rule bins use the same input filtering contract- bin step no longer depends on hidden defaults- future changes to Step 4 binning parameters require changing only one shared helper path
#### F3. Redefine history windows
Required target state:
- keep `expanding`- remove `recent_50`- remove `recent_200`- add `recent_90days`
Time-basis decision:
- `recent_90days` is defined per group using `closedTime`, not `snapshot_time`.
Semantic intent:
- For each grouping level, include all rows whose `closedTime` falls within the latest 90-day window for that group.
This intentionally preserves a settlement-time interpretation even though multiple horizons from the same market may enter the history frame.
Implementation scope:
1. Update history-window definitions in reusable history builders.2. Update all derived features and serving outputs that depend on window enumeration.3. Update schema expectations, tests, docs, and any inventory/report generators that hardcode old window names.
Acceptance criteria:
- only `expanding` and `recent_90days` remain as supported history windows- no derived feature or report still expects `recent_50` / `recent_200`
#### F4. Tighten `build_group_serving_features()` Step A
Required target state:
- Step A should start from only de-duplicated `group_key`- Step A must not carry forward any other rule-derived columns from `rules_df`
Specifically forbidden in the Step A skeleton:
- `group_unique_markets`- `group_snapshot_rows`- `group_market_share_global`- `group_median_logloss`- or any other non-`group_key` column directly copied from `rules_df`
Allowed sources for non-key group-serving fields after Step A:
- `history_feature_frames` merges- defaults / fallback logic
Acceptance criteria:
- `group_serving_features` non-key content is no longer seeded from `rules_df`- the only Step A carry-forward field is de-duplicated `group_key`
#### F5. Replace `round_horizon_hours()` lookup semantics in serving
Required target state in `features/serving.py`:
- do not round continuous horizon to arbitrary integers- map continuous horizon into the supported bucket set `[1, 2, 4, 6, 12, 24]`- use the training-side midpoint-boundary semantics aligned with `_derive_horizon_bounds()`
Target mapping:
- `0 - 1.5 -> 1`- `1.5 - 3 -> 2`- `3 - 5 -> 4`- `5 - 9 -> 6`- `9 - 18 -> 12`- `18+ -> 24`
Why:
- live `remaining_hours` should not miss fine-serving rows by rounding to unsupported keys such as `5` or `11`
Acceptance criteria:
- online fine-feature lookups use only supported training buckets- horizon lookup parity is aligned between training and serving
## Workstream G. Artifact Contract and Rule Output Cleanup
### Objective
Remove the obsolete `naive_rules/` artifact branch and make `trading_rules.csv` the only active rule master table while preserving the audit report under a new audit path.
### Required file set
This workstream explicitly includes at minimum:
- `rule_baseline/datasets/artifacts.py`- `rule_baseline/training/train_rules_naive_output_rule.py`- `rule_baseline/training/train_rules_naive_output_rule_strict.py`- all consumers, tests, docs, and README references
### Required changes
1. Remove the `naive_rules` folder contract entirely.2. Stop generating: - `naive_trading_rules.csv` - `naive_trading_rules.json`3. Keep `data/offline/edge/trading_rules.csv` as the only final rule master table.4. Preserve the current `naive_all_leaves_report.csv` content, but relocate it to: - `{offline}/audit/all_trading_rule_audit_report.csv`5. Remove: - `naive_rules_dir` - `rule_json_path` - all corresponding write logic6. Rename `rule_report_path` to the new audit CSV path and update all consumers.7. Update tests, README, and docs to remove `naive_rules/` references.8. Do not modify the schema of `trading_rules.csv`.9. Keep the new `all_trading_rule_audit_report.csv` as the audit-equivalent replacement for `naive_all_leaves_report.csv`.
Acceptance criteria:
- no active code path writes into `naive_rules/`- only `trading_rules.csv` remains as the rule master artifact- audit report is preserved under the new audit CSV path- all readers use the new contract
## Workstream H. Raw-Market Leakage Cleanup and Experiment Retirement
### Objective
Remove the remaining interaction features that directly depend on raw market terminal state and delete now-obsolete round-3 experiment code.
### Required changes
#### H1. Extend `DROP_COLS`
Add to `train_snapshot_model.py::DROP_COLS`:
- `log_horizon_x_liquidity`- `spread_over_liquidity`
Acceptance criteria:
- these two features no longer enter any offline model feature list generated via `DROP_COLS`
#### H2. Ensure downstream feature-list builders converge naturally
Required review set:
- `build_snapshot_training_audit.py`- `build_groupkey_feature_contract_preview.py`- `tune_snapshot_model.py`- `compare_calibration_methods.py`- `compare_baseline_families.py`
Requirement:
- do not add scattered special-case drops- make these consumers naturally converge through the updated `DROP_COLS` / feature-column construction path
Acceptance criteria:
- no downstream feature-list generator still emits either feature
#### H3. Execution engine parity cleanup
Required scope:
- inspect real online code paths, not docs only- inspect feature-contract alignment, live inference preparation, and pruning logic
Requirement:
- if execution still computes, preserves, or conditionally retains either feature, remove that logic- if execution only aligns to model `feature_contract`, update the source-side generation/pruning path and tests so the two features are not still built by default- if they exist only in docs and not in runtime code, document that explicitly in the final implementation report
Acceptance criteria:
- execution code no longer actively computes or retains these two features as live-path defaults- tests and docs reflect the new state
#### H4. Remove `market_structure_v2`
Required changes:
- delete the `market_structure_v2` branch from `rule_baseline/features/tabular.py`- delete docs describing it as a supported variant- delete experiment configuration references to it- do not alter default `interaction_features` behavior beyond the explicitly requested leakage cleanup
Acceptance criteria:
- `market_structure_v2` no longer exists as a supported code path or documented feature variant
#### H5. Remove `run_autogluon_round3_experiments.py`
Required changes:
- delete `rule_baseline/analysis/run_autogluon_round3_experiments.py`- remove stale references from docs and audit/report docs- if historical context is needed, convert those references into static historical notes instead of active script references
Acceptance criteria:
- the script no longer exists as an active runnable file- no docs present it as part of the supported workflow
#### H6. Documentation updates
Required doc set:
- `OFFLINE_PIPELINE_STEP_BY_STEP_GUIDE.md`- `FEATURE_INVENTORY.md`- `OFFLINE_PIPELINE_WORKFLOW_AND_USAGE_MAP.md`
Required content updates:
- document that `log_horizon_x_liquidity` and `spread_over_liquidity` are now in `DROP_COLS` because they directly depend on raw market terminal state- document that `market_structure_v2` has been removed- document that `run_autogluon_round3_experiments.py` has been deleted and is no longer runnable
#### H7. Testing and final reporting
Required deliverables from the implementation agent:
- update or remove affected tests- run directly relevant tests- final report must state: - files changed - whether any residual references remain - whether execution-engine cleanup was source-level complete or only contract-aligned due to repository constraints
## 13. Recommended Batch Plan For The Coding Agent
To reduce risk, the coding agent should not implement all workstreams in one undifferentiated change.
Recommended batch order:
### Batch 1. Structural refactor only
- execute Sections 4 through 11 of this document- keep behavior unchanged- do not mix in feature semantics changes
Confirmed execution decision:
- The first implementation pass should stop after `Batch 1`.
### Batch 2. Rule-training parity and serving alignment
- implement Workstream F- include tests and docs for: - binning parity - new history windows - group-serving Step A cleanup - horizon bucket remap
### Batch 3. Artifact contract cleanup
- implement Workstream G- include path contract updates, readers, tests, README, and docs
### Batch 4. Leakage cleanup and experiment retirement
- implement Workstream H- include `DROP_COLS`, execution-engine parity follow-up, variant removal, experiment-script deletion, and doc cleanup
This batch order is preferred because each batch has a coherent verification surface and minimizes accidental cross-breakage.
## 14. Global Acceptance Criteria For The Full Program
The combined structural + functional program is complete when all of the following are true:
- `training/` contains only the three supported offline entry scripts- `quality_check/` remains unchanged- Step 4 rule-binning parity is centralized and reused by baseline code- only `expanding` and `recent_90days` remain as supported history windows- group-serving Step A no longer seeds non-key columns from `rules_df`- serving horizon lookup uses supported training buckets instead of direct integer rounding- `naive_rules/` artifacts are removed from the active contract- `all_trading_rule_audit_report.csv` replaces `naive_all_leaves_report.csv` under the audit path- `log_horizon_x_liquidity` and `spread_over_liquidity` no longer enter offline or live-path default feature flows- `market_structure_v2` is removed- `run_autogluon_round3_experiments.py` is deleted- tests and docs are updated consistently
