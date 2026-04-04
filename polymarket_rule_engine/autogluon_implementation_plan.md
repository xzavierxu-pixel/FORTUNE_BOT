# AutoGluon Q-Model Implementation Design
## 1. Document Status
- Status: Draft for implementation- Scope: `polymarket_rule_engine/rule_baseline` offline artifact generation and `execution_engine` online inference integration- Primary audience: engineering- Primary decision: online production deploys one AutoGluon-based calibrated `q` model; `residual_q` remains offline research baseline only
## 2. Executive Summary
This design updates the snapshot-model stack from the current custom sklearn-style ensemble payload to an AutoGluon-based `q` model while preserving the current execution semantics inside `execution_engine`.
The latest codebase has already moved part of the rule contract forward:
1. rule artifacts now expose a compact production schema centered on `q_full`, `p_full`, `edge_full`, `edge_std_full`, `edge_lower_bound_full`, and `rule_score`2. rule training also writes audit outputs such as `rule_funnel_summary.json`3. snapshot-model training and online inference are still built around a `joblib` dict payload and runtime `target_mode` branching4. `execution_engine` still expects a single model path that resolves to `ensemble_snapshot_q.pkl`
This implementation keeps the current feature pipeline, rule matching semantics, and downstream trading logic, but replaces the model backend and artifact contract for the production `q` path.
The target production shape is:
1. rule artifacts remain the authoritative structural filter and match contract for online inference2. offline feature generation remains owned by the repository, not by AutoGluon internals3. AutoGluon trains the production `q` model on the generated feature table4. training exports a directory-style runtime bundle instead of a single sklearn/joblib payload as the primary production artifact5. online runtime loads the bundle once, loads the predictor once, and persists it in memory once6. online runtime continues to output: - `q_pred` - `trade_value_pred` - `direction_model` - `edge_prob` - `f_exec` - `growth_score`7. `residual_q` remains available offline for research comparison and is removed from the production runtime contract
## 3. Problem Statement
The latest codebase still has three mismatches that need to be resolved.
### 3.1 Production model backend is still tied to custom sklearn payloads
Current model training in `rule_baseline/training/train_snapshot_model.py` still:
1. imports `fit_model_payload`, `fit_regression_payload`, `predict_probabilities`, and `predict_regression`2. saves a single `joblib` payload to `models/ensemble_snapshot_q.pkl`3. relies on the old feature-contract-in-payload design
Current online runtime in `execution_engine/online/scoring/rule_runtime.py` still:
1. loads the payload via `joblib.load()`2. expects a dict payload3. extracts feature columns directly from that dict
This design is incompatible with the intended AutoGluon runtime shape.
### 3.2 Rule logic and documentation are no longer aligned
The latest rule-training code in `rule_baseline/training/train_rules_naive_output_rule.py` differs from older assumptions and from parts of `WORKFLOW_AND_MODULES.md`.
Key current code facts are:
1. offline rules now use `n_all`, `wins_all`, and `p_all` as the main rule-definition basis2. offline selected-rule schema is written as `q_full`, `p_full`, `edge_full`, `edge_std_full`, `edge_lower_bound_full`, `rule_score`, and `n_full`3. online rule logic still has extra train/valid gating and direction-consistency checks, so offline and online are not actually identical today4. audit outputs now include funnel reporting under `audit/rule_funnel_summary.json`
We already decided this divergence should be removed and that online rule logic should match offline rule logic.
### 3.3 Current online model training and documentation are inconsistent
The latest `train_snapshot_model.py` currently does this in `artifact_mode == "online"`:
1. keeps only `train` and `valid` rows after split assignment2. then sets `df_train = df_feat.copy()`3. sets `df_valid` to an empty frame4. therefore trains on all retained online rows and effectively disables validation-based calibration
This is materially different from the older workflow description that says online should fit on train and calibrate on recent valid.
The implementation needs to make this explicit and replace it with a deliberate production policy rather than keep the accidental current behavior.
## 4. Current Code Reality
This section records the latest repository behavior as of the current branch and should be treated as the factual baseline for implementation.
### 4.1 Current rule artifact generation
Primary files:
1. `rule_baseline/training/train_rules_naive_output_rule.py`2. `rule_baseline/datasets/snapshots.py`3. `rule_baseline/datasets/splits.py`4. `rule_baseline/datasets/artifacts.py`
Current behavior:
1. `prepare_rule_training_frame()` still computes split labels using `compute_artifact_split()`2. offline mode still produces `train/valid/test` split labels3. online mode still produces `train/valid` split labels4. offline `build_rule_bins()` still uses `train+valid` as the bin reference set, while online uses all retained rows as the bin reference set5. the latest offline rule-selection branch now selects rules using full labeled history statistics for each bucket6. the latest online rule-selection branch still requires train/valid minimum counts and direction consistency, but writes the same compact `q_full`-style rule schema6. rule outputs now include: - `edge/trading_rules.csv` - `naive_rules/naive_trading_rules.csv` - `naive_rules/naive_all_leaves_report.csv` - `metadata/rule_training_summary.json` - `audit/rule_funnel_summary.json`
Current production rule schema columns are:
1. `group_key`2. `domain`3. `category`4. `market_type`5. `leaf_id`6. `price_min`7. `price_max`8. `h_min`9. `h_max`10. `direction`11. `q_full`12. `p_full`13. `edge_full`14. `edge_std_full`15. `edge_lower_bound_full`16. `rule_score`17. `n_full`
### 4.2 Current snapshot-model training
Primary files:
1. `rule_baseline/training/train_snapshot_model.py`2. `rule_baseline/models/tree_ensembles.py`3. `rule_baseline/features/tabular.py`4. `rule_baseline/datasets/artifacts.py`
Current behavior:
1. snapshots are filtered by price range and `quality_pass`2. split assignment still uses `compute_artifact_split()`3. matched rules are loaded from `trading_rules.csv`4. `train_snapshot_model.py` now expects the rule file to contain `q_full` rather than the older `q_smooth`5. feature inputs are still generated by `preprocess_features()` over rule-matched snapshots plus `market_feature_cache`6. model training still uses the custom ensemble payload path7. `artifact_mode == "online"` still collapses train and valid into a single training frame and does not keep a live validation frame for calibration8. model artifacts are still written as a single payload file at `models/ensemble_snapshot_q.pkl`
### 4.3 Current online runtime integration
Primary files:
1. `execution_engine/online/pipeline/prewarm.py`2. `execution_engine/online/scoring/rule_runtime.py`3. `execution_engine/online/scoring/live.py`4. `execution_engine/runtime/config.py`
Current behavior:
1. `PegConfig` still resolves the default production model path to `ensemble_snapshot_q.pkl`2. prewarm still loads rules and model payload once at process startup3. model payload is still cached in-memory as a dict4. online inference still aligns rows to a feature contract extracted from the payload dict5. online inference still branches on `target_mode`6. `q` and `residual_q` are still both part of the live code path today
This means the latest repository has not yet switched the runtime contract to a directory-style AutoGluon bundle.
## 5. Product Decision
The implementation decisions are:
1. online production serves exactly one model objective: calibrated `q`2. `residual_q` stays in the repository only as an offline research baseline3. rule logic must be unified so that online and offline use the same definition and estimation logic4. production rules should use the latest `q_full` schema as the single authoritative runtime rule schema5. feature generation remains in repository code and is not delegated to hidden AutoGluon transforms upstream of the exported feature contract6. the production model artifact must move from a single sklearn/joblib dict payload to an explicit AutoGluon runtime bundle7. online deployment must optimize for latency with long-lived process preload and `persist()`8. production `q` calibration remains required and should keep the current grouped-calibration intent unless measurement shows a better option
## 6. Goals
The implementation must achieve the following:
1. replace the production `q` model backend with AutoGluon2. preserve the current online rule-match and trading semantics3. unify online and offline rule logic4. remove `residual_q` responsibility from live production code5. introduce an explicit runtime-bundle artifact contract for production inference6. keep feature-contract enforcement explicit in the online path7. support a production calibration step for the deployed `q` model8. define a clear data-window policy for offline evaluation versus deploy-time retraining
## 7. Non-Goals
This design does not aim to:
1. redesign the strategy into a true two-stage model2. replace the current rule feature space or feature-engineering logic3. change `compute_trade_value_from_q`, `compute_growth_and_direction`, selection, or stake allocation semantics4. keep `expected_pnl` or `expected_roi` as online runtime modes5. preserve runtime support for arbitrary research `target_mode` switching in production6. eliminate `residual_q` from offline comparison scripts in this phase
## 8. Target Architecture
### 8.1 Rule architecture
The target rule architecture is:
1. one authoritative rule schema for both offline and online artifacts2. one authoritative rule-selection logic for both offline and online artifacts3. offline may still keep additional evaluation columns and test-only diagnostics in reports4. online should consume the same selected-rule semantics as offline, not a separate special-case branch
Required outcome:
1. `train_rules_naive_output_rule.py` should no longer produce different logical rule-selection criteria depending on artifact mode2. offline and online should differ only in split availability and reporting, not in the meaning of selected rules
### 8.2 Model architecture
The target model architecture is:
1. production deploys one `q` predictor bundle2. offline research may still train `residual_q`3. online runtime calls one adapter API that returns calibrated `q_pred`4. downstream trade-value and growth logic remains unchanged
### 8.3 Artifact architecture
The production model artifact should become a directory-style runtime bundle.
Recommended bundle structure:
```textmodels/ q_model_bundle/ runtime_manifest.json feature_contract.json predictor/ calibration/ calibrator.pkl calibrator_meta.json metadata/ model_training_summary.json deployment_summary.json```
Required runtime-manifest fields:
1. artifact version2. model family3. target mode4. label column5. predictor path6. predictor name or deployed-model name7. whether `refit_full()` was used8. whether deployment optimization was used9. split boundaries10. calibration mode
### 8.4 Online loading architecture
The target runtime flow is:
1. `execution_engine` loads the runtime bundle once at startup2. `execution_engine` loads the predictor once via `TabularPredictor.load()`3. `execution_engine` calls `persist()` once at startup4. `execution_engine` loads feature contract and calibrator metadata once5. online batches only align features and run probability inference
The online path must not:
1. call predictor load per batch2. retain live branching for research-only target modes3. depend on implicit feature discovery at inference time
## 9. Data Policy
### 9.1 Unified rule policy
We have explicitly chosen to make online and offline rule logic consistent.
That means:
1. the rule-definition formula must be the same in both modes2. the rule-estimation source must be the same in both modes3. split-specific diagnostics may still be computed for reporting and evaluation4. test data in offline mode should be treated as evaluation-only, not as a reason to keep a separate production rule-selection formula
Practical implication:
1. offline keeps `test` to evaluate rule generalization2. offline and online should both use the full retained snapshot set as the rule-bin reference set because bin construction is label-free and should not drift between modes3. deployable rule artifacts can still be built from the full pre-cutoff labeled history once the rule logic has been validated offline4. online should not keep a separate train/valid-gated branch if offline selected rules are already defined differently
Explicit rule-bin policy:
1. `price_bin` and `horizon_bin` construction should use the full retained snapshot frame for both offline and online rule training2. this is acceptable because bin construction depends on observable snapshot-time structure such as `price`, `horizon_hours`, and per-group market counts, not on future labels3. this does introduce future distribution information into offline discretization, but that tradeoff is accepted in exchange for keeping offline and online rule contracts identical
### 9.2 Offline model-evaluation policy
Offline evaluation remains the place to choose the production approach.
Offline evaluation must keep:
1. train2. valid3. test
Offline evaluation uses these windows to decide:
1. feature set2. AutoGluon fit options3. latency constraints such as `infer_limit`4. whether to use `refit_full()` for deployment candidate creation5. calibration strategy
### 9.3 Deploy-time model-training policy
Deploy-time production model generation should use more data than strict offline train-only fitting, but must preserve a legal calibration policy.
Recommended default production policy:
1. train the deployable predictor on the full pre-cutoff history except the calibration tail window2. reserve a recent tail window for calibration fitting3. after validation, optionally create a `refit_full()` deployment candidate
This replaces the accidental current behavior where online mode simply merges train and valid into one training frame and silently drops validation-based calibration.
### 9.4 Calibration policy for maximum data use
Preferred production calibration order:
1. default: tail-window holdout grouped isotonic calibration2. advanced option: time-aware out-of-fold calibration followed by final full retrain
Not allowed as production default:
1. training and calibrating on exactly the same rows without an explicit OOF procedure
Grouped calibration should keep two safeguards:
1. minimum row threshold per group2. fallback from grouped calibration to global calibration when groups are too sparse
## 10. Detailed Implementation Design
### 10.1 Rule-training changes
Primary files:
1. `rule_baseline/training/train_rules_naive_output_rule.py`2. `rule_baseline/datasets/snapshots.py`3. `rule_baseline/WORKFLOW_AND_MODULES.md`
Required changes:
1. remove the logical divergence between offline and online rule selection2. make one branch authoritative for rule definition3. keep split-specific metrics in reports and summaries4. change offline rule-bin reference construction to use the full retained snapshot frame, matching online behavior5. update workflow documentation so it matches the latest rule code
Expected selected-rule contract:
1. selected rules always expose the compact `q_full` schema2. runtime matching always uses the same fields and meaning across offline and online artifacts
### 10.2 Snapshot-model training changes
Primary files:
1. `rule_baseline/training/train_snapshot_model.py`2. `rule_baseline/models/tree_ensembles.py` or a new `rule_baseline/models/autogluon_qmodel.py`3. `rule_baseline/datasets/artifacts.py`4. optional new runtime-bundle helper module
Required changes:
1. replace the production `q` training path with AutoGluon binary classification2. make `q` the only deployable online target mode3. keep `residual_q` training available only for offline comparison if retained at all4. export a runtime bundle rather than a single sklearn/joblib payload for the production path5. export the feature contract as standalone metadata6. export calibration state separately from the predictor7. stop relying on the current online-mode shortcut that trains on all retained rows and keeps no validation frame
### 10.3 Execution-engine integration changes
Primary files:
1. `execution_engine/online/scoring/rule_runtime.py`2. `execution_engine/online/pipeline/prewarm.py`3. `execution_engine/online/scoring/live.py`4. `execution_engine/runtime/config.py`5. `execution_engine/requirements-live.txt`
Required changes:
1. replace `joblib` dict loading with runtime-bundle loading for the production `q` path2. preserve feature-contract extraction as an explicit runtime step3. preload and persist the predictor at process startup4. simplify live inference to one production path for `q`5. remove online `residual_q` branching from live inference
### 10.4 Backtesting changes
Primary files:
1. `rule_baseline/backtesting/backtest_portfolio_qmodel.py`2. `rule_baseline/backtesting/backtest_execution_parity.py`3. any helper modules used to load model artifacts or run offline inference
Required changes:
1. update backtesting model loading so it can consume the AutoGluon production runtime bundle rather than only `joblib` dict payloads2. keep backtesting rule loading aligned with the unified `q_full` rule schema3. make production-parity backtests run through the same q-only inference contract intended for `execution_engine`4. remove production backtesting dependence on live `target_mode` branching for `expected_pnl` and `expected_roi`5. keep `residual_q` available only where it is explicitly needed as an offline research comparison baseline6. ensure decision-level parity metrics remain comparable before and after the runtime-bundle migration
Design rule:
1. production-parity backtesting should validate the same rule contract, model contract, and post-processing semantics that online runtime will use2. research backtesting may still compare `q` and `residual_q`, but this must not leak back into the production runtime contract
### 10.5 Analysis changes
Primary files:
1. `rule_baseline/analysis/analyze_q_model_calibration.py`2. `rule_baseline/analysis/analyze_alpha_quadrant.py`3. `rule_baseline/analysis/analyze_rules_alpha_quadrant.py`4. `rule_baseline/analysis/compare_calibration_methods.py`5. `rule_baseline/analysis/compare_baseline_families.py`6. `rule_baseline/analysis/analyze_qmodel_trades.py`
Required changes:
1. update analysis scripts that load model artifacts so they can read the AutoGluon production runtime bundle rather than assuming a sklearn/joblib dict payload2. keep prediction-table-driven scripts compatible with the new production outputs as long as they still expose `price`, `y`, and `q_pred`3. keep rule-analysis scripts aligned to the unified `q_full` rule schema and unified full-data bin policy4. make calibration-comparison scripts use the new AutoGluon q-training path instead of `fit_model_payload` once the production backend is migrated5. keep `compare_baseline_families.py` explicitly marked as research-only because it still depends on residual and two-stage experiments that are outside the production runtime contract6. keep `residual_q` available only where a script's purpose is offline model-family comparison, not production parity validation
Expected classification of analysis scripts:
1. light adaptation only: - `analyze_q_model_calibration.py` - `analyze_alpha_quadrant.py` - `analyze_rules_alpha_quadrant.py` - `analyze_qmodel_trades.py`2. substantive refactor required: - `compare_calibration_methods.py` - `compare_baseline_families.py`
Design rule:
1. analysis scripts that consume exported predictions or backtest outputs may remain lightweight if those outputs preserve their current column contract2. analysis scripts that fit models directly must be updated to the same backend and data-window policy used by the new implementation
### 10.6 Workflow and documentation changes
Primary files:
1. `polymarket_rule_engine/README.md`2. `rule_baseline/WORKFLOW_AND_MODULES.md`3. any deployment docs that describe model artifact format
Required changes:
1. update rule-training documentation to match the actual latest logic2. document the production model as q-only3. document the deploy-time data-window policy separately from offline evaluation4. document the new runtime bundle format and online load behavior
## 11. Runtime Adapter Contract
The runtime adapter should expose a minimal interface suitable for `execution_engine`.
Required responsibilities:
1. load runtime manifest2. load predictor3. persist predictor4. load feature contract5. align DataFrame columns to feature contract6. run `predict_proba()`7. apply calibrator
Not allowed responsibilities:
1. feature generation2. rule matching3. trade-value conversion4. direction and growth computation5. switching among research-only modes
## 12. Execution Semantics That Must Remain Unchanged
The following must remain unchanged in this implementation:
1. price and horizon rule matching semantics2. current feature-table construction semantics3. `compute_trade_value_from_q`4. `compute_growth_and_direction`5. `select_target_side`6. allocation and submission logic
This implementation changes the source of `q_pred`, not the semantics of what the trading system does with `q_pred`.
## 13. Migration Plan
### 13.1 Phase 1: Make current-state documentation and rule policy consistent
1. update workflow docs to match the latest rule and model code reality2. unify offline and online rule-selection logic3. keep current runtime stable while rule semantics are clarified
### 13.2 Phase 2: Add AutoGluon production training path
1. implement AutoGluon `q` training path2. export runtime bundle3. keep old payload path temporarily for comparison if needed4. keep offline metrics comparable to the existing outputs
### 13.3 Phase 3: Add production runtime adapter
1. add bundle loader and predictor preload logic2. persist predictor at startup3. keep downstream scoring unchanged4. add shadow inference support if needed during migration
### 13.4 Phase 4: Remove research-only production branches
1. remove online `residual_q` support2. remove online `expected_pnl` and `expected_roi` support3. simplify production config and runtime loading
### 13.5 File-Level Implementation Checklist
This checklist is the intended execution order for implementation.
#### Batch A: Unify rule logic and inputs
1. Update `rule_baseline/datasets/snapshots.py` - change offline `bin_source` to use the full retained snapshot frame - keep funnel reporting explicit about the new full-data bin reference behavior2. Update `rule_baseline/training/train_rules_naive_output_rule.py` - remove the offline/online selection-logic split - keep one authoritative rule-selection formula - preserve `q_full` rule schema as the only selected-rule contract3. Update `rule_baseline/WORKFLOW_AND_MODULES.md` - remove outdated statements about offline `train+valid`-only bin or rule definition behavior - document unified rule logic across offline and online4. Validate rule artifacts - regenerate `trading_rules.csv` - inspect `rule_training_summary.json` - inspect `rule_funnel_summary.json`
#### Batch B: Introduce AutoGluon production q training
1. Add a production training module such as `rule_baseline/models/autogluon_qmodel.py` - implement AutoGluon q-model fit - implement q-only probability inference adapter - implement bundle save/load helpers or call into a dedicated bundle module2. Add a runtime bundle helper module if needed - define manifest schema - define feature-contract serialization - define calibration artifact serialization3. Update `rule_baseline/training/train_snapshot_model.py` - route production `q` training through AutoGluon - keep offline `residual_q` path only if explicitly needed for research - stop treating online mode as train-plus-valid merged without calibration policy4. Update `rule_baseline/datasets/artifacts.py` - add explicit paths for the production bundle if the directory layout changes - keep legacy paths only if temporary migration support is required5. Validate training outputs - regenerate predictions - inspect feature contract - inspect model bundle contents - inspect training summary metadata
#### Batch C: Implement production calibration policy
1. Add calibration fitting and persistence for the AutoGluon q path - grouped isotonic default - sparse-group fallback behavior2. Update training summaries - record calibration mode - record calibration window policy - record whether grouped fallback occurred3. Validate calibration - compare raw and calibrated predictions - verify horizon-bucket calibration tables
#### Batch D: Switch execution_engine to runtime bundle loading
1. Update `execution_engine/runtime/config.py` - support new default model-bundle path resolution - keep environment overrides explicit2. Update `execution_engine/online/scoring/rule_runtime.py` - replace `joblib` dict loading with bundle loading - expose the same feature-contract concept through the new adapter3. Update `execution_engine/online/pipeline/prewarm.py` - preload the predictor bundle once - call `persist()` once during startup4. Update `execution_engine/online/scoring/live.py` - remove production `target_mode` branching - call q-only probability inference through the runtime adapter5. Update `execution_engine/requirements-live.txt` - add required AutoGluon runtime dependencies - remove obsolete production-only dependencies if possible after migration6. Validate runtime behavior - confirm preload works - confirm `persist()` succeeds - confirm q-only inference produces expected columns
#### Batch E: Align backtesting with the new production contract
1. Update `rule_baseline/backtesting/backtest_portfolio_qmodel.py` - replace direct `joblib` payload assumptions with bundle loading for the production q path - keep q-only production inference contract - keep residual baseline support only if explicitly marked research-only2. Update `rule_baseline/backtesting/backtest_execution_parity.py` - ensure parity path uses the same production q inference contract intended for runtime3. Update any shared helper used by backtesting model loading4. Validate backtesting - run production-parity path on the new bundle - confirm rule schema compatibility - compare decision overlap versus previous production baseline
#### Batch F: Align analysis scripts
1. Update `rule_baseline/analysis/analyze_q_model_calibration.py` - verify predictions-based flow still works unchanged or adjust output paths if needed2. Update `rule_baseline/analysis/analyze_alpha_quadrant.py` - verify predictions-based flow still works with the new outputs3. Update `rule_baseline/analysis/analyze_rules_alpha_quadrant.py` - ensure unified `q_full` rule schema is assumed explicitly4. Update `rule_baseline/analysis/analyze_qmodel_trades.py` - verify backtest output columns still match expected semantics5. Update `rule_baseline/analysis/compare_calibration_methods.py` - move from `fit_model_payload` to the AutoGluon q training path - preserve isolated calibration-comparison logic6. Update `rule_baseline/analysis/compare_baseline_families.py` - keep it marked research-only - update its direct model fitting to match the new backend where needed7. Validate analysis layer - run calibration, alpha, and rule analysis scripts - confirm research-only scripts do not dictate production contract assumptions
#### Batch G: Final cleanup and documentation
1. Update `polymarket_rule_engine/README.md` - document q-only production deployment - document residual_q as offline research baseline only - document runtime bundle layout2. Reconcile `rule_baseline/WORKFLOW_AND_MODULES.md` - ensure rule, model, analysis, and backtesting descriptions match actual implementation3. Remove obsolete production branches - retire online `expected_pnl` and `expected_roi` - retire production dependence on generic `target_mode` branching4. Final acceptance run - regenerate offline artifacts - run backtests - run key analysis scripts - verify execution_engine runtime loads the new bundle cleanly
## 14. Validation Plan
Validation must happen at four levels.
### 14.1 Rule-level validation
Required checks:
1. offline and online selected-rule logic produce the same semantics for the same input history2. rule schema remains compatible with online runtime matching3. rule funnel summaries remain correct after logic unification4. offline and online use the same full-data rule-bin reference construction
### 14.2 Model-level validation
Required checks:
1. log loss2. Brier score3. AUC4. calibration quality by horizon bucket5. comparison between old ensemble baseline and AutoGluon `q`
### 14.3 Decision-level validation
Required checks on the same candidate set:
1. `q_pred` distribution2. `edge_prob` distribution3. `direction_model` behavior4. `growth_score` distribution5. viable-candidate count6. selected-for-submission overlap rate
This layer is mandatory because production consumes post-processed scores, not raw probability metrics alone.
### 14.4 Backtesting-level validation
Required checks:
1. `backtest_portfolio_qmodel.py` can evaluate the new production bundle end to end2. `backtest_execution_parity.py` remains compatible with unified rule schema and q-only production inference3. offline backtests can still compare research baselines such as `residual_q` without reintroducing those branches into production runtime
### 14.5 Analysis-level validation
Required checks:
1. calibration and alpha analysis scripts still run successfully against the new prediction artifacts2. rule analysis still matches the unified `q_full` rule schema and full-data bin policy3. research-only comparison scripts clearly remain outside the production runtime contract4. no analysis script silently assumes the old sklearn/joblib payload path after the production migration
### 14.6 Deployment-level validation
Required checks:
1. startup load time2. predictor persist success3. memory footprint4. latency for batch size 1 and small micro-batches5. latency and quality comparison between original best model and any `refit_full()` deployment candidate
## 15. Acceptance Criteria
The implementation is complete when all of the following are true.
1. rule online and offline logic are unified2. offline training can produce a deployable AutoGluon-based `q` bundle3. `execution_engine` can load the bundle once and keep it resident in memory4. online inference no longer requires `residual_q` logic5. downstream selection and allocation semantics remain intact apart from expected score changes6. production docs state clearly that only `q` is deployed online7. `residual_q` remains usable offline as a research baseline8. deployment latency and memory are measured and accepted
## 16. Risks and Mitigations
### 16.1 Rule-policy migration risk
Risk:
Unifying rule logic may change which rule buckets survive selection compared with current online artifacts.
Mitigation:
1. compare old and new rule sets side by side2. preserve funnel reporting during migration3. validate downstream market coverage before cutover
### 16.2 Predictor size and dependency weight
Risk:
AutoGluon may increase artifact size and runtime dependency weight.
Mitigation:
1. use deployment optimization options2. evaluate `refit_full()` candidates3. measure startup and memory explicitly
### 16.3 Calibration mismatch after backend change
Risk:
The best AutoGluon raw predictor may interact differently with grouped calibration than the current ensemble.
Mitigation:
1. compare raw and calibrated outputs separately2. evaluate grouped and global alternatives offline3. do not assume the previous default remains best without measurement
### 16.4 Hidden feature-contract drift
Risk:
AutoGluon may accept wider inputs than intended if schema enforcement is relaxed.
Mitigation:
1. export and enforce explicit feature contract2. keep online default-fill behavior explicit3. test missing-column alignment directly
## 17. Future Work
This design intentionally stops short of the following future directions.
1. a true two-stage production model that predicts tradeability first and edge magnitude second2. formal all-samples versus rule-only model-training experiments as a production redesign3. deeper identity-feature ablation studies4. lower-level serving optimizations beyond the standard AutoGluon predictor deployment path
These remain valid future directions but are outside this implementation.