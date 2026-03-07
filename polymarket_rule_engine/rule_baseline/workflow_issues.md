# Workflow Issues Log

This file records every bug/error encountered while executing the full pipeline from scratch.

## 2026-03-06

### Issue 1: `DtypeWarning` while rebuilding canonical merged raw markets

- Step: `python rule_baseline/data_collection/fetch_raw_events.py`
- Symptom: `rule_baseline/utils/raw_batches.py` emitted `DtypeWarning: Columns (...) have mixed types. Specify dtype option on import or set low_memory=False.`
- Impact: The raw batch completed successfully, but canonical merge rebuild was noisy and dtype inference was unstable.
- Fix: Read batch CSVs with `low_memory=False` in the raw batch loader to stabilize parsing for mixed-type columns.

### Issue 2: `build_snapshots.py` timed out before producing any output

- Step: `python rule_baseline/data_collection/build_snapshots.py`
- Symptom: Full snapshot generation against `206,584` merged raw markets exceeded the execution time limit and terminated before `data/processed/snapshots.csv` was written.
- Impact: The workflow could not advance to training/backtesting, and the original implementation lost all in-memory progress when interrupted.
- Fix: Refactor snapshot generation to flush chunk results to a partial file during execution and support resume from already-processed `market_id`s.

### Issue 3: Ensemble training exhausted memory while expanding categorical features

- Step: `python rule_baseline/training/train_snapshot_lgbm_v2.py`
- Symptom: `OneHotEncoder(sparse_output=False)` attempted to materialize a dense matrix of shape `(131622, 78945)` and failed with `numpy._core._exceptions._ArrayMemoryError: Unable to allocate 77.4 GiB`.
- Impact: The unified ensemble model could not train, so no model payload or predictions export were produced.
- Root Cause: The training feature set still included several high-cardinality categorical columns (`groupItemTitle`, `gameId`, `marketMakerAddress`, `outcome_pattern`) and raw date strings (`startDate`, `endDate`) that should not have been one-hot encoded in the v1 pipeline.
- Fix: Remove those columns, along with duplicate merge artifacts (`domain_market`, `market_type_market`) and `sub_domain`, from the model feature set. Keep the model focused on planned domain/category/market_type features plus numeric text, volume, duration, and rule priors.

### Issue 4: `load_raw_markets()` still emitted `DtypeWarning` on merged raw CSV

- Step: feature-table inspection during ensemble training triage
- Symptom: `rule_baseline/utils/data_processing.py` loaded `raw_markets_merged.csv` with the default pandas parser settings and emitted a mixed-dtype `DtypeWarning`.
- Impact: No immediate failure, but dtype inference remained unstable and polluted the training logs.
- Fix: Read `raw_markets_merged.csv` with `low_memory=False`.

### Issue 5: Backtest feature assembly was incompatible with the saved model payload

- Step: `python rule_baseline/backtesting/backtest_portfolio_qmodel.py`
- Symptom: The backtest failed with `KeyError: "['leaf_id', 'direction', 'group_key'] not in index"` when reusing the trained payload.
- Impact: The q-model backtest could not score candidates, so no equity curve or trade log was produced.
- Root Cause: `match_rules_for_day()` renamed rule metadata to `rule_leaf_id`, `rule_direction`, and `rule_group_key` for backtest bookkeeping, but `predict_q_pred()` passed that dataframe into the saved preprocessor without restoring the training-time feature names.
- Fix: In `predict_q_pred()`, create a model-input copy that maps `rule_leaf_id -> leaf_id`, `rule_direction -> direction`, and `rule_group_key -> group_key` before calling `preprocess_features()`.

### Issue 6: The first successful q-model backtest exposed target leakage in the training feature set

- Step: second run of `python rule_baseline/backtesting/backtest_portfolio_qmodel.py`
- Symptom: The backtest completed but produced impossible metrics: final bankroll around `2.48e20`, `100%` win rate, and zero drawdown.
- Impact: The backtest output was not credible and could not be used for strategy evaluation or downstream analysis.
- Root Cause: `train_snapshot_lgbm_v2.py` still allowed `r_std` and `delta_hours` into the model feature set. Both fields encode post-resolution information, so the model was effectively seeing the answer key.
- Fix: Exclude `r_std` and `delta_hours` from the training feature set, retrain the ensemble payload, then rerun predictions and backtesting.

### Issue 7: `analyze_raw_markets.py` still used the default CSV parser and emitted `DtypeWarning`

- Step: `python rule_baseline/analysis/analyze_raw_markets.py`
- Symptom: Reading `raw_markets_merged.csv` triggered the same mixed-dtype `DtypeWarning` seen earlier in the workflow.
- Impact: The analysis completed, but logs remained noisy and dtype inference was inconsistent with the rest of the pipeline.
- Fix: Read `raw_markets_merged.csv` with `low_memory=False` in `analyze_raw_markets.py`.

### Issue 8: Backtest output remains economically implausible even after removing obvious leakage features

- Step: rerun of `python rule_baseline/backtesting/backtest_portfolio_qmodel.py` after excluding `r_std` and `delta_hours`
- Symptom: The backtest no longer showed `100%` wins, but it still ended with a final bankroll of `1,053,229,337.86` from a `10,000` start over `1,559` trades.
- Impact: The backtest artifacts were produced, but they should be treated as research outputs rather than credible performance estimates.
- Suspected Causes: Remaining evaluation leakage or optimistic assumptions are still likely present, especially around rule selection on the same validation horizon and the aggressiveness of the current Kelly sizing / exposure settings.
- Status: Logged for follow-up. Not auto-fixed in this pass because it requires a design decision on how to separate rule selection, model validation, and backtest periods.

### Issue 9: Analysis surfaced unresolved date-quality anomalies in the raw and snapshot datasets

- Step: `python rule_baseline/analysis/analyze_raw_markets.py` and `python rule_baseline/analysis/analyze_snapshots.py`
- Symptom: The merged raw analysis found `2,960` markets with negative `(endDate - startDate)` duration, while the snapshots analysis found `544` unparseable `scheduled_end` values and `99,841` rows where `resolve_time < scheduled_end`.
- Impact: These anomalies do not stop the pipeline, but they weaken trust in time-based features, split boundaries, and any downstream interpretation that depends on event timing.
- Status: Logged for follow-up. Not auto-fixed in this pass because the right remediation depends on whether these are upstream Polymarket data issues, market-type exceptions, or parsing/schema problems in our ingestion logic.
