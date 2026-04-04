# Rule Baseline Workflow And Modules

This file describes the current `rule_baseline` workflow after the refactor.

## 1. Main workflow

Run the full offline pipeline:

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline
```

Run a full offline pipeline with raw/snapshot rebuilds:

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline --full-refresh-fetch --full-refresh-snapshots
```

Run the online-style pipeline:

```powershell
python rule_baseline/workflow/run_online_pipeline.py
```

That is equivalent to:

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode online --skip-backtest --skip-baselines
```

The pipeline runs market annotations by default between raw fetch and snapshot build.
Use `--skip-annotations` only when you intentionally want to reuse an existing annotation artifact.

Hard rule:
- In `offline`, `test` is evaluation-only. It must never influence rule definition, model fitting, or calibration. It is reserved for final diagnostics and backtesting only.
- Rule-bin construction is now unified across `offline` and `online`: both modes derive price/horizon bins from the full retained labeled snapshot frame after quality filtering and split assignment.

Mode boundary table:

| Stage | Offline | Online | Live |
|---|---|---|---|
| Raw fetch | Resolved markets only | Resolved markets only | Unresolved live markets |
| Snapshot build | Resolved markets only | Resolved markets only | No training snapshots |
| Rule bins | all retained labeled rows (`train + valid + test`) | all retained labeled rows (`train + valid`) | read-only |
| Rule existence / bucket eligibility | all retained labeled rows | all retained labeled rows | read-only |
| Rule scoring / parameter estimation | all retained labeled rows | all retained labeled rows | read-only |
| Model fit | `train` only | `train` only | no fit |
| Calibration fit | `valid` only | latest 20-day `valid` only | no calibration fit |
| Final evaluation / backtest | `test` only | none | none |
| Published predictions | `test` diagnostics only | all labeled rows for diagnostics; serving artifacts for live use | score current unresolved markets |

## 2. Step-by-step workflow

### Step 1: Fetch raw markets

Script:

```powershell
python rule_baseline/data_collection/fetch_raw_events.py
```

Full refresh:

```powershell
python rule_baseline/data_collection/fetch_raw_events.py --full-refresh
```

Purpose:
- Fetch raw market batches from the source API
- Save batch files and update the batch manifest
- Rebuild the merged raw market table
- `--full-refresh` clears existing raw batches and rebuilds from `history_start`

Main data layer used:
- `rule_baseline/datasets/raw_market_batches.py`

### Step 2: Build market annotations

Script:

```powershell
python rule_baseline/domain_extractor/build_market_annotations.py
```

Purpose:
- Parse `source_url`
- Resolve `domain`, `category`, and `market_type`
- Derive `market_type` only from `outcomes`
- Collapse non-structured outcome combinations into `other`
- Override raw `category` with parsed `category` when parsed value is valid
- Save the canonical market annotation table
- Save an audit table of `other` outcome patterns by URL

Main data layer used:
- `rule_baseline/domain_extractor/market_annotations.py`

### Step 3: Build snapshot dataset

Script:

```powershell
python rule_baseline/data_collection/build_snapshots.py
```

Full refresh:

```powershell
python rule_baseline/data_collection/build_snapshots.py --full-refresh
```

Purpose:
- Build snapshot rows for each market/horizon
- Use `closedTime` as the actual market close timestamp
- Use `closedTime - startDate` as actual tradeable duration
- Save append-only snapshot batches plus canonical merged snapshot artifacts
- `--full-refresh` clears existing snapshot batches and rebuilds from all merged raw markets

Main data layer used:
- `rule_baseline/datasets/raw_market_batches.py`
- `rule_baseline/datasets/snapshot_batches.py`
- `rule_baseline/datasets/snapshots.py`

### Step 4: Train rules

Script:

```powershell
python rule_baseline/training/train_rules_naive_output_rule.py --artifact-mode offline
```

Purpose:
- Load enriched snapshots
- Consume the canonical market annotation artifact built earlier in the pipeline
- Apply rule bins on price and horizon
- In `offline`, split into train/valid/test and keep `test` for evaluation only
- In `online`, split into train/valid with the last 20 days as valid
- Use one unified rule-selection formula in both modes
- Select rule buckets from full labeled-history statistics with the compact `q_full` runtime schema

Main data layer used:
- `rule_baseline/datasets/snapshots.py`
- `rule_baseline/datasets/splits.py`
- `rule_baseline/datasets/artifacts.py`

### Step 5: Train the snapshot model

Script:

```powershell
python rule_baseline/training/train_snapshot_model.py --artifact-mode offline
```

Current default combination:
- predictor presets: `medium_quality`
- calibration mode: `grouped_isotonic`

Purpose:
- Match snapshots to selected rules
- Build the model feature table
- Train the production `q` model with AutoGluon Tabular
- In `offline`, fit on train and calibrate on valid
- In `online`, fit on train and use the most recent 20 days as valid for calibration
- Export predictions and a directory-style runtime bundle for the production `q` path
- Keep `residual_q` / `expected_*` only as offline research payloads

Main layers used:
- Data: `rule_baseline/datasets/`
- Features: `rule_baseline/features/`
- Models: `rule_baseline/models/`

### Step 6: Analyze and backtest

Analysis scripts:

```powershell
python rule_baseline/analysis/analyze_q_model_calibration.py --artifact-mode offline
python rule_baseline/analysis/analyze_alpha_quadrant.py --artifact-mode offline
python rule_baseline/analysis/analyze_rules_alpha_quadrant.py --artifact-mode offline
python rule_baseline/analysis/compare_baseline_families.py --artifact-mode offline
python rule_baseline/analysis/compare_calibration_methods.py
```

Default backtest script:

```powershell
python rule_baseline/backtesting/backtest_execution_parity.py --artifact-mode offline
```

Purpose:
- Evaluate calibration and alpha behavior
- Compare baseline families
- Simulate execution-parity trading and realized PnL
- Use the `offline` `test` window only as the final out-of-sample backtest window

## 3. Module layout

### `rule_baseline/datasets`

Purpose:
- Define canonical data semantics
- Centralize loading, annotation resolution, batch storage, split logic, and artifact paths

Files:
- `artifacts.py`: artifact output paths and JSON writing
- `splits.py`: temporal split logic
- `snapshots.py`: snapshot loading, enrichment, quality flags, rule bins
- `raw_market_batches.py`: raw batch manifest, batch writes, merged raw market rebuild

### `rule_baseline/features`

Purpose:
- Build reusable tabular features

Files:
- `market_feature_builders.py`: market-level feature construction from raw markets
- `tabular.py`: market feature cache and model feature preprocessing

### `rule_baseline/models`

Purpose:
- Keep model training and inference logic in one place

Files:
- `autogluon_qmodel.py`: AutoGluon `q` training and bundle export
- `runtime_bundle.py`: runtime-bundle paths and metadata helpers
- `runtime_adapter.py`: shared model-loader and q-only inference adapter
- `scoring.py`: shared q/trade-value conversion helpers
- `tree_ensembles.py`: legacy ensemble classifier/regressor helpers for research-only paths

### `rule_baseline/training`

Purpose:
- Pipeline entrypoints for model/rule training

Files:
- `train_rules_naive_output_rule.py`: current main rule trainer
- `train_snapshot_model.py`: current main snapshot model trainer

### `rule_baseline/analysis`

Purpose:
- Offline diagnostics and comparison scripts

Files:
- `analyze_q_model_calibration.py`: calibration metrics from exported predictions
- `analyze_alpha_quadrant.py`: alpha quadrant analysis
- `analyze_rules_alpha_quadrant.py`: rule-level alpha analysis
- `compare_baseline_families.py`: baseline family comparison
- `compare_calibration_methods.py`: calibration strategy comparison
- `analyze_qmodel_trades.py`: diagnostic script for trade logs
- `analyze_raw_markets.py`: diagnostic script for raw market timestamps

### `rule_baseline/backtesting`

Purpose:
- Portfolio simulation

Files:
- `backtest_portfolio_qmodel.py`: main portfolio backtest
- `backtest_execution_parity.py`: execution-parity backtest aligned with the online path

### `rule_baseline/data_collection`

Purpose:
- Data ingestion and snapshot construction

Files:
- `fetch_raw_events.py`: fetch raw event batches
- `build_snapshots.py`: construct append-only snapshot batches and rebuild the canonical merged snapshot dataset

### `rule_baseline/domain_extractor`

Purpose:
- Market annotation parsing and annotation artifact generation

Files:
- `market_annotations.py`: core market annotation parsing, taxonomy resolution, and audit artifact generation
- `build_market_annotations.py`: CLI entrypoint for market annotation generation

### `rule_baseline/workflow`

Purpose:
- High-level orchestration

Files:
- `run_pipeline.py`: full pipeline entrypoint
- `run_online_pipeline.py`: online-mode convenience wrapper

## 4. Current canonical execution path

If you only care about the current supported path, use this order:

1. `rule_baseline/data_collection/fetch_raw_events.py`
2. `rule_baseline/domain_extractor/build_market_annotations.py`
3. `rule_baseline/data_collection/build_snapshots.py`
4. `rule_baseline/training/train_rules_naive_output_rule.py`
5. `rule_baseline/training/train_snapshot_model.py`
6. `rule_baseline/analysis/analyze_q_model_calibration.py`
7. `rule_baseline/analysis/analyze_alpha_quadrant.py`
8. `rule_baseline/analysis/analyze_rules_alpha_quadrant.py`
9. `rule_baseline/backtesting/backtest_execution_parity.py`

Or just run:

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline
```

## 5. Compatibility / leftovers

These files are now compatibility leftovers and can be removed after one more cleanup pass if you do not need transition imports:
- `rule_baseline/utils/data_processing.py`
- `rule_baseline/utils/research_context.py`
- `rule_baseline/utils/research_data.py`
- `rule_baseline/utils/modeling.py`

These files are still real implementations, not compatibility wrappers:
- `rule_baseline/utils/config.py`

These scripts are not part of the main pipeline, but may still be useful as diagnostics or experiments:
- `rule_baseline/analysis/analyze_qmodel_trades.py`
- `rule_baseline/analysis/analyze_raw_markets.py`

## 6. Production bundle contract

The production `q` artifact is no longer a single `joblib` dict payload. The canonical runtime artifact is:

```text
data/<mode>/models/q_model_bundle/
  runtime_manifest.json
  feature_contract.json
  predictor/
  calibration/
    calibrator.pkl
    calibrator_meta.json
```

Runtime rules:
- `execution_engine` preloads the bundle once at startup.
- The predictor is persisted once at startup when the backend supports `persist()`.
- Live inference is q-only. Research-only `target_mode` branching is not part of the production runtime contract.
