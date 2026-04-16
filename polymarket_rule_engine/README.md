# Polymarket Rule Engine

This directory contains the offline training and analysis pipeline used to build the rule bundle and q-model bundle consumed by the execution stack.

## Supported Offline Entry Points

Only these three scripts are considered supported top-level training entry points under `rule_baseline/training/`:

- `train_rules_naive_output_rule.py`
- `train_snapshot_model.py`
- `build_groupkey_validation_reports.py`

Other reusable logic now lives under responsibility-specific packages:

- `rule_baseline/history/`: history feature builders and artifact helpers
- `rule_baseline/audits/`: training and rule-generation audit payload writers
- `rule_baseline/reports/`: GroupKey contract/runtime/inventory report builders
- `rule_baseline/quality_check/`: explicit quality-check tooling kept in place

## Offline Pipeline

Run the full offline pipeline from the repo root:

```powershell
.venv-execution\Scripts\python.exe .\polymarket_rule_engine\rule_baseline\workflow\run_pipeline.py --artifact-mode offline
```

Useful partial reruns:

```powershell
.venv-execution\Scripts\python.exe .\polymarket_rule_engine\rule_baseline\workflow\run_pipeline.py --artifact-mode offline --skip-fetch --skip-annotations --skip-snapshots
.venv-execution\Scripts\python.exe .\polymarket_rule_engine\rule_baseline\workflow\run_pipeline.py --artifact-mode offline --skip-analysis --skip-backtest --skip-baselines
```

## Current Offline Defaults

- Offline artifact splits now use `train + valid` only.
- The latest 30 days are saved as the offline `valid` dataset.
- `train_snapshot_model.py` samples up to 200,000 rows from the `train` split before AutoGluon fitting unless `--random-sample-rows` overrides it.
- Active history windows are `expanding` and `recent_90days`.
- Active rule audit output is `data/offline/audit/all_trading_rule_audit_report.csv`.

## Key Outputs

- `data/offline/edge/trading_rules.csv`
- `data/offline/edge/group_serving_features.parquet`
- `data/offline/edge/fine_serving_features.parquet`
- `data/offline/models/q_model_bundle_deploy/`
- `data/offline/models/q_model_bundle_full/`
- `data/offline/predictions/snapshots_with_predictions.csv`
- `docs/groupkey_migration_validation.md`
- `docs/groupkey_consistency_report.md`
- `docs/groupkey_serving_schema_reference.md`

## Notes

- Use `.venv-execution\Scripts\python.exe` for offline model training on this machine. The system Python in this repo may not have `autogluon.tabular`.
- `docs/legacy_docs/` contains historical planning and audit material. Treat it as reference, not as the active contract.
