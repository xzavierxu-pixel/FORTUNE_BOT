# Polymarket Rule Engine README

## 1. 运行约定

- 工作目录：`C:\Users\ROG\Desktop\fortune_bot\polymarket_rule_engine`
- Python：默认使用当前环境里的 `python`
- 所有命令都建议从项目根目录执行

```powershell
cd C:\Users\ROG\Desktop\fortune_bot\polymarket_rule_engine
```

---

## 2. 最常用命令

### 2.1 一键跑完整离线流程

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline
```

这个命令会顺序执行：

1. 拉取原始市场数据
2. 构建 snapshots
3. 训练规则
4. 训练主模型
5. 生成 calibration / alpha / rules alpha 分析
6. 跑离线回测
7. 跑 baseline family 和 walk-forward 对比

### 2.2 一键跑在线重训流程

```powershell
python rule_baseline/workflow/run_online_pipeline.py
```

等价于：

```powershell
python rule_baseline/workflow/run_pipeline.py --artifact-mode online --skip-backtest --skip-baselines
```

### 2.2.1 offline / online / live 样本边界

| 阶段 | offline | online | live |
|---|---|---|---|
| 原始数据抓取 | 只用已结算市场 | 只用已结算市场 | 读取未结算 live 市场 |
| snapshots 构建 | 只用已结算市场 | 只用已结算市场 | 不参与训练集构建 |
| rule bins | 只允许 `train + valid` 参与 | 允许全部 labeled 数据参与，也就是 `train + valid` | 只读 |
| rule 是否存在 | 只允许 `train + valid` 参与 | 允许全部 labeled 数据参与，也就是 `train + valid` | 只读 |
| rule 打分 / 参数估计 | 只允许 `train / valid` 参与 | 允许全部 labeled 数据参与，也就是 `train + valid` | 只读 |
| model 训练 | 只用 `train` | 只用 `train` | 不训练 |
| calibration | 只用 `valid` | 只用最近 20 天 `valid` | 不做 calibration 拟合 |
| 最终评估 / 回测 | 只用 `test` | 不做 offline 回测 | 不做历史回测 |
| live 打分 | 不负责线上信号 | 产出 serving artifacts | 对当前未结算市场打分 |

### 2.3 一键跑“小样本调试”

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline `
  --skip-fetch `
  --skip-snapshots `
  --max-rows 5000 `
  --recent-days 60 `
  --skip-analysis `
  --skip-baselines
```

---

## AutoGluon Migration Notes

The production `q` path now uses an AutoGluon runtime bundle instead of the old single-file `ensemble_snapshot_q.pkl` payload.

Key changes:
- Production online inference is `q`-only.
- Rule selection logic is unified between offline and online artifacts.
- The canonical model artifact is `data/<mode>/models/q_model_bundle/`.
- `residual_q`, `expected_pnl`, and `expected_roi` remain offline research paths only.

Runtime bundle layout:

```text
data/<mode>/models/q_model_bundle/
  runtime_manifest.json
  feature_contract.json
  predictor/
  calibration/
    calibrator.pkl
    calibrator_meta.json
```

Recommended training command:

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode q `
  --calibration-mode grouped_isotonic
```

适合做逻辑联调，不适合拿来判断策略是否有效。

---

## 3. 完整分步命令

### 3.1 原始数据抓取

```powershell
python rule_baseline/data_collection/fetch_raw_events.py
```

输出重点：

- `data/raw/batches/`
- `data/raw/batch_manifest.csv`
- `data/intermediate/raw_markets_merged.csv`
- `data/intermediate/raw_market_quarantine.csv`

### 3.2 构建快照数据

```powershell
python rule_baseline/data_collection/build_snapshots.py
```

输出重点：

- `data/processed/snapshots.csv`
- `data/processed/snapshots_quarantine.csv`
- `data/processed/snapshot_market_audit.csv`
- `data/processed/snapshot_horizon_hit_rate.csv`
- `data/processed/snapshot_missingness_by_domain.csv`
- `data/processed/snapshot_build_summary.json`

注意：

- 如果你要让新的 quarantine / stale quote / token mapping 字段真正写入产物，必须重新跑这一步。

### 3.3 训练规则

```powershell
python rule_baseline/training/train_rules_naive_output_rule.py --artifact-mode offline
```

调试版本：

```powershell
python rule_baseline/training/train_rules_naive_output_rule.py `
  --artifact-mode offline `
  --max-rows 10000 `
  --recent-days 90
```

输出重点：

- `data/offline/edge/trading_rules.csv`
- `data/offline/edge/history_features_global.parquet`
- `data/offline/edge/history_features_domain.parquet`
- `data/offline/edge/history_features_category.parquet`
- `data/offline/edge/history_features_market_type.parquet`
- `data/offline/edge/history_features_domain_x_category.parquet`
- `data/offline/edge/history_features_domain_x_market_type.parquet`
- `data/offline/edge/history_features_category_x_market_type.parquet`
- `data/offline/edge/history_features_full_group.parquet`
- `data/offline/edge/group_serving_features.parquet`
- `data/offline/edge/fine_serving_features.parquet`
- `data/offline/edge/serving_feature_defaults.json`
- `data/offline/naive_rules/naive_all_leaves_report.csv`
- `data/offline/metadata/rule_training_summary.json`
- `data/offline/metadata/split_summary.json`
- `data/offline/audit/rule_funnel_summary.json`
- `data/offline/audit/rule_generation_audit.json`
- `data/offline/audit/rule_generation_audit.md`
- `data/offline/audit/artifact_inventory.json`
- `data/offline/audit/artifact_inventory.md`
- `docs/groupkey_serving_schema_reference.md`
- `docs/groupkey_migration_validation.md`
- `docs/groupkey_consistency_report.md`

### 3.4 训练主模型

默认 `q` 目标：

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode q `
  --calibration-mode valid_isotonic
```

`residual_q` 目标：

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode residual_q `
  --calibration-mode domain_valid_isotonic
```

`expected_pnl` 目标：

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode expected_pnl `
  --calibration-mode valid_sigmoid
```

`expected_roi` 目标：

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode expected_roi `
  --calibration-mode valid_sigmoid
```

调试版本：

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode residual_q `
  --calibration-mode domain_valid_isotonic `
  --max-rows 15000 `
  --recent-days 180
```

输出重点：

- `data/offline/models/ensemble_snapshot_q.pkl`
- `data/offline/predictions/snapshots_with_predictions.csv`
- `data/offline/predictions/snapshots_with_predictions_all.csv`
- `data/offline/metadata/model_training_summary.json`

### 3.5 分析模型与规则

校准分析：

```powershell
python rule_baseline/analysis/analyze_q_model_calibration.py --artifact-mode offline
```

Alpha quadrant：

```powershell
python rule_baseline/analysis/analyze_alpha_quadrant.py --artifact-mode offline
```

规则 Alpha：

```powershell
python rule_baseline/analysis/analyze_rules_alpha_quadrant.py --artifact-mode offline
```

输出重点：

- `data/offline/analysis/calibration_metrics.csv`
- `data/offline/analysis/calibration_reliability.csv`
- `data/offline/analysis/calibration_edge_buckets.csv`
- `data/offline/analysis/alpha_quadrant_metrics.csv`
- `data/offline/analysis/alpha_by_domain.csv`
- `data/offline/analysis/alpha_by_category.csv`
- `data/offline/analysis/alpha_by_horizon.csv`
- `data/offline/analysis/rules_alpha_metrics.csv`

### 3.6 跑主回测

```powershell
python rule_baseline/backtesting/backtest_execution_parity.py --artifact-mode offline
```

`offline` 的 `test` 窗口只用于最终评估和回测，不得参与 rule 定义、分桶、模型训练或概率校准。

调试版本：

```powershell
python rule_baseline/backtesting/backtest_portfolio_qmodel.py `
  --artifact-mode offline `
  --max-rows 30000 `
  --recent-days 365
```

输出重点：

- `data/offline/backtesting/backtest_equity_execution_parity.csv`
- `data/offline/backtesting/backtest_trades_execution_parity.csv`
- `data/offline/backtesting/backtest_skipped_execution_parity.csv`
- `data/offline/backtesting/backtest_daily_execution_parity.csv`
- `data/offline/backtesting/backtest_filter_breakdown_execution_parity.csv`
- `data/offline/metadata/backtest_summary_execution_parity.json`

### 3.7 跑 baseline family 对比

```powershell
python rule_baseline/analysis/compare_baseline_families.py --artifact-mode offline
```

指定 top-k 和 walk-forward 窗口：

```powershell
python rule_baseline/analysis/compare_baseline_families.py `
  --artifact-mode offline `
  --top-k 50 `
  --walk-forward-windows 3 `
  --walk-forward-step-days 30
```

调试版本：

```powershell
python rule_baseline/analysis/compare_baseline_families.py `
  --artifact-mode offline `
  --max-rows 20000 `
  --recent-days 180 `
  --top-k 30 `
  --walk-forward-windows 2
```

输出重点：

- `data/offline/analysis/baseline_family_comparison.csv`
- `data/offline/analysis/baseline_family_backtest.csv`
- `data/offline/analysis/baseline_family_stability.csv`
- `data/offline/analysis/baseline_family_test_predictions.csv`
- `data/offline/analysis/baseline_family_walk_forward_summary.csv`
- `data/offline/analysis/baseline_family_walk_forward_backtest.csv`
- `data/offline/analysis/baseline_family_walk_forward_aggregate.csv`
- `data/offline/analysis/baseline_family_summary.json`

---

## 4. Workflow 参数总表

### 4.1 `run_pipeline.py`

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline|online `
  --target-mode q|residual_q|expected_pnl|expected_roi `
  --calibration-mode valid_isotonic|valid_sigmoid|domain_valid_isotonic|horizon_valid_isotonic|cv_isotonic|cv_sigmoid|none `
  --max-rows <int> `
  --recent-days <int> `
  --walk-forward-windows <int> `
  --walk-forward-step-days <int> `
  [--skip-fetch] `
  [--skip-snapshots] `
  [--skip-analysis] `
  [--skip-backtest] `
  [--skip-baselines]
```

参数说明：

- `--artifact-mode`
  - `offline`: 研究、分析、回测
  - `online`: 用全量可用历史重训线上产物
- `--target-mode`
  - `q`: 预测事件概率，最稳定的基线
  - `residual_q`: 市场价格作为 baseline，模型只学 residual
  - `expected_pnl`: 直接学交易价值
  - `expected_roi`: 直接学收益率
- `--calibration-mode`
  - `valid_isotonic`: 默认离线首选
  - `valid_sigmoid`: 当样本少、isotonic 过拟合时可试
  - `domain_valid_isotonic`: 不同 domain 概率结构差异明显时可试
  - `horizon_valid_isotonic`: 不同 horizon 概率结构差异明显时可试
  - `cv_isotonic` / `cv_sigmoid`: 不依赖单独 valid 做校准
  - `none`: 用于快速 smoke test
- `--max-rows`
  - 只保留最近 N 行 snapshots
  - 适合开发调试
  - 不适合最终策略评估
- `--recent-days`
  - 只保留最近 N 天数据
  - 适合快速回归测试
- `--walk-forward-windows`
  - baseline family 的 walk-forward 窗口数
- `--walk-forward-step-days`
  - walk-forward 相邻窗口平移步长
- `--skip-*`
  - 用于只跑流程中的某几段

---

## 5. 哪些参数最值得调整或测试

### 5.1 研究最先要测的参数

1. `target-mode`
   - 建议顺序：`q` -> `residual_q` -> `expected_pnl` -> `expected_roi`
   - 重点看 `model_training_summary.json` 和 `backtest_summary.json`

2. `calibration-mode`
   - 默认先试 `valid_isotonic`
   - 样本少时测试 `valid_sigmoid`
   - domain 差异大时测试 `domain_valid_isotonic`
   - horizon 差异大时测试 `horizon_valid_isotonic`

3. `walk-forward-windows` 和 `walk-forward-step-days`
   - 至少测试 `2`、`3`、`4` 个窗口
   - 步长至少测试 `15`、`30`、`45` 天

4. `top-k`
   - baseline family 默认 `50`
   - 建议测试 `20 / 30 / 50 / 100`

### 5.2 调试最先要改的参数

1. `--max-rows`
   - 建议先试 `5000`、`10000`、`30000`

2. `--recent-days`
   - 建议先试 `30`、`60`、`90`、`180`

3. `--skip-fetch` / `--skip-snapshots`
   - 当你只改了训练、分析、回测逻辑时，应优先跳过这两步加快迭代

### 5.3 代码内需要谨慎调整的参数

这些不是命令行参数，而是代码常量，改之前要先做 smoke test：

- `rule_baseline/utils/config.py`
  - `HORIZONS`
  - `VALIDATION_DAYS`
  - `TEST_DAYS`
  - `FEE_RATE`
  - `STALE_QUOTE_MAX_OFFSET_SEC`
  - `STALE_QUOTE_MAX_GAP_SEC`
  - `MIN_MARKET_VOLUME`
  - `MIN_MARKET_LIQUIDITY`
  - `MAX_MARKET_SPREAD`
  - `MAX_DOMAIN_EXPOSURE_F`
  - `MAX_CATEGORY_EXPOSURE_F`
  - `MAX_CLUSTER_EXPOSURE_F`
  - `MAX_SETTLEMENT_EXPOSURE_F`
  - `MAX_SIDE_EXPOSURE_F`
  - `MAX_TRADE_LIQUIDITY_F`
  - `MAX_TRADE_VOLUME24_F`
  - `BETA_PRIOR_STRENGTH`
  - `FDR_ALPHA`

- `rule_baseline/training/train_rules_naive_output_rule.py`
  - `MIN_GROUP_ROWS`
  - `MIN_TRAIN_ROWS`
  - `MIN_VALID_N`
  - `EDGE_AB_THRESHOLD`
  - `EDGE_STD_THRESHOLD`

- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`
  - `TOP_K_RULES`
  - `MAX_DAILY_TRADES`
  - `MAX_POSITION_F`
  - `MAX_DAILY_EXPOSURE_F`
  - `MIN_RULE_VALID_N`
  - `MIN_EDGE_TRADE`
  - `MIN_STD_TRADE`
  - `MIN_PROB_EDGE`
  - `KELLY_FRACTION`

---

## 6. 推荐测试顺序

### 6.1 只验证代码是否跑通

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline `
  --skip-fetch `
  --skip-snapshots `
  --skip-analysis `
  --skip-baselines `
  --max-rows 5000 `
  --recent-days 60 `
  --target-mode q `
  --calibration-mode none
```

### 6.2 验证新目标是否可训练

```powershell
python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode residual_q `
  --calibration-mode domain_valid_isotonic

python rule_baseline/training/train_snapshot_model.py `
  --artifact-mode offline `
  --target-mode expected_roi `
  --calibration-mode valid_sigmoid
```

### 6.3 验证回测是否还能跑通

```powershell
python rule_baseline/backtesting/backtest_portfolio_qmodel.py `
  --artifact-mode offline `
  --max-rows 30000 `
  --recent-days 365
```

### 6.4 验证 baseline family

```powershell
python rule_baseline/analysis/compare_baseline_families.py `
  --artifact-mode offline `
  --max-rows 20000 `
  --recent-days 180 `
  --top-k 30 `
  --walk-forward-windows 2
```

### 6.5 真正跑全量离线

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline `
  --target-mode residual_q `
  --calibration-mode domain_valid_isotonic
```

---

## 7. 什么时候必须重跑哪一步

### 7.1 只改了分析脚本

只需要重跑：

```powershell
python rule_baseline/analysis/analyze_q_model_calibration.py --artifact-mode offline
python rule_baseline/analysis/analyze_alpha_quadrant.py --artifact-mode offline
python rule_baseline/analysis/analyze_rules_alpha_quadrant.py --artifact-mode offline
```

### 7.2 只改了回测逻辑

只需要重跑：

```powershell
python rule_baseline/backtesting/backtest_portfolio_qmodel.py --artifact-mode offline
```

### 7.3 改了规则逻辑

至少需要重跑：

```powershell
python rule_baseline/training/train_rules_naive_output_rule.py --artifact-mode offline
python rule_baseline/training/build_groupkey_validation_reports.py --artifact-mode offline
python rule_baseline/training/train_snapshot_model.py --artifact-mode offline
python rule_baseline/backtesting/backtest_portfolio_qmodel.py --artifact-mode offline
```

### 7.4 改了特征工程、模型目标、校准逻辑

至少需要重跑：

```powershell
python rule_baseline/training/train_snapshot_model.py --artifact-mode offline
python rule_baseline/analysis/analyze_q_model_calibration.py --artifact-mode offline
python rule_baseline/backtesting/backtest_portfolio_qmodel.py --artifact-mode offline
python rule_baseline/analysis/compare_baseline_families.py --artifact-mode offline
```

### 7.5 改了 raw fetch / snapshot build

必须重跑全链路：

```powershell
python rule_baseline/data_collection/fetch_raw_events.py
python rule_baseline/data_collection/build_snapshots.py
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline
```

说明：

- `run_pipeline.py` 现在会在 `Train model` 之后自动执行 `build_groupkey_validation_reports.py`
- 规则训练步骤现在会自动物化全部 `history_features_*.parquet`，并写出 `rule_generation_audit.json/md`
- blueprint inventory 现在可通过 `python rule_baseline/training/build_groupkey_feature_inventory.py` 刷新，并产出 `docs/groupkey_feature_inventory_summary.md`

---

## 8. 结果文件位置

### 8.1 离线产物

- `data/offline/edge/`
- `data/offline/models/`
- `data/offline/predictions/`
- `data/offline/backtesting/`
- `data/offline/analysis/`
- `data/offline/metadata/`

### 8.2 在线产物

- `data/online/edge/`
- `data/online/models/`
- `data/online/predictions/`
- `data/online/analysis/`
- `data/online/metadata/`

### 8.3 源头数据和质量产物

- `data/raw/`
- `data/intermediate/raw_markets_merged.csv`
- `data/intermediate/raw_market_quarantine.csv`
- `data/processed/snapshots.csv`
- `data/processed/snapshots_quarantine.csv`
- `data/processed/snapshot_market_audit.csv`
- `data/processed/snapshot_horizon_hit_rate.csv`
- `data/processed/snapshot_missingness_by_domain.csv`
- `data/processed/snapshot_build_summary.json`

---

## 9. 建议的日常命令模板

### 9.1 每日增量更新线上产物

```powershell
python rule_baseline/workflow/run_online_pipeline.py
```

### 9.2 每周离线评估一次

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline `
  --target-mode residual_q `
  --calibration-mode domain_valid_isotonic
```

### 9.3 开发联调用模板

```powershell
python rule_baseline/workflow/run_pipeline.py `
  --artifact-mode offline `
  --skip-fetch `
  --skip-snapshots `
  --max-rows 10000 `
  --recent-days 90 `
  --target-mode q `
  --calibration-mode none `
  --skip-analysis `
  --skip-baselines
```
