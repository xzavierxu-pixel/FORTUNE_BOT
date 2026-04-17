# Polymarket Rule Engine — Offline Pipeline 工作流程与脚本使用全景图

> 入口：`rule_baseline/workflow/run_pipeline.py --artifact-mode offline`

---

## 1. 端到端工作流程总览

`run_pipeline.py` 以子进程方式串行调度 **12 个步骤**，每一步调用一个独立的 CLI 脚本。
可通过 `--skip-*` 参数跳过数据采集/分析/回测阶段。

```
┌─────────────────────────────────────────────────────────────────────┐
│                     run_pipeline.py (offline)                       │
│                                                                     │
│  Step 1  fetch_raw_events.py          ← --skip-fetch 可跳过        │
│     │                                                               │
│     ▼                                                               │
│  Step 2  build_market_annotations.py  ← --skip-annotations 可跳过  │
│     │                                                               │
│     ▼                                                               │
│  Step 3  build_snapshots.py           ← --skip-snapshots 可跳过    │
│     │                                                               │
│     ▼                                                               │
│  Step 4  train_rules_naive_output_rule.py   (必跑)                  │
│     │                                                               │
│     ▼                                                               │
│  Step 5  export_features.py                 (必跑)                  │
│     │                                                               │
│     ▼                                                               │
│  Step 6  train_snapshot_model.py            (必跑)                  │
│     │                                                               │
│     ▼                                                               │
│  Step 7  build_groupkey_validation_reports.py (必跑)                │
│     │                                                               │
│     ▼                                                               │
│  Step 8  analyze_q_model_calibration.py  ← --skip-analysis 可跳过  │
│  Step 9  analyze_alpha_quadrant.py       ← --skip-analysis 可跳过  │
│  Step 10 analyze_rules_alpha_quadrant.py ← --skip-analysis 可跳过  │
│     │                                                               │
│     ▼                                                               │
│  Step 11 backtest_execution_parity.py    ← --skip-backtest 可跳过  │
│     │                                                               │
│     ▼                                                               │
│  Step 12 compare_baseline_families.py    ← --skip-baselines 可跳过 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 2. Pipeline 直接调用的脚本（12 个 CLI 入口）

### 2.1 数据采集阶段

| # | 脚本 | 作用 | 关键依赖模块 |
|---|------|------|-------------|
| 1 | `data_collection/fetch_raw_events.py` | 从 Gamma API 拉取已结算市场，写入 append-only 批次文件，合并为 canonical raw CSV | `datasets.raw_market_batches`, `utils.config` |
| 2 | `domain_extractor/build_market_annotations.py` | 为每个市场生成 domain/category/market_type 标注 | `domain_extractor.market_annotations` |
| 3 | `data_collection/build_snapshots.py` | 从 CLOB API 拉取价格历史，按多时间窗（1h/4h/24h…）生成快照，计算 r_std，处理 stale quote 检测 | `datasets.snapshot_batches`, `datasets.raw_market_batches`, `datasets.artifacts`, `domain_extractor.market_annotations`, `utils.config` |

### 2.2 训练阶段

| # | 脚本 | 作用 | 关键依赖模块 |
|---|------|------|-------------|
| 4 | `training/train_rules_naive_output_rule.py` | 基于原始频率构建 naive rule buckets；在主 offline 训练链路中按固定 `RULE_PRICE_BIN_STEP = 0.1` 分桶，对 `(domain, category, market_type, price_bin, horizon_hours)` 分组计算 win rate 与 edge，保留 Wilson 下界作为 `edge_lower_bound_full` / `rule_score`，并按 `MIN_GROUP_UNIQUE_MARKETS = 15` 在 group_key 层过滤低频组，最终生成 leaf_id 与 serving features | `datasets.artifacts`, `datasets.snapshots`, `history.history_features`, `audits.rule_generation_audit`, `utils.config` |
| 5 | `training/export_features.py` | 将完整特征构建流程（快照加载 → 质量过滤 → 规则匹配 → serving 特征挂载 → 预处理 → 训练目标生成）独立执行，输出 `train.parquet` / `valid.parquet` / `feature_export_manifest.json`，供后续模型训练直接读取，默认 offline 训练集采样 200k 行 | `datasets.artifacts`, `datasets.snapshots`, `datasets.splits`, `datasets.raw_market_batches`, `domain_extractor.market_annotations`, `features` (核心特征工程), `features.annotation_normalization`, `features.snapshot_semantics`, `audits.snapshot_training_audit`, `training.train_snapshot_model`, `models.autogluon_qmodel`, `utils.config` |
| 6 | `training/train_snapshot_model.py` | 从导出的 parquet 特征帧训练 AutoGluon 集成模型（支持 q / residual_q / expected_pnl / expected_roi 四种 target mode），内含 isotonic/sigmoid 校准，导出 predictions 与 deploy/full model bundle | `datasets.artifacts`, `datasets.snapshots`, `datasets.splits`, `features.snapshot_semantics`, `models` (AutoGluon 拟合), `models.runtime_bundle`, `utils.config` |

### 2.3 验证阶段

| # | 脚本 | 作用 | 关键依赖模块 |
|---|------|------|-------------|
| 7 | `reports/build_groupkey_validation_reports.py` | 生成 GroupKey 迁移一致性报告、特征契约校验报告和 serving schema 参考文档 | `reports.groupkey_reports`, `datasets.artifacts`, `datasets.splits`, `datasets.snapshots`, `domain_extractor.market_annotations`, `features.serving`, `features.annotation_normalization`, `models.runtime_bundle`, `training.train_snapshot_model` |

### 2.4 分析阶段

| # | 脚本 | 作用 | 关键依赖模块 |
|---|------|------|-------------|
| 8 | `analysis/analyze_q_model_calibration.py` | 在 OOS predictions 上计算 logloss/brier/AUC delta，输出 reliability table 与 edge bucket 分析 | `datasets.artifacts` |
| 9 | `analysis/analyze_alpha_quadrant.py` | 将 predictions 分为 4 象限（contrarian_correct / consensus_correct / consensus_wrong / contrarian_wrong）, 按 category/domain/horizon 切片计算 alpha ratio | `datasets.artifacts` |
| 10 | `analysis/analyze_rules_alpha_quadrant.py` | 对 test split 的规则做象限分析，按 leaf_id/group_key 排名 weighted_score | `datasets.artifacts`, `datasets.snapshots`, `datasets.splits`, `utils.config` |

### 2.5 回测 & 基线对比阶段

| # | 脚本 | 作用 | 关键依赖模块 |
|---|------|------|-------------|
| 11 | `backtesting/backtest_execution_parity.py` | 模拟在线执行逻辑的 backtest：earliest-market-dedup、Kelly sizing、时间到期过滤、equity curve | `backtesting.backtest_portfolio_qmodel`, `datasets.artifacts`, `datasets.snapshots`, `datasets.splits`, `domain_extractor.market_annotations`, `features`, `models`, `utils.config` |
| 12 | `analysis/compare_baseline_families.py` | 在 walk-forward 窗口下对比 4 个 baseline family（q_only / residual_q / tradeable_only / two_stage），逐窗口训练+回测，汇总信号率/精度/AUC | `backtesting.backtest_portfolio_qmodel`, `datasets.*`, `domain_extractor.market_annotations`, `features`, `models`, `training.train_rules_naive_output_rule`, `training.train_snapshot_model`, `utils.config` |

---

## 3. Pipeline 间接依赖的核心模块（被 import 但不直接执行）

这些是被上述 12 个脚本 **import** 的库模块，虽不出现在 `run_pipeline.py` 的 subprocess 调用链中，但属于 pipeline 的关键代码路径。

| 模块 | 功能 | 被谁导入 |
|------|------|---------|
| `datasets/artifacts.py` | `ArtifactPaths` dataclass；统一管理 artifact 路径（rules_path, model_bundle_dir, full_model_bundle_dir, predictions_path, analysis_dir 等）；支持 offline/online 模式分离 | 几乎所有训练/分析/回测脚本 |
| `datasets/snapshots.py` | `load_research_snapshots()`, `load_online_parity_snapshots()`, `build_snapshot_base()`, `add_term_structure_features()`, `apply_earliest_market_dedup()` 等数据加载入口 | 训练、分析、回测脚本 |
| `datasets/splits.py` | `compute_temporal_split()` (3-way), `compute_train_valid_split()` (2-way), `compute_artifact_split()` (按模式分派), `assign_dataset_split()`, `build_walk_forward_splits()` | train_snapshot_model, backtest, compare_baselines |
| `datasets/raw_market_batches.py` | 批次 I/O: `rebuild_canonical_merged()`, `write_batch()`, `reset_raw_batches()` | fetch_raw_events, build_snapshots, train_snapshot_model |
| `datasets/snapshot_batches.py` | snapshot 批次 I/O: `load_processed_market_ids()`, `rebuild_canonical_snapshots()` 等 | build_snapshots |
| `domain_extractor/market_annotations.py` | `build_and_save_market_annotations()`, `load_market_annotations()` — 市场标注读写 | build_market_annotations, build_snapshots, train_snapshot_model, groupkey_reports, backtest, compare_baselines |
| `features/__init__.py` | `build_market_feature_cache()`, `preprocess_features()`, `apply_feature_variant()` — 特征工程入口 | train_snapshot_model, backtest, compare_baselines |
| `features/tabular.py` | 表格特征处理（由 `__init__` 代理调用），market feature cache 构建（50+ 条 market-level 特征） | 间接 |
| `features/market_feature_builders.py` | 构建 market-level 特征集（volume/liquidity/text/sentiment/pattern） | 由 features 模块内部调用 |
| `features/serving.py` | `ServingFeatureBundle`, `attach_serving_features()`, `build_group_key()`, `build_price_bin()`, `round_horizon_hours()` — serving 层特征挂载 | train_snapshot_model, groupkey_reports |
| `features/annotation_normalization.py` | `build_normalization_manifest()`, `normalize_market_annotations()` — 标注归一化 | train_snapshot_model, groupkey_reports |
| `features/snapshot_semantics.py` | `online_feature_columns()`, `split_feature_contract_columns()`, `FEATURE_SEMANTICS_VERSION` — 特征语义契约 | train_snapshot_model |
| `models/__init__.py` | `fit_autogluon_q_model()`, `load_model_artifact()`, `compute_trade_value_from_q()`, `infer_q_from_trade_value()` 等 — 模型训练 & 推理 | train_snapshot_model, backtest, compare_baselines |
| `models/autogluon_qmodel.py` | `AutoGluonQTrainingResult`; AutoGluon 训练/推理/校准实现；support grouped calibration on configurable column | models 模块内部 |
| `models/tree_ensembles.py` | 基础树模型参数定义、校准类（BetaCalibration, IsotonicRegression 等） | models 模块内部 (legacy) |
| `models/runtime_bundle.py` | `FeatureContract`, `RuntimeBundlePaths` — 部署产物的结构定义与 JSON 持久化；bundle 目录: `q_model_bundle_deploy` (deploy), `q_model_bundle_full` (full training) | train_snapshot_model, groupkey_reports |
| `models/runtime_adapter.py` | `ModelArtifactAdapter`, `build_legacy_adapter()` — 训练态到 serving 态的接口桥接 | backtest, serving/runtime 代码路径 |
| `models/scoring.py` | `compute_trade_value_from_q()`, `infer_q_from_trade_value()` — 概率 ↔ 交易价值转换 | backtest, serving |
| `history/history_features.py` | `LEVEL_DEFINITIONS` (8 层级), `HISTORY_WINDOWS` (expanding/recent_90days), `summarize_history_features()` — 层级历史特征聚合库 | train_rules_naive_output_rule |
| `reports/groupkey_reports.py` | `write_groupkey_reports()`, `build_migration_validation_markdown()`, `build_consistency_report_markdown()`, `build_schema_reference_markdown()` — GroupKey 校验报告生成 | build_groupkey_validation_reports |
| `audits/rule_generation_audit.py` | `build_artifact_inventory()`, `build_rule_generation_audit_payload()`, `write_rule_generation_audit()` — 规则生成漏斗审计与产物清单 | train_rules_naive_output_rule |
| `audits/snapshot_training_audit.py` | `build_snapshot_training_audit_payload()`, `write_snapshot_training_audit()` — 训练漏斗审计 | export_features |
| `backtesting/backtest_portfolio_qmodel.py` | `BacktestConfig`; Portfolio backtest 引擎：`load_rules()`, `match_rules()`, `predict_candidates()`, `compute_growth_and_direction()`, `prepare_candidate_book()`, `derive_domain_whitelist()` — Kelly sizing, rule matching, trade PnL, kill thresholds | backtest_execution_parity, compare_baseline_families |
| `utils/config.py` | 全局配置（路径、阈值、horizon 列表、fee rate、broad categories 等） | 几乎所有脚本 |
| `utils/feature_util.py` | 情感词典、关键词表、类别映射 | market_feature_builders |

---

## 4. Pipeline 未使用的脚本（Standalone / Legacy / 一次性实验）

以下脚本 **不被** `run_pipeline.py` 调用，也不被其调用链中的任何脚本 import。
它们是独立的 CLI 工具、一次性研究实验或按需诊断工具。

### 4.1 分析类（Ad-hoc / 探索性）

| 脚本 | 说明 | 分类 |
|------|------|------|
| `analysis/analyze_qmodel_trades.py` | 读取 backtest 交易 CSV，按 edge bucket、方向分组输出 PnL 摘要 | **Standalone 诊断** |
| `analysis/analyze_raw_markets.py` | 探索性分析 raw 市场的日期字段（startDate/endDate/closedTime）分布 | **Standalone EDA** |
| `analysis/analyze_snapshots.py` | Snapshot 数据质量扫描：缺失值、重复、分布、相关矩阵 | **Standalone EDA** |
| `analysis/compare_calibration_methods.py` | 对比多种校准方式（isotonic / sigmoid / beta / blend 等），输出 logloss/brier/AUC | **研究实验** |

### 4.2 训练辅助 / 报告生成类

| 脚本 | 说明 | 分类 |
|------|------|------|
| `reports/build_groupkey_feature_contract_preview.py` | 预览 GroupKey feature contract，对比现有 bundle | **按需审计工具** |
| `reports/build_groupkey_feature_inventory.py` | 盘点所有 serving feature 的 family/grain/window 元数据 | **按需审计工具** |
| `reports/build_groupkey_runtime_report.py` | 生成 GroupKey runtime coverage 报告（JSON + Markdown） | **按需审计工具** |
| `history/build_history_feature_artifacts.py` | 独立构建层级 history feature artifacts | **按需前置步骤** |
| `audits/build_snapshot_training_audit.py` | 独立生成训练漏斗审计报告 | **按需诊断** |

### 4.3 工作流 / 调参类

| 脚本 | 说明 | 分类 |
|------|------|------|
| `workflow/run_online_pipeline.py` | 对 `run_pipeline.py` 的薄封装，固定 `--artifact-mode online --skip-backtest --skip-baselines` | **Online 专用入口** |
| `workflow/tune_snapshot_model.py` | 多阶段超参调优框架：stage1 grid → stage2 refinement → execution parity backtest | **手动调参工具** |

### 4.4 质量检查

| 脚本 | 说明 | 分类 |
|------|------|------|
| `quality_check/feature_dqc.py` | Feature 级别 DQC：加载模型 → 构建特征表 → 输出特征元数据（11 个分类族群） | **按需审计工具** |

---

## 5. Pipeline 未使用的数据资产

以下目录/文件被生成或消费于上述 standalone 脚本，不属于核心 pipeline 的输入/输出链：

| 路径 | 说明 |
|------|------|
| `docs/audit/groupkey_reports/groupkey_*.md / .json` | GroupKey 文档/报告 — 部分由 pipeline Step 7 生成，部分由 `reports/build_groupkey_*` standalone 脚本生成 |
| `docs/polymarket_groupkey_500_feature_blueprint.md` | 500 特征蓝图规划文档 |

---

## 6. 模块依赖关系图

```
run_pipeline.py
  │
  ├── fetch_raw_events.py
  │     └── datasets.raw_market_batches
  │     └── utils.config
  │
  ├── build_market_annotations.py
  │     └── domain_extractor.market_annotations
  │
  ├── build_snapshots.py
  │     ├── datasets.snapshot_batches
  │     ├── datasets.raw_market_batches
  │     ├── datasets.artifacts
  │     ├── domain_extractor.market_annotations
  │     └── utils.config
  │
  ├── train_rules_naive_output_rule.py
  │     ├── datasets.artifacts
  │     ├── datasets.snapshots
  │     ├── history.history_features
  │     ├── audits.rule_generation_audit
  │     └── utils.config
  │
  ├── export_features.py
  │     ├── datasets.artifacts
  │     ├── datasets.snapshots
  │     ├── datasets.splits
  │     ├── datasets.raw_market_batches
  │     ├── domain_extractor.market_annotations
  │     ├── features/
  │     │     ├── __init__ (build_market_feature_cache, preprocess_features)
  │     │     ├── tabular
  │     │     ├── market_feature_builders
  │     │     │     └── utils.feature_util
  │     │     ├── serving
  │     │     ├── annotation_normalization
  │     │     └── snapshot_semantics
  │     ├── models.autogluon_qmodel
  │     ├── audits.snapshot_training_audit
  │     ├── training.train_snapshot_model (build_feature_table, add_training_targets, ...)
  │     └── utils.config
  │
  ├── train_snapshot_model.py
  │     ├── datasets.artifacts
  │     ├── datasets.snapshots
  │     ├── datasets.splits
  │     ├── features.snapshot_semantics
  │     ├── models/
  │     │     ├── __init__ (fit_autogluon_q_model, load_model_artifact)
  │     │     ├── autogluon_qmodel
  │     │     ├── tree_ensembles (legacy)
  │     │     ├── runtime_bundle
  │     │     └── scoring
  │     └── utils.config
  │
  ├── build_groupkey_validation_reports.py
  │     └── reports.groupkey_reports
  │           ├── datasets.artifacts
  │           ├── datasets.splits
  │           ├── datasets.snapshots
  │           ├── domain_extractor.market_annotations
  │           ├── features.serving
  │           ├── features.annotation_normalization
  │           ├── models.runtime_bundle
  │           └── training.train_snapshot_model (load_rules, load_serving_feature_bundle)
  │
  ├── analyze_q_model_calibration.py
  │     └── datasets.artifacts
  │
  ├── analyze_alpha_quadrant.py
  │     └── datasets.artifacts
  │
  ├── analyze_rules_alpha_quadrant.py
  │     ├── datasets.artifacts
  │     ├── datasets.snapshots
  │     ├── datasets.splits
  │     └── utils.config
  │
  ├── backtest_execution_parity.py
  │     ├── backtesting.backtest_portfolio_qmodel
  │     │     ├── datasets.artifacts
  │     │     ├── datasets.snapshots
  │     │     ├── datasets.splits
  │     │     ├── datasets.raw_market_batches
  │     │     ├── domain_extractor.market_annotations
  │     │     ├── features
  │     │     ├── models (load_model_artifact, runtime_adapter)
  │     │     └── utils.config
  │     ├── datasets.artifacts
  │     ├── datasets.snapshots
  │     ├── datasets.splits
  │     ├── domain_extractor.market_annotations
  │     ├── features
  │     ├── models (load_model_artifact)
  │     └── utils.config
  │
  └── compare_baseline_families.py
        ├── backtesting.backtest_portfolio_qmodel
        ├── datasets.artifacts
        ├── datasets.snapshots
        ├── datasets.splits
        ├── datasets.raw_market_batches
        ├── domain_extractor.market_annotations
        ├── features
        ├── models
        ├── training.train_rules_naive_output_rule (build_rules)
        ├── training.train_snapshot_model (build_feature_table)
        └── utils.config
```

---

## 7. 分类汇总

### 按使用状态统计

| 分类 | 文件数 | 说明 |
|------|--------|------|
| **Pipeline CLI 入口** | 12 | 被 `run_pipeline.py` 直接 subprocess 调用 |
| **Pipeline 核心模块** | 21 | 被 CLI 脚本 import，不可缺少 |
| **Standalone 诊断/审计工具** | 10 | 按需运行，不属于自动化流程 |
| **研究实验脚本** | 1 | `compare_calibration_methods.py` |
| **Online 专用入口** | 1 | `run_online_pipeline.py` |
| **手动调参工具** | 1 | `tune_snapshot_model.py` |

### 可考虑清理/归档的候选

以下仍存在的脚本如果不再活跃使用，可以移入 `_archive/` 或标记 deprecated：

1. **`analysis/compare_calibration_methods.py`** — 校准方式已选定为 `global_isotonic`（pipeline 默认值）
2. **`analysis/analyze_qmodel_trades.py`** — 依赖特定 CSV 格式，backtest 已迁移到 execution_parity
3. **`analysis/analyze_raw_markets.py`** — 早期 EDA 脚本
4. **`analysis/analyze_snapshots.py`** — 早期 EDA 脚本

---

*基于 `run_pipeline.py` 子进程调用链（12 步）及 `rule_baseline/` 全模块静态分析生成。*
