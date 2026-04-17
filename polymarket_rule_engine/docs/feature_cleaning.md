# Feature DQC Constant / High Cardinality / Imbalance Implementation Plan

> 目标：在不破坏在线 serving fallback 链路的前提下，清理离线训练特征中的 constant features、high-cardinality identity features 和 highly imbalanced categorical features。
> 本文档是 `FEATURE_DQC_EXACT_DUPLICATE_IMPLEMENTATION_PLAN.md` 的后续配套文档。
> 创建日期：2026-04-17

---

## 1. Objective

本计划定义当前 Feature DQC 的第二批治理范围：

- Constant Features
- High Cardinality Features
- Highly Imbalanced Categorical Features

目标不是简单在导出后删除列，而是先区分列角色，再按以下四种动作处理：

- 从源头停算
- 不再并入训练合同
- 降级为 metadata / monitoring / audit 字段
- 在导出阶段做 train-only 自动剪枝

同时必须满足一条额外约束：

- `execution_engine` 必须继续消费与离线训练 / 导出完全一致的同一套 canonical model feature contract

本计划不覆盖：

- exact duplicate columns 的清理实现
- train / valid split 设计

这些内容已在其他文档或讨论中单独处理。

---

## 2. Current DQC Snapshot

基于最新 `feature_dqc_report.md`：

- 248,196 rows
- 738 columns
- 70 constant features
- 14 high-cardinality features
- 16 highly imbalanced categorical features

这三类问题并不都意味着“特征无效”。

必须先区分两种情况：

### 2.1 Structural issues

如果某列因为当前架构设计而天然恒定、天然高基数、或天然极端偏斜，那么应做结构性清理。

典型例子：

- 全局 history 统计 merge 到每一行后天然变成常量
- token id / outcome label 这类身份字段天然高基数
- fallback 命中标记天然更适合监控，不适合训练

### 2.2 Sample-dependent issues

如果某列只是因为当前样本分布而暂时接近常量或极端稀疏，则不应直接删除其生成逻辑，而应优先通过训练合同剪枝控制。

典型例子：

- 稀疏文本模式列
- 稀疏业务类别列
- 某些时间窗口下恰好缺乏变化的统计列

---

## 3. Design Principles

### 3.1 Separate feature roles before pruning

训练表中的列必须先按角色分层：

- model features
- metadata
- monitoring flags
- audit / trace fields
- targets and split control fields

只有 `model features` 才应该参与后续常量、高基数和不平衡剪枝。

### 3.2 Prefer source-level fixes for structural problems

以下情况应优先从源头修正，而不是“算出来再删”：

- 固定步长导致的恒定衍生列
- 仅用于 join 的 key 列
- 仅用于 fallback 监控的状态列
- merge 后天然成为常量的全局统计列

### 3.3 Use train-only gates for sample-dependent sparsity

以下情况不宜直接删掉生成逻辑：

- 文本模式列当前训练集中过稀
- 某些类别列本批数据缺乏覆盖

对于这些列，应在导出阶段基于 train split 做自动准入控制。

### 3.4 One canonical model feature contract must be shared with execution engine

`execution_engine` 当前直接消费离线 bundle 中的 `feature_contract.json`，并在 live inference 前按该 contract 对输入特征做对齐。

这意味着本计划不能演化出两套不同的 model feature contract：

- 一套给离线训练
- 一套给 execution engine

允许分层和差异化的范围只能是非模型列：

- metadata
- monitoring flags
- audit / trace fields
- control columns

换句话说：

- offline 导出的 canonical `model_feature_columns`
- runtime bundle 中写入的 `feature_contract.feature_columns`
- execution engine live scoring 实际消费的 model inputs

必须是同一套列定义。

### 3.5 Non-model columns may differ, but not the model feature contract

在线链路可能仍需要：

- fallback indicators
- group defaults
- identity trace fields

但这些列不必进入 canonical model feature contract。

因此，本计划中的“分层”含义是：

- 把非模型列从 canonical feature contract 外移
- 而不是为 offline 和 execution engine 分别维护两份不同的 feature_columns

---

## 4. Target Contract Architecture

离线导出后的平面表应至少区分四类列。

其中只有 `model feature columns` 会进入 canonical feature contract，并由离线训练和 `execution_engine` 共同使用。

### 4.1 Model feature columns

真正用于模型学习的数值 / 类别特征。

### 4.2 Metadata columns

用于样本追踪、分析、join 或报告，但不进入模型：

- market / token / outcome identity fields
- group keys
- composite structural keys

### 4.3 Monitoring columns

用于 serving fallback、命中率审计、数据质量观察：

- group / fine match flags
- fallback-only flags
- stale / override flags

### 4.4 Control columns

用于训练流程控制，但不属于模型输入：

- dataset split
- targets
- publish selection columns

### 4.5 Shared canonical contract boundary

本计划要求把“平面表中的全部列”与“真正进入模型的 contract 列”明确区分。

边界定义如下：

- 平面表可以继续包含 metadata / monitoring / control 列
- `feature_contract.json` 只能包含 canonical `model feature columns`
- execution engine 只能按这套 canonical `model feature columns` 对 live frame 做对齐和推理

因此，任何 DQC 清理动作如果影响模型输入列，都必须同步反映到：

- offline export 的 feature contract 生成逻辑
- model bundle 写出的 `feature_contract.json`
- execution engine 读取并校验 contract 的 runtime 逻辑

### 4.6 Project-wide `rule_score` removal requirement

本计划新增一条跨整个项目的硬约束：

- `rule_score` 及其所有派生特征必须从整个项目中完全清除

这里的“完全清除”包括但不限于：

- 基础列：`rule_score`
- fine / group / default 变体：如 `fine_feature_rule_score`、`group_default_rule_score`
- 以 `rule_score_` 开头的派生列
- 以 `rule_score_minus_` 开头的派生列
- 名称中包含 `_rule_score` 的聚合或衍生列，例如 `max_rule_score`、`mean_rule_score`、`group_rule_score_x_edge_lower`
- runtime bundle、serving defaults manifest、reports、audits、analysis、backtesting、tests 和 `execution_engine` 中的相关引用

这条要求的语义不是“重定义 `rule_score`”，而是：

- 不再保留 `rule_score`
- 不再保留任何以 `rule_score` 为输入构造的衍生特征
- 不再让 offline/export/runtime/execution engine 依赖 `rule_score` 家族字段

如果后续需要新的排序或去重信号，必须基于其他明确批准的字段单独设计；该替代方案不在本文档中定义。

---

## 5. Constant Features

## CF-1. Remove global history raw metrics from the training contract

### Current problem

最新 DQC 中大量常量列来自：

- `group_feature_global_expanding_*`
- `group_feature_global_recent_90days_*`

这些列在全表上几乎必然为单值，因为 `global` level 只有一个聚合键。

### Source

- `rule_baseline/history/history_features.py`
- `rule_baseline/training/train_rules_naive_output_rule.py`

具体链路：

1. `summarize_history_features()` 生成 `global` level 的窗口统计
2. `build_group_serving_features()` 将这些全局统计 merge 到每个 `group_key`
3. merge 后每一行拿到相同值，于是训练表中变成常量

### Columns in scope

包括但不限于：

- `group_feature_global_expanding_snapshot_count`
- `group_feature_global_expanding_market_count`
- `group_feature_global_expanding_bias_mean`
- `group_feature_global_expanding_brier_mean`
- `group_feature_global_expanding_logloss_mean`
- `group_feature_global_recent_90days_snapshot_count`
- `group_feature_global_recent_90days_market_count`
- `group_feature_global_recent_90days_bias_mean`
- `group_feature_global_recent_90days_brier_mean`
- `group_feature_global_recent_90days_logloss_mean`

### Required change

训练合同不再直接保留原始 `global` level 统计列。

允许保留的形式：

- 相对差值
- 归一化比值
- z-score
- tail spread 与局部层级的比较项

不允许继续把全局绝对统计本体作为训练输入。

### Acceptance criteria

- 导出的训练特征合同不再包含 `group_feature_global_expanding_*` 原始列
- 导出的训练特征合同不再包含 `group_feature_global_recent_90days_*` 原始列
- 基于全局统计构造的相对差值列仍可保留

---

## CF-2. Stop generating fixed-width rule price width features

### Current problem

`rule_price_width` 与 `group_feature_group_default_rule_price_width` 在当前规则桶定义下为常量。

根因不是数据，而是价格桶步长固定。

### Source

- `rule_baseline/training/train_rules_naive_output_rule.py`
- `rule_baseline/features/tabular.py`

### Columns in scope

- `rule_price_width`
- `group_feature_group_default_rule_price_width`

### Required change

在当前固定价格桶设计下：

- 停止生成 `rule_price_width`
- 停止生成 `group_default_rule_price_width`
- 停止将其纳入训练特征合同

如果未来引入动态 price bins，再单独评估是否恢复。

### Acceptance criteria

- 训练表中不再出现 `rule_price_width`
- 训练表中不再出现 `group_feature_group_default_rule_price_width`
- serving fallback 资产不依赖这两个字段完成运行

---

## CF-3. Move default fallback placeholders out of training features

### Current problem

以下列本质上是 fallback 占位或默认值信号：

- `group_feature_group_default_direction`
- `group_feature_fine_match_found_default`
- `group_feature_group_match_found_default`

它们属于 runtime fallback 设计的一部分，不是模型要学习的可泛化信号。

### Source

- `rule_baseline/training/train_rules_naive_output_rule.py`

### Required change

这些列应从训练特征合同中移除，并转为：

- serving defaults manifest
- monitoring / audit 字段

### Acceptance criteria

- 上述默认占位列不再进入训练特征合同
- fallback manifest 仍能表达其默认语义
- 线上 fallback 行为不受影响

---

## CF-4. Reclassify selected reference side metadata

### Current problem

`selected_reference_side_index` 当前在训练集中为常量，本质上是样本结构 metadata，而不是当前模型真正依赖的建模信号。

### Source

- `rule_baseline/datasets/snapshots.py`
- `rule_baseline/features/snapshot_semantics.py`

### Required change

在未引入真正的 reference side 双边建模前：

- 将 `selected_reference_side_index` 从模型特征合同中移除
- 保留为 metadata / audit 字段

### Acceptance criteria

- `selected_reference_side_index` 不再作为训练输入列
- 样本追踪、审计和报告仍可访问该列

---

## CF-5. Prune low-value extreme history statistics at category level

### Current problem

报告中 category 层的一批 extrema 列长期为常量，例如：

- `group_feature_category_expanding_bias_min`
- `group_feature_category_expanding_bias_max`
- `group_feature_category_expanding_abs_bias_max`
- `group_feature_category_expanding_brier_max`
- `group_feature_category_expanding_logloss_max`
- recent_90days 同族列

这说明在当前合同设计下，category-level 极值统计稳定性差、训练价值低。

### Source

- `rule_baseline/history/history_features.py`

### Required change

history 统计收缩时，优先停掉下列类型：

- `*_min`
- `*_max`
- `*_abs_bias_max`
- `*_brier_max`
- `*_logloss_max`

优先保留：

- `mean`
- `p50`
- `p75`
- `p90`
- `std`
- 已定义的 gap / spread / z-score 列

### Acceptance criteria

- category 层极值统计列显著减少
- 保留下来的 history 列以稳定分布统计为主
- DQC 中 category 常量列数量明显下降

---

## 6. High Cardinality Features

## HC-1. Remove token and outcome identity fields from model features

### Current problem

以下列是典型身份字段，不具备稳定泛化意义：

- `token_0_id`
- `token_1_id`
- `selected_reference_token_id`
- `outcome_0_label`
- `outcome_1_label`
- `selected_reference_outcome_label`

它们的高基数来自市场 identity，而不是可迁移语义。

### Source

- `rule_baseline/datasets/snapshots.py`
- `rule_baseline/features/snapshot_semantics.py`

### Required change

这些字段应全部从训练模型输入中移除，并仅保留为：

- metadata
- trace fields
- audit / debugging 字段

模型可继续使用从文本中提炼出的语义特征，但不应直接使用原始 outcome label 或 token id。

### Acceptance criteria

- 上述 token / outcome identity 字段不在模型输入合同中
- 它们仍可用于审计和样本追踪

---

## 7. Highly Imbalanced Categorical Features

## IMB-1. Move control and monitoring flags out of model inputs

### Current problem

以下列在 DQC 中接近或达到 100% 单值：

- `group_match_found`
- `fine_match_found`
- `used_group_fallback_only`
- `group_decision`
- `dataset_split`

这些列本质是：

- 流程控制列
- 命中率监控列
- fallback 观察列

不属于模型应学习的特征。

### Source

- `rule_baseline/features/serving.py`
- `rule_baseline/training/train_rules_naive_output_rule.py`
- `rule_baseline/datasets/splits.py`
- `rule_baseline/training/export_features.py`

### Required change

全部从模型特征合同移出，并重分类为：

- monitoring columns
- control columns

### Acceptance criteria

- 训练输入中不包含上述列
- DQC 中这些列若继续保留，也只出现在非模型列集合中

---

## IMB-2. Upgrade stale quote from sparse feature to quality filter

### Current problem

`stale_quote_flag` 目前严重偏斜。它更像质量过滤信号，而不是稳定建模信号。

### Source

- `rule_baseline/data_collection/build_snapshots.py`
- `rule_baseline/datasets/snapshots.py`
- `rule_baseline/features/snapshot_semantics.py`

### Required change

优先将 `stale_quote_flag` 用于：

- data quality filtering
- audit summaries

而不是作为训练输入特征。

### Acceptance criteria

- `stale_quote_flag` 不再进入模型输入合同
- 训练前质量过滤和审计报告仍可使用该列

---

## IMB-3. Treat annotation override as audit signal first

### Current problem

`category_override_flag` 当前极端偏斜，更像标注质量或解析冲突信号，而不是主建模信号。

### Source

- `rule_baseline/domain_extractor/market_annotations.py`
- `rule_baseline/features/annotation_normalization.py`

### Required change

首批治理中：

- 将 `category_override_flag` 从训练特征合同中移除
- 保留到 annotation audit / diagnostics

后续如 feature importance 证明其有显著贡献，再单独评估回加。

### Acceptance criteria

- `category_override_flag` 不再属于默认模型输入列
- 标注与归类审计仍保留该字段

---

## IMB-4. Directly stop computing sparse lexical and category flags

### Current problem

以下列当前高度偏斜，并且本轮治理中不再采用 prevalence gate 作为保留条件，而是全部直接停算：

- `has_year`
- `has_dollar`
- `starts_can`
- `has_by`
- `has_above_below`
- `has_and`
- `is_team_total`
- `cat_entertainment`
- `dur_long`

当前决策是把这批列统一视为低价值稀疏布尔特征，直接从源头停算，避免继续占用 canonical model feature contract。

### Source

- `rule_baseline/features/market_feature_builders.py`

### Required change

本轮对以下列全部执行源头停算：

- `has_year`
- `has_dollar`
- `starts_can`
- `has_by`
- `has_above_below`
- `has_and`
- `is_team_total`
- `cat_entertainment`
- `dur_long`

具体要求：

- 在 `rule_baseline/features/market_feature_builders.py` 中停止生成这些列
- 不再把这些列纳入 canonical model feature contract 候选集
- execution engine 继续消费同一套 contract，但这批列不再出现在 shared `feature_contract.json` 中
- 如后续需要恢复，必须基于新的数据覆盖与增益评估单独立项，而不是通过默认 prevalence gate 自动回流

### Acceptance criteria

- 上述 9 个稀疏布尔列全部停止计算
- 这些列不再进入 canonical model feature contract
- shared `feature_contract.json` 与 execution engine live inference 中不再出现这些列

---

## 8. Export-Time Automatic Gates

本节处理的是“样本相关问题”，不是结构性错误的替代方案。

所有 gate 只允许作用于 canonical `model feature columns` 候选集，并且 gate 结果必须进入最终写出的 shared `feature_contract.json`，供 execution engine 复用。

## GATE-1. Zero-variance gate

### Requirement

在 `train` split 上计算每个候选模型特征的方差或唯一值数。

规则：

- 唯一值数 <= 1: 不进入训练合同

### Notes

该 gate 只应对 `model features` 生效，不应误删 metadata / monitoring / control 列。

---

## GATE-2. Binary prevalence gate

### Requirement

对二值 / 布尔候选模型特征，在 `train` split 上计算正类占比。

默认规则：

- `< 1%` 不进入训练合同
- `> 99%` 不进入训练合同

### Notes

阈值应可配置。

---

## GATE-3. Warning-only cardinality diagnostics for remaining categoricals

### Requirement

对保留下来的 canonical categorical 列，如 `domain` / `category` / `market_type`，仅输出诊断信息，不自动删除。

### Notes

高基数并不自动等于无价值；在源头移除身份字段后，剩余 canonical categorical 应以模型效果为准，而不是仅按唯一值数裁剪。

---

## 9. Implementation Touchpoints

本计划预计涉及以下文件：

- `rule_baseline/training/export_features.py`
  - 引入训练合同分层与自动 gates，并产出 shared canonical feature contract
- `rule_baseline/training/train_snapshot_model.py`
  - 确保模型只消费 canonical `model_feature_columns`，并去除 `rule_score` 相关排序与合同引用
- `rule_baseline/features/snapshot_semantics.py`
  - 明确 metadata / monitoring / feature 列的角色划分，并统一 contract 边界，同时移除 `rule_score` 家族字段
- `rule_baseline/features/serving.py`
  - 避免训练路径无意带入监控 / fallback 状态列
- `rule_baseline/training/train_rules_naive_output_rule.py`
  - 停止生成无信息量的默认占位或固定宽度字段，并清除 `rule_score` 及其派生列
- `rule_baseline/features/tabular.py`
  - 清除 `rule_score` 及其派生交叉特征
- `rule_baseline/history/history_features.py`
  - 收缩低价值 extrema 和全局原始统计输出
- `rule_baseline/features/market_feature_builders.py`
  - 直接停算 IMB-4 中定义的稀疏 lexical / category 布尔列
- `rule_baseline/quality_check/feature_dqc.py`
  - 在报告中区分“模型列问题”和“非模型列问题”，并不再把 `rule_score` 视为保留特征
- `rule_baseline/reports/build_groupkey_feature_inventory.py`
  - 移除 `rule_score` 家族字段的 inventory / 分类逻辑
- `rule_baseline/analysis/*`
  - 移除基于 `rule_score` 的排序、去重和分析引用
- `rule_baseline/backtesting/*`
  - 移除基于 `rule_score` 的候选排序和输出字段引用
- `execution_engine/online/scoring/rule_runtime.py`
  - 继续从 runtime bundle 读取同一份 `feature_contract.json`，同时去除 `rule_score` 相关排序键
- `execution_engine/online/scoring/live.py`
  - 继续按 shared canonical contract 对 live inference 输入做对齐
- `execution_engine/online/scoring/rules.py`
  - 清除 `group_default_rule_score` / `rule_score` fallback 注入逻辑
- `execution_engine/tests/test_rule_runtime_bundle.py`
  - 覆盖 runtime bundle 暴露 feature contract 的一致性
- `execution_engine/tests/test_live_snapshot_semantics.py`
  - 覆盖 live inference 对 shared contract 的校验与缺列行为
- `polymarket_rule_engine/tests/*` 与 `execution_engine/tests/*`
  - 全量移除 `rule_score` 及其派生字段的测试输入与断言

---

## 10. Recommended Rollout Order

### Batch 1. Structural contract cleanup

优先级最高，先做角色分层与结构性移除：

- control / monitoring 列移出训练合同
- token / outcome / key 类字段移出训练合同
- global history raw metrics 移出训练合同
- 固定宽度与默认占位列停算或移出训练合同

### Batch 2. History family pruning

- 收缩 category 层 extrema
- 只保留稳定分布统计与相对差值列

### Batch 3. Sparse feature gating

- 直接停算 IMB-4 中列出的稀疏 lexical / category 布尔列
- 引入 train-only zero-variance gates，并仅把 prevalence gate 作为其他未来稀疏模型列的可选机制

### Batch 4. DQC and audit alignment

- 让 DQC 报告区分模型列与非模型列
- 避免 metadata / monitoring 列继续污染模型质量判断

---

## 11. Acceptance Criteria

完成本计划后，应满足以下结果：

### Contract-level acceptance

- 训练输入合同只包含真正的模型列
- metadata / monitoring / control 列被明确分层
- identity fields 和 structural keys 不再进入模型输入
- 离线导出的 canonical `model_feature_columns` 与 runtime bundle 中的 `feature_contract.feature_columns` 完全一致
- canonical `model_feature_columns`、shared `feature_contract.json`、serving defaults manifest 和 execution engine live inputs 中均不再出现 `rule_score` 家族字段

### DQC-level acceptance

- constant features 数量显著下降
- high-cardinality 列中不再包含 token / outcome / key 身份字段作为模型输入
- highly imbalanced categorical 问题主要只剩真正保留的业务信号列

### Runtime safety acceptance

- 在线 serving fallback 行为不退化
- group / fine attach 逻辑仍可运行
- 审计、回测和报告仍可访问需要的 metadata / monitoring 字段
- `execution_engine` 继续使用同一套 canonical feature contract 完成 live scoring
- 不存在 offline 一套 `feature_columns`、execution engine 另一套 `feature_columns` 的分叉
- `execution_engine`、offline analysis、backtesting 和 tests 中不再存在对 `rule_score` 或其衍生字段的运行时依赖

---

## 12. Discussion Points for Next Iteration

后续讨论建议优先围绕以下问题展开：

1. `model_feature_columns` / `metadata_columns` / `monitoring_columns` 应由哪个模块统一声明
2. global history raw metrics 是直接停算，还是继续生成但不进入训练合同
3. `category` / `domain` / `market_type` 的 canonical 保留名单是否还要继续缩减
4. prevalence gate 的默认阈值是否采用 `0.5% / 99.5%`
5. `feature_dqc.py` 是否需要只对模型输入列输出主报告，并把非模型列单独附录展示
6. shared canonical contract 应继续由 `rule_baseline.features.snapshot_semantics` 统一定义，还是提升到更明确的跨 offline / execution_engine 边界模块
7. `rule_score` 被完全清除后，原先依赖它的排序、去重和候选优先级逻辑改由哪些字段接管
