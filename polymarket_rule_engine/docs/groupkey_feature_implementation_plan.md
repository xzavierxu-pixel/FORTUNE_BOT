# Polymarket group_key 特征化实施计划

## 1. 目标

基于以下两份文档形成一份可执行实施计划：

- `polymarket_groupkey_500_feature_blueprint.md`
- `phase1.md`

本计划的核心目标不是一次性把 500 个特征全部硬塞进训练流，而是按低风险顺序完成以下迁移：

1. 先完整审查当前 `trading_rules.csv` 的生成、消费和 rule 过滤链路。
2. 在**不改变现有训练行为**的前提下，完成 `group_key` 级 dry-run 统计与可行性判断。
3. 基于 blueprint 对 500 个特征做数据口径、实现位置、重复度和可实现性拆解。
4. 在 phase1 审查结果稳定后，再进入真实实现阶段，逐步把“rule 级筛选”迁移为“group_key 级筛选 + 分层历史特征 + 规则先验特征”。

## 2. 设计原则

### 2.1 先审查再改逻辑

`phase1.md` 明确要求第一阶段只能做审查、统计、设计和 dry-run，不能直接改训练行为。因此实施顺序必须严格分层：

- 第 1 层：现状审查
- 第 2 层：dry-run 统计
- 第 3 层：特征审计
- 第 4 层：真实接入

### 2.2 过滤逻辑与模型输入分离

根据 blueprint，后续应把两个概念分开：

- **粗筛逻辑**：未来由 `group_key` 质量阈值承担，替代现有 rule 级筛选
- **模型输入**：使用分层历史特征、价格曲面、规则先验、交互项等作为训练信号

不能把“是否进入训练”和“作为模型 feature”混成一套逻辑，否则后续很难排查行为漂移来源。

### 2.3 所有历史统计必须防 leakage

blueprint 已明确要求所有 `recent_50 / recent_200 / expanding` 类统计只能使用当前样本预测时点之前已 resolved 的市场。后续实现阶段要把这条写成硬约束，不能在落地时弱化为“近似历史”。

### 2.4 分层优先，不依赖单一 full_group_key

后续实现不能只保留 `full_group_key=domain|category|market_type` 这一层；至少要同时规划：

- `global`
- `domain`
- `category`
- `market_type`
- `domain×category`
- `domain×market_type`
- `category×market_type`
- `full_group_key`

这样在细粒度样本不足时，可以从更粗层级借信息，避免 full group 过 sparse。

## 3. 总体实施分期

## Phase A：现状审查与 dry-run 设计

### 目标

把 `phase1.md` 要求的审查、统计和分类全部做完，但不改任何训练逻辑。

### 交付物

1. 当前实现审查文档
2. `group_key` dry-run 统计结果
3. 500 特征审计结果（A/B/C/D/E 分类）
4. 风险与歧义清单

### 核心任务

1. 精确定位 `trading_rules.csv` 的生成路径
2. 精确定位 `trading_rules.csv` 的读取路径
3. 确认 rule 匹配筛选在哪个文件/函数/类生效
4. 确认样本真正被过滤掉的数据流位置
5. 识别训练、验证、回测、推理是否共用该逻辑
6. 计算 pooled 口径下每个 `group_key` 的 median logloss / median brier
7. 计算全局中位阈值并产出 keep/drop dry-run 标记
8. 评估未来启用该逻辑后对样本数、rule 行数、group_key 数的影响
9. 将 500 特征分成 A/B/C/D/E 五类

### 验收标准

- 能精确回答“当前系统在哪里生成 rule、在哪里消费 rule、在哪里过滤样本”
- 能在不改代码逻辑的情况下给出完整 dry-run 结果
- 能说明每类 feature 为什么保留、复用、去重、推迟或暂不可做

## Phase B：数据契约与中间产物设计

### 目标

在开始真实编码前，先确定数据结构和中间表契约，避免后面一边写代码一边改口径。

### 交付物

1. `group_key` 统计表 schema
2. 分层历史特征表 schema
3. 规则先验映射 schema
4. 字段命名与版本约定

### 核心任务

1. 定义 `group_key` 主键口径
2. 明确 `domain/category/market_type/full_group_key` 的拆分规则
3. 定义 historical stats 的时间约束字段
4. 定义窗口口径：`recent_50`、`recent_200`、`expanding`
5. 定义 quality 指标字段：bias、abs_bias、brier、logloss、quantile、count 等
6. 定义规则先验字段如何从 `trading_rules.csv` 映射到样本
7. 约定 feature 生成时的版本号与产物路径

### 验收标准

- 新老链路的数据契约可以并行存在，不互相覆盖
- 任一特征字段都能明确回答“来源是什么、时间边界是什么、在哪层统计出来”

## Phase C：先实现 dry-run 可复用统计产物

### 目标

先实现不影响训练行为的“旁路统计产物”，让后续切换逻辑建立在已经验证过的数据上。

### 交付物

1. `group_key` 级质量统计产物
2. 分层统计产物
3. keep/drop 建议清单
4. 与现有 rule 覆盖的对照报表

### 核心任务

1. 实现 pooled 版 `group_key` 统计
2. 实现分层统计骨架：global / 单层 / 两两组合 / full group
3. 保持 phase1 先不分 horizon 的约束
4. 输出建议过滤清单，但不接入训练
5. 补上统计完整性检查

### 验收标准

- dry-run 结果可重复
- 每次运行对同一输入得到稳定一致的 `keep/drop` 输出
- 不修改现有训练、验证、回测、推理的行为

## Phase D：500 特征分批落地

### 目标

不是一次性做完 500 个特征，而是按性价比和可验证性分批接入。

### 建议批次

#### D1. 结构特征

优先实现 blueprint 中最容易落地且低 leakage 风险的结构特征：

- `domain/category/market_type` 编码
- `full_group_key` 及两两组合编码
- `domain_is_unknown`
- 历史覆盖率类特征

原因：

- 依赖最少
- 容易校验
- 可直接为后续层级统计提供索引基础

#### D2. 价格曲面特征

优先接入当前样本时点的价格曲面特征：

- `p_1h ~ p_12h`
- `logit_p_*`
- 曲线斜率、加速度、极端距离类特征

原因：

- 信号直接
- 与后续分层质量特征能自然组合
- 比规则先验更容易做单元校验

#### D3. 历史定价质量特征

这是 blueprint 的主体，应按层级和窗口逐批接入：

- expanding 先于 recent
- global/单层 先于 full_group_key
- count/mean 先于 quantile/tail 指标

原因：

- expanding 更稳定
- 粗层级更不容易 sparse
- 可以先建立统计可靠性，再补尾部和高阶统计

#### D4. 稳定性 / 尾部 / 不确定性特征

在质量统计稳定后接入：

- abs bias 分位数
- logloss/brier 尾部分位数
- dispersion / instability / uncertainty 类指标

原因：

- 更依赖统计样本量
- 更容易因为窗口口径不严谨引入噪声

#### D5. 规则表先验特征

把 `trading_rules.csv` 从“筛选器”逐步降级为“先验信号来源”：

- matched rule count
- max/mean rule_score
- max/mean edge_full
- max/mean edge_lower_bound_full
- sum_n_full

原因：

- 这是从旧逻辑迁移到新逻辑的关键桥梁
- 能最大化复用已有规则资产
- 可以先作为 feature 使用，再决定是否彻底移除旧筛选路径

#### D6. 交互 / 漂移 / 二阶信号

最后接入高阶交互：

- 历史偏差 × 当前 logit price
- recent 与 expanding 的差值
- 规则分数 gap
- 尾部风险 × 当前极端程度

原因：

- 依赖前面多个家族先稳定
- 对训练分布漂移最敏感
- 最适合作为后期增益项而非第一批基线项

### 验收标准

- 每个批次都能独立训练和验证
- 特征接入顺序能支持回退
- 任一批次增益不明显时，可以停在前一批不影响主链路

## Phase E：从 rule 级筛选切到 group_key 级筛选

### 目标

在 dry-run 与旁路统计已稳定后，再迁移真正的过滤逻辑。

### 切换策略

1. 先并行输出两套过滤结果
2. 比较 rule-filter 与 group-filter 的样本覆盖差异
3. 对比训练集、验证集、回测结果变化
4. 设定回滚开关
5. 确认新逻辑没有明显伤害后，再默认启用

### 核心任务

1. 把 `drop_group` 逻辑参数化
2. 保留旧 rule-filter 开关
3. 增加对照报表：样本命中、覆盖率、收益、分组分布变化
4. 对“被旧 rule 保留但被新 group 丢弃”的样本做专项审查
5. 对“被旧 rule 丢弃但被新 group 保留”的样本做专项审查

### 验收标准

- 新旧过滤逻辑可切换
- 差异可解释
- 回测与验证结果没有出现无法解释的结构性退化

## 4. 特征落地优先级建议

### P0：必须先做

- 当前实现审查
- dry-run 统计
- feature 审计
- 数据契约设计
- leakage 边界定义

### P1：第一批真实实现

- 结构特征
- 价格曲面特征
- expanding 版粗层级质量统计
- 最基础规则先验特征

### P2：第二批增强

- recent_50 / recent_200 统计
- full_group_key 细粒度统计
- 尾部风险与稳定性特征

### P3：最后增强

- 高阶交互
- 漂移差分
- 规则 gap 类二阶特征

## 5. 风险清单

### 5.1 Leakage 风险

- 历史统计若误用了当前样本之后才 resolved 的市场，会直接污染训练
- pooled 统计若和未来真实训练 horizon 不一致，可能造成表面增益
- 规则先验若使用了包含未来信息的聚合列，会把旧规则中的 hindsight 带入模型

### 5.2 稀疏性风险

- `full_group_key` 数量多、样本分布不均，细层级统计很容易不稳定
- `domain×market_type` 或 `domain×category` 组合层也可能出现长尾

### 5.3 口径风险

- `group_key = domain|category|market_type` 的拼接规则必须固定
- `UNKNOWN`、缺失值、大小写和路径格式化若不统一，会生成伪新组
- price/horizon/direction 与 rule 匹配口径如果和旧逻辑不一致，会影响可比性

### 5.4 迁移风险

- 旧 rule-filter 可能隐含了不仅是质量筛选，还有风险控制意义
- 直接替换为 group-level 阈值，可能放回一些历史上少见但危险的样本
- 仅用 median 阈值切分 keep/drop，可能过于粗糙，后续可能需要 count floor 或稳定性约束

## 6. 建议的验收框架

每个阶段都至少做以下检查：

1. 统计口径检查：字段来源、去重、缺失、时间边界
2. 数据量检查：总样本数、各层级覆盖、长尾分布
3. 对照检查：新旧逻辑输出差异
4. 建模检查：训练/验证/回测指标变化
5. 稳定性检查：不同时间切片、不同类别、不同 market_type 下是否一致

## 7. 最终建议

基于这两份文档，最合理的实施顺序不是“直接实现 500 特征”，而是：

1. 先完成 `phase1.md` 要求的现状审查与 dry-run 统计
2. 再把数据契约和中间统计产物固定下来
3. 之后按“结构特征 -> 价格曲面 -> 历史质量 -> 尾部稳定性 -> 规则先验 -> 高阶交互”的顺序分批落地
4. 最后才切换过滤逻辑，从 rule-filter 迁移到 group_key-filter

这样做的好处是：

- 不会在当前系统行为尚未完全审清前贸然改主链路
- 可以把 `trading_rules.csv` 从硬过滤器平滑迁移为可复用先验资产
- 能把 500 特征拆成可验证、可回退、可解释的多个实现批次
