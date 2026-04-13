你是一名资深量化研究工程师 / ML pipeline 审查工程师。请你先只做“审查 + dry-run 分析”，不要改代码逻辑，不要写 patch。

我要对 Polymarket 的训练流程做较大重构，但第一阶段只允许你做审查、统计和方案设计，不允许改变现有训练行为。

## 已知背景

我有一个 `trading_rules.csv`，现有列如下，这些列后续必须全部保留：

- group_key
- domain
- category
- market_type
- leaf_id
- price_min
- price_max
- h_min
- h_max
- direction
- q_full
- p_full
- edge_full
- edge_std_full
- edge_lower_bound_full
- rule_score
- n_full

其中：

- `group_key = domain | category | market_type`

当前旧逻辑中，训练前会用 `trading_rules.csv` 做 rule 匹配筛选：
- 样本先匹配 rule
- 不符合 rule 的样本被筛掉
- 只有符合 rule 的样本进入训练

我未来想改成：
- 不再做 rule 级筛选
- 改为对 `group_key` 做筛选
- 但这一阶段先不要改，只做 dry-run

## 本阶段要求

### 1. 审查当前实现
请审查并精确说明：
- `trading_rules.csv` 在哪些文件/函数里生成
- `trading_rules.csv` 在哪些文件/函数里被读取
- rule 匹配筛选发生在哪些文件/函数/类
- 样本被筛掉发生在哪一步
- 训练、验证、回测、推理里是否都依赖这个逻辑

要求精确到文件 / 函数 / 类 / 数据流。

### 2. 做 group_key 级 dry-run 统计
这次明确：
- **不按 horizon 分层**
- 所有统计都按整体 pooled 做

请基于当前项目可用数据，计算每个 `group_key` 的：
- median log loss
- median brier

然后计算两个全局阈值：
- 全部 `group_key_median_logloss` 的中位数
- 全部 `group_key_median_brier` 的中位数

然后按以下默认规则打标：
- `drop_group = (group_key_median_logloss < global_median_of_group_median_logloss) AND (group_key_median_brier < global_median_of_group_median_brier)`

也就是说：
- 两个指标都低于全局阈值，认为这个 group_key 历史上已经预测得较好，应删除
- 否则保留

注意：
- 这一阶段只做 dry-run
- 不要真正修改训练流程
- 不要真正删数据
- 只输出分析结果

### 3. 做 feature 审计
我还有一份 `polymarket_groupkey_500_feature_blueprint.md`。

请审查：
- 哪些 feature 已经在 `trading_rules.csv` 里
- 哪些不在 csv 里，但已在代码其他地方实现
- 哪些语义重复，不建议重复落表
- 哪些是真正需要新增的
- 哪些暂时无法实现

请把 feature 分成：
- A 类：已存在于 csv
- B 类：已实现但未落表
- C 类：语义重复/高度重叠
- D 类：应新增
- E 类：暂时无法实现

## 输出要求

请输出以下内容：

### Part 1. 当前实现审查
- 生成路径
- 消费路径
- rule 过滤具体位置
- 受影响模块
- 当前数据流

### Part 2. group_key dry-run 统计
- 每个 group_key 的 median log loss / median brier
- 两个全局阈值
- 每个 group_key 的 keep/drop 标记
- 保留/删除的 group_key 列表
- 如果未来启用该逻辑，会影响多少样本、多少 rule 行、多少 group_key

### Part 3. feature 审计
按 A/B/C/D/E 分类输出，并说明理由。

### Part 4. 风险与歧义
指出：
- 你发现的任何 leakage 风险
- 当前数据口径风险
- 这个 group_key 阈值定义是否有潜在问题
- 任何你认为需要我确认的地方

## 重要限制
- 不要写代码
- 不要改任何逻辑
- 不要给 patch
- 只做审查、统计、设计、dry-run 分析