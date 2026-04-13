你是一名资深量化研究工程师 / 特征工程架构师。现在进入第三阶段：在第二阶段的新逻辑已经稳定运行的前提下，把 feature blueprint 中真正值得加、且未实现/不重复的 feature 加入 `trading_rules.csv`。

## 背景

`trading_rules.csv` 原有列如下，必须全部保留：

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

第二阶段已经完成：
- 不再按 rule 过滤样本
- 改为按 group_key 过滤
- 对保留下来的 group_key，保留全部 rule

现在要做的是：
- 基于 `polymarket_groupkey_500_feature_blueprint.md`
- 只新增“未实现、非重复、适合落到 trading_rules.csv”的 feature
- 不删除任何原有列
- 不机械地把 500 个特征全部硬塞进来

## 明确要求

### 1. 先基于第一阶段的 feature 审计结果执行
请先参考/复用之前的 feature 审计结论，只新增：
- 真正未实现
- 不重复
- 当前数据可支持
- 适合放入 `trading_rules.csv`
的特征

### 2. 区分粒度
请明确区分：
- group_key 级特征
- domain 级特征
- category 级特征
- market_type 级特征
- 两两组合层级特征
- rule 级特征
- 不适合落表、应该在训练时动态构造的特征

### 3. 保持 schema 可维护
如果你认为：
- 直接把所有新增特征都塞进 `trading_rules.csv` 不够合理
- 应该拆成附表再 join
请明确提出，但如果项目当前更适合先保守落在单表，也请给出最稳妥单表方案。

### 4. 不引入 leakage
请特别检查：
- 所有历史统计是否只来自当前样本时点之前的已知历史
- 是否存在未来信息进入落表特征

## 输出要求

### Part 1. 最终新增特征清单
请给出最终会新增的特征列表，并说明：
- 含义
- 粒度
- 是否重复
- 为什么应该落表

### Part 2. schema 设计
给出 `trading_rules.csv` 的最终 schema 设计：
- 原有列
- 新增列
- 哪些是 group 重复列
- 哪些是 rule 级列

### Part 3. 代码实现
然后再给出具体修改。

### Part 4. 验证
请验证：
- 原有列是否全部保留
- 新增列是否都已落表
- 是否与现有实现重复
- 下游训练能否正常读取
- 是否存在 leakage

### Part 5. 风险说明
指出任何你发现的问题或建议后续再拆分优化的点。

## 重要限制
- 不按 horizon 分层
- 不要删除原有 17 列
- 不要重复实现已有特征
- 不要为追求数量而机械加列
- 优先保证正确性、可维护性和无泄露