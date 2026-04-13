你是一名资深量化研究工程师 / ML pipeline 重构工程师。现在进入第二阶段：**真正切换过滤逻辑**。这一阶段允许你写代码并修改流程，但请只做“逻辑切换”，不要一次性加入大量新特征。

## 已知新业务逻辑

旧逻辑：
- 训练前先用 `trading_rules.csv` 做 rule 匹配
- 不符合 rule 的样本被筛掉
- 只有命中 rule 的样本进入训练

新逻辑：
- **不再做 rule 级筛选**
- **不再根据 leaf_id / direction / price range / horizon range 去筛样本**
- **改为只按 group_key 做筛选**
- 对保留下来的 `group_key`，保留该 group 下的全部 rule 行
- 对删除的 `group_key`，训练样本删掉，`trading_rules.csv` 下该 group 的所有 rule 行也删掉

## group_key 筛选规则

明确：
- **不按 horizon 分层**
- 所有统计 pooled

定义：
- 对每个 `group_key` 计算：
  - median log loss
  - median brier
- 再计算：
  - 全部 `group_key_median_logloss` 的中位数
  - 全部 `group_key_median_brier` 的中位数

删除条件：
- `drop_group = (group_key_median_logloss < global_median_of_group_median_logloss) AND (group_key_median_brier < global_median_of_group_median_brier)`

含义：
- 两个指标都低于全局阈值，说明这个 group_key 历史上已经预测得较好，应删除
- 否则保留

## 本阶段目标

### 1. 移除 rule 级过滤
请定位并修改当前代码，使得：
- 样本进入训练不再要求命中某条 rule
- 旧的 rule matching filter 不再参与训练样本裁剪

### 2. 加入 group_key 级过滤
请实现：
- 先计算/读取 group_key 级 keep/drop 标记
- 训练样本只按 `group_key_keep_flag` 过滤
- 被 drop 的 group_key 样本不进训练

### 3. 更新 trading_rules.csv 的保留逻辑
请修改相关生成逻辑，使得：
- 对保留下来的 group_key，保留该 group 的全部 rule 行
- 对被 drop 的 group_key，删除该 group 的全部 rule 行
- 不再做 rule 级筛选删行

### 4. 本阶段不要做大规模 feature 扩展
这一阶段只允许加入极少数必要字段，例如：
- group_median_logloss
- group_median_brier
- group_keep_flag / group_drop_flag
- global_threshold_logloss
- global_threshold_brier

不要把 500 个 feature 一次性都加进来。

## 输出要求

### Part 1. 改动方案
先说明：
- 你准备修改哪些文件
- 每个文件改什么
- 为什么这样改

### Part 2. 代码实现
然后再给出具体修改。

### Part 3. 验证结果
请验证并汇报：
- rule 级过滤是否已完全移除
- group_key 级过滤是否正确生效
- 保留下来的 group_key 是否保留了全部 rule 行
- 被 drop 的 group_key 是否从训练数据和 trading_rules.csv 同时消失
- 样本数、group 数、rule 行数前后变化

### Part 4. 风险说明
指出任何你发现的风险或需要我确认的地方。

## 重要限制
- 不按 horizon 分层
- 不要顺手加入大批 feature
- 优先保证逻辑切换正确、链路清晰、行为一致