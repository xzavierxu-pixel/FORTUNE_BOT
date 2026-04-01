# trading_rules.csv 列审计与精简建议

本文档回答一个具体问题：`data/offline/edge/trading_rules.csv` 列很多，其中哪些是当前运行路径必须保留的，哪些是重复/低价值列，哪些更适合迁移到审计型文件而不是继续放在运行时主文件里。

这里的"当前运行路径"不仅包含 `rule_baseline` 下的训练、回测、分析脚本，也包含 `execution_engine` 在线执行链路对规则文件的直接消费。

本文档现在分成两部分：

- 前半部分是当前代码状态审计。
- 后半部分是目标实现规范。

如果两部分有冲突，以"目标实现规范"为准。本文档只描述实现方案，不代表代码已经改完。

结论先行：

- 目标状态下，`trading_rules.csv` 应收敛为一份统一主 schema，保留 `28` 列。
- 这 `28` 列同时覆盖 `rule_baseline` 主回测路径和 `execution_engine` 共享规则契约。
- `train_rules_naive_output_rule_strict.py` 和 `backtest_portfolio_rules_only.py` 视为待删除旧路径，其相关列与遗留逻辑不再属于目标主 schema。

## 目标实现规范

本节定义目标状态，不描述当前已实现状态。

### 目标范围

目标是把 `trading_rules.csv` 收敛成一份统一的运行时规则文件，并同步清理旧训练/旧回测路径。

目标约束如下：

- 运行时规则文件只保留一套主 schema。
- `execution_engine`、`backtest_execution_parity.py`、`backtest_portfolio_qmodel.py` 使用同一套规则字段语义。
- 删除 `rule_baseline/training/train_rules_naive_output_rule_strict.py`。
- 删除 `rule_baseline/backtesting/backtest_portfolio_rules_only.py`。
- 删除上述两个文件对应的遗留文档、说明文字、兼容字段和 fallback 逻辑。

### 目标主路径

目标状态下，规则相关主路径只保留下面三类：

- 规则训练主入口：`rule_baseline/training/train_rules_naive_output_rule.py`
- 主回测入口：`rule_baseline/backtesting/backtest_execution_parity.py`
- 规则 schema 契约定义点：`rule_baseline/backtesting/backtest_portfolio_qmodel.py`

这里特别强调：

- `backtest_execution_parity.py` 是主回测入口。
- 但规则文件实际字段契约，仍由 `backtest_portfolio_qmodel.py` 中的 `load_rules`、`match_rules`、`compute_growth_and_direction` 这一组逻辑决定。
- `execution_engine` 通过 import 复用这组逻辑，因此 schema 变更必须先在这组共享契约上统一定义。

### 目标保留列

目标状态下，`trading_rules.csv` 必须保留以下 `28` 列，并按下面的语义解释：

1. `group_key`
2. `domain`
3. `category`
4. `market_type`
5. `leaf_id`
6. `price_min`
7. `price_max`
8. `h_min`
9. `h_max`
10. `direction`
11. `q_smooth`
12. `edge_lower_bound_valid`
13. `rule_score`
14. `n_train`
15. `n_valid`
16. `n_test`
17. `n_full`
18. `q_train`
19. `p_train`
20. `edge_train`
21. `edge_std_train`
22. `q_valid`
23. `p_valid`
24. `edge_valid`
25. `edge_std_valid`
26. `q_test`
27. `p_test`
28. `edge_test`

这 `28` 列是目标主 schema，不再把 `edge_sample_trade`、`edge_std_trade`、`q_raw_est`、`q_train_raw`、`selection_status` 等旧列视为主文件必需字段。

### 旧列替换关系

目标 schema 不是把所有旧列都做机械式重命名，而是按主路径职责重组字段语义。对于之前被其他脚本直接引用的旧列，应按下表迁移：

| 旧列 | 旧用途 | 目标 schema 中的替换方式 |
| --- | --- | --- |
| `edge_raw_valid` | `backtest_portfolio_qmodel.py` 主路径使用的 valid 主边际列 | 统一改为 `edge_valid`。含义保持为 valid split 的主边际列，但名称与 train / test split 对齐。 |
| `edge_sample_trade_valid` | `backtest_portfolio_qmodel.py` 的旧 fallback valid 边际列 | 不再单独保留；迁移后统一并入 `edge_valid`，同时删除 fallback 逻辑。 |
| `edge_std_trade_valid` | `backtest_portfolio_qmodel.py` 的旧 fallback valid 波动列 | 不再单独保留；迁移后统一并入 `edge_std_valid`，同时删除 fallback 逻辑。 |
| `edge_sample_trade` | `backtest_portfolio_rules_only.py` 用于 rules-only 选规则 | 不做一对一重命名。rules-only 路径删除后，其“主边际筛选”职责由 `edge_valid` 承担，“保守筛选”职责由 `edge_lower_bound_valid` 承担。 |
| `edge_std_trade` | `backtest_portfolio_rules_only.py` 用于 rules-only 波动阈值筛选 | 不再保留；统一由 `edge_std_valid` 承担 valid split 波动统计职责。 |
| `q_raw_est` | 旧 schema 中与 `q_smooth` 并存的估计概率列 | 不再保留；naive 主路径下直接收敛到 `q_smooth`。 |
| `q_train_raw` | 旧 schema 中与 `q_train` 并存的 train 原始概率列 | 不再保留；直接收敛到 `q_train`。 |
| `selection_status` | full report / 审计用途 | 退出运行时主 schema，只允许保留在 audit / full 产物中。 |
| `prior_mean`、`p_value_valid`、`p_value_valid_adj` | strict trainer 的实验/诊断字段 | 随 strict trainer 一起退出运行时主 schema，只允许保留在 audit / full 产物中。 |

这里最关键的不是“旧列名换成哪个新列名”，而是以下三条运行时契约调整：

1. `backtest_portfolio_qmodel.py` 主路径中的 valid 主边际列统一收敛到 `edge_valid`。
2. valid 波动列统一收敛到 `edge_std_valid`。
3. rules-only 那套 `edge_sample_trade` / `edge_std_trade` 旧列不再映射为新主列，而是连同旧回测路径一起退出。

### 目标字段语义

#### 1. 标识与匹配字段

这些字段语义不变：

- `group_key`
- `domain`
- `category`
- `market_type`
- `leaf_id`
- `price_min`
- `price_max`
- `h_min`
- `h_max`
- `direction`

它们继续承担规则匹配、规则键、execution engine 在线过滤与在线打分输入的职责。

#### 2. `q_smooth` 的目标语义

`q_smooth` 继续作为运行时执行估计概率保留。

目标语义：

- 它表示规则最终用于执行侧的估计概率。
- 它不等同于 `q_train` 这一训练分割统计列。
- 即使在某些 offline naive 产物里两者数值相同，文档和代码语义上也必须把二者视为不同概念。

#### 3. split 统计字段的目标语义

以下字段明确按 split 保存：

- train: `q_train`、`p_train`、`edge_train`、`edge_std_train`
- valid: `q_valid`、`p_valid`、`edge_valid`、`edge_std_valid`
- test: `q_test`、`p_test`、`edge_test`
- 计数: `n_train`、`n_valid`、`n_test`、`n_full`

其中：

- `q_*` 表示该 split 的原始命中率。
- `p_*` 表示该 split 的平均市场价格。
- `edge_*` 表示该 split 的方向归一化后边际。
- `edge_std_*` 表示该 split 的边际波动/当前 `edge_std_mean` 聚合统计输出。

`edge_lower_bound_valid` 不应当被视为普通 split 统计列，而应单独理解为一个保守下界指标。

#### 3A. `edge_lower_bound_valid` 的单独语义

`edge_lower_bound_valid` 虽然使用 valid split 的数据计算，但它不是与 `edge_valid` 并列的普通均值统计列，而是一个保守边际下界指标。

这列的目标要求是：逻辑保持不变。

也就是说，目标状态下不改变它的定义方式，只把它的语义写清楚并保留为主 schema 字段。

定义保持为：

- `direction = 1` 时，`edge_lower_bound_valid = q_valid_lower - p_valid`
- `direction = -1` 时，`edge_lower_bound_valid = p_valid - q_valid_upper`

其中：

- `q_valid_lower` / `q_valid_upper` 来自 valid split 命中率的 Wilson 区间
- `p_valid` 是 valid split 平均市场价格

因此这列应理解为：

- 仅基于 valid split 计算
- 按交易方向统一后的保守 edge 下界
- 对通过筛选的规则应保持为正
- 与当前代码逻辑保持一致，不在本次目标方案中改变计算公式

#### 4. 方向归一化的目标定义

目标状态下，以下字段都必须是 direction-adjusted，并且对已选中规则应当始终为正：

- `edge_train`
- `edge_valid`
- `edge_test`

定义为：

- 原始边际：`raw_edge_split = q_split - p_split`
- 方向归一化边际：`edge_split = direction * raw_edge_split`

等价地说：

- `direction = 1` 时，`edge_split = q_split - p_split`
- `direction = -1` 时，`edge_split = p_split - q_split`

因此目标状态下：

- `edge_train` 不再保留当前文件里的原始符号语义，而是改为方向归一化后的 edge；正值表示规则方向有优势，负值表示该方向没有优势。
- `edge_valid` 同理。
- `edge_test` 同理。

`edge_lower_bound_valid` 不纳入本节“方向归一化目标定义”的改写范围。对这列只要求遵循上文 3A 小节的单独说明，即保留当前计算逻辑不变。

#### 5. `edge_std_train` / `edge_std_valid` 的目标语义

目标状态下保留：

- `edge_std_train`
- `edge_std_valid`

它们分别表示 train / valid split 上的边际波动或当前 `edge_std_mean` 聚合统计的标准输出列。

这里需要特别注明两点：

- `edge_std_train` 当前文件中还没有落盘，目标实现需要新增。
- `edge_std_valid` 当前已经存在，但它在目标状态下不再依赖 rules-only 旧逻辑命名体系。

#### 6. `p_test` 的目标语义

目标状态下必须新增 `p_test`，定义为 test split 的平均市场价格。

当前文件中：

- 有 `q_test`
- 有 `edge_test`
- 没有显式输出 `p_test`

目标实现要求补齐 `p_test`，从而让 test split 统计与 train / valid 的字段结构对齐。

#### 7. `q_smooth`、`edge_std_valid`、`rule_score` 的计算逻辑

这一节只说明当前主训练脚本 `train_rules_naive_output_rule.py` 中的实际计算逻辑，用来定义目标 schema 迁移时应保留的数值语义。

##### `q_smooth`

当前主路径里：

- `q_smooth = q_est`
- `q_est = wins_est / n_est`

其中：

- offline 模式使用 train split，所以数值上 `q_smooth = q_train`
- online 模式使用 all split，所以 `q_smooth = wins_all / n_all`

因此 `q_smooth` 的保留理由不是它和 `q_train` 数值不同，而是它承担“最终执行估计概率”这个独立语义。

##### `edge_std_valid`

`edge_std_valid` 来自 valid split 上的 `edge_std_mean` 聚合值，而 `edge_std_mean` 本质上是 snapshot 级标准化边际 `r_std` 的均值。

底层定义可写成：

- `e_sample = y - price`
- `r_std = e_sample / sqrt(price * (1 - price))`
- `edge_std_valid = mean_valid(r_std)`

因此 `edge_std_valid` 不是 wins / n 这一类频率统计，而是 valid split 上标准化边际强度的平均值。

##### `rule_score`

当前主路径里的 `rule_score` 不是旧 rules-only 脚本那套乘积打分，而是基于 `edge_lower_bound_valid` 的保守 Sortino 型排序分数。

先计算 valid split 的保守下界：

- `direction = 1` 时：`edge_lower_bound_valid = q_valid_lower - p_valid`
- `direction = -1` 时：`edge_lower_bound_valid = p_valid - q_valid_upper`

再计算二元合约视角下的 downside 标准差：

- `downside_std = p_trade * sqrt(1 - q_trade_lower)`

最后：

- `rule_score = edge_lower_bound_valid / max(downside_std, eps)`

因此 `rule_score` 的目标语义应理解为：

- 它是排序分数，不是概率列；
- 它基于 valid split 的保守下界；
- 它奖励“保守边际更高且 downside 风险更低”的规则；
- `backtest_portfolio_qmodel.py`、`backtest_execution_parity.py` 和 `execution_engine` 后续都应继续把它当成排序键使用。

### 当前状态与目标状态的关键差异

以下差异必须在实现时显式处理：

1. `edge_train` 当前是未方向归一化的原始值；目标状态要求改成 direction-adjusted edge，正值表示规则方向有优势。
2. `edge_valid` 当前是未方向归一化的原始值；目标状态要求改成 direction-adjusted edge，正值表示规则方向有优势。
3. `edge_test` 当前是未方向归一化的原始值；目标状态要求改成 direction-adjusted edge，正值表示规则方向有优势。
4. `edge_lower_bound_valid` 当前已经按方向统一，但文档语义需要明确成“按现有公式计算的保守 valid edge 下界”；它保留方向统一语义，但数值本身不要求恒为正。
5. `edge_std_train` 当前不存在；目标状态要求新增。
6. `p_test` 当前不存在；目标状态要求新增。
7. `q_smooth` 当前在 offline naive 情况下可能数值等于 `q_train`；目标状态要求语义上分离。

#### 当前实现缺口对照

为了避免文档语义与现状混淆，以下列在当前实现中与目标状态不一致：

| 列名 | 当前实现状态 | 目标状态 |
| --- | --- | --- |
| `q_smooth` | 当前可在 offline naive 下与 `q_train` 数值相同 | 保持独立执行估计概率语义 |
| `edge_train` | 当前是未方向归一化原始值 | 改为 direction-adjusted edge，正值表示规则方向有优势 |
| `edge_valid` | 当前是未方向归一化原始值 | 改为 direction-adjusted edge，正值表示规则方向有优势 |
| `edge_test` | 当前是未方向归一化原始值 | 改为 direction-adjusted edge，正值表示规则方向有优势 |
| `edge_lower_bound_valid` | 当前已按方向统一 | 语义上明确为按现有公式计算的保守 valid edge 下界 |
| `edge_std_train` | 当前不存在 | 新增并保留 |
| `p_test` | 当前不存在 | 新增并保留 |

### 目标删除项

目标状态下，应删除以下脚本：

- `rule_baseline/training/train_rules_naive_output_rule_strict.py`
- `rule_baseline/backtesting/backtest_portfolio_rules_only.py`

同时删除与其绑定的遗留逻辑。

#### 1. 删除 strict trainer 遗留逻辑

删除或停止在主 schema 中承载以下 strict/实验性遗留语义：

- `prior_mean`
- `p_value_valid`
- `p_value_valid_adj`
- `selection_status`
- `q_raw_est`
- `q_train_raw`

这些字段不再属于目标运行时规则文件。

#### 2. 删除 rules-only 遗留逻辑

删除或停止在主 schema 中承载以下 rules-only 遗留语义：

- `edge_sample_trade`
- `edge_std_trade`
- `edge_sample_trade_valid`
- `edge_std_trade_valid`

目标状态下：

- rules-only 回测脚本被删除。
- `backtest_portfolio_qmodel.py` 不再允许依赖 `edge_sample_trade_valid` / `edge_std_trade_valid` fallback。
- `backtest_portfolio_qmodel.py`、`backtest_execution_parity.py`、`execution_engine` 统一使用 `edge_valid`、`edge_std_valid`、`edge_lower_bound_valid`。

#### 3. 删除冗余表达列

目标状态下不再保留以下冗余列：

- `price_bin`
- `horizon_bin`
- `rule_bounds`
- `p_mean`
- `edge_net`
- `edge_sample`
- `edge_std`
- `roi`
- `q_trade`
- `p_trade`
- `edge_net_trade`
- `roi_trade`
- `q_trade_valid`
- `p_trade_valid`
- `edge_net_trade_valid`
- `roi_trade_valid`
- `estimation_source`

### 文档和引用清理范围

目标状态下，除代码删除外，还必须同步清理所有遗留文档和说明：

- `polymarket_rule_engine/README.md` 中对 strict trainer 和 rules-only backtest 的引用
- `polymarket_rule_engine/rule_baseline/README.md` 中对 strict trainer 和 rules-only backtest 的引用
- `polymarket_rule_engine/rule_baseline/WORKFLOW_AND_MODULES.md` 中对 strict trainer 和 rules-only backtest 的引用
- 任何仍把 `edge_sample_trade` / `edge_std_trade` 视为主 schema 必需列的文档说明

### 目标列顺序

目标状态下，`trading_rules.csv` 列顺序应为：

```text
group_key,domain,category,market_type,leaf_id,price_min,price_max,h_min,h_max,direction,q_smooth,edge_lower_bound_valid,rule_score,n_train,n_valid,n_test,n_full,q_train,p_train,edge_train,edge_std_train,q_valid,p_valid,edge_valid,edge_std_valid,q_test,p_test,edge_test
```

### 目标字段定义总表

| 列名 | split / 类型 | 目标含义 |
| --- | --- | --- |
| `group_key` | 标识 | 规则分组键，格式为 `domain|category|market_type` |
| `domain` | 匹配维度 | 域名分组 |
| `category` | 匹配维度 | 市场大类 |
| `market_type` | 匹配维度 | 市场类型 |
| `leaf_id` | 标识 | 规则叶子唯一 ID |
| `price_min` | 匹配边界 | 价格区间下界 |
| `price_max` | 匹配边界 | 价格区间上界 |
| `h_min` | 匹配边界 | horizon 区间下界 |
| `h_max` | 匹配边界 | horizon 区间上界 |
| `direction` | 方向 | 规则交易方向，`1` 表示正向，`-1` 表示反向 |
| `q_smooth` | 执行估计 | 最终用于执行侧的估计概率 |
| `edge_lower_bound_valid` | valid | 按 3A 现有公式计算的保守 valid edge 下界；方向已统一，但数值可正可负 |
| `rule_score` | 排序 | 规则排序分数 |
| `n_train` | train 计数 | train split 样本数 |
| `n_valid` | valid 计数 | valid split 样本数 |
| `n_test` | test 计数 | test split 样本数 |
| `n_full` | 汇总计数 | 全部样本数 |
| `q_train` | train | train split 原始命中率 |
| `p_train` | train | train split 平均市场价格 |
| `edge_train` | train | direction-adjusted train edge；正值表示规则方向有优势 |
| `edge_std_train` | train | train split 边际波动统计 |
| `q_valid` | valid | valid split 原始命中率 |
| `p_valid` | valid | valid split 平均市场价格 |
| `edge_valid` | valid | direction-adjusted valid edge；正值表示规则方向有优势 |
| `edge_std_valid` | valid | valid split 边际波动统计 |
| `q_test` | test | test split 原始命中率 |
| `p_test` | test | test split 平均市场价格 |
| `edge_test` | test | direction-adjusted test edge；正值表示规则方向有优势 |

### 目标实现备注

这里再强调三点，避免后续实现时跑偏：

1. `q_smooth` 必须保留，即使在某些 offline naive 文件里它和 `q_train` 数值相同。
2. `edge_train`、`edge_valid`、`edge_test` 必须统一成 direction-adjusted 语义；正值表示规则方向有优势，负值表示该方向没有优势。`edge_lower_bound_valid` 不参与这一定义重写，继续沿用上文 3A 的现有公式。
3. `execution_engine` 复用 `backtest_execution_parity.py` / `backtest_portfolio_qmodel.py` 的规则契约，因此主 schema 变更必须围绕这两个入口一起设计，而不是只从训练脚本角度改列名。

## 审计范围

本文只保留和目标实现直接相关的范围，不再展开“当前下游依赖审计”或“历史 slim 方案”两类历史说明，避免把现状兼容逻辑误读成目标规范。

目标实现必须同步修改或验证的路径如下：

- `rule_baseline/training/train_rules_naive_output_rule.py`：唯一主产出脚本，负责输出 `28` 列主 schema。
- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`：共享规则加载、匹配、growth 评分契约，必须切换到目标列名并去掉旧 fallback。
- `rule_baseline/backtesting/backtest_execution_parity.py`：主回测入口，必须与 `backtest_portfolio_qmodel.py` 共同保持同一运行时契约。
- `execution_engine/online/scoring/rules.py`：在线规则加载与 coverage 统计，必须兼容目标主 schema。
- `execution_engine/online/scoring/rule_runtime.py`：在线执行复用 parity / q-model 逻辑，必须与目标主 schema 一致。
- `execution_engine/online/pipeline/eligibility.py`：在线结构化过滤，必须继续依赖统一后的边界列和方向列。
- 文档清理路径：`polymarket_rule_engine/README.md`、`rule_baseline/README.md`、`rule_baseline/WORKFLOW_AND_MODULES.md` 中所有 strict trainer / rules-only backtest / 旧列说明。

## 实现一致性约束

为避免实现后再次出现双语义或双契约，本文约束如下：

1. `trading_rules.csv` 的唯一目标运行时契约就是上文定义的 `28` 列主 schema，不再保留第二套 runtime schema。
2. `rule_baseline` 与 `execution_engine` 必须共享同一列语义，不能再出现“离线回测读一套列、在线执行读另一套列”的分叉。
3. `edge_train`、`edge_valid`、`edge_test` 的目标含义都是 direction-adjusted edge；正值表示规则方向有优势。`edge_lower_bound_valid` 单独沿用 3A 中的现有保守下界公式，不做重新定义。
4. `backtest_portfolio_qmodel.py` 中对 `edge_sample_trade_valid` / `edge_std_trade_valid` 的 fallback 必须随迁移一起移除，否则文档目标和运行时契约会再次分叉。
5. `train_rules_naive_output_rule_strict.py` 与 `backtest_portfolio_rules_only.py` 删除后，所有只服务于这两条旧路径的列都不得继续出现在主 schema 中。

## 实施落地结论

本文件的最终结论只有三条：

1. `trading_rules.csv` 收敛为上文 `28` 列统一主 schema。
2. `train_rules_naive_output_rule_strict.py` 与 `backtest_portfolio_rules_only.py` 及其遗留列语义一起退出主路径。
3. 任何历史审计列、实验列、fallback 列如果仍需保留，只能进入单独的 audit / full 产物，不能重新回流到运行时主 schema。
