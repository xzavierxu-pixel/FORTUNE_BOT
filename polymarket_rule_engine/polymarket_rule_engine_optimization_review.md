# polymarket_rule_engine 结果复盘与优化建议

生成时间: 2026-03-06

目标: 基于当前 `polymarket_rule_engine` 已产出的结果文件和项目代码，系统分析“哪些信号可信、哪些结果不可信”，并提出尽可能完整的优化方案，方向聚焦在:

1. 提高真实盈利能力
2. 降低回撤
3. 修复当前流程中的设计问题
4. 为后续代码改造提供优先级清晰的路线图

---

## 1. 结论先行

当前系统**有一定预测信号**，但**当前回测结果不可信，不能直接用于上线或放大资金交易**。

核心判断如下:

1. Q-Model 在验证期相对市场价格确实有小幅提升，但提升幅度是“中等偏弱”的，不足以支持当前回测中极端夸张的收益曲线。
2. 当前回测从 `10,000` 做到 `1,053,229,337.86`，33 个交易日收益超过 `105,321x`，这在统计和经济上都明显异常。
3. 异常收益更像是**泄漏 + 规则筛选污染 + 重复暴露 + 过于激进的组合假设**共同造成的，而不是策略真实 alpha。
4. 当前最重要的工作不是继续“调模型提收益”，而是先把**研究流程纠偏**，否则优化只会把错误放大。

一句话概括:

**模型信号可能有用，但回测框架暂时不能证明它真的能赚钱。**

---

## 2. 本次分析覆盖的文件

### 2.1 结果文件

- `data/predictions/snapshots_with_predictions.csv`
- `data/backtesting/backtest_equity_qmodel.csv`
- `data/backtesting/backtest_trades_qmodel.csv`
- `data/backtesting/rule_performance_qmodel.csv`
- `data/analysis/alpha_quadrant_metrics.csv`
- `data/analysis/alpha_by_category.csv`
- `data/analysis/alpha_by_domain.csv`
- `data/analysis/alpha_by_horizon.csv`
- `data/analysis/rules_alpha_metrics.csv`
- `data/edge/trading_rules.csv`
- `data/processed/snapshots.csv`
- `data/intermediate/raw_markets_merged.csv`

### 2.2 关键代码

- `rule_baseline/training/train_rules_naive_output_rule.py`
- `rule_baseline/training/train_snapshot_lgbm_v2.py`
- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`
- `rule_baseline/utils/data_processing.py`
- `rule_baseline/utils/modeling.py`
- `rule_baseline/data_collection/build_snapshots.py`
- `rule_baseline/analysis/analyze_q_model_calibration.py`
- `rule_baseline/analysis/analyze_alpha_quadrant.py`
- `rule_baseline/workflow_issues.md`

---

## 3. 结果复盘

## 3.1 模型预测能力: 有提升，但没有回测表现那么夸张

### 3.1.1 全样本指标

`snapshots_with_predictions.csv` 全样本结果:

| 指标 | 市场价格 price | 模型 q_pred | 改善 |
|---|---:|---:|---:|
| LogLoss | 0.6164 | 0.5787 | +0.0377 |
| Brier | 0.2180 | 0.2022 | +0.0157 |
| AUC | 0.6810 | 0.7446 | +0.0636 |

问题在于，这个“全样本指标”并不干净，因为预测导出是对**全量样本**做的，其中包含训练样本。

### 3.1.2 按 train/valid 拆分后的指标

基于 `resolve_time` 最后 30 天作为验证期，得到:

| 数据集 | 行数 | price LogLoss | q_pred LogLoss | price Brier | q_pred Brier | price AUC | q_pred AUC |
|---|---:|---:|---:|---:|---:|---:|---:|
| Train | 129,998 | 0.6006 | 0.5527 | 0.2115 | 0.1916 | 0.7006 | 0.7846 |
| Valid | 66,365 | 0.6475 | 0.6296 | 0.2307 | 0.2230 | 0.6416 | 0.6610 |

解读:

1. 验证期确实有提升，说明模型可能学到了一些可交易信息。
2. 但验证期提升远小于训练期，存在明显性能回落。
3. 所以“模型有效”可以初步成立，但只属于**弱到中等有效**，绝不是能解释千亿级回测收益的强度。

---

## 3.2 Alpha 分析结果: 表面很强，但目前同样偏乐观

`alpha_quadrant_metrics.csv` 给出的核心现象:

- `contrarian_correct`: 56,868 行，占比 30.59%
- `consensus_correct`: 63,305 行，占比 34.05%
- `consensus_wrong`: 40,018 行，占比 21.53%

分类维度表现:

- `FINANCE` alpha_ratio = `0.8444`
- `CRYPTO` alpha_ratio = `0.7508`
- `SPORTS` alpha_ratio = `0.6786`

时窗维度表现:

- `24h` alpha_ratio = `0.7493`
- `2h` alpha_ratio = `0.6856`
- `12h` alpha_ratio = `0.6872`

解读:

1. 这些结果说明模型确实经常和市场不一致，而且有时候是对的。
2. 但 `analyze_alpha_quadrant.py` 直接读取的是 `snapshots_with_predictions.csv`，而该文件是全量样本预测，不是严格独立测试集。
3. 因此这些 alpha 结果更适合视为“研究线索”，不适合视为“真实可交易结论”。

---

## 3.3 回测结果: 明显失真

`backtest_equity_qmodel.csv` 与 `backtest_trades_qmodel.csv` 的关键信息:

- 初始资金: `10,000`
- 最终资金: `1,053,229,337.86`
- 回测天数: `33`
- 总交易数: `1,559`
- 最大回撤: `-29.87%`
- 胜率: `46.76%`
- 平均每笔收益率: `50.12%`
- 中位数每笔收益率: `-100.10%`

这组数字本身就说明结果不可信:

1. 胜率不到 50%，却在 33 天内把资金放大到 10 亿级别，过于异常。
2. 单日收益率多次超过 `50%`，首个有交易日收益率甚至达到 `103.7%`。
3. 这种曲线更像是回测设计错误，而不是市场中的真实赚钱能力。

### 3.3.1 收益集中度极高

- `nba.com` 一个 domain 贡献了约 `96.40%` 的总 PnL
- 前 3 个 domain 贡献了约 `99.78%` 的总 PnL
- 前 10 条规则贡献了约 `96.29%` 的总 PnL

解读:

这说明系统本质上并不是一个分散化组合，而是一个**高度集中在少量体育子市场上的押注器**。只要这些局部模式一失效，回撤会非常大。

### 3.3.2 存在大量同一市场重复下注

回测交易中:

- 总交易数: `1,559`
- 唯一 `market_id`: `911`
- 有多次交易的市场数: `406`
- 单一市场最多交易次数: `6`
- 同一天同一市场最多交易次数: `6`

这意味着系统把同一事件不同 horizon 的快照，或者同一市场的多个时点，当成了多个近似独立机会。实际交易中，这些风险高度相关，不能按独立下注来估算组合增长。

### 3.3.3 严重偏向低价 YES 票

按价格桶看:

- `(0.2, 0.3]` 价格区间占了 `1,152 / 1,559` 笔交易
- YES 方向交易占比约 `86.72%`

这类仓位天然带有高赔率特征，账面收益容易很夸张，但也更容易被少量错误吃掉。当前资金管理没有对这种“低价高赔率偏置”做额外约束。

---

## 3.4 规则表本身就很脆弱

`trading_rules.csv` 当前特征:

- 规则数: `874`
- `n_valid` 中位数: `20`
- `n_valid` 10 分位: `9`
- `n_full` 中位数: `68`
- `q_smooth < 0.1` 的规则数: `74`
- `q_smooth > 0.9` 的规则数: `64`
- `abs(edge_sample_trade) > 0.2` 的规则数: `38`

解读:

1. 很多规则样本量太小。
2. 一堆规则的 `q_smooth` 直接接近 `0` 或 `1`，这在真实预测问题里通常意味着严重过拟合或信息泄漏。
3. top 规则里有明显“极端完美规则”的味道，例如某些 `twitch.tv` / `liquipedia.net` / `nba.com` 子类规则收益异常高，这类规则极可能是局部样本碰巧拟合出来的。

---

## 3.5 数据质量问题会直接污染时序特征

从原始数据和 snapshot 数据看:

- `raw_markets_merged.csv` 中，`2,960` 个市场 `endDate - startDate < 0`
- `raw_markets_merged.csv` 中，`4,368` 个市场时长小于 `1h`
- `raw_markets_merged.csv` 中，`1,295` 条缺失 `endDate`
- `snapshots.csv` 中，`544` 条 `scheduled_end` 无法解析
- `snapshots.csv` 中，`99,841` 行出现 `resolve_time < scheduled_end`
- `snapshots.csv` 中，`843,734` 行 `delta_hours > 1`

解读:

这些异常会直接影响:

1. horizon 对齐是否正确
2. 训练/验证切分是否干净
3. 价格快照是不是取到了错误时间附近
4. “接近结算”这一核心假设是否成立

---

## 4. 代码层面的核心问题

## 4.1 最大问题: 规则训练存在未来信息污染

文件: `rule_baseline/training/train_rules_naive_output_rule.py`

问题点:

1. `build_rules()` 先构造 `train_df` 和 `valid_df`，这一点本身没问题。
2. 但最终写进规则文件的核心统计量，很多来自 `full_group`，即**全样本**，不是训练集。
3. 例如:
   - `q_smooth = wins_full / n_full`
   - `edge_full`
   - `edge_raw_full`
   - `edge_std_full`
   - `rule_score`
4. 这些规则随后会被:
   - 直接写入 `trading_rules.csv`
   - 作为模型特征进入 `train_snapshot_lgbm_v2.py`
   - 作为回测筛选依据进入 `backtest_portfolio_qmodel.py`

影响:

规则先验本身已经“看过未来”，后面的模型训练、候选筛选、回测交易都会被污染。

---

## 4.2 第二大问题: 回测使用了与规则选择相同的验证期

文件:

- `rule_baseline/training/train_rules_naive_output_rule.py`
- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`

问题点:

1. 规则有效性依赖 `n_valid`、`edge_raw_valid`、`edge_std_valid` 等验证期统计。
2. 回测时 `select_top_rules()` 又按这些规则指标筛 top rules。
3. 同时回测样本 `snapshots = snapshots[snapshots["resolve_time"] >= valid_start]`，本质上还是那一段验证期。

影响:

这是典型的 “用测试集挑策略，再用同一个测试集证明策略赚钱”。

这不是轻微偏乐观，而是足以让收益曲线完全失真。

---

## 4.3 第三大问题: 校准和分析默认也不是独立测试

文件:

- `rule_baseline/training/train_snapshot_lgbm_v2.py`
- `rule_baseline/analysis/analyze_q_model_calibration.py`
- `rule_baseline/analysis/analyze_alpha_quadrant.py`

问题点:

1. `train_snapshot_lgbm_v2.py` 会把全量 `df_feat` 导出到 `snapshots_with_predictions.csv`
2. 校准分析和 alpha 分析都直接读这个全量导出
3. 因此当前分析报告默认混合了训练数据和验证数据

影响:

模型看上去比真实情况更强。

---

## 4.4 第四大问题: 资金管理忽略相关性

文件: `rule_baseline/backtesting/backtest_portfolio_qmodel.py`

问题点:

1. `MAX_POSITION_F = 0.02`
2. `MAX_DAILY_EXPOSURE_F = 1.0`
3. 候选交易按 `growth_score` 排序后，最多做 `300` 笔，当前结果里很多天做 `49-50` 笔
4. 每笔 stake 用 `bankroll_start` 计算，而不是随着已开仓风险动态衰减
5. 没有按事件、domain、market_type、方向、相关簇进行相关性约束

影响:

即使公式本身没写错，组合层面也会系统性高估增长率，低估回撤。

---

## 4.5 第五大问题: 同一市场多 horizon 被当成多次独立机会

文件:

- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`
- `rule_baseline/data_collection/build_snapshots.py`

问题点:

1. snapshot 数据天然是同一市场在多个 horizon 的重复观测
2. `dedup_by_growth_score()` 只按 `market_id + snapshot_time` 去重
3. 这并不能防止同一个 `market_id` 在不同 horizon 或同日多个时点被多次下注

影响:

样本独立性被破坏，Kelly 类 sizing 会显著过大。

---

## 4.6 第六大问题: 规则样本太小、粒度太细

文件: `rule_baseline/training/train_rules_naive_output_rule.py`

问题点:

1. 规则按 `domain × category × market_type × price_bin × horizon_bin` 切分
2. `PRICE_BIN_STEP = 0.03`
3. `MIN_VALID_N = 8`
4. `MIN_TRAIN_ROWS = 15`

影响:

这种切法在很多 domain 下会产生大量低样本叶子，极易把噪声当 alpha。

---

## 4.7 第七大问题: 目标定义偏“预测结果”，不是“最大化交易收益”

文件:

- `rule_baseline/training/train_snapshot_lgbm_v2.py`
- `rule_baseline/utils/modeling.py`

问题点:

1. 当前模型目标是二分类 `y`
2. 真实交易要优化的是 `expected pnl`, `expected log growth`, `drawdown-adjusted return`
3. `q_pred - price` 虽然是常见近似，但并没有显式纳入手续费、流动性、赔率非线性、仓位上限和相关性风险

影响:

分类更准，不一定等于更赚钱。

---

## 4.8 第八大问题: 数据采集与时间异常尚未被治理

文件:

- `rule_baseline/data_collection/build_snapshots.py`
- `rule_baseline/workflow_issues.md`

问题点:

1. 原始市场里存在负 duration、缺失 endDate、早于 scheduled_end 的 resolve_time
2. 当前流程主要是“记录问题”，但没有系统性隔离、修复和降权
3. `delta_hours` 先前甚至被误用进模型，说明时间数据边界还不够清晰

影响:

时序特征、horizon 特征、切分边界和训练标签都有可能受到污染。

---

## 5. 为什么当前回测会看起来这么赚钱

最可能是下面几个因素叠加:

1. 规则文件使用了全样本统计，带入未来信息。
2. 规则筛选和回测都在同一验证期上完成。
3. 分析报告默认包含训练样本，放大了模型好坏。
4. 同一市场被重复下注，风险没有按相关性折算。
5. 资金管理允许大量低价赔率仓位并行叠加。
6. 组合极度集中在 `nba.com` 相关规则，一旦局部样本恰好走强，资金曲线就会爆炸。

所以当前结果更接近:

**“研究环境中的过拟合样本内放大器”**

而不是:

**“已经验证过的可部署赚钱策略”**

---

## 6. 优化建议总表

以下建议按优先级从高到低排列。前 10 条是必须优先处理的。

## 6.1 P0: 不修这些，任何收益都不可信

### 1. 把流程改成严格三段式: train / valid / test

建议:

- 规则统计只允许用 train
- 超参和校准只允许用 valid
- 最终回测只允许用 test
- 任何分析报告都必须单独输出 valid 和 test

目标:

先恢复研究可信度。

### 2. 规则表中的 `q_smooth`、`edge_*`、`rule_score` 只用 train 计算

建议:

- 禁止 `full_group` 参与交易规则核心字段
- 只允许 train 数据产出:
  - `q_smooth_train`
  - `edge_train`
  - `std_train`
  - `rule_score_train`
- valid 仅用于“是否保留/调参”，不能写进交易先验

目标:

切断未来信息泄漏。

### 3. 回测期必须与规则筛选期完全分离

建议:

- `select_top_rules()` 不要读取 test 期统计
- 在 test 开始前就冻结规则列表
- 若要做 walk-forward，必须按窗口滚动冻结

目标:

避免“边考试边出题”。

### 4. 导出预测时拆分 train/valid/test 三套文件

建议:

- `snapshots_with_predictions_train.csv`
- `snapshots_with_predictions_valid.csv`
- `snapshots_with_predictions_test.csv`

目标:

避免分析脚本默认混样本。

### 5. 同一 `market_id` 在同一天只允许一个净暴露

建议:

- 不同 horizon 的候选要合并成一个决策
- 如果已经持有该市场，就只能:
  - 加仓
  - 减仓
  - 平仓
  - 保持不动
- 不能把它当成新独立交易

目标:

显著降低虚假复利。

### 6. 组合风控改为“按相关簇限仓”

建议至少增加这些上限:

- 单一 `market_id` 最大风险
- 单一事件族最大风险
- 单一 domain 最大风险
- 单一 category 最大风险
- 单一 side 最大风险
- 同一结算日最大风险

目标:

降低集中暴露造成的大回撤。

### 7. Kelly 改成保守版，并纳入不确定性折扣

建议:

- 从 `0.25 Kelly` 降到 `0.05~0.10 Kelly`
- 用 `edge_lower_bound` 而不是 `edge_point_estimate`
- 对低样本规则再乘 shrinkage

目标:

减少模型误差导致的过度下注。

### 8. 回测加入真实交易成本模型

建议:

- 手续费不能固定只按 `0.1%`
- 引入点差
- 引入滑点
- 引入成交量限制
- 引入部分成交
- 低流动性市场做额外冲击成本

目标:

把“纸面 edge”压缩成“可成交 edge”。

### 9. 提高规则最小样本阈值，并引入置信区间

建议:

- `MIN_VALID_N` 至少提高到 `30~50`
- `MIN_TRAIN_ROWS` 至少提高到 `50~100`
- `edge` 必须大于其置信区间下界

目标:

减少小样本幸运规则。

### 10. 所有报告默认展示 OOS 指标，而不是全样本指标

包括:

- calibration
- alpha quadrant
- rule alpha
- backtest summary

目标:

统一研究口径。

---

## 6.2 P1: 直接影响盈利和回撤的策略层优化

### 11. 把目标从“预测 y”改成“预测交易价值”

可选方向:

- 直接预测 `signed_true_edge`
- 直接预测 `expected pnl`
- 直接预测 `expected log growth`
- 两阶段模型:
  - 第一阶段预测方向是否有效
  - 第二阶段预测 edge 大小

收益:

模型更贴近真实交易目标。

### 12. 按 horizon 分模型，不要全 horizon 混在一起

原因:

- 1h、2h、24h 的价格行为很不一样
- 市场接近结算时的信息结构不同

建议:

- 至少拆成短时窗 `1/2/4h`
- 中时窗 `6/12h`
- 长时窗 `24h`

### 13. 按 domain 或 market_type 做分层模型

原因:

- `NBA player props`
- `crypto up/down`
- `finance yes/no`

这几类市场的噪声结构差异很大。

建议:

- 先做 global model
- 再做 domain-specific residual model
- 或用 mixture-of-experts / hierarchical model

### 14. 引入不确定性建模

建议:

- ensemble disagreement
- bootstrap interval
- conformal prediction
- Bayesian shrinkage

交易时用:

- `expected edge`
- `edge lower bound`
- `prob(edge > 0)`

而不是只看点估计 `q_pred`

### 15. 把规则先验改成经验贝叶斯收缩

当前问题:

- `q_smooth` 太容易跑到 0 或 1

建议:

- 使用 Beta-Binomial shrinkage
- 对低样本规则向:
  - domain 均值
  - category 均值
  - 全局均值
  逐级回归

收益:

能显著减少极端错杀和极端误判。

### 16. 对 price_bin / horizon_bin 做单调平滑

建议:

- 相邻 price bin 合并
- 相邻 horizon bin 合并
- 对 edge 曲线做 isotonic / spline 平滑

收益:

减少“离散切箱随机尖刺”。

### 17. 对规则做多重检验修正

建议:

- 使用 FDR / Benjamini-Hochberg
- 或最少用更严格的保留门槛

原因:

874 条规则里，大量规则只是随机显著。

### 18. 引入时间衰减

建议:

- 最近 30 天权重更高
- 更旧样本指数衰减

原因:

Polymarket 不同市场结构变化很快，老样本的参考价值会迅速下降。

### 19. 增加 regime 检测

建议:

- 按 domain 的 rolling calibration
- rolling Brier
- rolling edge realization
- 若漂移超阈值，自动降仓或停用该簇规则

收益:

避免 alpha 失效后继续重仓。

### 20. 增加“只做最强子市场”的白名单机制

但前提是:

必须基于独立测试集，而不是当前污染回测。

建议:

- 只有在 test 期连续稳定正贡献的 domain 才可交易
- 对 `nba.com`、`hltv.org`、`ncaa.com` 等分别评估

---

## 6.3 P1: 特征工程优化

### 21. 增加价格路径特征，而不只是单点价格

当前 snapshot 只有某个 horizon 的单个价格，信息量不够。

建议新增:

- `p_1h, p_2h, p_4h, p_6h, p_12h, p_24h`
- `delta_p_1_2`
- `delta_p_2_4`
- `delta_p_4_12`
- closing drift
- 价格加速度
- 波动率
- price reversal

### 22. 增加盘口和流动性特征

如果数据源允许，建议加入:

- bid/ask spread
- depth
- top-of-book size
- quote update rate
- order imbalance
- 成交速度

收益:

这类特征对“能不能成交”和“edge 会不会被吃掉”非常关键。

### 23. 增加市场热度变化特征

建议:

- volume 变化率
- liquidity 变化率
- 24h volume / total volume
- 近几小时成交加速

收益:

很多信息不是价格本身，而是交易活跃度变化。

### 24. 增加文本语义特征

当前文本特征偏人工词袋，仍然较浅。

建议:

- question embedding
- description embedding
- 使用轻量 sentence transformer
- 提取 event template / entity / threshold / deadline

收益:

更好地区分:

- 选举类
- 球员数据类
- 价格突破类
- 财报类

### 25. 增加事件结构特征

建议:

- 是否球员 props
- 是否 team totals
- 是否 finance threshold
- 是否 date-based settlement
- 是否 binary threshold question
- 是否 high ambiguity wording

收益:

不同市场结构下，市场定价误差模式很不一样。

### 26. 增加时间质量特征

建议:

- `scheduled_end_parse_ok`
- `resolve_before_sched_flag`
- `duration_is_negative_flag`
- `delta_hours_bucket`

但注意:

这些特征只能用交易时可知的信息，不能再引入未来字段。

### 27. 把“市场价格”从唯一锚点升级成“市场 term structure”

建议:

- 同一市场多个 horizon 的价格曲线
- 与最终价差的历史收敛模式
- 不同离到期时间的偏差模式

收益:

更容易找到系统性 mispricing。

---

## 6.4 P1: 回测框架优化

### 28. 改成事件驱动持仓回测，不要逐日静态打点

建议:

- 有仓位状态
- 有加减仓逻辑
- 有持仓生命周期
- 同一市场的后续 snapshot 更新已有持仓，而不是生成新仓

### 29. 支持提前平仓，而不是默认持有到结算

原因:

- 一些 edge 会在临近结算前被市场修复
- 持有到结算方差大，回撤重

建议研究:

- 固定收益目标止盈
- edge 回归后平仓
- 时间止损
- 最大持仓时长

### 30. 加入流动性约束下的最大可成交金额

建议:

- 单笔 stake 不得超过近 24h volume 的某一比例
- 不得超过订单簿深度的某一比例

否则:

回测里的大额持仓在真实盘中根本成交不了。

### 31. 组合级 drawdown 控制

建议:

- 若 rolling drawdown 超过阈值，自动:
  - 降杠杆
  - 提高最小 edge
  - 缩窄白名单
  - 暂停高波动 domain

### 32. 把 stake 基准从 `bankroll_start` 改为“可用风险预算”

建议:

- 已开仓风险先占用预算
- 新仓只使用剩余风险空间

收益:

更真实地描述并发风险。

### 33. 避免同方向拥挤

建议:

- 低价 YES 方向总风险上限
- 高价 NO 方向总风险上限
- 单一赔率带上限

收益:

减少赔率分布偏置导致的尾部风险。

### 34. 用更合理的评分替代 `growth_score / f_position`

可以尝试:

- `expected_pnl / expected_shortfall`
- `edge_lower_bound * liquidity_score`
- `prob(edge > fee + slippage)`

原因:

当前排序可能过度偏爱赔率高但不稳定的低价票。

### 35. 对规则 kill-switch 引入统计显著性，而不是简单 rolling sum

建议:

- 观察 rolling t-stat
- Wilson interval
- 贝叶斯后验失败概率

收益:

减少因为短期噪声过早杀死有效规则，或者让无效规则活得太久。

---

## 6.5 P1: 数据与标签优化

### 36. 严格校验 outcome token 顺序

当前 `build_snapshots.py` 的 `determine_outcome()` 默认 `final_prices[0] > 0.9` 对应 `y=1`，这隐含了 token 顺序固定。

建议:

- 明确保存 YES token / NO token 映射
- 校验 outcome label
- 发现映射异常时直接丢弃

### 37. 对异常时间样本设立 quarantine 表

建议将以下样本从主训练集剥离:

- `resolve_time < scheduled_end`
- `endDate < startDate`
- `duration < min_horizon`
- `scheduled_end` 无法解析

不要只是打印 warning。

### 38. 记录快照成功率和缺失模式

建议输出:

- 各 horizon 命中率
- 哪些 domain 经常缺快照
- 哪些市场只在某些 horizon 有价格

收益:

这能识别系统性数据缺口。

### 39. 为价格历史抓取增加质量标记

例如:

- snapshot 是否使用左侧点
- 是否使用右侧点
- 与目标时刻偏差多少秒
- 该窗口内历史点密度

收益:

可直接筛掉 stale quote。

### 40. 原始市场过滤要更严格

当前 `fetch_raw_events.py` 只做了基础过滤，建议增加:

- 最低流动性门槛
- 最大点差门槛
- 非标准结算市场排除
- 结构模糊市场排除
- 低质量 domain 黑名单

---

## 6.6 P2: 建模框架升级

### 41. 尝试直接使用 CatBoost / LightGBM 原生类别特征

原因:

- 当前 `OneHotEncoder(..., sparse_output=False)` 已经证明过容易内存炸裂
- 规则/市场元数据包含大量类别字段

收益:

建模和部署都更稳。

### 42. 尝试 ranking / uplift 风格目标

如果实盘核心是“从很多候选里挑少数最值得做的”，可以尝试:

- learning to rank
- pairwise ranking
- uplift / delta model

### 43. 增加 baseline family 对比

至少同时维护:

- market-only baseline
- rules-only baseline
- model-only baseline
- rules + model ensemble
- domain-specific specialists

收益:

知道利润到底来自哪里。

### 44. 做 walk-forward CV，而不是单次 rolling split

建议:

- 多个时间窗口
- 每个窗口冻结训练、验证、测试
- 汇总均值与方差

收益:

减少“碰巧某一个月好”的错觉。

### 45. 做概率校准对比

当前只支持:

- `valid_isotonic`
- `cv_isotonic`
- `none`

建议补充:

- Platt scaling
- Beta calibration
- 分 domain 校准
- 分 horizon 校准

### 46. 用 residual learning 替代直接学 `y`

建议:

- 先用市场 price 作为 base probability
- 模型只学 `residual = y - price`
- 或学 `edge_true`

收益:

更符合“市场是强 baseline”的现实。

### 47. 做双模型: 方向模型 + 幅度模型

建议:

- 模型 A: edge 是否显著大于成本
- 模型 B: edge 大小

交易只在 A 高置信通过时才参考 B。

### 48. 做 domain-specific calibration

因为:

- sports
- crypto
- finance

概率分布与赔率结构明显不同，统一校准很可能不够好。

---

## 6.7 P2: 提高真实盈利的更激进方向

以下是“研究价值高，但实施复杂度也更高”的方向。

### 49. 引入跨市场对冲

例如:

- 同一事件关联合约配对
- 相近阈值市场对冲
- 同一比赛多盘口联动约束

收益:

提高 Sharpe，降低回撤。

### 50. 建立退出策略，而不是只做 entry

实盘里提升盈利最明显的地方往往不在 entry，而在 exit。

可研究:

- 到价止盈
- 时间止盈
- 盘口反转止损
- 概率回归平仓

### 51. 把“edge”拆成多来源

建议分别建模:

- 信息 edge
- 流动性 edge
- 文本结构 edge
- 事件类型 edge

再做 meta-allocation。

### 52. 研究市场过度反应 / 反转模式

尤其适合:

- 临近结算
- 热门比赛
- 高关注 crypto 短周期市场

### 53. 做交易容量研究

很多策略在小资金下成立，在大资金下会失效。

建议单独输出:

- 1k
- 10k
- 100k
- 1m

不同资金规模下的可交易收益。

### 54. 对“只做低价 YES”偏置做反偏优化

建议单独比较:

- 低价 YES
- 中价 YES
- 中高价 NO

收益:

减少收益集中在一种赔率结构上的风险。

### 55. 尝试只交易高流动性白名单市场

虽然会减少交易数，但通常会:

- 降低滑点
- 降低噪声
- 提高策略稳定性

---

## 7. 建议的落地顺序

## 7.1 第一阶段: 先修研究可信度

必须先做:

1. 三段式切分
2. 规则统计只用 train
3. 回测期与规则筛选期分离
4. 同一 market 只保留一个净暴露
5. 输出严格 OOS 报告

如果这一步做完后策略仍然赚钱，后续所有优化才有价值。

## 7.2 第二阶段: 先压回撤，再谈放大利润

优先建议:

1. 降低 Kelly
2. 加相关簇限仓
3. 加 domain 白名单
4. 加真实成本模型
5. 只做高置信、高流动性机会

## 7.3 第三阶段: 在干净框架上继续找 alpha

建议顺序:

1. residual / edge 建模
2. horizon-specific model
3. domain-specific calibration
4. 文本和价格路径特征
5. 退出策略

---

## 8. 我认为最值得优先尝试的 12 条改动

如果只能选一小批最有价值的改动，建议按这个顺序做:

1. 规则统计全面去未来信息
2. train/valid/test 三段式重构
3. walk-forward 回测
4. 同市场单净暴露
5. 组合相关性限仓
6. 0.25 Kelly 降到 0.05~0.10 Kelly
7. 规则样本阈值显著提高
8. 引入贝叶斯收缩规则先验
9. 用 OOS 指标替代全样本分析
10. 预测目标改成 edge / expected pnl
11. 增加价格路径和流动性特征
12. 增加提前平仓逻辑

---

## 9. 最终判断

当前系统不是“完全无效”，而是处在一个很典型的阶段:

- 信号可能存在
- 研究框架还不够干净
- 回测收益严重被放大
- 风险估计明显偏乐观

如果目标是**提高真实盈利、减少回撤**，正确顺序不是马上追求更高收益，而是:

1. 先让回测可信
2. 再让仓位保守
3. 再放大高质量 alpha

只有在严格 OOS、加入真实成本、去掉重复暴露之后仍然赚钱的部分，才值得继续工程化和实盘化。

