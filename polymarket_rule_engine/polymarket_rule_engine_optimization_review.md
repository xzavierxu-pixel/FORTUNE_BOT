# Polymarket Rule Engine Optimization Review

更新时间: 2026-03-13

这份文件只记录当前工作流里仍然存在的问题，不再保留已经修复的历史问题。

## 当前工作流

当前主流程已经是:

1. `fetch_raw_events.py`
2. `build_market_annotations.py`
3. `build_snapshots.py`
4. `train_rules_naive_output_rule.py`
5. `train_snapshot_model.py`
6. `analyze_q_model_calibration.py`
7. `analyze_alpha_quadrant.py`
8. `analyze_rules_alpha_quadrant.py`
9. `backtest_portfolio_qmodel.py`

离线主结果:

- 模型测试集 `AUC = 0.7006`
- 模型测试集 `LogLoss = 0.6129`
- 模型测试集 `Brier = 0.2145`
- 回测 `ROI = 16.34%`
- 回测 `total_trades = 46`

这些结果已经比之前可信很多，但仍然不能说明工作流已经稳定。

## 当前主要问题

### 1. 主模型只在 rule 匹配后的样本上训练

当前 `train_snapshot_model.py` 不是用全量 `snapshots` 训练，而是:

1. 先把 snapshot 匹配到规则
2. 只保留能匹配到规则的样本
3. 再在这些样本上训练主模型

这有两个问题:

- 可能丢掉大量未匹配样本，训练样本利用率低
- 模型目标更像“在 rule 已筛过的局部空间里做排序”，而不是学习更广泛的市场定价误差

风险:

- 规则本身如果有偏差，主模型会继承这种偏差
- 全局校准看起来可以，但在更广泛市场上泛化能力未知
- 目前无法判断 `rule_only` 是否优于 `all_samples` 或 `all_samples_weighted`

建议:

- 后续增加三种训练模式对比:
  - `rule_only`
  - `all_samples`
  - `all_samples_weighted`
- 比较时不要只看全局指标，要只在最终可交易候选集上比较 `AUC / LogLoss / Brier / top-k edge / backtest`

### 2. 规则方向分布明显偏单边

当前 workflow 下，规则和回测候选几乎都偏向 `YES` 方向，`NO` 方向覆盖不足。

风险:

- 当前回测更接近“选择性做多”，不是更对称的双边交易系统
- 模型与回测的收益来源可能过度集中在某类赔率结构
- 如果市场 regime 切换，单边偏置会放大回撤

建议:

- 专门审计规则训练环节为什么高分规则主要落在 `YES`
- 对规则按 `direction / price band / domain` 输出分层统计
- 比较 `YES-only` 与 `YES+NO` 的候选质量和回测表现

### 3. 回测仍然不是完整持仓生命周期模拟

当前回测已经修复为按结算日记账，不再当天直接复利，但它仍然是简化版:

- 没有提前平仓
- 没有同一市场持仓更新逻辑
- 没有在持仓期间根据新 snapshot 做加减仓或撤单

这意味着当前回测更像:

- “在某个 snapshot 开仓，持有到结算”

而不是:

- “真实交易生命周期管理”

建议:

- 后续实现事件驱动持仓状态机
- 显式支持:
  - 开仓
  - 持仓中更新
  - 提前平仓
  - 到期结算

### 4. 交易成本和成交约束仍然偏乐观

虽然已经去掉了终态流动性字段对回测的污染，但当前回测仍没有真实成交层建模:

- 没有 order book depth
- 没有 partial fill
- 没有冲击成本
- 没有 quote update rate
- 没有盘口滑点动态

风险:

- 当前收益更像“可成交理论上限”
- 一旦放大资金或放宽候选数，回测收益可能明显下修

建议:

- 后续引入成交能力约束
- 至少增加:
  - 按报价新鲜度分层的滑点
  - 按 price band 的保守成交折扣
  - 按候选拥挤度的容量限制

### 5. 高基数 market identity 特征仍值得继续审计

目前已经确认:

- `leaf_id`
- `group_key`

不是主要性能来源，去掉后测试集指标变化很小。

但还有一批高基数特征仍可能携带强 market identity:

- `source_url_market`
- `sub_domain_market`
- `groupItemTitle_market`
- `gameId_market`
- `text_embed_*`

风险:

- 模型可能学到的是“某类固定 market 模板”而不是可迁移 alpha
- 这类特征在时间外推和新 market 上更容易失效

建议:

- 继续做分组消融
- 优先比较:
  - 去掉 `source_url_market`
  - 去掉 `groupItemTitle_market`
  - 去掉 `gameId_market`
  - 去掉全部 `text_embed_*`

### 6. 当前 calibration 仍然只有单一默认主线

当前默认是 `valid_isotonic`。

虽然它现在工作正常，但仍缺少系统化结论:

- 不同 calibration mode 在当前数据上是否稳定
- grouped calibration 是否真的优于 global calibration
- 在回测候选集上，哪个 calibration mode 最好

建议:

- 固定当前特征和模型，单独对比:
  - `none`
  - `valid_sigmoid`
  - `valid_isotonic`
  - `domain_valid_isotonic`
  - `horizon_valid_isotonic`
- 比较指标时同时看:
  - 全测试集 calibration
  - 最终候选集 backtest

### 7. 当前 DQC 已有，但还没有红旗输出

现在已经有:

- `training_feature_describe.csv`
- `quality_check/feature_dqc.csv`
- `quality_check/feature_numeric_drift.csv`

但仍缺一个更直接的红旗视图。

建议:

- 自动输出“可疑特征清单”
- 至少筛出:
  - `abs(test_train_mean_gap_std)` 高的数值特征
  - `train_top != test_top` 的类别特征
  - 缺失率在 `train/test` 间差异明显的特征

### 8. 当前 workflow 缺少正式的实验矩阵

现在虽然离线 workflow 可以稳定运行，但实验仍偏人工。

缺的不是脚本能不能跑，而是“同一套问题如何系统比较”。

建议建立固定实验矩阵:

- 样本模式:
  - `rule_only`
  - `all_samples`
  - `all_samples_weighted`
- 模型:
  - `xgb+lgbm+catboost`
  - 去掉高基数 identity 特征后的版本
- calibration:
  - `none`
  - `valid_sigmoid`
  - `valid_isotonic`
- 输出:
  - `test metrics`
  - `candidate metrics`
  - `backtest summary`

## 当前优先级

### P0

1. 比较 `rule_only` 与 `all_samples / all_samples_weighted`
2. 审计规则训练为什么明显偏 `YES`
3. 继续做高基数 identity 特征消融

### P1

1. 增加 calibration mode 对比
2. 增加 DQC 红旗清单
3. 增加更真实的成交成本假设

### P2

1. 事件驱动持仓生命周期回测
2. 提前平仓逻辑
3. 更细的容量与滑点建模

## 当前结论

当前工作流已经解决了最严重的泄漏和回测记账问题，离线结果现在具备基本参考价值。

但它仍然不是最终稳定方案，主要风险集中在:

- 训练样本空间过窄
- 规则方向偏单边
- 持仓生命周期模拟过于简化
- 高基数 identity 特征可能仍有过拟合风险
- 缺少成体系的实验矩阵

下一阶段不应该先追求更高回测收益，而应该先回答:

1. 主模型到底应不应该只在 rule 样本上训练
2. 当前收益是否依赖少数 market identity 特征
3. 当前回测收益在更真实成交假设下还能剩多少
