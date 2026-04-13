# group_key Phase 1 实现审查

## 1. 审查范围

本审查基于当前仓库代码，目标是精确回答以下问题：

- `trading_rules.csv` 在哪里生成
- `trading_rules.csv` 在哪里被读取
- rule 匹配筛选发生在哪些文件 / 函数 / 类
- 样本真正被筛掉发生在哪一步
- 训练、验证、回测、推理哪些模块依赖这套逻辑

本文件只描述现状，不修改任何行为。

## 2. 当前文件产物位置

当前规则文件的标准产物路径由 [artifacts.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/datasets/artifacts.py:53) 定义：

- `offline`: `polymarket_rule_engine/data/offline/edge/trading_rules.csv`
- `online`: `polymarket_rule_engine/data/online/edge/trading_rules.csv`

`ArtifactPaths.rules_path` 明确固定为 `root / "edge" / "trading_rules.csv"`，见 [artifacts.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/datasets/artifacts.py:66) 和 [artifacts.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/datasets/artifacts.py:79)。

当前仓库内现成产物概况：

- `offline trading_rules.csv`: 718 行，152 个 `group_key`
- `online trading_rules.csv`: 69 行，25 个 `group_key`

## 3. 生成路径

### 3.1 主生成入口

`trading_rules.csv` 的主生成入口是：

- [train_rules_naive_output_rule.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py:325) `main()`

这里的主流程是：

1. 调用 `prepare_rule_training_frame(...)`
2. 调用 `build_rules(df, artifact_mode)`
3. 将结果写入 `artifact_paths.rules_path`

直接写文件的位置在：

- [train_rules_naive_output_rule.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py:349)

同时还会同步输出：

- `naive_trading_rules.csv`
- `naive_trading_rules.json`
- `naive_all_leaves_report.csv`
- `rule_training_summary.json`
- `rule_funnel_summary.json`

### 3.2 规则训练数据口径

规则训练输入不是直接从某个预聚合表读取，而是从 snapshot 基表现算。关键函数：

- [snapshots.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/datasets/snapshots.py:520) `prepare_rule_training_frame(...)`

它的流程是：

1. `load_snapshots(config.SNAPSHOTS_PATH)` 读取 canonical snapshot 表
2. `load_raw_markets(config.RAW_MERGED_PATH)` 读取合并后的 raw markets
3. `load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)` 读取 `domain/category/market_type`
4. `build_snapshot_base(...)` 生成 rule training 的 snapshot 基表
5. 过滤 `quality_pass == True`
6. 做数据集切分 `assign_dataset_split(...)`
7. 调用 `build_rule_bins(...)` 生成 `price_bin` / `horizon_bin`

这里的关键点是：

- rule 训练使用的是 quality-filter 后的 retained snapshot
- `offline` 保留 `train + valid + test`
- `online` 保留 `train + valid`
- rule bin 的构造也是基于 retained snapshot 完成，不是基于 live 数据

### 3.3 规则选择逻辑

规则网格与选中逻辑的核心函数：

- [train_rules_naive_output_rule.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py:228) `build_rule_grid(...)`
- [train_rules_naive_output_rule.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py:236) `evaluate_rule_candidate(...)`
- [train_rules_naive_output_rule.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py:287) `build_rules(...)`

`evaluate_rule_candidate(...)` 里当前 rule 的主键仍然是：

- `group_key = domain|category|market_type`
- 再叠加 `price_bin + horizon_bin`
- 形成 `leaf_id`

也就是说，当前 `trading_rules.csv` 的每一行本质上仍然是：

- 一个 `group_key`
- 一个 price bucket
- 一个 horizon bucket
- 一个方向和统计摘要

并不是“每个 group_key 一行”。

## 4. 消费路径

### 4.1 训练链路

训练阶段的规则读取入口：

- [train_snapshot_model.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py:300) `load_rules(path)`

训练阶段的规则匹配入口：

- [train_snapshot_model.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py:328) `match_snapshots_to_rules(...)`
- [train_snapshot_model.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py:392) `build_feature_table(...)`

训练主流程中规则是这样接入的：

1. `main()` 读取 snapshots
2. `rules = load_rules(artifact_paths.rules_path)`
3. `df_feat = build_feature_table(snapshots, market_feature_cache, market_annotations, rules)`
4. 若 `df_feat.empty`，直接报错

对应代码：

- [train_snapshot_model.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py:524)

结论：

- 训练集并不是先构完整 feature 再筛
- 而是先按 rule 匹配得到 matched snapshots
- 只有 matched snapshots 才会进入 `preprocess_features(...)`

### 4.2 回测链路

回测的核心规则读取和匹配来自：

- [backtest_portfolio_qmodel.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_portfolio_qmodel.py:98) `load_rules(path)`
- [backtest_portfolio_qmodel.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_portfolio_qmodel.py:188) `match_rules(...)`

执行平价回测链路继续复用这套逻辑：

- [backtest_execution_parity.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_execution_parity.py:13)
- [backtest_execution_parity.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_execution_parity.py:53) `prepare_execution_candidates(...)`
- [backtest_execution_parity.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_execution_parity.py:92) `compute_filter_breakdown(...)`

回测流程中的行为是：

1. snapshots 与 rules 先做匹配
2. 按 `market_id + snapshot_time + rule_score` 排序
3. 同一 market-snapshot 只保留 top rule
4. 再进入模型打分与 growth 计算
5. 再做 earliest-market 去重

所以回测明确依赖这套 rule 匹配过滤。

### 4.3 分析链路

以下分析脚本直接读取并匹配 rules：

- [analyze_rules_alpha_quadrant.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/analysis/analyze_rules_alpha_quadrant.py:45)
- [compare_calibration_methods.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/analysis/compare_calibration_methods.py:45)
- [run_autogluon_round3_experiments.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/analysis/run_autogluon_round3_experiments.py:252)
- [compare_baseline_families.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/analysis/compare_baseline_families.py:112)
- [feature_dqc.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/quality_check/feature_dqc.py:41)

这意味着一旦替换 `trading_rules.csv` 口径，不只是训练和回测要改，分析与 DQC 也要同步改。

### 4.4 在线推理 / 执行链路

在线链路的规则读取入口在：

- [execution_engine/online/scoring/rules.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/scoring/rules.py:31) `load_rules_frame(cfg)`

它内部不是自己解析 CSV，而是反向复用：

- `rule_baseline.backtesting.backtest_execution_parity.load_rules`

也就是说，在线执行和回测共享同一套 runtime schema。

在线 runtime 预热在：

- [prewarm.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/pipeline/prewarm.py:26) `build_runtime_container(cfg)`

会同时预热：

- `rules_frame`
- `rule_runtime`
- `horizon_profile`
- `model_payload`
- `feature_contract`

在线配置默认规则路径在：

- [config.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/runtime/config.py:86) `_resolve_rule_engine_defaults()`
- [config.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/runtime/config.py:533) `rule_engine_rules_path`

默认优先顺序是：

1. `polymarket_rule_engine/rule_baseline/datasets/trading_rules.csv`
2. `polymarket_rule_engine/rule_baseline/datasets/edge/trading_rules.csv`
3. `polymarket_rule_engine/data/offline/edge/trading_rules.csv`

当前仓库里真正稳定存在并可被默认命中的，是第 3 个路径。

## 5. rule 过滤具体发生位置

### 5.1 训练中真正筛掉样本的位置

训练链路里，样本被筛掉发生在：

- [train_snapshot_model.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/training/train_snapshot_model.py:328) `match_snapshots_to_rules(...)`

机制是：

1. 先按 `domain/category/market_type` inner join
2. 再按 `price in [price_min, price_max]`
3. 再按 `horizon_hours in [h_min, h_max]`
4. 再按 `rule_score` 取同一 `market_id + snapshot_time` 的 top 1

因此训练中真正被排除的样本有三类：

- family 对不上，inner join 直接丢失
- family 对上但 price/horizon 不在任何 rule bucket 中，被 mask 丢失
- 命中多个 rule 但不是最高分规则，被去重淘汰

只有最终 `matched` 的行才会进入 `build_feature_table(...)` 和 `preprocess_features(...)`。

### 5.2 回测中真正筛掉样本的位置

回测中样本被筛掉发生在：

- [backtest_portfolio_qmodel.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_portfolio_qmodel.py:188) `match_rules(...)`
- [backtest_execution_parity.py](/C:/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_execution_parity.py:53) `prepare_execution_candidates(...)`

它和训练逻辑一致，也是：

- family inner join
- price/horizon band 过滤
- rule_score top-1 去重

### 5.3 在线执行前的结构性筛选

在线执行链路在模型打分前就已经依赖 rules 做粗筛：

- [eligibility.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/pipeline/eligibility.py:103) `apply_structural_coarse_filter(...)`

这里依赖 rules 做两层过滤：

1. `rule_family_miss`
   当前 market 的 `(domain, category, market_type)` 不在 rules 家族集合里，直接淘汰
2. `rule_horizon_miss`
   当前 `remaining_hours` 不落在任何规则定义的 `(h_min, h_max)` 区间里，直接淘汰

因此，在线执行在进入 live price 阶段之前，就已经把“没有 rule family / horizon 覆盖”的市场剔除了。

### 5.4 在线 live price 阶段的规则覆盖过滤

在线执行在 live mid price 拿到后，还会再次用 rules 做 band 过滤：

- [eligibility.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/pipeline/eligibility.py:226) `apply_live_price_filter(...)`

这里调用：

- [rules.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/scoring/rules.py:87) `score_frame_rule_coverage(...)`

过滤条件是：

- family 匹配
- `remaining_hours` 落在某个 rule 的 `[h_min, h_max]`
- `live_mid_price` 落在某个 rule 的 `[price_min, price_max]`

若不满足，则标记为：

- `LIVE_PRICE_MISS`
- reason: `live_price_outside_rule_band`

### 5.5 在线推理阶段的再次规则匹配

在在线 live inference 中，进入模型前还会再次做一次完整的 rules 匹配：

- [live.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/scoring/live.py:323) `run_live_inference(...)`
- [rule_runtime.py](/C:/Users/ROG/Desktop/fortune_bot/execution_engine/online/scoring/rule_runtime.py:84) `load_rule_runtime(cfg)`

运行时复用的仍然是：

- `rule_baseline.backtesting.backtest_execution_parity.match_rules`

因此在线链路里，rules 不是只用于一个地方，而是至少用于三处：

1. 结构 coarse filter
2. live price 覆盖 filter
3. live inference 规则命中与模型输入构造

## 6. 当前数据流

### 6.1 规则生成数据流

`raw markets`  
-> `market annotations`  
-> `snapshots`  
-> `build_snapshot_base(...)`  
-> `quality_pass` 过滤  
-> `dataset_split`  
-> `build_rule_bins(...)`  
-> `aggregate_rule_stats(...)`  
-> `evaluate_rule_candidate(...)`  
-> `trading_rules.csv`

### 6.2 模型训练数据流

`snapshots`  
-> `load_rules(trading_rules.csv)`  
-> `match_snapshots_to_rules(...)`  
-> `preprocess_features(...)`  
-> `df_feat`  
-> `train / valid / test`

### 6.3 回测数据流

`test snapshots`  
-> `load_rules(trading_rules.csv)`  
-> `match_rules(...)`  
-> `predict_candidates(...)`  
-> `compute_growth_and_direction(...)`  
-> `apply_earliest_market_dedup(...)`  
-> backtest

### 6.4 在线执行数据流

`candidate markets`  
-> `apply_structural_coarse_filter(...)` 基于 family + horizon 粗筛  
-> `apply_live_price_filter(...)` 基于 live price + rule coverage 继续筛  
-> `run_live_inference(...)`  
-> `match_rules(...)`  
-> `prepare_feature_inputs(...)`  
-> model inference  
-> selection / submit

## 7. 受影响模块

如果后续按计划用 `group_key` 逻辑替换旧 `trading_rules.csv`，至少会影响以下模块：

### 7.1 必改主链路

- `rule_baseline/training/train_rules_naive_output_rule.py`
- `rule_baseline/training/train_snapshot_model.py`
- `rule_baseline/datasets/artifacts.py`
- `rule_baseline/backtesting/backtest_portfolio_qmodel.py`
- `rule_baseline/backtesting/backtest_execution_parity.py`
- `execution_engine/online/scoring/rules.py`
- `execution_engine/online/scoring/rule_runtime.py`
- `execution_engine/online/scoring/live.py`
- `execution_engine/online/pipeline/eligibility.py`
- `execution_engine/runtime/config.py`

### 7.2 需要同步改口径的分析 / 质检

- `rule_baseline/analysis/analyze_rules_alpha_quadrant.py`
- `rule_baseline/analysis/compare_baseline_families.py`
- `rule_baseline/analysis/compare_calibration_methods.py`
- `rule_baseline/analysis/run_autogluon_round3_experiments.py`
- `rule_baseline/quality_check/feature_dqc.py`

## 8. 关键结论

### 8.1 旧规则逻辑不是只影响训练

当前 `trading_rules.csv` 不只是训练前筛选器，它同时是：

- 训练样本入口约束
- 回测样本入口约束
- 在线结构粗筛依据
- 在线 live price band 依据
- 在线 inference 规则命中依据

所以后续替换它，不能只改 `train_snapshot_model.py`。

### 8.2 当前系统对 rule schema 有强依赖

当前运行时代码假设规则文件至少包含：

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
- `q_full`
- `rule_score`

回测 runtime 还要求：

- `p_full`
- `edge_full`
- `edge_std_full`
- `edge_lower_bound_full`
- `n_full`

这意味着如果新 `trading_rules.csv` 变成“每个 group_key 一行”的新格式，在线和回测层会直接断。

### 8.3 当前在线链路存在多重 rule 依赖

即使未来你决定完全去掉“旧 rule 过滤逻辑”，在线链路仍要明确替换以下三个行为：

1. `rule_family_miss`
2. `rule_horizon_miss`
3. `live_price_outside_rule_band`

否则即使训练改完，线上也还会继续按旧 rule 模式筛市场。

## 9. 对后续实现的直接含义

基于当前代码结构，后续实现更稳的顺序应当是：

1. 先新增一套 `group_key` 统计与新 `trading_rules.csv` 生成逻辑
2. 再替换训练链路的 `load_rules / match_snapshots_to_rules`
3. 再替换回测链路的 `load_rules / match_rules`
4. 最后替换在线链路中的：
   - 结构 coarse filter
   - live price coverage filter
   - live inference rule match

否则会出现“离线训练已经切到新逻辑，但在线执行还在按旧 rule family/horizon/price band 过滤”的不一致。

## 10. pooled group_key dry-run 统计

本节使用当前规则训练输入口径做 dry-run：

- 数据入口：`prepare_rule_training_frame(artifact_mode='offline')`
- 数据范围：当前 retained labeled snapshots
- 不按 horizon 分层
- `group_key = domain|category|market_type`
- 先按 `unique market < 15` 标记 `insufficient_data`
- 再仅对剩余组计算中位阈值

### 10.1 输入规模

- snapshot rows: `912,038`
- unique markets: `248,471`
- unique group_key: `478`

### 10.2 阈值

- `global_median_logloss = 0.5025268209512956`
- `global_median_brier = 0.15602500000000002`

### 10.3 分组结果

- `drop`: `202` 个 `group_key`
- `keep`: `207` 个 `group_key`
- `insufficient_data`: `69` 个 `group_key`

### 10.4 市场覆盖影响

- `drop`: `52,228` unique markets，`180,624` snapshot rows
- `keep`: `195,768` unique markets，`729,651` snapshot rows
- `insufficient_data`: `475` unique markets，`1,763` snapshot rows

按你当前确认的规则，未来新 `trading_rules.csv` 默认只应包含：

- `keep` 组

不进入新文件的包括：

- `drop`
- `insufficient_data`

### 10.5 top drop 组

按 `unique_markets` 从高到低，前 20 个 `drop` 组是：

1. `yahoo.com|FINANCE|no_yes`
2. `binance.com/en/trade/BTC_USDT|CRYPTO|no_yes`
3. `UNKNOWN|POLITICS|no_yes`
4. `binance.com/en/trade/ETH_USDT|CRYPTO|no_yes`
5. `UNKNOWN|OTHER|no_yes`
6. `binance.com/en/trade/XRP_USDT|CRYPTO|no_yes`
7. `binance.com/en/trade/SOL_USDT|CRYPTO|no_yes`
8. `UNKNOWN|SPORTS|no_yes`
9. `nhl.com.spread|SPORTS|other`
10. `laliga.com|SPORTS|over_under`
11. `efl.com|SPORTS|over_under`
12. `gol.gg|SPORTS|no_yes`
13. `rollcall.com|POLITICS|no_yes`
14. `uefa.com|SPORTS|over_under`
15. `bundesliga.com|SPORTS|over_under`
16. `ufc.com|SPORTS|no_yes`
17. `dotabuff.com|SPORTS|no_yes`
18. `premierleague.com|SPORTS|over_under`
19. `legaseriea.it|SPORTS|over_under`
20. `seekingalpha.com|FINANCE|no_yes`

### 10.6 top keep 组

按 `unique_markets` 从高到低，前 20 个 `keep` 组是：

1. `ncaa.com.basketball|SPORTS|other`
2. `nba.com.total|SPORTS|no_yes`
3. `ncaa.com.basketball|SPORTS|over_under`
4. `hltv.org|SPORTS|other`
5. `nba.com.total|SPORTS|over_under`
6. `binance.com/en/trade/BTC_USDT|CRYPTO|down_up`
7. `binance.com/en/trade/ETH_USDT|CRYPTO|down_up`
8. `binance.com/en/trade/SOL_USDT|CRYPTO|down_up`
9. `atptour.com.spread|SPORTS|over_under`
10. `binance.com/en/trade/XRP_USDT|CRYPTO|down_up`
11. `nba.com.spread|SPORTS|other`
12. `wtatennis.com.spread|SPORTS|over_under`
13. `nfl.com.total|SPORTS|over_under`
14. `nba.com.moneyline|SPORTS|other`
15. `atptour.com.moneyline|SPORTS|other`
16. `ausopen.com|SPORTS|over_under`
17. `nba.com|SPORTS|no_yes`
18. `liquipedia.net/dota2|SPORTS|other`
19. `ncaa.com.football|SPORTS|other`
20. `mlb.com.moneyline|SPORTS|other`

### 10.7 top insufficient_data 组

按 `unique_markets` 从高到低，前 20 个 `insufficient_data` 组是：

1. `soumu.go.jp|POLITICS|no_yes`
2. `binance.com/en/trade/BNB_USDT|CRYPTO|no_yes`
3. `kick.com|SPORTS|no_yes`
4. `ncaa.com.basketball|SPORTS|no_yes`
5. `olympics.com|SPORTS|no_yes`
6. `twitch.tv/lck|SPORTS|no_yes`
7. `twitch.tv/lcs|SPORTS|other`
8. `x.com|POLITICS|other`
9. `kleague.com|SPORTS|no_yes`
10. `ligafutbolprofesional.pe|SPORTS|no_yes`
11. `nfl.com.spread|SPORTS|no_yes`
12. `nikeliga.sk|SPORTS|other`
13. `premierliga.ru|SPORTS|no_yes`
14. `twitch.tv/lplenglish|SPORTS|no_yes`
15. `afa.com.ar|SPORTS|no_yes`
16. `liga1.pe|SPORTS|no_yes`
17. `fortunaliga.cz|SPORTS|no_yes`
18. `hnl.hr|SPORTS|over_under`
19. `infinex.xyz|CRYPTO|no_yes`
20. `slstat.com|SPORTS|no_yes`

### 10.8 现阶段结论

如果严格按你当前确认的规则替换旧逻辑，则：

- 新 `trading_rules.csv` 将从“718 条 leaf 规则”转向“按 group_key keep/drop 结果保留的组集合”
- 从市场覆盖上看，默认会排除约 `52,703` 个 unique markets
- 其中绝大部分来自 `drop` 组，`insufficient_data` 的影响很小

这说明：

1. `min_unique_market >= 15` 的门槛足够轻，不会大幅缩小样本
2. 真正决定覆盖率的是 `drop` 判定，而不是 `insufficient_data`
3. 后续实现必须把线上 coarse filter 和 live filter 同步改成 group_key 逻辑，否则线上覆盖行为会和新文件定义不一致
