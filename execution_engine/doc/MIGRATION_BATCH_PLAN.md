# Execution Migration Batch Plan

## 1. 文档目的

这份文档把前面的计划、矩阵和实施清单进一步压缩成“分批迁移计划”。

它现在同时吸收了原先独立的一页式实施清单。

目标不是再解释字段语义，而是回答四个实施问题：

1. 先动哪一批。
2. 每一批具体收缩什么。
3. 为什么这一批应该先做。
4. 每一批做完必须验证什么。

配套文档：

- 总计划：`execution_engine/doc/GAMMA_PARITY_AND_FEATURE_PRUNING_PLAN.md`
- 字段矩阵：`execution_engine/doc/FIELD_AND_FEATURE_ALIGNMENT_MATRIX.md`

这三份文档已经构成完整交付，不再需要额外的清单或裁决入口。

## 2. 总体迁移原则

### 2.1 先删“低风险、无真实消费者”的部分

第一批不应该碰：

- rule 主键
- selection / submission 主键
- 持仓与 lifecycle 主键

第一批应该优先清掉：

- 默认停算、且明显无规则消费者的特征族

### 2.2 先做 Gamma 语义对齐，再做深层裁剪

对于同源 Gamma 字段：

- 如果 execution 仍与 offline 存在命名或默认值偏差，应该先把语义收齐。
- 语义没收齐之前，不应直接做 aggressive pruning。

### 2.3 每一批都必须可单独验收

每一批都要满足：

1. 可以明确说明删掉了哪些字段或特征族。
2. 可以明确说明没有碰哪些核心消费者。
3. 可以列出固定的回归检查点。

## 3. 实施前统一清单

这一节吸收了原先独立的一页式实施清单。

### 3.1 必保留字段

Rule 路径必保留：

- `market_id`
- `end_time_utc`
- `remaining_hours`
- `accepting_orders`
- `uma_resolution_statuses`
- `domain`
- `category`
- `market_type`
- `selected_reference_token_id`

Live price / snapshot / submission 必保留：

- `best_bid`
- `best_ask`
- `last_trade_price`
- `order_price_min_tick_size`
- `price`
- `horizon_hours`
- `snapshot_time` 或 `snapshot_time_utc`
- `closedTime` 或等价结束时间字段
- `token_0_id`
- `token_1_id`
- `outcome_0_label`
- `outcome_1_label`
- `direction_model`
- `q_pred`
- `edge_final`
- `f_exec`
- `stake_usdc`
- `selected_token_id`
- `selected_outcome_label`
- `first_seen_at_utc`

Lifecycle / monitor 必保留：

- `run_id`
- `batch_id`
- `rule_group_key`
- `rule_leaf_id`
- `position_side`
- `settlement_key`
- `cluster_key`

### 3.2 条件保留字段

只有在真实 `feature_contract`、下游真实消费者或被消费派生特征存在时才保留：

- annotation 扩展字段：`domain_parsed`, `sub_domain`, `source_url`, `category_raw`, `category_parsed`, `category_override_flag`, `outcome_pattern`
- 市场结构原料：`volume`, `liquidity`, `volume24hr`, `volume1wk`, `volume24hr_clob`, `volume1wk_clob`, `liquidity_clob`, `liquidity_amm`, `line`, `rewards_max_spread`, `rewards_min_size`, `neg_risk`
- 文本和时间原料：`question`, `description`, `game_id`, `group_item_title`, `market_maker_address`, `start_time_utc`, `created_at_utc`, `source_market_updated_at_utc`
- historical-price 特征组
- quote-window 统计字段

### 3.3 默认停算字段和特征族

在没有真实消费者证明之前，默认不要算：

- `text_embed_*`
- 文本长度与关键词特征族
- `cat_*` 和 `cat_*_str` 类别词命中特征
- duration 分桶与节奏特征
- execution 当前无法按 offline 同义真实计算的 quote-window 特征

### 3.4 Final parity acceptance gate

这一节补齐最终验收口径，专门覆盖三类核心问题：

1. annotation 语义不一致
2. live 特征处理不一致
3. live snapshot 构造语义不一致

#### Annotation parity gate

实施完成后，必须对同一批 live candidate rows 执行下面的比对：

1. 先按字段矩阵中的 canonical input spec 生成 annotation 输入。
2. 调用 offline `build_market_annotations()` 作为 reference。
3. 对下面这些 authoritative 列逐列比较：
   - `domain`
   - `domain_parsed`
   - `sub_domain`
   - `source_url`
   - `category`
   - `category_raw`
   - `category_parsed`
   - `category_override_flag`
   - `market_type`
   - `outcome_pattern`
4. 这些列在 merge 进入 execution candidate frame 之后，不应再被 execution 二次重写。

验收标准：

- authoritative annotation 列必须逐列一致。
- 如存在唯一允许的 domain allowlist 兼容层，也只能改变 domain 域收敛结果，不能带动 category 或 market_type 漂移。

#### Live-feature parity gate

对所有继续保留的 same-name same-meaning 特征，必须做 reference-helper 级比对：

1. 市场派生特征必须来自 offline `extract_market_features()` / `preprocess_features()` / `apply_feature_variant()` 同一套 helper。
2. historical-price 特征必须来自 `build_historical_price_features()` 同一公式。
3. 数值列比较容差默认控制在 `1e-6` 内；超出视为语义未对齐。

验收标准：

- 若特征名继续与 offline 同名，则数值必须与 reference 公式一致。
- 若无法证明一致，则该特征必须被删除或改名成 live-only 审计列，不能继续沿用 offline 同名。

#### Snapshot parity gate

对所有继续保留在 snapshot / model 路径中的同名字段，必须满足下面的约束：

1. `price`, `horizon_hours`, `closedTime`, `source_host` 必须满足字段矩阵中的 canonical construction spec。
2. quote-window 组字段若继续保留同名：
   - `selected_quote_ts`
   - `selected_quote_side`
   - `selected_quote_offset_sec`
   - `selected_quote_points_in_window`
   - `selected_quote_left_gap_sec`
   - `selected_quote_right_gap_sec`
   - `selected_quote_local_gap_sec`
   - `stale_quote_flag`
   则必须基于同一组 `merged_points` 和同一选窗逻辑与 offline reference 对齐。
3. `snapshot_quality_score` 只有在 quote-window 语义已经 canonical 化后才允许继续保留同名。
4. `remaining_hours` 不允许继续在 model input table 中与 `horizon_hours` 长期并存。

验收标准：

- 继续保留同名的 snapshot 字段必须满足 exact match 或仅有浮点舍入误差。
- 任何 live-only 近似字段都必须改名或移出 model path。

#### Contract and width gate

1. `model_input_table` 只允许保留：
   - `feature_contract.feature_columns`
   - 最小主键与必要桥接列
2. 缺失合同列必须显式报告。
3. 无真实消费者的文本、duration、embedding、quote-window 占位列不得继续出现在 model input table。

## 4. 批次划分

本文建议分五批。

1. 批次 A：停算默认无消费者的文本和语义特征族
2. 批次 B：停算默认无消费者的 duration、历史价格和二级交互特征族
3. 批次 C：收紧 annotation 输入和 Gamma 同源字段语义
4. 批次 D：收紧 snapshot 字段，只保留真实 rule / model / downstream 消费者
5. 批次 E：最终切成阶段化窄表，并补齐验证护栏

## 5. 批次 A

## 批次 A：停算默认无消费者的文本和语义特征族

### 4.1 目标

先清掉最明显的“先算后删”部分，同时尽量不碰核心 rule 和 submission 主路径。

### 4.2 建议清理对象

#### 文本 embedding 特征族

- `text_embed_00` 到 `text_embed_15`

#### 文本长度和表面句式特征族

- `question_length_chars`
- `description_length_chars`
- `text_has_year`
- `text_has_date_word`
- `text_has_percent`
- `text_has_currency`
- `text_has_deadline_word`
- `q_len`
- `q_chars`
- `avg_word_len`
- `max_word_len`
- `word_diversity`
- `num_count`
- `has_number`
- `has_year`
- `has_percent`
- `has_dollar`
- `has_million`
- `has_date`
- `starts_will`
- `starts_can`
- `has_by`
- `has_before`
- `has_after`
- `has_above_below`
- `has_or`
- `has_and`
- `cap_ratio`
- `punct_count`

#### 语义词、类别词、情绪词特征族

- `cat_*`
- `cat_*_str`
- `cat_count`
- `primary_cat_str`
- `is_player_prop`
- `is_team_total`
- `is_finance_threshold`
- `is_date_based`
- `is_high_ambiguity`
- `strong_pos`
- `weak_pos`
- `outcome_pos`
- `outcome_neg`
- `sentiment`
- `sentiment_abs`
- `total_sentiment`
- `certainty`
- `pos_ratio`
- `neg_ratio`
- `sentiment_vol`
- `sentiment_activity`

### 4.3 为什么先做这批

原因很直接：

1. 这批字段对 rule 路径没有硬依赖。
2. 这批字段通常只依赖 `question` / `description` 文本扫描，资源消耗明显。
3. 这批字段是最容易演变成“先完整构造再在模型合同之外被丢掉”的部分。

### 4.4 本批不应触碰的部分

- `market_id`
- `domain`
- `category`
- `market_type`
- `remaining_hours`
- `selected_reference_token_id`
- `best_bid`
- `best_ask`
- `price`
- `q_pred`
- `direction_model`
- `selected_token_id`
- `stake_usdc`

### 4.5 本批回归检查

1. Stage1 / Stage2 候选数量不应因文本特征裁剪而变化。
2. `select_target_side()`、`allocate_candidates()`、`submit_selected_orders()` 输出列集不应丢主键。
3. 若真实 `feature_contract` 包含任何被本批停算的列，则本批不能执行，必须先标记为例外保留。

## 6. 批次 B

## 批次 B：停算默认无消费者的 duration、历史价格和二级交互特征族

### 5.1 目标

继续缩减模型热路径里高成本、但不一定有真实合同消费者的特征组。

### 5.2 建议清理对象

#### Duration 特征族

- `log_duration`
- `dur_very_short`
- `dur_short`
- `dur_medium`
- `dur_long`
- `dur_very_long`
- `vol_per_day`
- `log_vol_per_day`
- `engagement_x_duration`
- `sentiment_x_duration`

#### Historical price 特征族

- `p_1h`
- `p_2h`
- `p_4h`
- `p_6h`
- `p_12h`
- `p_24h`
- `delta_p_1_2`
- `delta_p_2_4`
- `delta_p_4_12`
- `delta_p_12_24`
- `term_structure_slope`
- `path_price_mean`
- `path_price_std`
- `path_price_min`
- `path_price_max`
- `path_price_range`
- `price_reversal_flag`
- `price_acceleration`
- `closing_drift`

#### 二级交互特征族

- `vol_x_sentiment`
- `activity_x_catcount`
- `vol_x_diversity`
- `book_mid_gap`
- `spread_to_mid_ratio`
- `quote_quality_score`
- `liquidity_pressure`
- `clob_turnover_24h`
- `clob_turnover_1w`
- `uncertainty_normalized_edge`
- `rule_confidence_gap`
- `reward_spread_alignment`
- `horizon_term_structure`

### 5.3 为什么这是第二批

1. 这批比文本特征更容易与真实模型合同有交集，所以不能作为第一批盲删。
2. 其中 historical price 特征还涉及 CLOB 历史调用，裁掉后对资源节省很直接。
3. 这批仍然基本不碰 rule 主路径和 submission 主路径。

### 5.4 本批不应触碰的部分

- Stage1 / Stage2 rule coverage 关键字段
- selection / submission / monitor 主键和延迟字段
- 仍被真实 `feature_contract` 明确消费的 interaction 特征

### 5.5 本批回归检查

1. 如果禁用了 historical price 特征，请确认不再调用对应的 CLOB history 拉取逻辑。
2. 若真实 `feature_contract` 中存在这批特征，必须先将它们从“默认停算”转为“条件保留”。
3. live submission、monitor、shared export 不应受影响。

## 7. 批次 C

## 批次 C：收紧 annotation 输入和 Gamma 同源字段语义

### 6.1 目标

把 execution 与 offline 的同源 Gamma 字段先语义收齐。

### 6.2 建议处理对象

#### annotation 输入层

- `resolution_source`
- `description`
- `outcome_0_label`
- `outcome_1_label`
- `game_id`
- `category_raw` / `category`

#### Gamma 同源市场字段

- `volume`
- `liquidity`
- `best_bid`
- `best_ask`
- `spread`
- `last_trade_price`
- `volume24hr`
- `volume1wk`
- `volume24hr_clob`
- `volume1wk_clob`
- `order_price_min_tick_size`
- `neg_risk`
- `rewards_min_size`
- `rewards_max_spread`
- `line`
- `one_hour_price_change`
- `one_day_price_change`
- `one_week_price_change`
- `liquidity_amm`
- `liquidity_clob`
- `group_item_title`
- `market_maker_address`
- `start_time_utc`
- `created_at_utc`
- `end_time_utc`

### 6.3 为什么这是第三批

因为这批开始会触碰 execution 与 offline 之间的共享语义层。

前两批先清掉无消费者特征族之后，再处理这层更安全，因为：

1. 需要对齐的字段集合会更小。
2. 不会一边对齐语义，一边继续背着大量无消费者特征族。

### 6.4 本批回归检查

1. `domain` / `category` / `market_type` 的最终输出不得发生非预期漂移。
2. execution annotation 产物应能与 offline annotation 语义逐项比对。
3. 所有 Gamma 同源字段必须明确唯一命名和唯一默认值策略。

## 8. 批次 D

## 批次 D：收紧 snapshot 字段，只保留真实消费者

### 7.1 目标

把当前宽 live snapshot 缩到“真实 rule / model / downstream 需要”的最小集。

### 7.2 重点处理对象

#### 需要重新审视的 snapshot 字段

- `snapshot_date`
- `scheduled_end`
- `delta_hours_bucket`
- `selected_quote_offset_sec`
- `selected_quote_points_in_window`
- `selected_quote_left_gap_sec`
- `selected_quote_right_gap_sec`
- `selected_quote_local_gap_sec`
- `selected_quote_ts`
- `snapshot_target_ts`
- `selected_quote_side`
- `stale_quote_flag`
- `snapshot_quality_score`
- `source_host`
- `primary_outcome`
- `secondary_outcome`
- `market_duration_hours`
- `tick_size`
- `token_state_age_sec`
- `remaining_hours` 与 `horizon_hours` 的重复保留问题

### 7.3 为什么这是第四批

因为这批会真正触碰 live snapshot 语义层。

这一步必须建立在前面三批完成之后：

1. 无消费者特征族已经清掉。
2. Gamma 同源字段和 annotation 语义已经收齐。
3. 否则很难判断 snapshot 字段到底是为谁保留。

### 7.4 本批回归检查

1. 任何继续保留同名的 snapshot 字段都必须与 offline 同义。
2. 任何无法同义的 live-only 近似字段都必须改名或只保留在审计层。
3. `prepare_feature_inputs()`、`select_target_side()`、`submit_selected_orders()` 必需字段不能丢。

## 9. 批次 E

## 批次 E：切成阶段化窄表并补齐验证护栏

### 8.1 目标

完成最终结构收敛：

1. 不再让一张宽表贯穿所有阶段。
2. 明确每个阶段的最小字段集。
3. 对缺失合同列建立显式告警或失败机制。

### 8.2 建议目标结构

#### `gamma_context_table`

只保留：

- annotation 输入所需字段
- 市场特征构建原料中仍有真实消费者的部分
- rule 主路径必需字段

#### `model_input_table`

只保留：

- 真实 `feature_contract.feature_columns`
- 最少主键：`market_id`、`snapshot_time`
- 极少数推理后仍需传递到 selection 的桥接列

#### `execution_decision_table`

只保留：

- selection
- submission
- monitor
- lifecycle export

所需字段

### 8.3 需要新增的验证护栏

1. 缺失模型合同列时必须显式报告缺列集合。
2. 需要区分：
   - 可接受默认值的列
   - 绝不应缺失的列
3. 为每一批迁移提供固定 smoke checks 和 parity checks。

### 8.4 为什么这是最后一批

因为只有前四批做完后，execution 才真正知道：

1. 哪些字段还活着。
2. 哪些字段有真实消费者。
3. 哪些语义已经与 offline 收齐。

只有这时切窄表才不会变成第二次混乱重构。

## 10. 每批统一检查模板

每一批都建议用同一套检查模板：

### 9.1 消费者检查

1. rule 路径有没有丢字段。
2. 真实 `feature_contract` 有没有缺字段。
3. submission / monitor / lifecycle 有没有丢主键。

### 9.2 语义检查

1. 同名 Gamma 字段是否仍与 offline 同义。
2. annotation 最终输出是否仍与 offline 语义一致。
3. snapshot 同名字段是否存在“同名不同义”。

### 9.3 资源检查

1. 是否减少了文本扫描或 embedding 构造。
2. 是否减少了市场派生特征构造。
3. 是否减少了 CLOB history 拉取。
4. 是否减少了热路径 DataFrame 宽度。

## 11. 推荐实施顺序总结

最推荐的真实落地顺序是：

1. 批次 A
2. 批次 B
3. 批次 C
4. 批次 D
5. 批次 E

一句话解释就是：

- 先砍掉明显无消费者的重特征族，再收齐共享语义，最后再重构表结构。
