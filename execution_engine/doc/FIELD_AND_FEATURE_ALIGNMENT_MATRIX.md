# Execution Field And Feature Alignment Matrix

## 1. 文档目的

这份文档只做一件事：把 `execution_engine` 当前涉及的字段和特征逐项落成对齐规范。

如果你更关心“哪些字段被 rule、模型、submission、monitor 真正消费”，本文底部已经附带消费者视图摘要。

如果你只想看收敛后的实施清单，见：

- `execution_engine/doc/MIGRATION_BATCH_PLAN.md`

如果你想直接看分批推进顺序，见：

- `execution_engine/doc/MIGRATION_BATCH_PLAN.md`

如果你想直接看总体边界和最终裁决，见：

- `execution_engine/doc/GAMMA_PARITY_AND_FEATURE_PRUNING_PLAN.md`

核心约束有两条：

1. 如果 execution 和 offline 使用的是同一类 Gamma 原始数据，则默认要求强对齐，不能各自定义不同语义。
2. 如果某个字段或特征没有被规则、真实模型 `feature_contract` 或下游执行链路消费，就不应该在 execution 热路径里先计算再删除。

这份文档是执行规范，不代表代码已经完成迁移。

## 2. 重要前提

当前工作区里没有实际部署中的 `q_model_bundle/feature_contract.json`，因此所有涉及“模型是否消费”的项目都必须按下面规则解释：

- 如果真实 `feature_contract` 包含该列：执行路径必须按本文定义的方式对齐并保留。
- 如果真实 `feature_contract` 不包含该列，且规则与下游也不消费：该列应停止计算。

因此本文中的“模型候选”表示“可能被模型消费，但必须以真实 bundle 为准”。

## 3. 对齐标签

| 标签 | 含义 |
|---|---|
| `强对齐` | Gamma 同源字段，execution 与 offline 必须保持同名语义、同默认规则、同类型约束。 |
| `共享注解` | 应直接复用 offline annotation 输入契约和 annotation 逻辑，不允许 execution 维护独立语义。 |
| `语义对齐` | 属于 live-only 数据，不能与 offline 完全同源，但字段名代表的统计意义必须一致；如果做不到，要改名。 |
| `执行专用` | 只服务于 live 执行、提交、监控或运维，不要求与 offline 完全同构。 |
| `按需计算` | 只有在规则、真实 `feature_contract` 或下游明确消费时才允许计算。 |
| `默认停算` | 当前没有稳定消费者时不应进入 execution 热路径；只有真实消费者出现才恢复。 |

## 4. Execution Source 原始字段对齐矩阵

下表对应 `execution_engine/online/universe/page_source.py` 中的 `EXECUTION_SOURCE_COLUMNS`。

| execution 字段 | offline 规范字段 | 主要消费者 | 对齐标签 | 对齐方式 |
|---|---|---|---|---|
| `market_id` | `market_id` | 全部 | `强对齐` | 作为唯一主键，禁止 execution 生成第二套 ID 语义。 |
| `question` | `question` | 注解、市场特征、模型候选 | `强对齐` + `按需计算` | 保持 Gamma 原文；只要文本特征族没有真实消费者，就不应继续向后保留。 |
| `description` | `description` | 注解、市场特征、模型候选 | `强对齐` + `按需计算` | 与 offline 使用相同文本输入语义；无文本消费者时不应保留。 |
| `resolution_source` | `resolutionSource` | 注解 | `共享注解` | 必须进入与 offline 一致的 annotation 输入契约。 |
| `game_id` | `gameId` | 注解、市场特征、模型候选 | `强对齐` + `按需计算` | 先保留 Gamma 原始含义；无模型或下游消费者时不继续保留。 |
| `remaining_hours` | `horizon_hours` 上游来源 | 规则、模型候选、下游 | `强对齐` | 统一表示“距市场结束还有多少小时”；由 `end_time_utc - now` 计算。 |
| `category` | `category` | 规则、模型候选 | `共享注解` | 以 offline annotator 输出为准，不接受 execution 自己定义最终语义。 |
| `category_raw` | `category_raw` | 注解、模型候选 | `共享注解` | 表示注解前或解析前类别，不允许 execution 单独发明语义。 |
| `category_parsed` | `category_parsed` | 模型候选 | `共享注解` + `按需计算` | 只有真实消费者存在时才继续保留。 |
| `category_override_flag` | `category_override_flag` | 模型候选 | `共享注解` + `按需计算` | 保持布尔语义；无消费者则不进入热路径。 |
| `domain` | `domain` | 规则、模型候选 | `共享注解` | 以 offline domain taxonomy 为准。 |
| `domain_parsed` | `domain_parsed` | 模型候选 | `共享注解` + `按需计算` | 只在真实消费者存在时保留。 |
| `sub_domain` | `sub_domain` | 模型候选 | `共享注解` + `按需计算` | 只在真实消费者存在时保留。 |
| `source_url` | `source_url` | 注解、模型候选 | `共享注解` + `按需计算` | 由 offline annotator 的 canonical URL 解析逻辑决定。 |
| `market_type` | `market_type` | 规则、模型候选 | `共享注解` | 以 offline annotation 产物为唯一来源。 |
| `outcome_pattern` | `outcome_pattern` | 模型候选 | `共享注解` + `按需计算` | 只在真实消费者存在时保留。 |
| `accepting_orders` | `acceptingOrders` | Stage1 | `执行专用` | 保留 Gamma 布尔语义；不进入模型。 |
| `volume` | `volume` | 市场特征、模型候选、下游 | `强对齐` | 与 offline 原始市场字段同义；如无消费者，不应仅为了派生后再丢弃而保留。 |
| `best_bid` | `bestBid` | 市场特征、snapshot、下游 | `强对齐` | 与 offline 原始报价字段同义。 |
| `best_ask` | `bestAsk` | 市场特征、snapshot、下游 | `强对齐` | 与 offline 原始报价字段同义。 |
| `spread` | `spread` | 市场特征、snapshot、下游 | `强对齐` | 与 offline 原始价差字段同义。 |
| `last_trade_price` | `lastTradePrice` | 市场特征、snapshot、下游 | `强对齐` | 与 offline 原始成交价字段同义。 |
| `liquidity` | `liquidity` | 市场特征、snapshot、下游 | `强对齐` | 与 offline 原始流动性字段同义。 |
| `volume24hr` | `volume24hr` | 市场特征、snapshot | `强对齐` | 与 offline 原始近 24h 成交量字段同义。 |
| `volume1wk` | `volume1wk` | 市场特征 | `强对齐` + `按需计算` | 无真实消费者时不应继续保留。 |
| `volume24hr_clob` | `volume24hrClob` | 市场特征 | `强对齐` + `按需计算` | 只有相关派生特征或模型列被消费时才保留。 |
| `volume1wk_clob` | `volume1wkClob` | 市场特征 | `强对齐` + `按需计算` | 同上。 |
| `order_price_min_tick_size` | `orderPriceMinTickSize` | 市场特征、snapshot、下游 | `强对齐` | 保持与 Gamma tick size 完全一致。 |
| `neg_risk` | `negRisk` | 市场特征、模型候选 | `强对齐` + `按需计算` | 布尔/类别语义必须与 offline 一致；无消费者时不保留。 |
| `rewards_min_size` | `rewardsMinSize` | 市场特征、下游候选 | `强对齐` + `按需计算` | 只有真实消费者存在时保留。 |
| `rewards_max_spread` | `rewardsMaxSpread` | 市场特征、模型候选 | `强对齐` + `按需计算` | 只在相关特征或下游使用时保留。 |
| `line` | `line` | 市场特征、模型候选 | `强对齐` + `按需计算` | 与 offline line 字段同义；无消费者时不保留。 |
| `one_hour_price_change` | `oneHourPriceChange` | 市场特征 | `强对齐` + `按需计算` | 保持 Gamma 价格变化语义；无消费者时不保留。 |
| `one_day_price_change` | `oneDayPriceChange` | 市场特征 | `强对齐` + `按需计算` | 同上。 |
| `one_week_price_change` | `oneWeekPriceChange` | 市场特征 | `强对齐` + `按需计算` | 同上。 |
| `liquidity_amm` | `liquidityAmm` | 市场特征 | `强对齐` + `按需计算` | 只在真实派生特征或模型列需要时保留。 |
| `liquidity_clob` | `liquidityClob` | 市场特征、模型候选 | `强对齐` + `按需计算` | 同上。 |
| `group_item_title` | `groupItemTitle` | 市场特征、模型候选 | `强对齐` + `按需计算` | 如无真实消费者，不应继续保留到模型前。 |
| `market_maker_address` | `marketMakerAddress` | 市场特征、模型候选 | `强对齐` + `按需计算` | 如无消费者则停止保留。 |
| `outcome_0_label` | `outcome_0_label` / annotation `outcomes[0]` | 注解、snapshot、下游 | `强对齐` | 保持 Gamma outcome 标签原文。 |
| `outcome_1_label` | `outcome_1_label` / annotation `outcomes[1]` | 注解、snapshot、下游 | `强对齐` | 同上。 |
| `token_0_id` | `token_0_id` | snapshot、下游 | `执行专用` | 主要用于 live 引用代币，不要求 offline 训练同构。 |
| `token_1_id` | `token_1_id` | snapshot、下游 | `执行专用` | 同上。 |
| `selected_reference_token_id` | 无直接 offline 对应 | Stage2、snapshot、下游 | `执行专用` | 表示 live 参考代币选择结果，不要求 offline 同构。 |
| `selected_reference_outcome_label` | 无直接 offline 对应 | snapshot、下游 | `执行专用` | 只服务于 live 参考侧。 |
| `selected_reference_side_index` | 无直接 offline 对应 | snapshot、下游 | `执行专用` | 只服务于 live 参考侧。 |
| `uma_resolution_statuses` | `umaResolutionStatuses` | Stage1 | `执行专用` | 作为 live 结构拒绝条件，不进入模型。 |
| `start_time_utc` | `startDate` | 市场特征、duration 派生 | `强对齐` + `按需计算` | 与 offline duration 计算使用同一时间语义。 |
| `created_at_utc` | `createdAt` / `creationDate` | 市场特征、duration 派生 | `强对齐` + `按需计算` | 只有 duration 或文本/时间特征真实消费时保留。 |
| `end_time_utc` | `endDate` / `closedTime` | 规则、snapshot、duration、下游 | `强对齐` | 统一作为 live 市场结束时点。 |
| `source_market_updated_at_utc` | `updatedAt` | 审计候选 | `执行专用` + `按需计算` | 若无监控/审计消费者，不应进入热路径。 |
| `first_seen_at_utc` | 无 offline 对应 | 下游、监控 | `执行专用` | 只保留给 live 生命周期追踪。 |

## 5. Annotation 字段对齐矩阵

下表对应 `execution_engine/online/scoring/annotations.py` 中的 `_ANNOTATION_COLUMNS`。

| annotation 字段 | 主要消费者 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `market_id` | 注解 merge、规则、模型候选 | `强对齐` | 作为 annotation 主键，必须与 raw `market_id` 一致。 |
| `domain` | 规则、模型候选 | `共享注解` | 使用 offline `build_market_annotations()` 的最终 domain 语义。 |
| `domain_parsed` | 模型候选 | `共享注解` + `按需计算` | 只有真实消费者存在时保留。 |
| `sub_domain` | 模型候选 | `共享注解` + `按需计算` | 同上。 |
| `source_url` | 模型候选 | `共享注解` + `按需计算` | 必须来自 offline annotation parser，而不是 execution 自己拼接。 |
| `category` | 规则、模型候选 | `共享注解` | 以 offline annotation 最终分类为准。 |
| `category_raw` | 模型候选 | `共享注解` + `按需计算` | 仅在真实消费者存在时保留。 |
| `category_parsed` | 模型候选 | `共享注解` + `按需计算` | 同上。 |
| `category_override_flag` | 模型候选 | `共享注解` + `按需计算` | 同上。 |
| `market_type` | 规则、模型候选 | `共享注解` | 作为规则匹配关键维度，必须与 offline 完全同义。 |
| `outcome_pattern` | 模型候选 | `共享注解` + `按需计算` | 只有真实消费者存在时保留。 |

### Annotation 输入规范补充

execution 当前构造的 annotation 输入为：

- `id`
- `resolutionSource`
- `description`
- `outcomes`
- `gameId`
- `category`

规范要求是：

1. execution 应尽量与 offline 共用同一 annotation 输入契约。
2. 如果 execution 只构造缩减版输入，必须证明其不会改变 offline annotator 的最终语义。
3. domain normalization 只能作为兼容层，不能替代 canonical offline annotation 语义。

### Annotation canonical input spec

这一节把前面的原则收成实施时必须遵守的最终输入契约。

当前 offline annotator 真实消费的 canonical 输入就是下面六列。只要 offline 这部分接口不变，execution 就必须逐列镜像，不允许再发明第二套输入形状。

| canonical 输入列 | execution 来源 | 必填性 | 规范化规则 | 备注 |
|---|---|---|---|---|
| `id` | `market_id` | 必填 | 转成字符串；空字符串视为无效 market，不允许造 synthetic id。 | annotation 主键。 |
| `resolutionSource` | `resolution_source` | 可空 | 原样转字符串；缺失写空字符串。 | 不在 execution 侧额外裁剪域名。 |
| `description` | `description` | 可空 | 原样转字符串；缺失写空字符串。 | 不做 execution 专用文本清洗。 |
| `outcomes` | `outcome_0_label`, `outcome_1_label` | 必填 | 始终编码成长度为 2 的 JSON array，保持原始顺序。 | 不允许改 outcome 顺序。 |
| `gameId` | `game_id` | 可空 | 先转字符串；值为 `UNKNOWN` 时写空字符串。 | 与当前 `_normalize_game_id()` 一致。 |
| `category` | `category_raw` 优先，否则 `category` | 必填 | `strip().upper()`；空值写 `UNKNOWN`。 | 与当前 `_normalize_category_text()` 一致。 |

补充约束：

1. execution 在调用 offline annotator 之前，不允许自行生成最终 `domain`、`category`、`market_type`。
2. `domain_candidate` 只能作为审计辅助字段，不能替代 annotator 的最终输出字段。
3. 如果未来 offline annotator 输入契约扩展，execution 必须同步扩展到完全同构，而不是继续维护缩减版子集。

### Annotation output merge spec

这一节定义 annotation 结果回写到 execution candidate frame 时的唯一规则。

1. authoritative 输出列只有这些：
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
2. 这些列一旦由 offline annotator 产出，就应覆盖 execution 现有同名列；execution 侧旧值只能作为缺失回退，不能反向覆盖 annotator 输出。
3. 允许存在的唯一兼容层是 domain allowlist 归一化：它只能把 annotator 产出的候选域收敛到 offline 已知域集合，不能改写 `category`、`market_type`、`outcome_pattern`。
4. merge 之后的默认填充值必须固定：
   - `domain`, `category`, `market_type`, `domain_parsed`, `source_url`, `category_raw`, `category_parsed`, `outcome_pattern` -> `UNKNOWN`
   - `sub_domain` -> 空字符串
   - `category_override_flag` -> `False`
5. 如果 annotation build 失败，execution 允许保留原始市场行，但必须把失败视为 parity 风险，而不是当作成功注解。

## 6. Live Snapshot 字段对齐矩阵

下表对应 `execution_engine/online/scoring/live.py` 中 `_build_live_snapshot_rows()` 直接生成的字段。

| snapshot 字段 | 主要消费者 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `market_id` | 全部 | `强对齐` | 与 raw/annotation/规则主键一致。 |
| `batch_id` | 下游、审计 | `执行专用` | 只服务于 live 批次追踪。 |
| `first_seen_at_utc` | 下游、监控 | `执行专用` | 只服务于 live 生命周期。 |
| `price` | 规则、模型候选、下游 | `语义对齐` | 表示当前用于判定和推理的参考价格；必须与 offline `price` 一样表示“用于该 snapshot 的成交/中位价格语义”。 |
| `horizon_hours` | 规则、模型候选 | `强对齐` | 统一表示距结束剩余小时数。 |
| `snapshot_time` | 规则、模型候选、下游 | `语义对齐` | 表示当前 snapshot 的生成时刻。 |
| `snapshot_date` | 模型候选、审计 | `语义对齐` + `按需计算` | 若无真实消费者，不需要提前物化。 |
| `scheduled_end` | 审计候选 | `语义对齐` + `按需计算` | 与 `end_time_utc` 同义时不应重复长时间保留。 |
| `closedTime` | 模型候选、duration 派生 | `强对齐` | 若保留该名字，则必须保持 offline `closedTime` 语义。 |
| `delta_hours_bucket` | 无稳定消费者 | `默认停算` | 当前固定写 `0.0`，在存在真实消费者前不应继续计算或保留。 |
| `selected_quote_offset_sec` | 模型候选 | `语义对齐` + `按需计算` | 如果保留此名，必须表示“选中报价点距目标时刻的偏移秒数”；不能长期用 `token_state_age_sec` 近似替代而不说明。 |
| `selected_quote_points_in_window` | 模型候选 | `语义对齐` + `按需计算` | 如果保留此名，必须表示选窗内点数；无消费者则停算。 |
| `selected_quote_left_gap_sec` | 模型候选 | `语义对齐` + `按需计算` | 若保留，则应按 offline 选窗语义计算；不能长期固定为 `0.0`。 |
| `selected_quote_right_gap_sec` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `selected_quote_local_gap_sec` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `selected_quote_ts` | 模型候选 | `语义对齐` + `按需计算` | 如果保留，必须是真实被选中报价点的时间戳。 |
| `snapshot_target_ts` | 模型候选 | `语义对齐` + `按需计算` | 表示 snapshot 目标时点；若无消费者可不保留。 |
| `selected_quote_side` | 模型候选 | `语义对齐` + `按需计算` | 若保留，应表示真实选中的 quote 侧；不能只是占位常量。 |
| `stale_quote_flag` | 模型候选、下游审计 | `语义对齐` + `按需计算` | 语义应与 offline 一致：目标窗口内无有效报价或报价不可用。 |
| `snapshot_quality_score` | 模型候选、审计 | `语义对齐` + `按需计算` | 若使用同名，最好与 offline 同一公式；若继续使用 live 专用公式，应改名或只做审计列。 |
| `domain` | 规则、模型候选 | `共享注解` | 来自 annotation 产物，不得在 snapshot 层重解释。 |
| `category` | 规则、模型候选 | `共享注解` | 同上。 |
| `market_type` | 规则、模型候选 | `共享注解` | 同上。 |
| `source_host` | 模型候选 | `共享注解` + `按需计算` | 若保留，应与 offline `source_host` 含义一致，不应简单回落为 `domain`。 |
| `primary_outcome` | 模型候选、下游 | `按需计算` | 只有真实消费者存在时保留。 |
| `secondary_outcome` | 模型候选、下游 | `按需计算` | 同上。 |
| `market_duration_hours` | 模型候选 | `强对齐` + `按需计算` | 使用与 offline 相同的 start/end 时间差语义。 |
| `outcome_0_label` | 下游、注解候选 | `强对齐` | 与 Gamma outcome 标签一致。 |
| `outcome_1_label` | 下游、注解候选 | `强对齐` | 同上。 |
| `token_0_id` | 下游 | `执行专用` | 只用于 live token 引用。 |
| `token_1_id` | 下游 | `执行专用` | 同上。 |
| `selected_reference_token_id` | Stage2、下游 | `执行专用` | live 参考代币选择结果。 |
| `selected_reference_outcome_label` | 下游 | `执行专用` | live 参考侧标签。 |
| `selected_reference_side_index` | 下游 | `执行专用` | live 参考侧索引。 |
| `best_bid` | 模型候选、下游 | `语义对齐` | 表示 snapshot 时刻参考 token 的实时 bid。 |
| `best_ask` | 模型候选、下游 | `语义对齐` | 表示 snapshot 时刻参考 token 的实时 ask。 |
| `mid_price` | 模型候选、下游 | `语义对齐` | 若与 `price` 等价，后续应尽快只保留一个消费者真正需要的列。 |
| `last_trade_price` | 模型候选、下游 | `语义对齐` + `按需计算` | 无消费者时不保留。 |
| `tick_size` | 下游、模型候选 | `语义对齐` + `按需计算` | 若保留，应与实际提交价格粒度一致。 |
| `liquidity` | 模型候选、下游 | `强对齐` | 与 Gamma 流动性原义一致。 |
| `volume24hr` | 模型候选 | `强对齐` + `按需计算` | 无消费者时不保留。 |
| `token_state_age_sec` | 下游、审计候选 | `执行专用` + `按需计算` | 只要不直接作为模型合同列，就不应替代 quote offset 语义。 |
| `remaining_hours` | 下游候选 | `执行专用` | 与 `horizon_hours` 重复时，应在后续窄表阶段去重。 |

### Live snapshot canonical construction spec

这一节定义 live snapshot 的唯一构造口径，目的是把“同名字段必须同义”落实成实现步骤。

#### 6.1 Batch clock

1. 每次 `_build_live_snapshot_rows()` 调用只捕获一次 batch 级 UTC 时间。
2. `snapshot_time` 必须来自这个 batch 级时间，当前 batch 内所有行共享同一个 `snapshot_time`。
3. `snapshot_target_ts` 只有在真正构造 quote-window 统计时才允许保留；如果只是简单等于 batch 当前时间但没有真正选窗，就不应进入 model input table。

#### 6.2 Core snapshot fields

| 字段 | 允许的唯一构造方式 | 不允许的行为 |
|---|---|---|
| `price` | 直接使用上游 live filter 已确认的 `live_mid_price`。 | 不允许在 snapshot builder 内再次私自改成别的 bid/ask 回退语义。 |
| `horizon_hours` | 使用当前市场行的 `remaining_hours`，作为进入模型路径的 canonical horizon。 | 不允许在模型热路径同时长期保留 `remaining_hours`。 |
| `closedTime` | 直接等于 `end_time_utc`。 | 不允许引入第二套结束时间语义。 |
| `scheduled_end` | 只作为审计列，来源仍是 `end_time_utc`。 | 不允许与 `closedTime` 长期并行作为模型输入。 |
| `best_bid` / `best_ask` / `last_trade_price` | 直接来自 Stage2 更新后的 live candidate row。 | 不允许在 snapshot 层发明新的报价字段语义。 |
| `tick_size` | 优先使用真实 `tick_size`；缺失时允许回退 `order_price_min_tick_size`。 | 不允许静默写死常量且不暴露来源。 |
| `market_duration_hours` | 使用 `start_time_utc` 与 `end_time_utc` 的时间差。 | 不允许用其他时间对替代。 |

#### 6.3 `source_host` canonical rule

offline 历史 snapshot 的 canonical 语义是：从 `resolutionSource` 或 `source_url` 中提取 host，再转成小写；缺失时写 `UNKNOWN`。

execution 的实现规范应为：

1. 若 annotation 后存在可解析的 `source_url`，优先从 `source_url` 解析 host。
2. 若 `source_url` 不可用，再回退到 `resolution_source`。
3. 若两者都不可用，才允许回退到 `domain`，且必须把这一行为视为兼容回退，而不是 canonical 来源。
4. `source_host` 不能再简单等于 `domain` 却继续冒充 canonical 字段。

#### 6.4 Quote-window canonical rule

offline 对这组字段的 canonical 语义来自 `find_prices_batch(..., window_sec=300)`：

- `selected_quote_ts`: 目标窗口内被选中的报价点时间戳
- `selected_quote_side`: 选中的侧，取值应是 `left` 或 `right`
- `selected_quote_offset_sec`: `abs(selected_quote_ts - snapshot_target_ts)`
- `selected_quote_points_in_window`: 目标窗口内可用点数
- `selected_quote_left_gap_sec`: 目标时刻到左邻点的距离
- `selected_quote_right_gap_sec`: 目标时刻到右邻点的距离
- `selected_quote_local_gap_sec`: 被选中点到最近邻点的局部间隔
- `stale_quote_flag`: 当没有可用点，或 `offset_sec > STALE_QUOTE_MAX_OFFSET_SEC`，或 `local_gap_sec > STALE_QUOTE_MAX_GAP_SEC` 时为真

因此 execution 只能有两种合法做法：

1. canonical 模式：真正用 merged price points 复现上述选窗逻辑，然后继续保留这些 offline 同名字段。
2. prune-or-rename 模式：如果没有真正选窗，就不得把 `token_state_age_sec`、常量 `0.0`、固定字符串 `reference_token` 继续塞进这些 offline 同名列。此时应：
   - 从 `model_input_table` 移除这组字段；或者
   - 改名成明确的 live-only 审计列。

当前实现中：

- `selected_quote_offset_sec <- token_state_age_sec`
- `selected_quote_points_in_window <- raw_event_count`
- `selected_quote_left_gap_sec <- 0.0`
- `selected_quote_right_gap_sec <- 0.0`
- `selected_quote_local_gap_sec <- 0.0`
- `selected_quote_ts <- now_ts`
- `snapshot_target_ts <- now_ts`
- `selected_quote_side <- "reference_token"`
- `stale_quote_flag <- False`

这些值只能被视为临时 live audit 近似量，不能继续作为 offline 同名字段进入模型热路径。

#### 6.5 Historical-price feature construction rule

如果 execution 保留 historical-price 特征，则唯一允许的构造方式是：

1. 先拉取 CLOB history。
2. 如果当前 `price > 0`，再把当前 live price 作为 `PricePoint(ts=now_ts, price=price, source="live_token_state")` 追加到 `merged_points`。
3. 仅通过 `build_historical_price_features(current_price, now_ts, end_ts, merged_points)` 生成 `p_1h` 到 `closing_drift`。
4. 数值对齐按同名同公式处理，容差只允许浮点舍入误差。

#### 6.6 `snapshot_quality_score` rule

如果 execution 不能用 offline 同义的 quote-window 质量定义来计算 `snapshot_quality_score`，就不应继续在模型路径保留这个同名字段。

允许的做法只有两种：

1. 复用 canonical quote-window 统计后再定义同义 `snapshot_quality_score`。
2. 把当前基于 `token_state_age_sec` 和 `raw_event_count` 的启发式质量分改名成 live-only 审计字段，不再叫 `snapshot_quality_score`。

## 7. Historical Price 特征对齐矩阵

下表对应 `execution_engine/online/scoring/price_history.py` 中 `build_historical_price_features()`。

| 特征 | 主要消费者 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `p_1h` | 模型候选 | `语义对齐` + `按需计算` | 与 offline 同名列保持一致，表示距结束 1 小时位置的历史价格。 |
| `p_2h` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `p_4h` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `p_6h` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `p_12h` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `p_24h` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `delta_p_1_2` | 模型候选 | `语义对齐` + `按需计算` | 保持与 offline 同名差分定义一致。 |
| `delta_p_2_4` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `delta_p_4_12` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `delta_p_12_24` | 模型候选 | `语义对齐` + `按需计算` | 同上。 |
| `term_structure_slope` | 模型候选 | `语义对齐` + `按需计算` | 保持 `p_1h - p_24h` 的同名语义。 |
| `path_price_mean` | 模型候选 | `语义对齐` + `按需计算` | 保持同名价格路径均值语义。 |
| `path_price_std` | 模型候选 | `语义对齐` + `按需计算` | 保持同名价格路径标准差语义。 |
| `path_price_min` | 模型候选 | `语义对齐` + `按需计算` | 保持同名最小路径价语义。 |
| `path_price_max` | 模型候选 | `语义对齐` + `按需计算` | 保持同名最大路径价语义。 |
| `path_price_range` | 模型候选 | `语义对齐` + `按需计算` | 保持同名路径价差语义。 |
| `price_reversal_flag` | 模型候选 | `语义对齐` + `按需计算` | 保持同名“短端与长端变动方向是否反转”的语义。 |
| `price_acceleration` | 模型候选 | `语义对齐` + `按需计算` | 保持同名短腿减长腿的定义。 |
| `closing_drift` | 模型候选 | `语义对齐` + `按需计算` | 保持 `current_price - p_24h` 的同名语义。 |

### Historical Price 规则补充

1. 这些特征来自 CLOB 历史价格与 live token state，不是 Gamma 原始字段。
2. 因此不要求“同源强对齐”，但要求“同名同义”。
3. 如果真实 `feature_contract` 不包含这些列，就不应在 execution 热路径里构造它们。

## 8. Market Feature Cache 派生特征对齐矩阵

下表对应 `polymarket_rule_engine/rule_baseline/features/market_feature_builders.py` 中 `extract_market_features()` 生成的特征。

统一规则先行：

1. 这些特征一旦需要计算，必须直接复用 offline 的 `extract_market_features()` 逻辑，不允许 execution 自己重写公式。
2. 这些特征只有在真实 `feature_contract`、规则或下游存在明确消费者时才应该计算。
3. 如果上游原始 Gamma 字段只为了这些特征而保留，而这些特征没有消费者，则上游字段也应一起从热路径移除。

### 8.1 成交量、流动性、盘口结构特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `log_vol` | `volume` | `按需计算` | 只有真实消费者存在时，按 offline `log1p(volume)` 计算。 |
| `log_liq` | `liquidity` | `按需计算` | 同上。 |
| `log_v24` | `volume24hr` | `按需计算` | 同上。 |
| `log_v1w` | `volume1wk` | `按需计算` | 同上。 |
| `vol_ratio_24` | `volume`, `volume24hr` | `按需计算` | 按 offline 比例定义计算。 |
| `vol_ratio_1w` | `volume`, `volume1wk` | `按需计算` | 同上。 |
| `liq_ratio` | `liquidity`, `volume` | `按需计算` | 同上。 |
| `daily_weekly` | `volume24hr`, `volume1wk` | `按需计算` | 按 offline 周/日转换公式计算。 |
| `vol_tier_ultra` | `volume` | `按需计算` | 只在真实消费者存在时按 offline 阈值分层。 |
| `vol_tier_high` | `volume` | `按需计算` | 同上。 |
| `vol_tier_med` | `volume` | `按需计算` | 同上。 |
| `vol_tier_low` | `volume` | `按需计算` | 同上。 |
| `activity` | `volume` | `按需计算` | 按 offline `log1p(volume)/17` 定义。 |
| `engagement` | `vol_ratio_24`, `vol_ratio_1w` | `按需计算` | 必须基于 offline 同一定义。 |
| `momentum` | `vol_ratio_24`, `vol_ratio_1w` | `按需计算` | 同上。 |
| `best_bid` | `bestBid` | `按需计算` | 作为市场特征时仍应沿用 offline builder，而不是 execution 自己另算。 |
| `best_ask` | `bestAsk` | `按需计算` | 同上。 |
| `mid_price` | `bestBid`, `bestAsk`, `lastTradePrice` | `按需计算` | 按 offline builder 的 bid/ask 回退逻辑计算。 |
| `quoted_spread` | `spread`, `bestBid`, `bestAsk` | `按需计算` | 按 offline builder 的 `spread` 优先定义计算。 |
| `quoted_spread_pct` | `quoted_spread`, `mid_price` | `按需计算` | 同上。 |
| `book_imbalance` | `liquidity`, `volume` | `按需计算` | 只在真实消费者存在时计算。 |
| `log_liquidity_clob` | `liquidityClob` | `按需计算` | 同上。 |
| `log_liquidity_amm` | `liquidityAmm` | `按需计算` | 同上。 |
| `clob_share_liquidity` | `liquidityClob`, `liquidity` | `按需计算` | 同上。 |
| `clob_share_volume24` | `volume24hrClob`, `volume24hr` | `按需计算` | 同上。 |
| `clob_share_volume1w` | `volume1wkClob`, `volume1wk` | `按需计算` | 同上。 |
| `price_change_1h` | `oneHourPriceChange` | `按需计算` | 按 offline builder 同名定义。 |
| `price_change_1d` | `oneDayPriceChange` | `按需计算` | 同上。 |
| `price_change_1w` | `oneWeekPriceChange` | `按需计算` | 同上。 |
| `price_change_accel` | `oneHourPriceChange`, `oneDayPriceChange` | `按需计算` | 按 offline builder 同名定义。 |
| `line_value` | `line` | `按需计算` | 同名同义。 |
| `has_line` | `line` | `按需计算` | 同名同义。 |

### 8.2 文本长度、阈值与句式特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `q_len` | `question` | `按需计算` | 只有文本相关真实消费者存在时计算。 |
| `q_chars` | `question` | `按需计算` | 同上。 |
| `avg_word_len` | `question` | `按需计算` | 同上。 |
| `max_word_len` | `question` | `按需计算` | 同上。 |
| `word_diversity` | `question` | `按需计算` | 同上。 |
| `num_count` | `question` | `按需计算` | 同上。 |
| `has_number` | `question` | `按需计算` | 同上。 |
| `has_year` | `question` | `按需计算` | 同上。 |
| `has_percent` | `question` | `按需计算` | 同上。 |
| `has_dollar` | `question` | `按需计算` | 同上。 |
| `has_million` | `question` | `按需计算` | 同上。 |
| `has_date` | `question` | `按需计算` | 同上。 |
| `starts_will` | `question` | `按需计算` | 同上。 |
| `starts_can` | `question` | `按需计算` | 同上。 |
| `has_by` | `question` | `按需计算` | 同上。 |
| `has_before` | `question` | `按需计算` | 同上。 |
| `has_after` | `question` | `按需计算` | 同上。 |
| `has_above_below` | `question` | `按需计算` | 同上。 |
| `is_binary` | `tokens` 或 `outcomes` | `按需计算` | 若无真实消费者，不应只为了这个特征而保留 tokens 文本解析。 |
| `has_or` | `question` | `按需计算` | 同上。 |
| `has_and` | `question` | `按需计算` | 同上。 |
| `cap_ratio` | `question` | `按需计算` | 同上。 |
| `punct_count` | `question` | `按需计算` | 同上。 |
| `threshold_max` | `question` | `按需计算` | 同上。 |
| `threshold_min` | `question` | `按需计算` | 同上。 |
| `threshold_span` | `threshold_max`, `threshold_min` | `按需计算` | 同上。 |
| `is_player_prop` | `question`, `description` | `按需计算` | 仅在真实消费者存在时保留文本关键词扫描。 |
| `is_team_total` | `question`, `description` | `按需计算` | 同上。 |
| `is_finance_threshold` | `question`, `description` | `按需计算` | 同上。 |
| `is_date_based` | `has_date`, `has_before`, `has_after` | `按需计算` | 同上。 |
| `is_high_ambiguity` | `question`, `description` | `按需计算` | 同上。 |

### 8.3 情绪与方向词特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `strong_pos` | `question`, `description` | `按需计算` | 仅在真实消费者存在时做关键词计数。 |
| `weak_pos` | `question`, `description` | `按需计算` | 同上。 |
| `outcome_pos` | `question`, `description` | `按需计算` | 同上。 |
| `outcome_neg` | `question`, `description` | `按需计算` | 同上。 |
| `sentiment` | `outcome_pos`, `outcome_neg` | `按需计算` | 按 offline builder 同名公式。 |
| `sentiment_abs` | `outcome_pos`, `outcome_neg` | `按需计算` | 同上。 |
| `total_sentiment` | `outcome_pos`, `outcome_neg` | `按需计算` | 同上。 |
| `certainty` | `strong_pos`, `weak_pos` | `按需计算` | 同上。 |
| `pos_ratio` | `outcome_pos`, `outcome_neg` | `按需计算` | 同上。 |
| `neg_ratio` | `outcome_pos`, `outcome_neg` | `按需计算` | 同上。 |
| `sentiment_vol` | `sentiment`, `log_vol` | `按需计算` | 同上。 |
| `sentiment_activity` | `sentiment`, `activity` | `按需计算` | 同上。 |

### 8.4 类别词命中特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `cat_sports` | `question`, `description` | `按需计算` | 只有真实消费者存在时按 offline `CATEGORIES` 词表计算。 |
| `cat_crypto` | `question`, `description` | `按需计算` | 同上。 |
| `cat_politics` | `question`, `description` | `按需计算` | 同上。 |
| `cat_world` | `question`, `description` | `按需计算` | 同上。 |
| `cat_tech` | `question`, `description` | `按需计算` | 同上。 |
| `cat_finance` | `question`, `description` | `按需计算` | 同上。 |
| `cat_entertainment` | `question`, `description` | `按需计算` | 同上。 |
| `cat_sports_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_crypto_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_politics_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_world_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_tech_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_finance_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_entertainment_str` | `question`, `description` | `按需计算` | 同上。 |
| `cat_count` | 类别词命中特征 | `按需计算` | 同名同义。 |
| `primary_cat_str` | 类别词命中特征 | `按需计算` | 同名同义。 |

### 8.5 Duration 与节奏特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `log_duration` | `startDate` / `createdAt`, `endDate` | `按需计算` | 仅在真实消费者存在时按 offline duration 逻辑计算。 |
| `dur_very_short` | duration days | `按需计算` | 同上。 |
| `dur_short` | duration days | `按需计算` | 同上。 |
| `dur_medium` | duration days | `按需计算` | 同上。 |
| `dur_long` | duration days | `按需计算` | 同上。 |
| `dur_very_long` | duration days | `按需计算` | 同上。 |
| `vol_per_day` | `volume`, duration days | `按需计算` | 同上。 |
| `log_vol_per_day` | `volume`, duration days | `按需计算` | 同上。 |

### 8.6 二级交互特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `vol_x_sentiment` | `log_vol`, `sentiment` | `按需计算` | 只在真实消费者存在时保留。 |
| `activity_x_catcount` | `activity`, `cat_count` | `按需计算` | 同上。 |
| `engagement_x_duration` | `engagement`, `log_duration` | `按需计算` | 同上。 |
| `sentiment_x_duration` | `sentiment`, `log_duration` | `按需计算` | 同上。 |
| `vol_x_diversity` | `log_vol`, `word_diversity` | `按需计算` | 同上。 |

### 8.7 文本嵌入特征族

当前 `TEXT_EMBED_DIM = 16`，因此这一族特征是：

- `text_embed_00`
- `text_embed_01`
- `text_embed_02`
- `text_embed_03`
- `text_embed_04`
- `text_embed_05`
- `text_embed_06`
- `text_embed_07`
- `text_embed_08`
- `text_embed_09`
- `text_embed_10`
- `text_embed_11`
- `text_embed_12`
- `text_embed_13`
- `text_embed_14`
- `text_embed_15`

它们统一适用同一条规则：

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `text_embed_00` | `question`, `description` | `默认停算` | 只有真实 `feature_contract` 明确包含时才允许恢复计算。 |
| `text_embed_01` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_02` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_03` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_04` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_05` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_06` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_07` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_08` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_09` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_10` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_11` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_12` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_13` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_14` | `question`, `description` | `默认停算` | 同上。 |
| `text_embed_15` | `question`, `description` | `默认停算` | 同上。 |

## 9. preprocess_features / apply_feature_variant 派生特征对齐矩阵

下表对应 `polymarket_rule_engine/rule_baseline/features/tabular.py` 中的派生特征。

### 9.1 默认 interaction 特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `log_horizon` | `horizon_hours` | `按需计算` | 只在真实消费者存在时计算。 |
| `abs_price_q_gap` | `price`, `q_smooth` | `按需计算` | 复用 offline 同名公式。 |
| `abs_price_center_gap` | `price` | `按需计算` | 同上。 |
| `horizon_q_gap` | `horizon_hours`, `abs_price_q_gap` | `按需计算` | 同上。 |
| `log_horizon_x_liquidity` | 已移除 | 不再对齐 | 该特征已从 offline/live 默认特征流中删除，避免引入 terminal-state leakage。 |
| `spread_over_liquidity` | 已移除 | 不再对齐 | 该特征已从 offline/live 默认特征流中删除，避免引入 terminal-state leakage。 |
| `quote_staleness_x_horizon` | `selected_quote_offset_sec`, `horizon_hours` | `按需计算` | 只有 `selected_quote_offset_sec` 已语义对齐时才允许计算。 |
| `rule_score_x_q_full` | `rule_score`, `q_smooth` | `按需计算` | 同上。 |
| `edge_lower_bound_over_std` | `edge_lower_bound_full`, `edge_std_full` | `按需计算` | 只有真实消费者存在时计算。 |

### 9.2 `market_structure_v2`

该变体已移除，不再是支持的特征变体，也不应再作为 offline/live 对齐目标。

### 9.3 `interaction_plus_textlite` 变体特征

| 特征 | 上游字段 | 对齐标签 | 对齐方式 |
|---|---|---|---|
| `question_length_chars` | `question_market` | `默认停算` | 只有真实消费者存在且文本列保留时才计算。 |
| `description_length_chars` | `description_market` | `默认停算` | 同上。 |
| `text_has_year` | `question_market`, `description_market` | `默认停算` | 同上。 |
| `text_has_date_word` | `question_market`, `description_market` | `默认停算` | 同上。 |
| `text_has_percent` | `question_market`, `description_market` | `默认停算` | 同上。 |
| `text_has_currency` | `question_market`, `description_market` | `默认停算` | 同上。 |
| `text_has_deadline_word` | `question_market`, `description_market` | `默认停算` | 同上。 |

## 10. 明确的裁剪规则

### 10.1 绝对不能先算再删的字段族

下面这些字段族，如果没有真实规则、模型或下游消费者，就不应进入 execution 热路径：

1. 文本长度与关键词特征
2. `cat_*` 与 `cat_*_str` 特征族
3. `text_embed_*` 特征族
4. duration 分桶特征
5. 仅为 `selected_quote_*` 占位统计而存在、但当前无法按 offline 语义真实计算的字段

### 10.2 只要同名就必须同义的字段

下面这些字段一旦保留同名，就必须与 offline 保持同一统计含义：

1. `price`
2. `horizon_hours`
3. `closedTime`
4. `selected_quote_offset_sec`
5. `selected_quote_points_in_window`
6. `selected_quote_left_gap_sec`
7. `selected_quote_right_gap_sec`
8. `selected_quote_local_gap_sec`
9. `stale_quote_flag`
10. `snapshot_quality_score`
11. `p_1h` 到 `p_24h`
12. `delta_p_*`
13. `term_structure_slope`
14. `path_price_*`
15. `price_reversal_flag`
16. `price_acceleration`
17. `closing_drift`

如果 live 无法提供同一语义，就应该：

1. 改名明确声明为 live-only 近似值；或者
2. 直接停止计算，直到真实消费者证明必须保留。

### 10.3 建议的最终阶段化窄表

文档层面的推荐形态是三张窄表：

1. `gamma_context_table`
   - 只保留注解和市场特征构建所需原料。
2. `model_input_table`
   - 只保留真实 `feature_contract.feature_columns` 加最少主键。
3. `execution_decision_table`
   - 只保留选择、提交、监控必需字段。

## 11. 文档级验收标准

这份矩阵真正落地后，代码实现应满足：

1. 同源 Gamma 字段只有一套 canonical 语义。
2. annotation 输入不再是 execution 自定义的一套缩减语义。
3. 任何无消费者的文本、类别、嵌入、duration、quote-window 特征都不会继续在热路径中先算再删。
4. live-only 近似字段不会再用 offline 同名字段掩盖语义差异。
5. execution 的模型前热路径只保留真实 `feature_contract`、规则和下游所需字段。

## 12. 消费者视图摘要

这一节吸收了原先独立的消费者视图文档。

### 12.1 Rule 路径必需字段

Stage1 结构粗筛必须稳定保留：

- `market_id`
- `end_time_utc`
- `remaining_hours`
- `accepting_orders`
- `uma_resolution_statuses`
- `domain`
- `category`
- `market_type`

Stage2 live 价格精筛必须稳定保留：

- `selected_reference_token_id`
- `best_bid`
- `best_ask`
- `last_trade_price`
- `order_price_min_tick_size`
- `remaining_hours`
- `domain`
- `category`
- `market_type`

这些字段属于 rule-path 强保留集，不应在早期迁移批次中处理。

### 12.2 模型路径硬前置字段

即使不一定全部进入最终合同列，下面这些字段通常仍是模型前处理的硬前置：

- `market_id`
- `price`
- `horizon_hours`
- `snapshot_time`
- `q_smooth` 或 `q_full`
- `rule_score`
- `domain`
- `category`
- `market_type`

此外，市场原料字段只有在真实合同需要对应派生特征时才应继续保留，例如：

- `volume`, `liquidity`, `volume24hr`, `volume1wk`
- `best_bid`, `best_ask`, `spread`, `last_trade_price`
- `liquidity_clob`, `liquidity_amm`, `volume24hr_clob`, `volume1wk_clob`
- `line`, `one_*_price_change`, `rewards_max_spread`
- `question`, `description`
- `start_time_utc`, `created_at_utc`, `end_time_utc`

### 12.3 下游执行链路必需字段

Selection 必需字段：

- `direction_model`
- `token_0_id`
- `token_1_id`
- `outcome_0_label`
- `outcome_1_label`
- `price`
- `q_pred`
- `edge_final`
- `f_exec`
- `market_id`
- `snapshot_time`
- `closedTime`
- `source_host`
- `category`

Submission / monitor / lifecycle 必需字段：

- `selected_for_submission`
- `selected_token_id`
- `market_id`
- `stake_usdc`
- `q_pred`
- `price`
- `direction_model`
- `category`
- `first_seen_at_utc`
- `snapshot_time_utc`
- `selected_outcome_label`
- `run_id`
- `batch_id`
- `rule_group_key`
- `rule_leaf_id`
- `position_side`
- `settlement_key`
- `cluster_key`

### 12.4 默认可删的非消费者字段

下面这些字段不是 rule-path 必需项，只能在模型或下游存在真实消费者时继续保留：

- `question`
- `description`
- `game_id`
- `group_item_title`
- `market_maker_address`
- `source_market_updated_at_utc`
- 所有 `cat_*`、`text_embed_*`、文本长度特征、duration 特征、无消费者 quote-window 统计
