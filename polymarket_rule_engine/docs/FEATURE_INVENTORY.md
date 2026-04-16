# Polymarket Snapshot Model �?特征说明文档

> **模型**: AutoGluon 集成模型 (TabularPredictor)  
> **特征语义版本**: `decision_time_v1`  
> **默认特征变体**: `interaction_features`  
> **总特征数**: 生成 ~900+ 列，�?DROP_COLS 过滤�?**实际入模�?200�?00 �?*（取决于 serving features 匹配率）

---

## 目录

1. [特征全景统计](#1-特征全景统计)
2. [A �? 核心快照特征 (Core Snapshot)](#a-�?核心快照特征)
3. [B �? 报价质量特征 (Quote Quality)](#b-�?报价质量特征)
4. [C �? 市场域标注特�?(Domain Annotation)](#c-�?市场域标注特�?
5. [D �? 规则先验特征 (Rule Priors)](#d-�?规则先验特征)
6. [E �? 规则强度特征 (Rule Strength)](#e-�?规则强度特征)
7. [F �? 交互特征 (Interaction Features)](#f-�?交互特征)
8. [G �? 组级质量特征 (Group Quality)](#g-�?组级质量特征)
9. [H �? Term Structure 特征 (Price Path)](#h-�?term-structure-特征)
10. [I �? 文本分析特征 (Text Analysis)](#i-�?文本分析特征)
11. [J �? 情感特征 (Sentiment)](#j-�?情感特征)
12. [K �? 持续时间特征 (Duration)](#k-�?持续时间特征)
13. [L �? 文本嵌入特征 (Text Embedding)](#l-�?文本嵌入特征)
14. [M �? Fine Serving 特征 (Rule-Level)](#m-�?fine-serving-特征)
15. [N �? Group Serving 特征 (Group-Level)](#n-�?group-serving-特征)
16. [O �? 结构键特�?(Structural Keys)](#o-�?结构键特�?
17. [P �? 类别 One-Hot 特征 (Category Flags)](#p-�?类别-one-hot-特征)
18. [被丢弃的�?(DROP_COLS)](#被丢弃的�?
19. [Critical vs Noncritical 特征](#critical-vs-noncritical-特征)
20. [可��特征变�?(Feature Variants)](#可��特征变�?

---

## 1. 特征全景统计

### 按功能分组统�?

| 组别 | 功能 | 特征数量 | 类型 |
|------|------|---------|------|
| **A** | 核心快照 | 3 | Numeric |
| **B** | 报价质量 | 6 | Numeric |
| **C** | 市场域标�?| 7 | Categorical + Numeric |
| **D** | 规则先验 | 14 | Numeric |
| **E** | 规则强度 | 8 | Numeric |
| **F** | 交互特征 | 19 | Numeric |
| **G** | 组级质量 | 12 | Numeric |
| **H** | Term Structure (价格路径) | 15 | Numeric |
| **I** | 文本分析 | 19 | Numeric |
| **J** | 情感分析 | 9 | Numeric |
| **K** | 持续时间 | 7 | Numeric |
| **L** | 文本嵌入 | 16 | Numeric |
| **M** | Fine Serving (规则�? | ~76 | Numeric |
| **N** | Group Serving (组级) | ~50+ 衍生 + 648 历史 | Numeric |
| **O** | 结构�?| 4 | Categorical |
| **P** | 类别 One-Hot | ~15 | Numeric |
| | **合计（典型入模）** | **~220�?00** | |

### 按数据来源统�?

| 来源 | 特征�?| 说明 |
|------|--------|------|
| `snapshots.csv` 直接字段 | ~8 | price, horizon_hours �?|
| `snapshots.csv` �?term structure 衍生 | ~15 | �?horizon 的价格路�?|
| `market_domain_features.csv` 标注 | ~7 | domain, category, market_type |
| `raw_markets_merged.csv` �?market features | ~55 | 文本/情感/时长/嵌入（大量被 DROP�?|
| `trading_rules.csv` 规则先验 | ~14 | edge, direction, q_full |
| `tabular.py` �?interaction 衍生 | ~31 | 交叉特征、质�?gap、信噪比 |
| `fine_serving_features.parquet` | ~76 | 规则×价格×horizon 精细特征 + 规则聚合 |
| `group_serving_features.parquet` | ~50+ 衍生 + 648 历史 | 组级历史质量聚合 |

---

## A �? 核心快照特征

> **来源**: `snapshots.csv` �?`build_snapshot_base()`
> **含义**: 决策时刻的最基本信息

| 特征 | 类型 | 公式 / 含义 | 取��范�?|
|------|------|-------------|---------|
| `price` | float | 决策时刻�?primary outcome 报价 | [0.01, 0.99] |
| `horizon_hours` | float | 距离市场结算的小时数 | {1, 2, 4, 6, 12, 24} |
| `log_horizon` | float | `log(1 + horizon_hours)` �?对数化的 horizon | [0.69, 3.22] |

**Critical**: `price`, `horizon_hours` �?**必须存在** �?critical 特征，缺失则拒绝预测�?

---

## B �? 报价质量特征

> **来源**: `build_snapshots.py` �?CLOB 价格历史 API
> **含义**: 报价本身的可信程�?

| 特征 | 类型 | 公式 / 含义 |
|------|------|-------------|
| `selected_quote_offset_sec` | float | 报价时间与目�?snapshot 时间的偏移（秒）。越小越好��?|
| `selected_quote_points_in_window` | float | ±5min 窗口内的报价数据点数量��越多表示流动��越好��?|
| `selected_quote_left_gap_sec` | float | 报价点左侧（更早时间）的间隔秒数 |
| `selected_quote_right_gap_sec` | float | 报价点右侧（更晚时间）的间隔秒数 |
| `selected_quote_local_gap_sec` | float | 报价点附近的朢�大间隔秒数��过大（>900s）标记为 stale |
| `snapshot_quality_score` | float | 综合质量评分�?~1），结合 offset、gap、staleness 计算 |

**Critical**: `selected_quote_offset_sec`, `selected_quote_points_in_window`, `snapshot_quality_score`�?

---

## C �? 市场域标注特�?

> **来源**: `market_domain_features.csv` �?`build_market_annotations()`、`normalize_market_annotations()`
> **含义**: 市场的分类分组信�?

| 特征 | 类型 | 含义 | 示例�?|
|------|------|------|--------|
| `domain` | categorical | 市场来源域名（归丢�化后�?| `espn.nfl.moneyline`, `gol.gg`, `UNKNOWN` |
| `category` | categorical | 大类 | `SPORTS`, `CRYPTO`, `FINANCE`, `POLITICS`, `OTHER` |
| `market_type` | categorical | 市场结构类型 | `moneyline`, `spread`, `total`, `prop`, `generic` |
| `group_key` | categorical | `domain\|category\|market_type` 的拼接键 | `espn.nfl.moneyline\|SPORTS\|moneyline` |
| `domain_is_unknown` | float | domain == "UNKNOWN" 则为 1.0 | {0.0, 1.0} |
| `domain_category_key` | categorical | `domain\|category` | `espn.nfl\|SPORTS` |
| `category_market_type_key` | categorical | `category\|market_type` | `SPORTS\|moneyline` |

**Critical**: `domain`, `category`, `market_type`, `group_key`�?

---

## D �? 规则先验特征

> **来源**: `trading_rules.csv` �?`train_rules_naive_output_rule.py`
> **含义**: 该快照匹配到的规则桶的历史频率统�?

| 特征 | 类型 | 公式 / 含义 |
|------|------|-------------|
| `q_full` | float | 桶内价格均��?�?市场隐含概率 |
| `p_full` | float | 桶内价格中位�?|
| `edge_full` | float | `win_rate - q_full` �?原始 edge |
| `edge_std_full` | float | edge 的标准差 |
| `edge_lower_bound_full` | float | Wilson 置信区间下界 �?edge 的保守估�?|
| `n_full` | float | 桶内样本量（快照条数�?|
| `direction` | float | +1（买�?Yes）或 -1（买�?No�?|
| `rule_score` | float | 规则综合评分（结�?edge、confidence、支撑量�?|
| `h_min` / `h_max` | float | 规则�?horizon 范围上下�?|
| `horizon_hours_rule` | float | 规则的目�?horizon |
| `group_unique_markets` | float | 组内的唯丢�市场�?|
| `group_snapshot_rows` | float | 组内的快照��行�?|
| `global_total_unique_markets` | float | 全局唯一市场�?|
| `global_total_snapshot_rows` | float | 全局快照总行�?|

**Critical**: `q_full`, `rule_score`, `direction`�?

---

## E �? 规则强度特征

> **来源**: `tabular.py::apply_feature_variant()` 衍生
> **含义**: 衡量规则的可靠程度和信号质量

| 特征 | 类型 | 公式 | 含义 |
|------|------|------|------|
| `rule_edge_buffer` | float | `edge_full - edge_lower_bound_full` | 置信区间宽度 �?越小表示越确�?|
| `rule_confidence_ratio` | float | `edge_lower_bound / (abs(edge_full) + ε)` | 下界�?edge 的比�?�?越接�?1 越可�?|
| `rule_support_log1p` | float | `log(1 + n_full)` | 规则样本充裕度（对数化） |
| `rule_snapshot_support_log1p` | float | `log(1 + group_snapshot_rows)` | 组快照充裕度 |
| `rule_price_center` | float | `(p_full + price) / 2` | 规则价格中心 |
| `rule_price_width` | float | `price_max - price_min` | 规则价格 bin 宽度 |
| `rule_horizon_center` | float | `(h_min + h_max) / 2` | 规则 horizon 中心 |
| `rule_horizon_width` | float | `h_max - h_min` | 规则 horizon 范围宽度 |

---

## F �? 交互特征

> **来源**: `tabular.py::apply_feature_variant("interaction_features")`
> **含义**: 不同维度变量之间的交叉信�?

### F.1 价格-概率 Gap 特征

| 特征 | 公式 | 含义 |
|------|------|------|
| `abs_price_q_gap` | `abs(price - q_smooth)` | 当前价格与规则隐含概率的绝对偏差 |
| `abs_price_center_gap` | `abs(price - 0.5)` | 价格偏离中��的程度 �?0 表示完全不确�?|
| `horizon_q_gap` | `horizon × abs_price_q_gap` | 时间加权的价格偏�?|

### F.2 流动�?& 报价交互

| 特征 | 公式 | 含义 |
|------|------|------|
| `quote_staleness_x_horizon` | `offset_sec × (horizon + 1)` | 报价陈旧 × 时间窗口 �?长horizon容忍更多staleness |
| `rule_score_x_q_full` | `rule_score × q_smooth` | 规则评分与概率的交互 |

> Note: `log_horizon_x_liquidity` �?`spread_over_liquidity` 已从默认特征流中移除，并加入 `DROP_COLS`，因为它们直接依赖于 raw market terminal state �?liquidity / spread 列��?

### F.3 信噪比特�?

| 特征 | 公式 | 含义 |
|------|------|------|
| `edge_lower_bound_over_std` | `edge_lower / edge_std` | edge 下界 / 波动�?�?Sharpe-like |
| `rule_edge_over_std` | `edge_full / (edge_std + ε)` | 原始 edge 的信噪比 |
| `rule_edge_over_logloss` | `edge_full / (group_logloss + ε)` | edge 相对于组�?logloss 的规�?|
| `rule_edge_over_brier` | `edge_full / (group_brier + ε)` | edge 相对于组�?brier 的规�?|
| `group_rule_score_x_edge_lower` | `rule_score × edge_lower` | 复合: 评分 × 保守 edge |

### F.4 组共�?× 质量 Gap

| 特征 | 公式 | 含义 |
|------|------|------|
| `group_share_x_logloss_gap` | `market_share × logloss_gap` | 组的全局份额与质量偏差的交互 |
| `group_share_x_brier_gap` | `snapshot_share × brier_gap` | 同上，用 brier 度量 |

### F.5 匹配状��?

| 特征 | 类型 | 含义 |
|------|------|------|
| `group_match_found` | float | 是否成功匹配�?group serving 特征 |
| `fine_match_found` | float | 是否成功匹配�?fine (规则�? serving 特征 |
| `used_group_fallback_only` | float | 仅匹配到 group 级别（无 fine 级别匹配）→ 特征较粗 |

---

## G �? 组级质量特征

> **来源**: `tabular.py` �?`serving.py`
> **含义**: 当前市场扢��?(domain, category, market_type) 组的历史预测质量

| 特征 | 公式 | 含义 |
|------|------|------|
| `group_market_share_global` | `group_unique_markets / global_total` | 该组在全屢�市场中的占比 |
| `group_snapshot_share_global` | `group_snapshots / global_total` | 该组在全屢�快照中的占比 |
| `group_median_logloss` | 组内 logloss 中位�?| 组的历史预测精度 (logloss) |
| `group_median_brier` | 组内 brier 中位�?| 组的历史预测精度 (brier) |
| `global_group_logloss_q25` | logloss 的全屢� 25 百分�?| 全局质量基线 |
| `global_group_brier_q25` | brier 的全屢� 25 百分�?| 全局质量基线 |
| `group_logloss_gap_q25` | `group_logloss - global_q25` | 该组相对全局�?logloss 偏差 |
| `group_brier_gap_q25` | `group_brier - global_q25` | 该组相对全局�?brier 偏差 |
| `group_quality_pass_q25` | `gap >= 0` �?1.0 | 该组质量达标（优于全屢� q25�?|
| `group_quality_fail_q25` | `gap < 0` �?1.0 | 该组质量不达�?|
| `group_market_density` | `unique_markets / n_full` | 每个样本代表多少个不同市�?|
| `group_snapshot_density` | `snapshot_rows / unique_markets` | 每个市场的平均快照数 |

---

## H �? Term Structure 特征

> **来源**: `datasets/snapshots.py::add_term_structure_features()`
> **含义**: 同一市场在不�?horizon 下的价格变化路径（仅使用当前 horizon 及之前可观察的价格）

### H.1 历史价格观察�?

| 特征 | 含义 | 注意事项 |
|------|------|---------|
| `p_1h` | 结算�?1 小时的价�?| 仅在 horizon >= 1 时可�?|
| `p_2h` | 结算�?2 小时的价�?| 仅在 horizon >= 2 时可�?|
| `p_4h` | 结算�?4 小时的价�?| 仅在 horizon >= 4 时可�?|
| `p_12h` | 结算�?12 小时的价�?| 仅在 horizon >= 12 时可�?|
| `p_24h` | 结算�?24 小时的价�?| 仅在 horizon >= 24 时可�?|

**防泄漏机�?*: 对于当前 horizon 比某个观察点更远的情�?�?horizon=4 �?p_1h �?p_2h 对未来有信息泄漏)，代码会将这些未来不可见的列置为 NaN�?

### H.2 价格变化差分

| 特征 | 公式 | 含义 |
|------|------|------|
| `delta_p_1_2` | `p_1h - p_2h` | 1h �?2h 之间的价格变�?|
| `delta_p_2_4` | `p_2h - p_4h` | 2h �?4h 之间的价格变�?|
| `delta_p_4_12` | `p_4h - p_12h` | 4h �?12h 之间的价格变�?|
| `delta_p_12_24` | `p_12h - p_24h` | 12h �?24h 之间的价格变�?|

### H.3 路径统计

| 特征 | 公式 | 含义 |
|------|------|------|
| `term_structure_slope` | `p_1h - p_24h` | 整条价格曲线的斜�?�?正��表示价格趋势向�?|
| `path_price_mean` | 扢�有可�?horizon 价格的均�?| 路径平均水平 |
| `path_price_std` | 扢�有可�?horizon 价格的标准差 | 路径波动程度 |
| `path_price_min` / `max` | 路径朢��?朢�高价�?| 价格极端�?|
| `path_price_range` | `max - min` | 路径振幅 |

### H.4 动��特�?

| 特征 | 公式 | 含义 |
|------|------|------|
| `price_reversal_flag` | `short_leg × long_leg < 0` 则为 1 | 短期趋势与长期趋势方向相�?|
| `price_acceleration` | `(p_1h - p_2h) - (p_12h - p_24h)` | 短期变化速度 vs 长期变化速度 |
| `closing_drift` | `price - p_24h` | 当前价格相对 24h 前的偏移 |

---

## I �? 文本分析特征

> **来源**: `market_feature_builders.py::extract_market_features()`
> **含义**: 分析市场问题文本 (question) 的结构和内容

### I.1 文本结构

| 特征 | 含义 |
|------|------|
| `q_len` | 问题的单词数�?(capped at 50) |
| `q_chars` | 问题的字符数�?(capped at 300) |
| `avg_word_len` | 平均单词长度 |
| `max_word_len` | 朢�长单词长�?|
| `word_diversity` | `unique_words / total_words` �?词汇丰富�?|
| `num_count` | 数字字符数量 |
| `punct_count` | 标点符号 (`?!.,`) 数量 |

### I.2 内容标记

| 特征 | 含义 | 为什么有�?| DROP? |
|------|------|-----------|-------|
| `has_number` | 包含数字 �?1 | 数��型问题通常有不同的 edge 模式 | |
| `has_year` | 包含年份 (�?"2025") �?1 | 时间锚定的市�?| |
| `has_date` | 包含月份�?�?1 | 日期相关市场 | |
| `has_dollar` | 包含 "$" �?"dollar" �?1 | 金融类问题标�?| |
| `has_by` | 包含 " by " �?1 | 截止日期暗示 | |
| `has_above_below` | 包含 "above"/"below" �?1 | 阈��型问题 | |
| `starts_will` | �?"will" 弢��?�?1 | 二元�?否问�?| |
| `starts_can` | �?"can" 弢��?�?1 | 能力型问�?| |
| `has_or` / `has_and` | 包含 "or"/"and" �?1 | 复合条件 | |
| `is_player_prop` | 包含球员 prop 关键�?�?1 | 体育投注类型 | |
| `is_team_total` | 包含球队总分关键�?�?1 | 体育投注类型 | |
| `is_finance_threshold` | 包含金融阈��关键词 �?1 | 金融衍生型预测市�?| |
| `is_high_ambiguity` | 包含高模糊��关键词 �?1 | 结果不明确的市场 | |
| `has_percent` | 包含 "%" �?"percent" �?1 | 百分比类问题 | �?DROP |
| `has_million` | 包含 "million" �?1 | 大额数��问�?| �?DROP |
| `has_before` / `has_after` | 包含 "before"/"after" �?1 | 时间约束 | �?DROP |
| `is_binary` | 问题为二元结�?�?1 | 箢��?yes/no 问题 | �?DROP |
| `cap_ratio` | 大写字母占比 | 文本风格 | �?DROP |
| `is_date_based` | 基于日期的问�?�?1 | 日期驱动型市�?| �?DROP |

### I.3 数��阈�?

| 特征 | 含义 |
|------|------|
| `threshold_max` | 问题中的朢�大数�?(regex 提取) |
| `threshold_min` | 问题中的朢�小数�?|
| `threshold_span` | `max - min` �?数��范�?|
| `has_line` | 是否�?line �?(盘口�? |

---

## J �? 情感特征

> **来源**: `market_feature_builders.py` + `feature_util.py` 词典
> **含义**: 基于关键词的情感倾向分析

| 特征 | 公式 | 含义 |
|------|------|------|
| `weak_pos` | 弱正面词匹配�?(max 5) | "可能", "也许" 等不确定�?|
| `outcome_pos` | 正面结果词匹配数 (max 5) | "�?, "上涨", "超过" �?|
| `outcome_neg` | 负面结果词匹配数 (max 5) | "�?, "下跌", "低于" �?|
| `sentiment` | `(pos - neg) / max(pos + neg, 1)` | 凢�情感倾向 [-1, 1] |
| `sentiment_abs` | `abs(pos - neg) / max(pos + neg, 1)` | 情感强度 [0, 1] |
| `total_sentiment` | `min(pos + neg, 10)` | 情感词��数 |
| `certainty` | `(strong - weak) / max(strong + weak, 1)` | 确定性程�?|
| `pos_ratio` | `pos / max(pos + neg, 1)` | 正面情感占比 |
| `neg_ratio` | `neg / max(pos + neg, 1)` | 负面情感占比 |

> 注意: `strong_pos`, `sentiment_vol`, `vol_x_sentiment` �?DROP_COLS 中被丢弃�?
> 另有 `sentiment_activity` 特征�?`market_feature_builders` 生成但��常�?DROP�?

---

## K �? 持续时间特征

> **来源**: `market_feature_builders.py`  
> **含义**: 市场从创建到结算的生命周�?

| 特征 | 公式 / 含义 |
|------|-------------|
| `log_duration` | `log(1 + days)` �?市场持续天数的对�?|
| `dur_very_short` | 持续 �?3�?�?1 |
| `dur_short` | 持续 3~7�?�?1 |
| `dur_medium` | 持续 7~30�?�?1 |
| `dur_long` | 持续 30~90�?�?1 |
| `market_duration_hours` | 市场持续小时�?(�?startDate �?closedTime) |
| `engagement_x_duration` | `engagement × log_duration` �?参与度与时长的交�?|

> 注意: `dur_very_long`, `vol_per_day`, `log_vol_per_day` �?DROP_COLS 中被丢弃。`activity_x_catcount` 也被丢弃�?

---

## L �? 文本嵌入特征

> **来源**: `market_feature_builders.py::_hash_text_embedding()`
> **含义**: 基于 SHA-1 哈希的轻量级文本嵌入，将问题+描述映射到固定维�?

| 特征 | 维度 | 含义 |
|------|------|------|
| `text_embed_00` ~ `text_embed_15` | 16 �?| 每个 token 通过 SHA-1 映射�?16 �?bucket，��为 ±1 累加�?L2 归一�?|

**算法**:
```
for token in text.split():
    hash = SHA1(token)
    index = hash[0:4] % 16
    sign = +1 if hash[4] is even else -1
    bucket[index] += sign
bucket = bucket / L2_norm(bucket)
```

这是丢��?**SimHash / 随机投影** 抢�术，不依赖预训练模型，能捕获词汇级别的语义相似����?

---

## M �? Fine Serving 特征

> **来源**: `fine_serving_features.parquet` �?`attach_serving_features()`
> **含义**: 精确�?`(group_key × price_bin × horizon)` 粒度的规则特�?
> **前缀**: `fine_feature_`
> **总列�?*: ~76 列（�?4 粒度 × 8 聚合的规则汇总特征）

### M.1 规则几何

| 特征 | 含义 |
|------|------|
| `fine_feature_rule_price_center` | 规则价格 bin 中心 |
| `fine_feature_rule_price_width` | 规则价格 bin 宽度 |
| `fine_feature_rule_horizon_center` | 规则 horizon 中心 |
| `fine_feature_rule_horizon_width` | 规则 horizon 范围宽度 |

### M.2 规则不确定��?

| 特征 | 含义 |
|------|------|
| `fine_feature_rule_edge_buffer` | edge - edge_lower_bound |
| `fine_feature_rule_confidence_ratio` | edge_lower / edge_std |
| `fine_feature_rule_support_log1p` | log(1 + 桶内样本�? |
| `fine_feature_rule_snapshot_support_log1p` | log(1 + 组快照量) |

### M.3 Edge vs 历史偏差（多层级�?

| 特征 | 含义 |
|------|------|
| `fine_feature_rule_edge_minus_domain_expanding_bias` | edge - domain 级偏�?|
| `fine_feature_rule_edge_minus_category_expanding_bias` | edge - category 级偏�?|
| `fine_feature_rule_edge_minus_market_type_expanding_bias` | edge - market_type 级偏�?|
| `fine_feature_rule_edge_minus_domain_x_category_expanding_bias` | edge - domain×category 偏差 |
| `fine_feature_rule_edge_minus_domain_x_market_type_expanding_bias` | edge - domain×market_type 偏差 |
| `fine_feature_rule_edge_minus_category_x_market_type_expanding_bias` | edge - category×market_type 偏差 |
| `fine_feature_rule_edge_minus_full_group_expanding_bias` | edge - 完整组的扩展偏差 |
| `fine_feature_rule_edge_minus_recent_90days_bias` | edge - ��� 90 ��ƫ�� |
| `fine_feature_rule_edge_over_full_group_logloss` | edge / �?logloss �?质量调整 edge |

### M.4 Score vs 历史 Logloss（多层级�?

| 特征 | 含义 |
|------|------|
| `fine_feature_rule_score_minus_full_group_expanding_logloss` | score - 组扩�?logloss |
| `fine_feature_rule_score_minus_recent_90days_logloss` | score - ��� 90 �� logloss |
| `fine_feature_rule_score_minus_domain_expanding_logloss` | score - domain �?logloss |
| `fine_feature_rule_score_minus_category_expanding_logloss` | score - category �?logloss |
| `fine_feature_rule_score_minus_market_type_expanding_logloss` | score - market_type �?logloss |
| `fine_feature_rule_score_minus_domain_x_category_expanding_logloss` | score - domain×category logloss |
| `fine_feature_rule_score_minus_domain_x_market_type_expanding_logloss` | score - domain×market_type logloss |
| `fine_feature_rule_score_minus_category_x_market_type_expanding_logloss` | score - category×market_type logloss |

### M.5 历史交互

| 特征 | 含义 |
|------|------|
| `fine_feature_hist_price_x_full_group_expanding_bias` | price_center × 组扩展偏�?|
| `fine_feature_hist_price_x_full_group_recent_90days_bias` | price_center �� ��� 90 ��ƫ�� |
| `fine_feature_hist_price_x_full_group_expanding_logloss` | price_center × 组扩�?logloss |
| `fine_feature_tail_risk_x_price` | price_center × logloss 尾部风险 |
| `fine_feature_price_x_full_group_expanding_abs_bias_tail_spread` | price × 偏差尾部散度 |

### M.6 规则聚合特征�? 粒度 × 8 指标 = 32 列）

> **新增**: 对同丢�粒度下所有规则的 edge/score/n 进行聚合，提供上下文丰富�?

**4 个聚合粒�?*:

| 粒度前缀 | 分组维度 |
|----------|---------|
| `rule_full_group_key_` | domain × category × market_type |
| `rule_domain_` | domain |
| `rule_category_` | category |
| `rule_market_type_` | market_type |

**每个粒度�?8 个聚合指�?*:

| 指标后缀 | 含义 |
|----------|------|
| `matched_rule_count` | 该粒度下匹配的规则条�?|
| `max_edge_full` | 该粒度下朢��?edge |
| `max_edge_lower_bound_full` | 该粒度下朢��?edge lower bound |
| `max_rule_score` | 该粒度下朢�高评�?|
| `mean_edge_full` | 该粒度下平均 edge |
| `mean_edge_lower_bound_full` | 该粒度下平均 edge lower bound |
| `mean_rule_score` | 该粒度下平均评分 |
| `sum_n_full` | 该粒度下样本量��和 |

**命名格式**: `fine_feature_rule_{grain}_{metric}`

**示例**:
- `fine_feature_rule_full_group_key_matched_rule_count` �?�?group_key 下有多少条规�?
- `fine_feature_rule_domain_max_edge_full` �?�?domain 下最�?edge
- `fine_feature_rule_category_mean_rule_score` �?�?category 下平均评�?
- `fine_feature_rule_market_type_sum_n_full` �?�?market_type 下样本量总和

---

## N �? Group Serving 特征

> **来源**: `group_serving_features.parquet` �?`build_group_serving_features()`
> **含义**: `(domain, category, market_type)` 组级聚合的历史质量指�?
> **前缀**: `group_feature_`

### N.1 历史质量指标�? 层级 × 2 窗口 × 7 指标族）

组级特征�?*历史特征的查表版�?*，��过 `summarize_history_features()` 计算后存�?parquet 供查表使用��?

**8 个层�?*:

| 层级前缀 | 分组维度 |
|----------|---------|
| `global` | 全局（不分组�?|
| `domain` | �?domain |
| `category` | �?category |
| `market_type` | �?market_type |
| `domain_x_category` | �?domain × category |
| `domain_x_market_type` | �?domain × market_type |
| `category_x_market_type` | �?category × market_type |
| `full_group` | �?domain × category × market_type |

**2 个时间窗�?*:

| 窗口后缀 | 含义 |
|----------|------|
| `expanding` | 全量历史数据 |
| `recent_90days` | �� `closedTime` ��ÿ�� level_key ��ȡ���� 90 �� |

**每个窗口�?7 个指标族�?7 个具体指标）**:

| 指标�?| 具体指标 | 含义 |
|--------|---------|------|
| **计数** | `snapshot_count`, `market_count` | 样本�?|
| **偏差** | `bias_mean`, `bias_std`, `bias_min`, `bias_max`, `bias_p50` | `y - price` 的分�?|
| **绝对偏差** | `abs_bias_mean`, `abs_bias_p25/p50/p75/p90`, `abs_bias_max` | 偏差绝对值的分位�?|
| **Brier** | `brier_mean`, `brier_p25/p50/p75/p90`, `brier_std`, `brier_max` | Brier Score 分布 |
| **Logloss** | `logloss_mean`, `logloss_p25/p50/p75/p90`, `logloss_std`, `logloss_max` | Log Loss 分布 |

**命名格式**: `group_feature_{level}_{window}_{metric}`

**示例**:
- `group_feature_full_group_expanding_bias_mean` �?完整组全量历史偏差均�?
- `group_feature_domain_recent_90days_logloss_p90` �� ͬ domain ��� 90 �� logloss �� 90 �ٷ�λ
- `group_feature_global_expanding_brier_std` �?全局 brier 标准�?

**理论总量**: 8 层级 × 2 窗口 × 27 指标 = **432 �?*（实际取决于数据可用性和 defaults fallback�?

---

## O �? 结构键特�?

> **来源**: `tabular.py::apply_feature_variant()`
> **含义**: 用于层级化分析的复合分类�?

| 特征 | 类型 | 含义 |
|------|------|------|
| `group_key` | categorical | `domain\|category\|market_type` �?核心分组�?|
| `domain_category_key` | categorical | `domain\|category` |
| `domain_market_type_key` | categorical | `domain\|market_type` |
| `category_market_type_key` | categorical | `category\|market_type` |

---

## P �? 类别 One-Hot 特征

> **来源**: `market_feature_builders.py::extract_market_features()`
> **含义**: 基于关键词匹配的市场类别标记

### P.1 类别标志 (cat\_{category})

对以下每个类别生成一�?0/1 标志和一个关键词命中计数�?

| 类别 | 关键词数 | 示例关键�?|
|------|---------|-----------|
| `sports` | 24 | win, beat, game, match, nba, nfl, mlb, ufc, boxing |
| `crypto` | 20 | bitcoin, btc, eth, ethereum, solana, defi, nft |
| `politics` | 18 | trump, biden, election, vote, president, congress |
| `world` | 17 | war, russia, ukraine, china, israel, nato, military |
| `tech` | 19 | ai, openai, gpt, apple, google, meta, nvidia, spacex |
| `finance` | 17 | stock, market, fed, rate, inflation, gdp, recession |
| `entertainment` | 12 | oscar, grammy, emmy, movie, album, netflix |

每个类别生成�?
- `cat_{category}`: 0/1 标志（命中任意关键词 �?1�?
- `cat_{category}_str`: 关键词命中数�?

额外生成�?
- `cat_count`: 总命中类别数
- `primary_cat_str`: 朢�高命中的类别名称

> 注意: `cat_entertainment_str` �?`cat_finance` �?DROP_COLS 中被丢弃�?

---

## 被丢弃的�?

> **定义�?*: `train_snapshot_model.py::DROP_COLS`（共 ~130 列）

### 标识�?& 时间戳（非特征）

`y`（标签），`market_id`, `closedTime`, `snapshot_time`, `snapshot_date`, `scheduled_end`, `snapshot_target_ts`, `selected_quote_ts`, `batch_id`, `batch_fetched_at`, `batch_window_start`, `batch_window_end`, `dataset_split`

### 原文市场文本

`question`, `description`, `source_url`, `source_host`, `question_market`, `description_market`

### Token & 标识

`primary_token_id`, `secondary_token_id`, `primary_outcome`, `secondary_outcome`, `selected_quote_side`, `groupItemTitle`, `gameId`, `marketMakerAddress`, `leaf_id`

### 与衍生特征重复的原始字段

| 被丢弃的原始�?| 对应的保留衍生特�?|
|---------------|-------------------|
| `volume`, `log_vol`, `log_v24`, `log_v1w` | �?market features 替代 |
| `vol_ratio_24`, `vol_ratio_1w`, `daily_weekly` | 被交互特征替�?|
| `vol_tier_ultra`, `vol_tier_high`, `vol_tier_med`, `vol_tier_low` | �?`log_vol` 系列替代 |
| `activity`, `engagement`, `momentum` | �?`engagement_x_duration` 等交互替�?|
| `liquidity`, `log_liq`, `liq_ratio` | 不再进入默认入模路径 |
| `liquidityAmm`, `liquidityClob`, `log_liquidity_clob`, `log_liquidity_amm` | 已同步从默认入模路径移除 |
| `clob_share_liquidity`, `clob_share_volume24`, `clob_share_volume1w` | 已不再作为默认间接字段依�?|
| `bestBid`, `bestAsk`, `spread`, `lastTradePrice` | 被衍生特征替�?|
| `best_bid`, `best_ask`, `mid_price`, `quoted_spread`, `quoted_spread_pct`, `book_imbalance` | �?market structure 变体替代 |
| `price_change_1h`, `price_change_1d`, `price_change_1w`, `price_change_accel` | �?term structure 特征替代 |
| `line_value` | 保留 `has_line` 标志 |
| `volume24hr`, `volume1wk`, `volume24hrClob`, `volume1wkClob` | 被交互特征替�?|
| `oneHourPriceChange`, `oneDayPriceChange`, `oneWeekPriceChange` | �?term structure 替代 |
| `vol_per_day`, `log_vol_per_day` | 被持续时间特征替�?|
| `vol_x_sentiment`, `activity_x_catcount` | 冗余交互 |
| `sentiment_vol` | 冗余情感特征 |

### 训练辅助字段

`trade_value_true`, `expected_pnl_target`, `expected_roi_target`, `residual_q_target`, `winning_outcome_index`, `winning_outcome_label`

### 质量标志位（已��过 quality_pass 预过滤）

`quality_pass`, `price_in_range_flag`, `duration_is_negative_flag`, `duration_below_min_horizon_flag`, `delta_hours_exceeded_flag`

### 冗余标注 / 元数�?

`domain_parsed`, `domain_parsed_market`, `domain_market`, `domain_domain`, `market_type_market`, `market_type_domain`, `sub_domain`, `sub_domain_market`, `outcome_pattern`, `outcome_pattern_market`, `source_host_market`, `source_url_market`, `category_raw_market`, `category_parsed_market`, `category_override_flag_market`, `category_source`, `startDate`, `endDate`, `startDate_market`, `endDate_market`, `closedTime_market`, `groupItemTitle_market`, `gameId_market`, `marketMakerAddress_market`

### 冗余 / 低信息量特征

`r_std`, `e_sample`, `delta_hours`, `delta_hours_bucket`, `price_bin`, `horizon_bin`, `negRisk`, `is_date_based`, `has_percent`, `has_million`, `has_before`, `has_after`, `is_binary`, `cap_ratio`, `strong_pos`, `cat_finance`, `cat_entertainment_str`, `dur_very_long`

---

## Critical vs Noncritical 特征

### Critical 特征（缺失则拒绝预测�?

```python
_DEFAULT_CRITICAL_COLUMNS = (
    "price",
    "horizon_hours",
    "selected_quote_offset_sec",
    "selected_quote_points_in_window",
    "snapshot_quality_score",
    "domain",
    "category",
    "market_type",
    "q_full",
    "rule_score",
    "direction",
    "group_key",
)
```

**含义**: �?12 个特征是模型预测的最低要求��在线推理时如果任意丢�个缺失，系统将拒绝对该快照出预测值��?

### Noncritical 特征（缺失则用默认��填充）

扢�有非 critical 的特征均�?noncritical。缺失时�?
- **Numeric**: 填充 `0.0`
- **Categorical**: 填充 `"UNKNOWN"`
- **Fine serving 特征**: �?fine 匹配失败，fallback �?group 级特征（通过 `FINE_DEFAULT_AGGREGATIONS` 聚合�?

### Online 不可用特�?

```python
ONLINE_UNAVAILABLE_FEATURES = frozenset({"delta_hours_bucket"})
```

`delta_hours_bucket` 依赖�?`closedTime`（实际结算时间），在线环境中市场尚未结算故不可用。该列在 `online_feature_columns()` 过滤器中被排除，不会进入在线模型�?

---

## 可��特征变�?

> **定义�?*: `features/tabular.py::apply_feature_variant()`
> **默认变体**: `interaction_features`（pipeline 默认使用�?

除默认的 `interaction_features` 变体外，还支持以下实验��变体��这些变体在默认交互特征之上新增额外列��?

### Removed Variant: `market_structure_v2`

`market_structure_v2` 已删除，不再是可选特征变体��当前支持的变体仅剩 `interaction_features`�?`interaction_plus_textlite` �?`baseline`�?

### Variant: `interaction_plus_textlite`

增加基于市场文本�?7 个轻量特征（不依赖预训练模型）：

| 特征 | 含义 |
|------|------|
| `question_length_chars` | 问题文本的字符数 |
| `description_length_chars` | 描述文本的字符数 |
| `text_has_year` | 文本中是否包含年�?(�?"2025") |
| `text_has_date_word` | 文本中是否包含日期词 (january, today, tomorrow �? |
| `text_has_percent` | 文本中是否包�?"%"/"percent" |
| `text_has_currency` | 文本中是否包�?"$"/"usd"/"dollar"/"million"/"billion" |
| `text_has_deadline_word` | 文本中是否包含截止相关词 (before, after, by, deadline �? |

### Variant: `baseline`

不添加任何衍生特征，仅使用裸特征。用于消融实验��?

---

*基于 `features/`, `training/`, `datasets/` 模块源码静��分析生成��特征语义版�? `decision_time_v1`*




