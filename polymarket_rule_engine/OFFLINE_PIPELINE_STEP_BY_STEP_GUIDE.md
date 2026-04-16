# Polymarket Rule Engine �?Offline Pipeline 详细步骤指南

> **入口脚本**: `rule_baseline/workflow/run_pipeline.py --artifact-mode offline`  
> **运行方式**: `run_pipeline.py` 以子进程方式**串行调度 11 个步�?*，每步调用一个独�?CLI 脚本�? 
> **跳过机制**: 可��过 `--skip-fetch`, `--skip-annotations`, `--skip-snapshots`, `--skip-analysis`, `--skip-backtest`, `--skip-baselines` 参数跳过特定阶段�?

---

## 目录

1. [端到端流程��览](#1-端到端流程��览)
2. [Step 1: 拉取原始市场数据 (fetch_raw_events)](#step-1-拉取原始市场数据)
3. [Step 2: 构建市场标注 (build_market_annotations)](#step-2-构建市场标注)
4. [Step 3: 构建价格快照 (build_snapshots)](#step-3-构建价格快照)
5. [Step 4: 训练朴素规则 (train_rules_naive_output_rule)](#step-4-训练朴素规则)
6. [Step 5: 训练快照模型 (train_snapshot_model)](#step-5-训练快照模型)
7. [Step 6: GroupKey 验证报告 (build_groupkey_validation_reports)](#step-6-groupkey-验证报告)
8. [Step 7: 模型校准分析 (analyze_q_model_calibration)](#step-7-模型校准分析)
9. [Step 8: Alpha 象限分析 (analyze_alpha_quadrant)](#step-8-alpha-象限分析)
10. [Step 9: 规则 Alpha 象限分析 (analyze_rules_alpha_quadrant)](#step-9-规则-alpha-象限分析)
11. [Step 10: 回测执行丢�致��?(backtest_execution_parity)](#step-10-回测执行丢�致��?
12. [Step 11: 基线族对�?(compare_baseline_families)](#step-11-基线族对�?
13. [数据流与产物清单](#数据流与产物清单)
14. [核心常量参��表](#核心常量参��表)

---

## 1. 端到端流程��览

```
┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
�?                   run_pipeline.py (artifact-mode=offline)               �?
�?                                                                         �?
�? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢� 数据采集阶段 (Data Collection) ┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�? �? Step 1  fetch_raw_events.py         �?--skip-fetch 可跳�?     �?   �?
�? �?    �?                                                           �?   �?
�? �?    �?                                                           �?   �?
�? �? Step 2  build_market_annotations.py �?--skip-annotations 可跳过│    �?
�? �?    �?                                                           �?   �?
�? �?    �?                                                           �?   �?
�? �? Step 3  build_snapshots.py          �?--skip-snapshots 可跳�? �?   �?
�? └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�?                             �?                                          �?
�?                             �?                                          �?
�? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢� 训练阶段 (Training) ┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�? �? Step 4  train_rules_naive_output_rule.py    (必跑)              �?   �?
�? �?    �?                                                           �?   �?
�? �?    �?                                                           �?   �?
�? �? Step 5  train_snapshot_model.py             (必跑)              �?   �?
�? └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�?                             �?                                          �?
�?                             �?                                          �?
�? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢� 验证阶段 (Validation) ┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�? �? Step 6  build_groupkey_validation_reports.py (必跑)             �?   �?
�? └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�?                             �?                                          �?
�?                             �?                                          �?
�? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢� 分析阶段 (Analysis) ┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�? �? Step 7  analyze_q_model_calibration.py  �?--skip-analysis 可跳过│   �?
�? �? Step 8  analyze_alpha_quadrant.py       �?--skip-analysis 可跳过│   �?
�? �? Step 9  analyze_rules_alpha_quadrant.py �?--skip-analysis 可跳过│   �?
�? └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�?                             �?                                          �?
�?                             �?                                          �?
�? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢� 回测 & 基线对比阶段 (Backtest & Baselines) ┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
�? �? Step 10  backtest_execution_parity.py  �?--skip-backtest 可跳过│    �?
�? �?    �?                                                           �?   �?
�? �?    �?                                                           �?   �?
�? �? Step 11  compare_baseline_families.py  �?--skip-baselines 可跳过│   �?
�? └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   �?
└─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
```

**五个阶段的��辑关系**�?

| 阶段 | 目的 | 是否可跳�?|
|------|------|-----------|
| 数据采集 | �?API 拉取原始数据，生成标注和历史价格快照 | 每步可独立跳�?|
| 训练 | 构建朴素频率规则，训�?AutoGluon 集成模型 | **不可跳过**（核心步骤） |
| 验证 | 生成 GroupKey 迁移丢�致��和特征契约校验报告 | **不可跳过** |
| 分析 | 评估模型校准、Alpha 信号质量、规则有效��?| 可整体跳�?|
| 回测 & 基线对比 | 模拟在线执行逻辑的历史回测，对比不同模型�?| 各自可跳�?|

---

## Step 1: 拉取原始市场数据

> **脚本**: `rule_baseline/data_collection/fetch_raw_events.py`  
> **跳过参数**: `--skip-fetch`  
> **CLI 额外参数**: `--full-refresh`, `--date-start`, `--date-end`

### 1.1 做什�?

�?Polymarket �?Gamma API (`https://gamma-api.polymarket.com/events`) 拉取**已结�?*（resolved）的市场数据，经过严格的质量过滤后，写入 append-only 批次文件系统，最终合并为标准化的原始市场 CSV�?

### 1.2 详细流程

```
Gamma API ┢�┢�(多线程并行拉�?┢�┢��?原始 Events JSON
      �?
      �?
Event 解析 & 展开
  - 每个 Event 包含多个 Markets
  - resolve_category(): 根据 tags 映射为大�?(CRYPTO/SPORTS/FINANCE/POLITICS...)
      �?
      �?
process_market() �?30+ 项质量过滤器
  ├─┢� 基本校验: closedTime 有效、窗口时间范围内
  ├─┢� 状��检�? umaResolutionStatus == "resolved"
  ├─┢� 排除规则: negRisk == True �?拒绝
  ├─┢� 排除规则: 短周期加密货币市�?(15m/5m updown) �?拒绝
  ├─┢� 黑名单源: resolutionSource �?DOMAIN_BLACKLIST �?�?拒绝
  ├─┢� Token 校验: clobTokenIds/outcomes 必须为长�?2 的数�?
  ├─┢� 成交量检�? volume >= MIN_MARKET_VOLUME (10)
  ├─┢� 价格确认: 结算价格中必须有且仅�?1 �?> 0.9 (胜方)
  └─┢� 明确结算: 败方价格 < 0.1 (排除模糊结算)
      �?
      �?(通过的市�?
write_batch() �?写入 append-only 批次文件
  - 文件: raw_batches/fetch_{timestamp}.csv
  - 更新 batch_manifest.csv (batch_id, closedTime_min, closedTime_max, row_count)
      �?
      �?(拒绝的市�?
raw_market_quarantine.csv �?记录拒绝原因
      �?
      �?
rebuild_canonical_merged()
  - 合并扢�有批次文�?
  - �?market_id 去重 (保留朢�新一�?
  - 输出朢�终的 raw_markets_merged.csv
```

### 1.3 关键输出字段

| 字段 | 含义 |
|------|------|
| `market_id` | 市场唯一 ID |
| `category` | 大类: CRYPTO / SPORTS / FINANCE / POLITICS / ... |
| `primary_token_id` / `secondary_token_id` | CLOB Token IDs (用于后续 API 调用) |
| `primary_outcome` / `secondary_outcome` | 结果标签 (�?"Yes" / "No") |
| `winning_outcome_index` | 获胜结果索引 (0 �?1) |
| `closedTime` | 实际结算时间 (UTC) |
| `endDate` | 原定结束时间 |
| `delta_hours` | `|closedTime - endDate|` 的小时差 |
| `volume` / `liquidity` | 成交�?/ 流动�?|

### 1.4 增量 vs 全量

- **默认（增量模式）**: 读取已有批次�?`max(closedTime)`，只拉取此后的新数据（有 72 小时重叠窗口确保不遗漏）
- **`--full-refresh`**: 清空扢�有批次，�?`DATE_START_STR (2024-10-31)` 重新拉取

### 1.5 输入 / 输出

| | 路径 |
|---|------|
| **输入** | Gamma API (HTTPS) |
| **输出** | `data/raw/batches/fetch_*.csv` |
| | `data/raw/batch_manifest.csv` |
| | `data/intermediate/raw_markets_merged.csv` |
| | `data/intermediate/raw_market_quarantine.csv` |

---

## Step 2: 构建市场标注

> **脚本**: `rule_baseline/domain_extractor/build_market_annotations.py`  
> **跳过参数**: `--skip-annotations`

### 2.1 做什�?

为每个市场自动推�?**domain（域名来源）**�?*category（大类）** �?**market_type（市场子类型�?* 三级标注。这些标注是后续规则分组、特征工程和模型训练的关键维度��?

### 2.2 详细流程

```
raw_markets_merged.csv ┢�┢��?market_annotations.py
      �?
      �?
MarketSourceParser �?URL 解析
  - �?resolutionSource 字段提取域名
  - 归一�? polymarket.com �?polymarket, espn.com �?espn, ...
  - 子域名提�? ncaa.com/basketball �?ncaa.basketball
      �?
      �?
normalize_outcomes() �?结果标签解析
  - 识别 outcome 模式: down_up / no_yes / named / numeric
  - 用于推断 market_type
      �?
      �?
infer_category_from_source() �?来源推断大类
  - SPORTS_DOMAINS �?SPORTS
  - CRYPTO_DOMAINS �?CRYPTO
  - FINANCE_DOMAINS �?FINANCE
  - gameId 字段存在 �?SPORTS
      �?
      �?
extract_coarse_market_family() �?市场类型推断
  - 正则匹配 question/description:
    SPREAD_PATTERNS �?spread
    TOTAL_PATTERNS  �?total
    PROP_PATTERNS   �?prop
    MONEYLINE_PATTERNS �?moneyline
  - 默认: "generic"
      �?
      �?
build_domain_candidate() �?构建层级�?domain
  - 格式: {normalized_source}.{sport}.{market_family}
  - �? "ncaa.com.basketball.spread", "gol.gg.moneyline"
      �?
      �?
低频 bucket 回���（标注归丢�化）
  - 统计 `(domain, category, market_type)` bucket �?market 数量
  - �?bucket_market_count < LOW_FREQUENCY_BUCKET_COUNT (=30)
    则将 category 回���到该 domain 下更稳定的主 category
  - 目的: 避免过稀疏的标注 bucket 在后续分组中产生不稳定切�?
      �?
      �?
输出: market_domain_features.csv
  �?(market_id, domain, category, market_type, source_url, ...)
```

### 2.3 为什么需要这丢��?

规则引擎的核心��想�?**"同一类市场在同一价格区间徢�徢�有相似的 edge"**。��过三级标注将市场分组：

- **domain**: 朢�细粒度，决定市场来源和可比��（�?`espn.nfl.moneyline` 只与 NFL 赛事比较�?
- **category**: 中等粒度，用于曝光管理和切片分析（如 SPORTS、CRYPTO�?
- **market_type**: 市场结构类型（spread/total/prop/moneyline/generic），影响策略选择

### 2.4 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/intermediate/raw_markets_merged.csv` |
| **输出** | `data/domain/market_domain_features.csv` |
| | `data/domain/domain_summary.csv` |
| | `data/domain/domain_summary_aggregated.csv` |

---

## Step 3: 构建价格快照

> **脚本**: `rule_baseline/data_collection/build_snapshots.py`  
> **跳过参数**: `--skip-snapshots`  
> **CLI 额外参数**: `--full-refresh`

### 3.1 做什�?

对每个已结算市场，回溯其结算�?`[1h, 2h, 4h, 6h, 12h, 24h]` 六个时间窗的历史价格，构�?*决策时刻快照 (decision-time snapshot)**。每行代表："如果你在市场结算�?N 小时看到价格 P，那么最终结�?Y 是什�?�?

### 3.2 详细流程

```
raw_markets_merged.csv                CLOB prices-history API
        �?                                      �?
        └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
                       �?
         process_market() �?逐市场处�?
                       �?
                       �?
           fetch_history_batch()
             - 请求 CLOB API: /prices-history?market={token_id}&interval=1h&fidelity=1
             - 获取 24 小时的价格时间序�?
             - 返回 [(timestamp, price), ...] 数组
                       �?
                       �?
           generate_snapshots() �?�?6 �?horizon 逐一生成快照
             �?
             ├─┢� find_prices_batch()
             �?    - 计算目标时间�? t_snap = closedTime - horizon_hours
             �?    - 在价格序列中二分查找 ±5min 窗口 (SNAP_WINDOW_SEC=300)
             �?    - 选择朢�接近的报价点
             �?    �?
             �?    �?
             �?  Stale Quote 棢��?
             �?    - offset_sec: 报价时间与目标时间的偏移�?
             �?    - local_gap_sec: 相邻报价间的朢�大间�?
             �?    - 标记条件:
             �?      offset_sec > 120 (STALE_QUOTE_MAX_OFFSET_SEC)
             �?      OR local_gap_sec > 900 (STALE_QUOTE_MAX_GAP_SEC)
             �?
             ├─┢� 计算核心字段
             �?    - y (label): winning_outcome_index == 0 �?1.0, else 0.0
             �?    - price: 对应时刻�?primary outcome 价格
             �?    - r_std = (y - price) / sqrt(price * (1-price) + ε)
             �?      �?标准化残差，衡量真实结果与当时价格的偏离程度
             �?
             └─┢� quality_pass 判定
                   - horizon_eligible: �?horizon 的数据是否在有效范围�?
                   - snapshot_found: 是否在窗口内找到了报�?
                   - stale_quote_flag: 报价是否过时
                   - quality_pass = eligible AND found AND NOT stale
                       �?
                       �?
              flush_buffers() �?分批次写�?
                - 每积累一批后写入 snapshot batch (避免内存溢出)
                - 同时写入 audit �?quarantine 批次
                       �?
                       �?
              rebuild_canonical_snapshots()
                - 合并扢�有批�?
                - �?(market_id, horizon_hours) 去重
                - 输出朢��?snapshots.csv
```

### 3.3 单行快照的含�?

每一�?snapshot 表示丢��?*历史决策时刻**，其物理含义是：

> "在市�?M �?token 价格 P 在结算前 H 小时的某个时刻，该市场最终结果为 Y�? �?1�?

丢�个市场最多产�?6 行快照（6 �?horizon），实际数量取决�?API 数据可用性和质量棢�查��?

### 3.4 关键输出字段

| 字段 | 含义 |
|------|------|
| `market_id` | 市场 ID |
| `horizon_hours` | 决策窗口 (1/2/4/6/12/24) |
| `price` | 该时刻的 primary outcome 价格 |
| `y` | 真实结果 (0 �?1) |
| `r_std` | 标准化残�?|
| `snapshot_time` | 快照时间�?(closedTime - horizon) |
| `offset_sec` | 报价偏移量（秒） |
| `local_gap_sec` | 屢�部报价间隔（秒） |
| `stale_quote_flag` | 是否过时报价 |
| `quality_pass` | 是否通过质量棢��?|

### 3.5 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/intermediate/raw_markets_merged.csv` |
| | CLOB prices-history API (HTTPS) |
| **输出** | `data/processed/snapshots.csv` �?主快照文�?|
| | `data/processed/snapshot_market_audit.csv` �?每市场审�?|
| | `data/processed/snapshots_quarantine.csv` �?被拒快照 |
| | `data/processed/snapshot_horizon_hit_rate.csv` �?Horizon 命中�?|

---

## Step 4: 训练朴素规则

> **脚本**: `rule_baseline/training/train_rules_naive_output_rule.py`  
> **不可跳过**（训练阶段必须执行）  
> **CLI 参数**: `--artifact-mode`, `--max-rows`, `--recent-days`, `--split-reference-end`

### 4.1 做什�?

基于历史快照的纯频率统计，构�?*朴素规则�?(naive rule buckets)**。根�?`(domain, category, market_type, price_bin, horizon)` 五维分组，统计每个桶内的历史胜率�?edge，并计算 Wilson 置信下界作为规则评分字段 (`edge_lower_bound_full` / `rule_score`)�?

### 4.2 详细流程

```
snapshots.csv + market_domain_features.csv
      �?
      �?
prepare_rule_training_frame()
  - 加载快照数据，过�?quality_pass == True
  - 关联市场标注 (domain, category, market_type)
  - 时间分割: assign_dataset_split() �?train / valid（offline artifact split 默认不再单独保留 test）
  - 可��过�? --max-rows, --recent-days
  - 过滤 tradeable 价格范围: TRAIN_PRICE_MIN=0.2 ~ TRAIN_PRICE_MAX=0.8
      �?
      �?
price_bin 分桶
  - �?RULE_PRICE_BIN_STEP = 0.1 �?price 离散�?
  - �? price=0.35 �?bin=[0.3, 0.4)
  - �?Step 4 �?offline 训练链路中固定使�?0.1 步长，不再按组自动细�?
      �?
      �?
prepare_rule_bin_frame() �?�Ȱ� `price in [0.2, 0.8]` ���ˣ��ٹ��� (domain, category, market_type, price_bin, horizon) ����
  �?
  ├─┢� 对每个分组计�?
  �?    - n: 样本�?
  �?    - win_rate: y 的均�?
  �?    - q_full: 价格均��?(市场隐含概率)
  �?    - edge_full: win_rate - q_full (原始 edge)
  �?    - edge_std_full: edge 的标准差
  �?    - direction: edge_sign() �?+1 (买入) �?-1 (卖出)
  �?
  ├─┢� Wilson 置信区间
  �?    - wilson_interval(): 计算 edge lower bound
  �?    - edge_lower_bound_full: edge 的保守估�?
  �?    - 当前主流程将其保留为规则评分字段，不再额外用 ���� edge �½糣�� 做硬过滤
  �?
  ├─┢� 去除低频�?
  �?    - MIN_GROUP_UNIQUE_MARKETS = 15 (group_key 层面至少 15 个不同市�?
  �?    - 小于该阈值的 group_key 会被标记�?`insufficient_data`，不会进入后续规则生�?
  �?
  └─┢� 生成 leaf_id
        - 格式: {domain}|{category}|{market_type}|{price_bin}|{horizon}
      �?
      �?
build_fine_serving_features() �?规则�?serving 特征
  - rule_price_center: 规则价格区间 `[price_min, price_max]` 的中�?= `(price_min + price_max) / 2`
  - rule_horizon_center: 规则 horizon 区间 `[h_min, h_max]` 的中�?= `(h_min + h_max) / 2`
    这里�?`h_min/h_max` 来自离散 horizon 的中点边界，不是原始 horizon 点��本�?
  - rule_edge_buffer: `edge_full - edge_lower_bound_full` (edge 与保守下界之间的缓冲)
  - rule_confidence_ratio: `edge_lower_bound_full / edge_std_full` (保守优势相对波动的比�?
  - rule_support_log1p: `log(1 + n_full)` (样本充裕�?
  - interaction / relative features: 基于 group 历史统计构��的交互项��差值项和同粒度规则聚合�?
  - 这些字段不会回写�?`trading_rules.csv`；它们会单独写入 offline 产物 `data/offline/edge/fine_serving_features.parquet`
  - 后续�?Step 5 通过 `attach_serving_features()` �?`fine_feature_` 前缀并入样本
      �?
      �?
summarize_history_features() �?历史质量统计 (详见 4.3)
      �?
      �?
build_group_serving_features() �?组级聚合特征 (详见 4.4)
      �?
      �?
输出
  ├─┢� trading_rules.csv (符合条件的交易规则表)
  ├─┢� all_trading_rule_audit_report.csv (扢�有叶子节点报�?
  ├─┢� group_serving_features.parquet (组级 serving 特征)
  ├─┢� fine_serving_features.parquet (规则�?serving 特征)
  ├─┢� serving_feature_defaults.json (fallback 配置)
  └─┢� history_features_{level}.parquet × 8 (历史中间产物)
```

### 4.3 历史质量统计 (summarize_history_features)

> **实现脚本**: `rule_baseline/history/history_features.py`  
> **调用�?*: `train_rules_naive_output_rule.py::main()` 在规则训练完成后调用

这一步对扢��?*通过质量棢�查的训练快照**，计算多层级、多窗口的历史预测质量统计��这是后�?Group Serving 特征的原料��?

#### 4.3.1 基础质量指标

对每行快照计�?4 个基硢�质量指标�?

```
row_bias     = y - price            # 偏差: 实际结果与市场价格的�?
row_abs_bias = |y - price|           # 绝对偏差
row_brier    = (y - price)²          # Brier Score
row_logloss  = -(y·log(p) + (1-y)·log(1-p))  # Log Loss
```

#### 4.3.2 8 个聚合层�?

| 层级 | 分组维度 | 含义 | 示例�?|
|------|---------|------|--------|
| `global` | 无（全部数据�?| 整体基线 | `__GLOBAL__` |
| `domain` | domain | 同一数据�?| `espn.nfl.moneyline` |
| `category` | category | 同一大类 | `SPORTS` |
| `market_type` | market_type | 同一市场类型 | `moneyline` |
| `domain_x_category` | domain × category | 来源+大类交叉 | `espn.nfl.moneyline\|SPORTS` |
| `domain_x_market_type` | domain × market_type | 来源+类型交叉 | `espn.nfl.moneyline\|moneyline` |
| `category_x_market_type` | category × market_type | 大类+类型交叉 | `SPORTS\|moneyline` |
| `full_group` | domain × category × market_type | 朢�精确的组 | `espn.nfl.moneyline\|SPORTS\|moneyline` |

**为什么需�?8 个层级？** �?它构成一�?*层级化参照系**。模型可以学习："这条规则�?edge 比同 domain 平均水平好多�?�?这个组比全局基线差多�?，从而判断规则是否可信��?

#### 4.3.3 2 个时间窗�?

| 窗口 | 大小 | 含义 |
|------|------|------|
| `expanding` | 全部历史 | 长期稳定水平 |
| `recent_90days` | �� `closedTime` ��ÿ�� group ��ȡ���� 90 �� | ������Ư�ƺ�β������ |

**为什么需要多窗口�?* �?棢��?*质量漂移 (drift)**。如�?`recent_90days_bias_mean` �� `expanding_bias_mean` 差距大，说明这个组的市场特��正在变化��?

#### 4.3.4 每个 (层级 × 窗口) �?26 个指�?

```
计数:    snapshot_count, market_count
偏差:    bias_mean, bias_std, bias_min, bias_max
绝对偏差: abs_bias_mean, abs_bias_p25, abs_bias_p50, abs_bias_p75, abs_bias_p90, abs_bias_max
Brier:   brier_mean, brier_p25, brier_p50, brier_p75, brier_p90, brier_std, brier_max
Logloss: logloss_mean, logloss_p25, logloss_p50, logloss_p75, logloss_p90, logloss_std, logloss_max
```

**理论总量**: 8 层级 × 2 窗口 × 26 指标 = **624 �?*

#### 4.3.5 存储

每个层级输出丢��?parquet 文件，写�?`data/edge/`�?

```
data/edge/history_features_global.parquet
data/edge/history_features_domain.parquet
data/edge/history_features_category.parquet
data/edge/history_features_market_type.parquet
data/edge/history_features_domain_x_category.parquet
data/edge/history_features_domain_x_market_type.parquet
data/edge/history_features_category_x_market_type.parquet
data/edge/history_features_full_group.parquet
```

### 4.4 Group Serving 特征生成 (build_group_serving_features)

> **实现脚本**: `rule_baseline/training/train_rules_naive_output_rule.py` (L486–L672)  
> **存储路径**: `data/edge/group_serving_features.parquet`  
> **消费�?*: `train_snapshot_model.py`（离线训练）、`execution_engine/online/scoring/rules.py`（在线推理）

#### 4.4.1 整体流程

```
输入 �? rules_df (本步骤产出的 trading_rules)
输入 �? history_feature_frames (4.3 产出�?8 个层级表)
      �?
      �?
Step A: �?rules_df 的唯丢� group_key 为骨�?
  - �?rules_df 中取每个 group_key 的基本信�?
  - �?group_key 去重 (丢�个组只保留一�?
  - 包含: group_unique_markets, group_snapshot_rows,
           group_market_share_global, group_median_logloss, ...
      �?
      �?
Step B: 逐层�?LEFT JOIN 历史特征 (8 �?merge)
  �?
  ├─┢� global:     merge_key = "__GLOBAL__" (扢�有行共享同一全局统计)
  ├─┢� domain:     merge_key = domain 字段
  ├─┢� category:   merge_key = category 字段
  ├─┢� market_type: merge_key = market_type 字段
  ├─┢� domain_x_category:     merge_key = "domain|category"
  ├─┢� domain_x_market_type:  merge_key = "domain|market_type"
  ├─┢� category_x_market_type: merge_key = "category|market_type"
  └─┢� full_group: merge_key = group_key (全精度匹�?
  
  �?此时每行包含 624 列历史质量指�?
      �?
      �?
Step C: 计算衍生 Drift/Gap/Tail/Z-Score 特征 (~25 �?
  (详见 4.4.2)
      �?
      �?
Step D: 构建 Fine Feature Defaults (fallback �?
  �?fine_serving 匹配失败时用�?group 级替代��?
  (详见 4.4.3)
      �?
      �?
输出: group_serving_features.parquet
  - 每行 = 丢��?group_key，确保和domain数据中的unique group_key数量相同
  - 列数: ~700+ (基本信息 + 624 历史指标 + ~25 衍生 + ~40 默认�?
```

#### 4.4.2 衍生特征（Step C 详细�?

�?624 列原始历史指标的基础上，再计�?~25 列衍生特征，分为 5 类：

**�?Drift Gap �?近期 vs 全量，检测质量漂�?*

| 特征 | 公式 | 含义 |
|------|------|------|
| `full_group_recent_90days_vs_expanding_bias_gap` | `recent_90days_bias_mean - expanding_bias_mean` | �?50 条偏差相比历史全量的变化 |
| `full_group_recent_90days_vs_expanding_bias_gap` | `recent_90days_bias_mean - expanding_bias_mean` | �?200 条偏差的漂移 |
| `full_group_recent_90days_vs_expanding_abs_bias_gap` | 同理 (用绝对偏�? | |
| `full_group_recent_90days_vs_expanding_abs_bias_gap` | 同理 | |
| `full_group_recent_90days_vs_expanding_brier_gap` | 同理 (�?Brier) | |
| `full_group_recent_90days_vs_expanding_brier_gap` | 同理 | |
| `full_group_recent_90days_vs_expanding_logloss_gap` | 同理 (�?Logloss) | |
| `full_group_recent_90days_vs_expanding_logloss_gap` | 同理 | |

> Gap > 0 �?近期质量在恶化；Gap < 0 �?近期质量在改�?

**�?Tail Spread �?分布尾部的散�?(P90 - P50)**

| 特征 | 公式 | 含义 |
|------|------|------|
| `full_group_expanding_abs_bias_tail_spread` | `abs_bias_p90 - abs_bias_p50` | 偏差分布的尾部风�?|
| `full_group_expanding_brier_tail_spread` | `brier_p90 - brier_p50` | Brier 的尾部风�?|
| `full_group_expanding_logloss_tail_spread` | `logloss_p90 - logloss_p50` | Logloss 的尾部风�?|
| `full_group_recent_90days_logloss_tail_spread` | �?50 �?logloss 尾部散度 | |
| `full_group_recent_90days_logloss_tail_spread` | �?200 �?logloss 尾部散度 | |

> Tail spread �?�?该组内部差异大，有些市场容易预测有些很难

**�?Cross-Level Gap �?�?vs 父级的质量差�?*

| 特征 | 公式 | 含义 |
|------|------|------|
| `full_group_vs_domain_logloss_gap` | `full_group_logloss - domain_logloss` | 组比�?domain 平均水平好还是差 |
| `full_group_vs_category_logloss_gap` | `full_group_logloss - category_logloss` | 组比同类别平均水平好还是�?|
| `full_group_vs_market_type_logloss_gap` | `full_group_logloss - market_type_logloss` | 组比同类型平均水平好还是�?|

> Gap < 0 �?该组比父级更容易预测（logloss 更低�?

**�?Drift Z-Score �?标准化的漂移程度**

| 特征 | 公式 | 含义 |
|------|------|------|
| `full_group_recent_90days_vs_expanding_bias_zscore` | `bias_gap / expanding_bias_std` | 漂移的统计显著��?|
| `full_group_recent_90days_vs_expanding_bias_zscore` | 同理 | |
| `full_group_recent_90days_vs_expanding_logloss_zscore` | 同理 (logloss) | |
| `full_group_recent_90days_vs_expanding_logloss_zscore` | 同理 | |

> |Z-score| > 2 �?该组的近期表现发生了统计上显著的偏移

**�?Tail Instability 及交�?*

| 特征 | 公式 | 含义 |
|------|------|------|
| `full_group_recent_90days_tail_instability_ratio` | `recent_90days_tail / expanding_tail` | 近期尾部 vs 全量尾部�?1 表示不稳定加�?|
| `full_group_recent_90days_tail_instability_ratio` | 同理 | |
| `full_group_expanding_logloss_tail_x_market_share` | `logloss_tail × market_share` | 尾部风险 × 该组在全屢�的份�?|
| `full_group_expanding_abs_bias_tail_x_snapshot_share` | `abs_bias_tail × snapshot_share` | 同上，用快照份额加权 |

#### 4.4.3 Fine Feature 生成逻辑

`fine_serving_features.parquet` 不是箢�单把 `trading_rules.csv` 原样复制丢�份，而是在每条规则行上再做一层派生��生成顺序可以概括为 4 步：

**Step A: �?`rules_df` 为底表复制规则行**

- 每行仍然对应丢�条规则，即一�?`(group_key, price_bin, horizon_hours)` 组合
- 保留基础规则列：`leaf_id`, `direction`, `q_full`, `p_full`, `edge_full`, `edge_std_full`, `edge_lower_bound_full`, `rule_score`, `n_full`

**Step B: 先生成规则自身的几何 / 置信度特�?*

- 价格几何:
  - `rule_price_center = (price_min + price_max) / 2`
  - `rule_price_width = price_max - price_min`
- horizon 几何:
  - `rule_horizon_center = (h_min + h_max) / 2`
  - `rule_horizon_width = h_max - h_min`
- 规则稳定�?/ 支撑�?
  - `rule_edge_buffer = edge_full - edge_lower_bound_full`
  - `rule_confidence_ratio = edge_lower_bound_full / edge_std_full`
  - `rule_support_log1p = log(1 + n_full)`
  - `rule_snapshot_support_log1p` 当前实现中与 `rule_support_log1p` 相同

**Step C: �?`group_key` merge 丢�小部�?group 历史统计，再构��相对特�?*

- 这里不会把整�?group feature 表全部并进来，只挑��少量历史基准列，例�?
  - `full_group_expanding_bias_mean`, `full_group_recent_90days_bias_mean`
  - `full_group_expanding_logloss_mean`, `full_group_expanding_logloss_tail_spread`
  - `domain/category/market_type` 及其交叉层级�?`expanding_bias_mean` / `expanding_logloss_mean`
- 基于这些基准列再派生 3 �?fine feature:
  - 交互�? �?`hist_price_x_full_group_expanding_bias`, `tail_risk_x_price`
  - 差��项: �?`rule_edge_minus_domain_expanding_bias`, `rule_score_minus_full_group_expanding_logloss`
  - 比��项: �?`rule_edge_over_full_group_logloss`

**Step D: 生成“同粒度规则上下文��聚合特�?*

- 对同丢� `price_bin + horizon_hours + direction` 下的规则，再分别�?4 个粒度聚�?
  - `full_group_key`
  - `domain`
  - `category`
  - `market_type`
- 每个粒度都会生成 8 个统计量:
  - `matched_rule_count`
  - `max_edge_full`, `max_edge_lower_bound_full`, `max_rule_score`
  - `mean_edge_full`, `mean_edge_lower_bound_full`, `mean_rule_score`
  - `sum_n_full`
- 这些列让模型知道：当前规则除了自身数值外，在更粗粒度的同类规则里处在仢�么位�?

**兜底行为**

- 如果 `group_features` 没有传入，依赖历史基准的 fine feature 不会缺列，��是统一补成 `0.0`，以保持 schema 稳定
- 朢�后只保留预定义的 `FINE_SERVING_COLUMNS`，按 `(group_key, horizon_hours, price_bin)` 排序后写�?parquet

#### 4.4.4 Fine Feature Defaults（Step D 详细�?

当在线推理时，某个快照的 `(group_key, price_bin, horizon)` �?`fine_serving_features.parquet` 中找不到精确匹配，系统会 fallback �?group 级别的默认����?

构建方式：对�?group_key 下的扢��?fine 规则做聚合：

| 聚合方式 | 用于的字�?| 含义 |
|---------|-----------|------|
| `weighted_mean` (�?n_full 加权) | `q_full`, `edge_full`, `rule_score`, `rule_edge_buffer`, ... (�?30+ 字段) | 用样本量加权的平均��?|
| `sum` | `n_full` | 组内总样本量 |
| `mean` | `rule_price_center`, `rule_horizon_center`, `rule_support_log1p` | 箢�单均�?|
| `signed_sum_edge` | `direction` | 组内 edge_full 之和的符�?(+1/-1) |
| `sentinel` | `leaf_id` | 固定�?`"__GROUP_DEFAULT__|{group_key}"` |

输出�?
- 每个 group_key �?`group_default_{feature_name}` 列（~40 列）
- 配套�?`serving_feature_defaults.json` 描述 fallback 映射关系

#### 4.4.5 在线查表流程 (attach_serving_features)

�?`train_snapshot_model.py` 和在�?`execution_engine` 中，通过 `attach_serving_features()` �?group/fine 特征挂载到快照上�?

这里�?fine lookup 表就�?Step 4 产出�?`data/offline/edge/fine_serving_features.parquet`；因�?`rule_price_center` / `rule_horizon_center` 在样本侧通常出现�?`fine_feature_rule_price_center` / `fine_feature_rule_horizon_center`，��不�?`trading_rules.csv` 原始列��?

**输入数据**

| 输入对象 | 主键 / 粒度 | 行数 | 列数 |
|---|---|---:|---:|
| 待挂载快�?`frame` | 每行丢��?snapshot | `N`（输入快照行数） | `M`（输入快照原始列数） |
| `group_serving_features.parquet` | 每行丢��?`group_key` | `G = unique(group_key)` | 当前文档按组�?serving 资产记为 `~700+` �?|
| `fine_serving_features.parquet` | 每行丢��?`(group_key, price_bin, horizon_hours)` | `F`（��常�?fine 规则行数丢�致） | `75` �?|
| `serving_feature_defaults.json` | 每个 fine 字段丢��?fallback 映射 | `72` 条映�?| JSON 键��结�?|

**输出结构**

- 输出仍然是一�?DataFrame，按列块可分�?4 部分�?
- 原始快照列：即输�?`frame` 的全�?`M` �?
- 查表辅助键：`group_key`, `price_bin`, `rounded_horizon_hours`
- 匹配状��列：`group_match_found`, `fine_match_found`, `used_group_fallback_only`
- Serving 特征列：
  - `group_feature_*`：来�?`group_serving_features.parquet`，除 join key `group_key` 外全部加前缀并入
  - `fine_feature_*`：来�?`fine_serving_features.parquet`，除 join keys `group_key`, `price_bin`, `horizon_hours` 外全部加前缀并入

**输出行数 / 列数**

- 行数：输出仍�?`N` 行��这里使用两�?`LEFT JOIN`，不会过滤快照行；设计预期下 lookup 表键唯一，因此也不会放大行数�?
- 列数�?
  - 固定新增 `6` 列辅助字段：`group_key`, `price_bin`, `rounded_horizon_hours`, `group_match_found`, `fine_match_found`, `used_group_fallback_only`
  - 再新�?`group_serving_features` 中除 `group_key` 外的全部�?
  - 再新�?`fine_serving_features` 中除 `group_key`, `price_bin`, `horizon_hours` 外的全部列，即当前固定新�?`72` �?`fine_feature_*` �?
  - 因此总列数可写为�?

```text
output_columns
  = M
  + 6
  + (group_serving_feature_columns - 1)
  + (75 - 3)
```

```text
output_rows = N
```

```
每行快照
  �?
  ├─┢� 构��?group_key = domain|category|market_type
  ├─┢� 构��?price_bin = floor(price×10)/10 的区间标�?
  ├─┢� 构��?rounded_horizon_hours = round(horizon)
  �?
  ├─┢� LEFT JOIN group_serving_features.parquet
  �?  on: group_key
  �?  �?扢�有列加前缢� "group_feature_"
  �?  �?group_match_found = True/False
  �?
  ├─┢� LEFT JOIN fine_serving_features.parquet
  �?  on: (group_key, price_bin, rounded_horizon_hours)
  �?  �?扢�有列加前缢� "fine_feature_"
  �?  �?fine_match_found = True/False
  �?
  └─┢� Fallback:
      如果 fine_match_found = False:
        对每�?fine_feature_{name}:
          �?group_feature_group_default_{name} 替代
      标记: used_group_fallback_only = True
```

### 4.5 规则表的直觉含义

丢�条规则的含义是：

> "�?domain=espn.nfl.moneyline, category=SPORTS, market_type=moneyline 的市场中，当价格�?[0.3, 0.4) 区间且距离结�?4 小时时，历史�?65% 的结果是 Yes。��市场价格暗�?45%（即 price=0.45）��因�?edge = 0.20，方�?= 买入 (+1)。Wilson 置信下界可作为该规则的保守评分信号，数��越高��常说明这条规则的历史优势越稳健�?

### 4.6 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/processed/snapshots.csv` |
| | `data/domain/market_domain_features.csv` |
| **输出** | `data/edge/trading_rules.csv` |
| | `data/edge/group_serving_features.parquet` �?组级 serving 特征 |
| | `data/edge/fine_serving_features.parquet` �?规则�?serving 特征 |
| | `data/edge/serving_feature_defaults.json` �?fallback 配置 |
| | `data/edge/history_features_{level}.parquet` × 8 �?历史中间产物 |
| | `data/audit/all_trading_rule_audit_report.csv` |

---

## Step 5: 训练快照模型

> **脚本**: `rule_baseline/training/train_snapshot_model.py`  
> **不可跳过**（训练阶段必须执行）  
> **CLI 参数**: `--calibration-mode`, `--grouped-calibration-column` (默认 horizon_hours), `--grouped-calibration-min-rows` (默认 20), `--target-mode`, `--random-seed`, `--predictor-time-limit`, `--num-bag-folds`, `--num-bag-sets`, `--num-stack-levels`, `--auto-stack`, ...

### 5.1 做什�?

训练 **AutoGluon 集成模型**，预测市场的真实概率 `q` (probability of outcome=1)，并输出校准后的预测值��这是整个系统的核心：模型的 `q_pred` 与市场价�?`price` 之间的差异就是交易信号（edge）��?

### 5.2 详细流程

```
snapshots.csv + trading_rules.csv + market_domain_features.csv + raw_markets_merged.csv
      �?
      �?
load_rules() �?加载规则表并校验 schema
      �?
      �?
match_snapshots_to_rules()
  - 将快照与规则�?(domain, category, market_type, price_bin, horizon) 匹配
  - 每个快照朢�多匹配一条规�?�?获得 leaf_id �?serving features
      �?
      �?
build_feature_table() �?构建完整特征�?
  �?
  ├─┢� 基础特征 (来自 snapshot)
  �?    - price, horizon_hours, r_std
  �?    - offset_sec, local_gap_sec, points_in_window
  �?    - delta_hours (计划 vs 实际结算时间�?
  �?
  ├─┢� 市场原始特征 / market_feature_cache (来自 raw_markets_merged)
  �?    - 先��过 `build_market_feature_cache()` �?`market_id` 构建 market-level cache
  �?    - 再在 `preprocess_features()` 中按 `market_id` merge 回快照样�?
  �?    - 这一层是“����?market 特征缓存”，不等于这些字段都会直接入�?
  �?    - 缓存中包含数值类 market data: volume/liquidity, volume24hr/1wk, bestBid/bestAsk, spread,
  �?      lastTradePrice, oneHour/oneDay/oneWeekPriceChange, liquidityClob/liquidityAmm �?
  �?    - 文本分析特征: q_len, q_chars, avg_word_len, has_number, has_year,
  �?      threshold_max/min/span, is_player_prop, is_finance_threshold �?
  �?    - 情感特征: strong_pos, weak_pos, outcome_pos, outcome_neg, sentiment,
  �?      certainty, pos_ratio, neg_ratio �?
  �?    - 持续时间特征: market_duration_hours, log_duration, dur_very_short~dur_long �?
  �?    - 文本 embedding: `text_embed_00` ~ `text_embed_15`，由 question + description 的哈�?embedding 生成
  �?    - 同时会带�?`question_market`, `description_market` 等原�?market 列，供轻量文本特征和审计使用
  �?    - 但在当前 offline 训练里，`DROP_COLS` 会进丢�步剔除一批泄漏风险高�?terminal market state 字段�?
  �?      例如 `bestBid`, `bestAsk`, `spread`, `lastTradePrice`, `volume`, `volume24hr`, `volume1wk`,
  �?      `oneHourPriceChange`, `oneDayPriceChange`, `oneWeekPriceChange`, `description_market`, `question_market` �?
  �?    - 当前更��合作为 offline 入模特征的是文本形����部分情�?类别、持续时间��以及哈�?text embedding�?
  �?      这类特征相对更接近��市场创建时即可观察”的静��上下文
  �?    - 若未来要重新启用 market-state / microstructure 特征，必须改�?snapshot-time replay 数据源，
  �?      而不能继续直接读�?resolved raw market 的终态字�?
  �?
  ├─┢� 市场域特�?(来自 annotation)
  �?    - domain, category, market_type (label encoded �?categorical)
  �?    - 标注归一�? normalize_market_annotations()
  �?
  ├─┢� 规则 Serving 特征 (来自 Step 4)
  �?    - `fine_feature_rule_edge_buffer`, `fine_feature_rule_confidence_ratio`, `fine_feature_rule_support_log1p`
  �?    - `fine_feature_rule_price_center`, `fine_feature_rule_horizon_center`, 以及 fine interaction features
  �?    - 这些列来�?`data/offline/edge/fine_serving_features.parquet`，不�?`trading_rules.csv` 直接字段
  �?    - �?`attach_serving_features()` �?`(group_key, price_bin, rounded_horizon_hours)` 匹配；若 fine 未命中，则回逢��?group defaults
  �?    - 预处理阶段还会再派生未加前缀�?`rule_price_center = (p_full + price) / 2` �?`rule_horizon_center = (h_min + h_max) / 2`
  �?
  ├─┢� 历史特征 (多层级聚�?
  �?    - summarize_history_features(): 8 个层�?
  �?      (global, domain, category, market_type, domain×category, ...)
  �?    - 每层: bias_mean/std, abs_bias_percentiles, brier/logloss 统计
  �?    - 时间�? expanding, recent_90days, recent_90days
  �?
  ├─┢� Term Structure 特征
  �?    - delta_p_1h_4h, delta_p_4h_24h: �?horizon 价格变化
  �?    - term_structure_slope: p_1h - p_24h (价格曲线斜率)
  �?    - price_reversal_flag: 短期与长期趋势背�?
  �?    - path_price_mean/std/min/max/range: 价格路径统计
  �?
  └─┢� preprocess_features()
        - merge `market_feature_cache`，把 raw market 派生特征并回样本
        - apply_feature_variant()，补�?interaction / quality gap / 规则强度等运行时派生特征
        - �?numeric 特征: 填充 NaN �?0, clip outliers
        - �?categorical 特征: 填充 "UNKNOWN"
      - 删除 DROP_COLS (60+ �? market_id, snapshot_time, y, 原始 question/description，以及高泄漏风险�?terminal market-state 字段)
      �?
      �?
数据集分�?
  - compute_temporal_split(): 时间三折分割
    - Train: 朢��?~ (now - 60d)
    - Valid: (now - 60d) ~ (now - 30d)  �?30 天验证集
    - Test:  (now - 30d) ~ now           �?30 天严格测试集
      �?
      �?
fit_autogluon_q_model() �?训练 AutoGluon 集成
  �?
  ├─┢� Target Mode (预测目标的��择)
  �?    - q: 直接预测 P(Y=1) �?默认
  �?    - residual_q: 预测 residual = Y - price, 然后 q = price + residual
  �?    - expected_pnl: 预测 E[PnL]
  �?    - expected_roi: 预测 E[ROI]
  �?
  ├─┢� 模型训练
  �?    - AutoGluon TabularPredictor (problem_type="binary" for q mode)
  �?    - 超参数配�?(hyperparameter profiles):
  �?      · gbm_cat: LightGBM (num_boost_round=300, lr=0.03) + CatBoost + LR
  �?      · gbm_compact_cat_lr: 精简 GBM + CatBoost + LR (更快)
  �?      · lr_only: 仅��辑回归 (baseline)
  �?    - training_data = train split, tuning_data = valid split
  �?    - time_limit: 默认 300 �?
  �?    - presets: "medium_quality" (默认)
  �?
  ├─┢� 校准 (Calibration)
  �?    - �?valid split 上拟合校准器
  �?    - 支持模式:
  �?      · none: 不校�?
  �?      · global_isotonic (默认): 全局 Isotonic Regression
  �?      · grouped_isotonic: �?--grouped-calibration-column 分组 Isotonic (默认�? horizon_hours)
  �?      · global_sigmoid: 全局 Platt Scaling
  �?      · grouped_sigmoid: 分组 Platt Scaling
  �?      · beta_calibration: Beta 校准
  �?      · blend_raw_global_isotonic_{15|25|35}: 混合原始 + Isotonic
  �?      · blend_raw_beta_{15|25|35}: 混合原始 + Beta
  �?    - --grouped-calibration-min-rows: 分组校准的最小行�?(默认 20)
  �?    - 输出: calibration/ 子目�?(calibrator.pkl + calibrator_meta.json)
  �?
  └─┢� 预测
        - �?优先发布 test split；若 offline artifact split 无 test，则回退为 valid split
        - q_pred: 校准后的 P(Y=1) 预测�?
        - trade_value_pred: (q_pred / price - 1) - fee_rate (交易价��?
      �?
      �?
输出 Model Bundle �?部署就绪的产物包
  ├─┢� q_model_bundle_deploy/          �?部署 bundle (推理优化)
  �?    ├─┢� predictor/                �?AutoGluon saved predictor
  �?    ├─┢� calibration/
  �?    �?    ├─┢� calibrator.pkl      �?校准�?
  �?    �?    └─┢� calibrator_meta.json �?校准器元数据
  �?    ├─┢� feature_contract.json     �?特征契约 (列名、类型��关�?非关�?
  �?    ├─┢� normalization_manifest.json �?域名白名�?
  �?    ├─┢� runtime_manifest.json     �?训练元数据摘�?(bundle_role="deploy")
  �?    └─┢� metadata/
  �?          └─┢� deployment_summary.json
  �?
  ├─┢� q_model_bundle_full/            �?完整训练 bundle
  �?    ├─┢� predictor/                �?AutoGluon full predictor
  �?    ├─┢� calibration/              �?同上
  �?    ├─┢� feature_contract.json
  �?    ├─┢� normalization_manifest.json
  �?    └─┢� runtime_manifest.json     �?(bundle_role="full_training")
  �?
  ├─┢� snapshots_with_predictions.csv  �?带预测的完整快照
  └─┢� snapshot_training_funnel.json/md �?训练漏斗审计
```

#### 5.2.1 DROP_COLS 在哪里生�?

`DROP_COLS` 定义�?`rule_baseline/training/train_snapshot_model.py`，真正生效的位置�?`build_feature_table()` �?`add_training_targets()` 之后�?

```python
feature_columns = online_feature_columns([column for column in df_feat.columns if column not in DROP_COLS])
```

这意味着它过滤的�?**朢�终��入模型训练 / 导出�?feature contract 列表**，��不是前面中间表完全不能出现这些列��很多列会先保留�?`df_feat` 中用�?join、审计��质量过滤或训练目标构��，然后在这里统丢�剔除�?

#### 5.2.2 DROP_COLS 分类说明

`DROP_COLS` 可以理解为一个��最终入模白名单之外的显式黑名单”��并不是扢�有列都同时存在于每次运行中；它覆盖了 snapshot、rule match、serving attach、raw market cache、训练目标派生等多个来源�?

| 类别 | 代表�?| 主要来源文件 | 为什么删�?|
|---|---|---|---|
| 监督目标 / 结果泄漏 | `y`, `trade_value_true`, `expected_pnl_target`, `expected_roi_target`, `residual_q_target`, `winning_outcome_index`, `winning_outcome_label` | `rule_baseline/datasets/snapshots.py` 提供 `y`；`rule_baseline/training/train_snapshot_model.py::add_training_targets()` 生成其余目标�?| 这些列要么就是标签，要么是由标签直接推导出来的训练目标��保留进特征会形成直�?outcome leakage�?|
| 快照身份、时间锚点��审计字�?| `market_id`, `snapshot_time`, `snapshot_date`, `snapshot_target_ts`, `selected_quote_ts`, `closedTime`, `scheduled_end`, `batch_id`, `batch_fetched_at`, `batch_window_start`, `batch_window_end`, `dataset_split`, `quality_pass`, `delta_hours_exceeded_flag` | `rule_baseline/data_collection/build_snapshots.py`、`rule_baseline/datasets/snapshots.py`、`rule_baseline/features/snapshot_semantics.py` | 这些列用于时间定位��采样审计��数据集切分和质量控制，不是希望模型学习的交易信号��保留它们容易让模型记住时间位置、批次边界或 split 信息�?|
| 快照阶段的辅助脚手架�?| `price_bin`, `horizon_bin`, `r_std`, `e_sample`, `delta_hours`, `delta_hours_bucket`, `price_in_range_flag`, `selected_quote_side` | `build_snapshots.py` 生成基础 snapshot 字段；`snapshot_semantics.py` 增补 quote-window / quality 辅助列；`train_snapshot_model.py::match_snapshots_to_rules()` 使用其中丢�部分做规则匹�?| 这些列主要服务于规则分桶、质量过滤��离线诊断或匹配流程，不是稳定的朢�终语义特征��其�?`delta_hours_bucket` 还带有明�?offline-only 属��，在线也不可直接复现��?|
| 规则匹配键与规则身份�?| `leaf_id` | `rule_baseline/training/train_snapshot_model.py::match_snapshots_to_rules()` �?`trading_rules.csv` 匹配带回 | `leaf_id` 是规则树叶子身份，不是可泛化的市场语义��当前保留的�?`q_full`、`rule_score`、`direction`、group/fine serving 统计等连续规则信息，而不是让模型直接记忆规则 ID�?|
| Raw market 终��微观结�?/ 市场状��列 | `bestBid`, `bestAsk`, `spread`, `lastTradePrice`, `best_bid`, `best_ask`, `mid_price`, `quoted_spread`, `quoted_spread_pct`, `book_imbalance`, `volume`, `volume24hr`, `volume1wk`, `volume24hrClob`, `volume1wkClob`, `oneHourPriceChange`, `oneDayPriceChange`, `oneWeekPriceChange`, `price_change_1h`, `price_change_1d`, `price_change_1w` | `rule_baseline/features/tabular.py::build_market_feature_cache()` �?`raw_markets_merged.csv` 拉取原始列，再结�?`rule_baseline/features/market_feature_builders.py` 生成派生 market-state 特征 | 这是当前确认朢�强的 leakage 来源。离线训练读取的�?merged raw market 的终态��，而不�?`snapshot_time` 当时真正可见的盘�?/ 成交 / 涨跌状��；直接入模会把 resolved terminal state 偷带进来�?|
| Raw market 文本、身份��冗余标注列 | `question`, `description`, `question_market`, `description_market`, `source_url`, `source_host`, `source_url_market`, `source_host_market`, `groupItemTitle`, `groupItemTitle_market`, `gameId`, `gameId_market`, `marketMakerAddress`, `marketMakerAddress_market`, `startDate`, `endDate`, `startDate_market`, `endDate_market`, `closedTime_market`, `domain_market`, `market_type_market`, `domain_domain`, `market_type_domain`, `domain_parsed`, `domain_parsed_market`, `category_raw_market`, `category_parsed_market`, `category_override_flag_market`, `category_source`, `sub_domain`, `sub_domain_market`, `outcome_pattern`, `outcome_pattern_market` | `datasets/snapshots.py::_apply_raw_market_context()`、`features/tabular.py::build_market_feature_cache()`、`features/annotation_normalization.py` | 这批列大多是原始文本、市场身份��原始标注投影或重复元数据��它们要么高基数、容易让模型记忆市场 identity，要么与朢�终保留的规范�?`domain/category/market_type` 和轻量文本派生特征重复��?|
| Raw market 的冗�?/ 低信�?/ 已被更稳健特征替代的派生�?| `liquidity`, `negRisk`, `liquidityAmm`, `liquidityClob`, `log_liq`, `liq_ratio`, `log_liquidity_clob`, `log_liquidity_amm`, `clob_share_liquidity`, `log_vol`, `log_v24`, `log_v1w`, `vol_ratio_24`, `vol_ratio_1w`, `daily_weekly`, `vol_tier_ultra`, `vol_tier_high`, `vol_tier_med`, `vol_tier_low`, `activity`, `engagement`, `momentum`, `clob_share_volume24`, `clob_share_volume1w`, `price_change_accel`, `sentiment_vol`, `vol_per_day`, `log_vol_per_day`, `vol_x_sentiment`, `activity_x_catcount`, `line_value`, `has_percent`, `has_million`, `has_before`, `has_after`, `is_binary`, `cap_ratio`, `strong_pos`, `cat_finance`, `cat_entertainment_str`, `dur_very_long`, `is_date_based`, `duration_is_negative_flag`, `duration_below_min_horizon_flag`, `log_horizon_x_liquidity`, `spread_over_liquidity` | `rule_baseline/features/market_feature_builders.py` �?`rule_baseline/features/tabular.py::apply_feature_variant()` | 这类列被删的原因不完全相同，但大致分三类：一是本身从终��?market state 派生，风险继承了上面�?leakage 问题；二是和当前保留�?interaction / term-structure / rule-quality 特征高度冗余；三是分布过于稀疏��近常量或信息增益很低��?|
| outcome/token 侧的身份�?| `primary_token_id`, `secondary_token_id`, `primary_outcome`, `secondary_outcome` | `rule_baseline/features/snapshot_semantics.py::build_decision_time_snapshot_row()` | 这些列本质上�?outcome identity。它们容易引�?market-specific memorization，��且对跨市场泛化帮助有限，因此不进入朢�终模型输入��?|

可以把这套规则记成一句话�?*先把扢�有可能有用的上下文拼宽，再在 `DROP_COLS` 里移除标签��身份��审计字段��终态泄漏字段，以及已经被更稳健特征替代的原�?market 列��?*

> Note: `log_horizon_x_liquidity` �?`spread_over_liquidity` 已同步从 `apply_feature_variant()` 默认特征路径移除，不只是“计算后再从 `DROP_COLS` 丢弃”��?

丢�个实用判断标准是：如果某列回答的是��这是哪丢�个市�?/ 哪一批数�?/ 朢�终结果是仢�么��，或��它来自 resolved market 的终态状态，而不�?`snapshot_time` 当下可观测状态，那么它��常应当留在审计层，而不应进入最终模型特征��?

### 5.3 特征契约 (Feature Contract)

模型输出丢�个明确的 **Feature Contract** (`feature_contract.json`)，定义了推理时需要的精确特征列表�?

```json
{
  "feature_columns": ["price", "horizon_hours", "r_std", "domain", "category", ...],
  "numeric_columns": ["price", "horizon_hours", "r_std", "offset_sec", ...],
  "categorical_columns": ["domain", "category", "market_type"],
  "critical_columns": ["price", "horizon_hours", "domain", "category", "market_type"],
  "noncritical_columns": ["rule_edge_buffer", "rule_confidence_ratio", ...]
}
```

这确保了在线推理时特征的丢�致��?�?如果缺失 critical 特征则拒绝预测，如果缺失 noncritical 特征则用默认值填充��?

### 5.4 训练漏斗审计

`snapshot_training_audit.py` 追踪数据在每个环节的保留率：

```
snapshots_loaded: 50,000 �?
  �?quality_pass:    42,000 �? (84%)
  �?in_date_range:   38,000 �? (90%)
  �?rule_matched:    30,000 �? (79%)
  �?train_split:     18,000 �?
  �?valid_split:      6,000 �?
  �?test_split:       6,000 �?
```

### 5.5 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/processed/snapshots.csv` |
| | `data/edge/trading_rules.csv` |
| | `data/domain/market_domain_features.csv` |
| | `data/intermediate/raw_markets_merged.csv` |
| **输出** | `{offline}/models/q_model_bundle_deploy/` (部署模型�? |
| | `{offline}/models/q_model_bundle_full/` (完整训练模型�? |
| | `{offline}/predictions/snapshots_with_predictions.csv` |
| | `{offline}/audit/snapshot_training_funnel.json` |
| | `{offline}/audit/snapshot_training_funnel.md` |

---

## Step 6: GroupKey 验证报告

> **脚本**: `rule_baseline/training/build_groupkey_validation_reports.py`  
> **不可跳过**（训练阶段后必须执行�? 
> **CLI 参数**: `--artifact-mode`

### 6.1 做什�?

在规则训�?(Step 4) 和模型训�?(Step 5) 完成后，�?GroupKey 的迁移一致����特征契约完整��和 serving 资产 schema 进行自动化校验，生成 Markdown 格式的诊断报告��?

### 6.2 详细流程

```
trading_rules.csv + feature_contract.json + serving features
      �?
      �?
write_groupkey_reports(artifact_mode)
  �?
  ├─┢� build_migration_validation_markdown()
  �?    - 加载 snapshots + rules + serving feature bundles
  �?    - 追踪快照�?raw �?quality_pass �?rule_matched �?model_scored 的漏�?
  �?    - �?selection_status 统计市场影响
  �?    - 生成迁移验证 Markdown
  �?
  ├─┢� build_consistency_report_markdown()
  �?    - 加载 feature_contract.json (deploy �?full_training bundle)
  �?    - 对比 feature contract �?vs 实际 serving asset �?
  �?    - 棢�测列名不匹配、类型冲突��缺失列
  �?    - 生成丢�致��报�?Markdown
  �?
  └─┢� build_schema_reference_markdown()
        - �?trading_rules.csv, group_serving_features.parquet,
          fine_serving_features.parquet, serving_feature_defaults.json
          进行 schema 描述
        - 对列按功能分�?
          · KEY_COLUMNS: group_key, domain, category, market_type, price_bin, horizon_hours
          · RULE_PRIOR_COLUMNS: leaf_id, direction, q_full, p_full, edge_*, rule_score, n_full
          · group_safe_serving, fine_only, fallback_defaults, �?
        - 生成 schema 参��?Markdown
      �?
      �?
输出 (写入 docs/ 目录)
  ├─┢� groupkey_migration_validation.md
  ├─┢� groupkey_consistency_report.md
  └─┢� groupkey_serving_schema_reference.md (如有)
```

### 6.3 为什么需要这丢��?

GroupKey �?(domain, category, market_type) 三维组合的唯丢�标识，贯穿规则训�?�?serving 特征 �?在线推理的全链路。此步骤确保�?

1. **迁移丢�致��?*: features_contract 中的列与实际 serving parquet 资产丢�丢�对应
2. **漏斗可见�?*: 从原始快照到朢�终模型预测，每个环节的数据保留率清晰可查
3. **Schema 文档�?*: 扢��?serving 资产的列语义、用途��分组自动记�?

### 6.4 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `{offline}/edge/trading_rules.csv` |
| | `{offline}/models/q_model_bundle_deploy/feature_contract.json` |
| | `{offline}/edge/group_serving_features.parquet` |
| | `{offline}/edge/fine_serving_features.parquet` |
| | `{offline}/edge/serving_feature_defaults.json` |
| **输出** | `docs/groupkey_migration_validation.md` |
| | `docs/groupkey_consistency_report.md` |

---

## Step 7: 模型校准分析

> **脚本**: `rule_baseline/analysis/analyze_q_model_calibration.py`  
> **跳过参数**: `--skip-analysis`

### 7.1 做什�?

�?*严格�?test split**（若存在）或 latest valid split（当前 offline 默认）上，评估模型预测�?`q_pred` 的校准质量��核心问题是�?当模型说某事件有 70% 概率发生时，它是否真的约�?70% 的时间发生？"

### 7.2 详细流程

```
snapshots_with_predictions.csv (published evaluation split: test if present, otherwise valid)
      �?
      �?
compute_metrics() �?计算三大指标 (vs 市场价格基线)
  �?
  ├─┢� Logloss Delta
  �?    - model_logloss = -mean(y*log(q) + (1-y)*log(1-q))
  �?    - baseline_logloss = 同上 但用 price 代替 q
  �?    - delta = model - baseline
  �?    - delta < 0 表示模型优于市场
  �?
  ├─┢� Brier Score Delta
  �?    - model_brier = mean((q - y)²)
  �?    - baseline_brier = mean((price - y)²)
  �?    - delta = model - baseline
  �?    - delta < 0 表示模型优于市场
  �?
  └─┢� AUC Delta
        - model_auc = ROC-AUC(y, q)
        - baseline_auc = ROC-AUC(y, price)
        - delta = model - baseline
        - delta > 0 表示模型优于市场
      �?
      �?
Reliability Table �?校准可靠性表
  - �?q_pred 分为 10 �?quantile buckets
  - 对每�?bucket:
    - q_bucket: bucket 中心
    - y_rate: 实际发生�?
    - q_mean: 模型预测均��?
    - edge_true = y_rate - price_mean (真实 edge)
    - edge_model = q_mean - price_mean (模型预测 edge)
  - 完美校准: y_rate �?q_mean (对角�?
      �?
      �?
Edge Bucket Analysis �?Edge 大小 vs 准确�?
  - �?|edge_model| = |q_pred - price| 分为 5 �?quintile
  - 分析�?edge 预测是否真的更盈�?
  - 验证 edge magnitude �?actual profit 的单调��?
      �?
      �?
输出
  ├─┢� calibration_metrics.csv (logloss_delta, brier_delta, auc_delta)
  ├─┢� calibration_reliability.csv (10-bin 校准�?
  └─┢� calibration_edge_buckets.csv (5-quintile edge 分析)
```

### 7.3 如何解读结果

| 指标 | 好的信号 | 坏的信号 |
|------|---------|---------|
| Logloss Delta < 0 | 模型的概率估计优于市�?| 模型不如市场定价 |
| Brier Delta < 0 | 模型的均方误差更�?| 模型更不准确 |
| AUC Delta > 0 | 模型的排序能力更�?| 模型排序能力不如市场 |
| Reliability 对角线偏�?| 偏差 < 2% 为优秢� | > 5% 霢�要调校准 |

### 7.4 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `{offline}/predictions/snapshots_with_predictions.csv` |
| **输出** | `{offline}/analysis/calibration_metrics.csv` |
| | `{offline}/analysis/calibration_reliability.csv` |
| | `{offline}/analysis/calibration_edge_buckets.csv` |

---

## Step 8: Alpha 象限分析

> **脚本**: `rule_baseline/analysis/analyze_alpha_quadrant.py`  
> **跳过参数**: `--skip-analysis`

### 8.1 做什�?

将模型预测分�?*四个象限**，衡量模型是否能�?*逆市场共�?*时做出正确判断��真正的 alpha 来自 "市场错了，模型对�? 的情况��?

### 8.2 四象限定�?

```
                    模型预测正确          模型预测错误
             ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
  逆共�?     �? contrarian_correct �? contrarian_wrong  �?
  |q-p|>5%   �? (真正�?Alpha �?  �? (被市场惩�?      �?
             ├─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┼─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
  顺共�?     �? consensus_correct  �? consensus_wrong   �?
  |q-p|�?%   �? (跟随市场也对)    �? (市场丢�起错)      �?
             └─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┴─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
```

- **逆共�?(Contrarian)**: `|q_pred - price| > 0.05` �?模型认为市场价格有误
- **顺共�?(Consensus)**: `|q_pred - price| �?0.05` �?模型认同市场价格
- **正确的判�?*: 模型预测方向与最终结果一�?

### 8.3 详细流程

```
snapshots_with_predictions.csv (published evaluation split)
      �?
      �?
classify_quadrant() �?逐行分类
  - is_contrarian = |q_pred - price| > 0.05
  - model_correct = (q_pred > 0.5 AND y == 1) OR (q_pred <= 0.5 AND y == 0)
  - quadrant = 2维组�?�?contrarian_correct / contrarian_wrong / ...
      �?
      �?
compute_quadrant_metrics() �?象限汇��?
  - 每个象限: n�? 占比%, mean_edge, mean_signal, brier_market, brier_model
      �?
      �?
compute_alpha_score() �?Alpha 得分
  - alpha_ratio = contrarian_correct / (contrarian_correct + contrarian_wrong)
  - net_alpha = alpha_ratio - (1 - consensus_accuracy)
  - weighted_score = alpha_ratio × contrarian% (考虑信号密度)
      �?
      �?
slice_alpha() �?多维度切�?
  ├─┢� alpha_by_category: SPORTS / CRYPTO / FINANCE / ...
  ├─┢� alpha_by_domain: 细粒度域
  └─┢� alpha_by_horizon: 1h / 2h / 4h / 6h / 12h / 24h
      �?
      �?
输出
  ├─┢� alpha_quadrant_metrics.csv
  ├─┢� alpha_summary.csv (overall alpha_ratio, net_alpha)
  ├─┢� alpha_by_category/domain/horizon.csv
  └─┢� predictions_with_quadrant.csv (标注了象限的预测)
```

### 8.4 如何解读结果

| 指标 | 强信�?| 弱信�?|
|------|--------|--------|
| `alpha_ratio > 60%` | 逆市场判断有超过 60% 准确�?| 逆市场判断不可靠 |
| `net_alpha > 10%` | 扣除共识误差后仍有净 alpha | 霢�要更好的模型 |
| `contrarian% > 30%` | 模型频繁产生逆共识信�?| 信号太稀�?|

### 8.5 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `{offline}/predictions/snapshots_with_predictions.csv` |
| **输出** | `{offline}/analysis/alpha_quadrant_metrics.csv` |
| | `{offline}/analysis/alpha_summary.csv` |
| | `{offline}/analysis/alpha_by_*.csv` |
| | `{offline}/analysis/predictions_with_quadrant.csv` |

---

## Step 9: 规则 Alpha 象限分析

> **脚本**: `rule_baseline/analysis/analyze_rules_alpha_quadrant.py`  
> **跳过参数**: `--skip-analysis`

### 9.1 做什�?

�?Step 4 产生的规则（而非 Step 5 的模型）进行象限分析。评估每条规则在优先 test、否则 valid 的评估 split 上的 alpha 质量，按 `leaf_id` / `group_key` 排名�?

### 9.2 详细流程

```
trading_rules.csv + snapshots.csv (test split)
      �?
      �?
match_rules_to_snapshots()
  - �?test 快照与规则按 (domain, category, market_type) + price/horizon 范围匹配
  - 每个快照得到对应�?rule direction �?edge 预测
      �?
      �?
classify_rule_quadrant()
  - is_contrarian: 规则方向是否与市场价格暗示相�?
    · rule_says_buy (+1) AND price > 0.5 �?共识 (市场也看�?
    · rule_says_buy (+1) AND price < 0.5 �?逆共�?(市场看跌但规则看�?
  - rule_correct: 规则方向 × 实际 edge > 0
  - quadrant = contrarian_correct / consensus_correct / ...
      �?
      �?
compute_rule_metrics() �?逐规则统�?
  - 每条规则 (leaf_id):
    · alpha_ratio: 逆共识正�?
    · actual_edge: 真实平均 edge
    · contrarian_pct: 逆共识信号占�?
    · mean_pnl: 平均每笔 PnL
    · n_test_snaps: test 样本�?
      �?
      �?
输出
  ├─┢� rules_alpha_metrics.csv (逐规�?alpha 统计)
  └─┢� rules_predictions_with_quadrant.csv
```

### 9.3 �?Step 8 的区�?

| 维度 | Step 8 (模型 Alpha) | Step 9 (规则 Alpha) |
|------|---------------------|---------------------|
| 信号来源 | AutoGluon 模型�?`q_pred` | 朴素频率规则�?`direction` |
| 粒度 | 全局或按 category/domain/horizon 切片 | 逐条规则 (leaf_id) |
| 用��?| 评估模型整体 alpha 质量 | 识别哪些规则真正有效 |

### 9.4 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/edge/trading_rules.csv` |
| | `data/processed/snapshots.csv` |
| **输出** | `{offline}/analysis/rules_alpha_metrics.csv` |
| | `{offline}/analysis/rules_predictions_with_quadrant.csv` |

---

## Step 10: 回测执行丢�致��?

> **脚本**: `rule_baseline/backtesting/backtest_execution_parity.py`  
> **跳过参数**: `--skip-backtest`  
> **仅在 `--artifact-mode offline` 时执�?*

### 10.1 做什�?

�?*与在线执行完全一致的逻辑**�?test split 上进行完整的 portfolio backtest。验证系统从信号生成到下单执行的全链路盈利能力��?

### 10.2 详细流程

```
snapshots_with_predictions.csv + trading_rules.csv
      �?
      �?
prepare_execution_candidates()
  �?
  ├─┢� 规则匹配: 快照 × 规则 �?候��交�?
  ├─┢� 模型打分: predict_candidates() �?q_pred, trade_value
  ├─┢� Earliest-Market Dedup
  �?    - 同一 market_id 只保留最早的 horizon 的快�?
  �?    - 避免对同丢�市场重复下注
  �?
  └─┢� 计算交易参数
        - direction: +1 (�?Yes) �?-1 (�?No)
        - kelly_size: Kelly 分数 × bankroll × MAX_POSITION_F
        - entry_price: price (if buy Yes) or 1-price (if buy No)
      �?
      �?
run_execution_parity_backtest() �?日级 Portfolio 模拟
  �?
  ├─┢� 初始�?
  �?    - INITIAL_BANKROLL = $10,000
  �?    - equity_curve = [10000]
  �?    - open_positions = {}
  �?
  ├─┢� 逐日循环 (�?snapshot_date 排序)
  �?    �?
  �?    ├─┢� 棢�查结�?(Settlement)
  �?    �?    - 已结算的市场 (closedTime <= current_date):
  �?    �?      · 计算 realized PnL
  �?    �?      · 释放冻结资金
  �?    �?      · 更新 equity
  �?    �?
  �?    ├─┢� 风险棢��?(Risk Limits)
  �?    �?    - MAX_POSITION_F = 0.02 (单笔朢��?2% equity)
  �?    �?    - MAX_DOMAIN_EXPOSURE_F = 0.20 (单域朢��?20% equity)
  �?    �?    - MAX_CATEGORY_EXPOSURE_F = 0.25 (单类朢��?25% equity)
  �?    �?    - MAX_SETTLEMENT_EXPOSURE_F = 0.20 (近结算最�?20%)
  �?    �?    - MAX_SIDE_EXPOSURE_F = 0.30 (单边朢��?30%)
  �?    �?    - MAX_DAILY_TRADES = 80 (日内朢�大交易数)
  �?    �?
  �?    ├─┢� 弢��?(Entry)
  �?    �?    - �?trade_value 排序，依次开�?
  �?    �?    - 仓位 = min(kelly_size, MAX_TRADE_AMOUNT=1000)
  �?    �?    - 冻结资金 = position × entry_price
  �?    �?
  �?    └─┢� 记录日终状��?
  �?          - equity, NAV, open_positions_count
  �?          - realized_pnl_today, unrealized_pnl
  �?
  └─┢� 朢�终清�? 扢�有未结算仓位按模拟价格清�?
      �?
      �?
计算业绩指标
  �?
  ├─┢� 收益�?
  �?    - total_return, annualized_return, daily_avg_pnl
  �?
  ├─┢� 风险�?
  �?    - max_drawdown, max_drawdown_duration
  �?    - daily_volatility, downside_deviation
  �?
  ├─┢� 风险调整�?
  �?    - Sharpe Ratio = annualized_return / (daily_vol × �?52)
  �?    - Sortino Ratio = annualized_return / (downside_dev × �?52)
  �?    - Calmar Ratio = annualized_return / max_drawdown
  �?
  └─┢� 交易�?
        - total_trades, win_rate
        - avg_holding_hours, same_day_settlement_%
        - pnl_per_trade
      �?
      �?
输出
  ├─┢� equity_df.csv (日级 equity curve)
  ├─┢� trades_df.csv (逐笔交易记录)
  ├─┢� daily_df.csv (日终统计)
  └─┢� skip_records.csv (被风控跳过的交易)
```

### 10.3 Kelly Sizing 公式

```
edge = q_pred - entry_price
kelly_fraction = 0.25 × (edge / odds)     # odds = 1/entry_price - 1

position_size = min(
    kelly_fraction × current_equity × MAX_POSITION_F,
    MAX_TRADE_AMOUNT   # $1,000 硬限
)
```

### 10.4 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `{offline}/predictions/snapshots_with_predictions.csv` |
| | `data/edge/trading_rules.csv` |
| | `data/domain/market_domain_features.csv` |
| **输出** | `{offline}/backtesting/equity_df.csv` |
| | `{offline}/backtesting/trades_df.csv` |
| | `{offline}/backtesting/daily_df.csv` |
| | `{offline}/backtesting/skip_records.csv` |

---

## Step 11: 基线族对�?

> **脚本**: `rule_baseline/analysis/compare_baseline_families.py`  
> **跳过参数**: `--skip-baselines`  
> **仅在 `--artifact-mode offline` 时执�?*  
> **CLI 参数**: `--walk-forward-windows` (默认 3), `--walk-forward-step-days`

### 11.1 做什�?

�?**Walk-Forward 验证框架**下对�?4 个不同的 baseline 模型族，评估哪种建模策略朢�稳定有效。这不是选择朢�终模型，而是**验证 q_only 方法是否朢��?*，以及不同策略在不同 domain/horizon 上的表现差异�?

### 11.2 四个 Baseline Family

| Family | 策略 | 预测目标 | Edge 计算 |
|--------|------|---------|-----------|
| **q_only** (默认) | 直接预测 P(Y=1) | y (binary) | edge = q_pred - price |
| **residual_q** | 预测残差 | residual = y - price | q = price + residual_pred, edge = residual_pred |
| **tradeable_only** | 盈利概率分类�?| is_profitable (binary) | edge = P(profitable) - 0.5 |
| **two_stage** | 两阶段模�?| (1) P(profitable) (2) E[edge\|profitable] | combined_edge |

### 11.3 详细流程

```
snapshots.csv + raw_markets_merged.csv + market_domain_features.csv
      �?
      �?
build_walk_forward_splits()
  - 将时间线分成 N 个滚动窗�?(默认 N=3)
  - 每个窗口: train_window �?valid_window �?test_window
  - step_days: 相邻窗口的移动步�?
      �?
      �?
┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢� 对每个窗口循�?┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
�?                                         �?
�? build_window_feature_frame()            �?
�?   - 构建该窗口的规则 + 特征�?          �?
�?   - train_rules �?match �?features      �?
�?                                         �?
�? fit_baselines() �?训练 4 个模�?        �?
�?   ├─┢� q_only:       AutoGluon binary    �?
�?   ├─┢� residual_q:   AutoGluon regressor �?
�?   ├─┢� tradeable:    AutoGluon binary    �?
�?   └─┢� two_stage:    2× AutoGluon        �?
�?                                         �?
�? compute_slice_metrics() �?评估每个模型  �?
�?   ├─┢� top_k_precision (k=50/100/200)    �?
�?   ├─┢� top_k_recall                      �?
�?   ├─┢� AUC                               �?
�?   ├─┢� mean_signed_edge                  �?
�?   └─┢� �?domain/category/horizon 切片   �?
�?                                         �?
�? run_flat_backtest() �?箢�化回�?         �?
�?   - 每日�?trade_value 排序下注         �?
�?   - MAX_DAILY_TRADES 限制              �?
�?   - 计算 cumulative PnL                 �?
�?                                         �?
└─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
      �?
      �?
aggregate_walk_forward() �?跨窗口聚�?
  - 每个 family 在所有窗口上�?
    · mean/std precision, recall, AUC
    · 信号稳定�?(跨窗�?rank correlation)
    · Sharpe ratio across windows
      �?
      �?
输出
  ├─┢� baseline_comparison_latest.csv (朢�新窗口结�?
  ├─┢� baseline_comparison_walk_forward.csv (滚动验证汇��?
  ├─┢� baseline_signals_by_domain.csv (域级信号�?
  └─┢� baseline_stability_summary.csv (稳定性报�?
```

### 11.4 如何解读结果

理想�?baseline family 应该在所有维度上表现丢�致：

- **精度 (Precision)**: top 100 信号中实际盈利的比例
- **信号�?*: 每个 domain 上是否产生足够的交易信号
- **稳定�?*: 跨窗口的 performance 波动尽量�?
- **Sharpe**: 风险调整收益

### 11.5 输入 / 输出

| | 路径 |
|---|------|
| **输入** | `data/processed/snapshots.csv` |
| | `data/intermediate/raw_markets_merged.csv` |
| | `data/domain/market_domain_features.csv` |
| **输出** | `{offline}/analysis/baseline_comparison_latest.csv` |
| | `{offline}/analysis/baseline_comparison_walk_forward.csv` |
| | `{offline}/analysis/baseline_signals_by_domain.csv` |
| | `{offline}/analysis/baseline_stability_summary.csv` |

---

## 数据流与产物清单

### 完整数据流图

```
                         Gamma API
                            �?
                            �?
                   ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
Step 1 ┢�┢�┢�┢�┢�┢��?   �?raw_markets_merged�?
                   �?   .csv          �?
                   └─┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢��?
                            �?
                   ┌─┢�┢�┢�┢�┢�┢�┢�┴─┢�┢�┢�┢�┢�┢�┢�┢��?
                   �?                 �?
         ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?   ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
Step 2 ┢�▶│market_domain �?   �? CLOB API    �?
         │_features.csv �?   �?prices-hist) �?
         └─┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢��?   └─┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢��?
                �?                  �?
                └─┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢�┢��?
                         �?
                ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
Step 3 ┢�┢�┢�┢�┢�┢��? �? snapshots.csv    �?
                └─┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢�┢��?
                         �?
              ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┴─┢�┢�┢�┢�┢�┢�┢�┢�┢��?
              �?                    �?
    ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��? ┌─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
    �?trading_rules.csv�? �?                          �?
    �?  (Step 4)       �? �? snapshots_with_          �?
    └─┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢��? �? predictions.csv          �?
             �?           �?  (Step 5)                �?
             └─┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┴─┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢�┢��?
                   �?
         ┌─┢�┢�┢�┢�┢�┢�┢�┢�┼─┢�┢�┢�┢�┢�┢�┢�┢�┢�┬─┢�┢�┢�┢�┢�┢�┢�┢�┢��?
         �?        �?         �?         �?
   Step 7-9    Step 10    Step 11    Model Bundle
   (Analysis) (Backtest) (Baselines) (Deploy)
```

### 产物目录结构

```
data/
├─┢� raw/
�?  ├─┢� batches/fetch_*.csv              �?Step 1
�?  └─┢� batch_manifest.csv              �?Step 1
├─┢� intermediate/
�?  ├─┢� raw_markets_merged.csv           �?Step 1
�?  └─┢� raw_market_quarantine.csv        �?Step 1
├─┢� domain/
�?  ├─┢� market_domain_features.csv       �?Step 2
�?  └─┢� domain_summary*.csv              �?Step 2
├─┢� processed/
�?  ├─┢� snapshots.csv                    �?Step 3
�?  ├─┢� snapshot_market_audit.csv        �?Step 3
�?  └─┢� snapshots_quarantine.csv         �?Step 3
├─┢� edge/
�?  └─┢� trading_rules.csv               �?Step 4
├─┢� audit/
�?  └─┢� all_trading_rule_audit_report.csv �?Step 4
└─┢� offline/
    ├─┢� models/
    �?  ├─┢� q_model_bundle_deploy/       �?Step 5 (部署 bundle)
    �?  └─┢� q_model_bundle_full/         �?Step 5 (完整训练 bundle)
    ├─┢� predictions/
    �?  ├─┢� snapshots_with_predictions.csv     �?Step 5
    �?  └─┢� snapshots_with_predictions_all.csv �?Step 5
    ├─┢� audit/
    �?  ├─┢� snapshot_training_funnel.*    �?Step 5
    �?  ├─┢� rule_funnel_summary.json     �?Step 4
    �?  ├─┢� rule_generation_audit.*      �?Step 4
    �?  └─┢� artifact_inventory.*         �?Step 4
    ├─┢� metadata/
    �?  ├─┢� split_summary.json           �?Step 4/5
    �?  ├─┢� rule_training_summary.json   �?Step 4
    �?  └─┢� model_training_summary.json  �?Step 5
    ├─┢� analysis/
    �?  ├─┢� calibration_*.csv            �?Step 7
    �?  ├─┢� alpha_*.csv                  �?Step 8
    �?  ├─┢� rules_alpha_*.csv            �?Step 9
    �?  └─┢� baseline_*.csv              �?Step 11
    └─┢� backtesting/
        ├─┢� equity_df.csv               �?Step 10
        ├─┢� trades_df.csv               �?Step 10
        └─┢� daily_df.csv                �?Step 10

docs/ (项目根目录下)
    ├─┢� groupkey_migration_validation.md    �?Step 6
    └─┢� groupkey_consistency_report.md      �?Step 6
```

---

## 核心常量参��表

### 数据质量阈��?

| 常量 | �?| 用��?|
|------|---|------|
| `MIN_MARKET_VOLUME` | 10.0 | 朢�低市场成交量 |
| `MIN_MARKET_LIQUIDITY` | 50.0 | 朢�低市场流动��?|
| `SNAP_WINDOW_SEC` | 300 (5min) | 价格查找窗口 |
| `STALE_QUOTE_MAX_OFFSET_SEC` | 120 (2min) | 报价时间偏移上限 |
| `STALE_QUOTE_MAX_GAP_SEC` | 900 (15min) | 报价间隔上限 |
| `HORIZONS` | [1,2,4,6,12,24] | 决策时间窗口 (小时) |

### 规则训练参数

| 常量 | �?| 用��?|
|------|---|------|
| `TRAIN_PRICE_MIN` | 0.2 | 可交易价格下�?|
| `TRAIN_PRICE_MAX` | 0.8 | 可交易价格上�?|
| `RULE_PRICE_BIN_STEP` | 0.1 | 价格分桶步长 |
| `MIN_GROUP_UNIQUE_MARKETS` | 15 | 朢�少不同市场数 |
| `BETA_PRIOR_STRENGTH` | 20.0 | 先验强度 |

### 时间分割参数

| 常量 | �?| 用��?|
|------|---|------|
| `VALIDATION_DAYS` | 30 | 验证集天�?|
| `TEST_DAYS` | 30 | walk-forward / strict test 天数（offline artifact split 默认不单独发布 test） |
| `ONLINE_VALIDATION_DAYS` | 20 | 在线模式验证天数 |
| `DATE_START_STR` | "2024-10-31" | 历史数据起始�?|
| `RAW_FETCH_OVERLAP_HOURS` | 72 | 增量拉取重叠窗口 |

### 回测风控参数

| 常量 | �?| 用��?|
|------|---|------|
| `INITIAL_BANKROLL` | $10,000 | 初始资金 |
| `KELLY_FRACTION` (execution_parity) | 0.25 | 执行丢�致��回�?Kelly 缩减系数 |
| `KELLY_FRACTION` (portfolio_qmodel) | 0.10 | 组合回测 Kelly 缩减系数 |
| `MAX_POSITION_F` | 0.02 (2%) | 单笔仓位上限 (% equity) |
| `MAX_TRADE_AMOUNT` | $1,000 | 单笔金额硬限 (execution_parity) |
| `MAX_TIME_TO_EXPIRY_HOURS` | 24 | 朢�大距结算小时�?(execution_parity) |
| `MAX_DAILY_TRADES` | 80 | 日内交易数上�?(portfolio_qmodel) |
| `MAX_DAILY_EXPOSURE_F` | 0.50 (50%) | 日曝光上�?(portfolio_qmodel) |
| `MAX_DOMAIN_EXPOSURE_F` | 0.20 (20%) | 单域曝光上限 |
| `MAX_CATEGORY_EXPOSURE_F` | 0.25 (25%) | 单类曝光上限 |
| `MAX_CLUSTER_EXPOSURE_F` | 0.15 (15%) | 单集群曝光上�?|
| `MAX_SETTLEMENT_EXPOSURE_F` | 0.20 (20%) | 近结算曝光上�?|
| `MAX_SIDE_EXPOSURE_F` | 0.30 (30%) | 单边曝光上限 |
| `TOP_K_RULES` | 100 | 组合回测保留的规则数 |
| `MIN_RULE_VALID_N` | 20 | 规则有效的最小样本量 |
| `MIN_EDGE_TRADE` | 0.02 | 朢�低交�?edge |
| `MIN_PROB_EDGE` | 0.02 | 朢�低概�?edge |
| `RULE_ROLLING_WINDOW_TRADES` | 50 | 规则滚动窗口交易�?|
| `RULE_KILL_THRESHOLD` | -0.2 | 规则淘汰阈��?|
| `RULE_COOLDOWN_DAYS` | 5 | 规则冷却天数 |
| `FEE_RATE` | 0.0 | 回测手续费率 |

### 模型训练参数

| 常量 | �?| 用��?|
|------|---|------|
| `DEFAULT_AUTOGUON_PRESETS` | "medium_quality" | AutoGluon 预设精度 |
| `DEFAULT_TIME_LIMIT` | 300s (5min) | AutoGluon 训练时限 |
| `DEFAULT_CALIBRATION_MODE` | "global_isotonic" | 默认校准方式 |
| `DEFAULT_GROUP_COLUMN` | "horizon_hours" | 分组校准默认�?|
| `DEFAULT_GROUP_MIN_ROWS` | 20 | 分组校准朢�小行�?|
| `DEFAULT_RANDOM_SEED` | 21 | 默认随机种子 |
| `EDGE_THRESHOLD` | 0.05 | Alpha 象限逆共识阈�?|
| `FDR_ALPHA` | 0.10 | 错误发现率控制阈�?|

---

## 附录: Pipeline CLI 完整用法

```bash
# 完整 offline pipeline (扢��?11 �?
python rule_baseline/workflow/run_pipeline.py --artifact-mode offline

# 跳过数据采集，只跑训�?验证+分析+回测
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --skip-fetch --skip-annotations --skip-snapshots

# 仅训�?验证 (跳过分析和回�?
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --skip-fetch --skip-annotations --skip-snapshots \
    --skip-analysis --skip-backtest --skip-baselines

# 指定时间范围
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --date-start 2025-01-01 --date-end 2025-06-30

# 调整模型参数
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --calibration-mode grouped_isotonic \
    --grouped-calibration-column horizon_hours \
    --grouped-calibration-min-rows 20 \
    --target-mode residual_q \
    --predictor-time-limit 600 \
    --random-seed 42

# Bagging & Stacking 配置
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --num-bag-folds 5 \
    --num-bag-sets 2 \
    --num-stack-levels 1 \
    --auto-stack

# 全量数据刷新
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --full-refresh-fetch --full-refresh-snapshots

# Walk-forward windows 调整
python rule_baseline/workflow/run_pipeline.py \
    --artifact-mode offline \
    --walk-forward-windows 5 --walk-forward-step-days 14

# Online pipeline (通过封装脚本)
python rule_baseline/workflow/run_online_pipeline.py
# 等价�? --artifact-mode online --skip-backtest --skip-baselines
```

---

*基于 `run_pipeline.py` 子进程调用链�?`rule_baseline/` 全模块静态分析生成��?



