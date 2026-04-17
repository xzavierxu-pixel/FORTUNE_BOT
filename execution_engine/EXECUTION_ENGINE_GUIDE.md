# Execution Engine — 在线执行管线详细步骤指南

> **入口脚本**: `execution_engine/app/cli/online/main.py`  
> **运行方式**: 统一 CLI，通过子命令调度 **6 个核心功能**：`run-submit-window`、`stream-market-data`、`monitor-orders`、`label-analysis-daily`、`run-submit-window-post-submit`、`compact-run-artifacts`。  
> **核心子命令**: `run-submit-window` 是主交易管线，串行执行"分页拉取 → 结构过滤 → WebSocket 流式报价 → 实时推理 → 选单分配 → 限价单提交 → 提交后监控"的完整周期。  
> **环境变量**: 所有运行时行为通过 `PEG_*` 环境变量控制（详见 [配置参考表](#配置参考表)）。

---

## 目录

1. [端到端流程总览](#1-端到端流程总览)
2. [CLI 入口与子命令](#2-cli-入口与子命令)
3. [运行时配置 (PegConfig)](#3-运行时配置-pegconfig)
4. [Submit Window 主管线](#4-submit-window-主管线)
   - [Phase 1: Prewarm — 预热加载离线产物](#phase-1-prewarm--预热加载离线产物)
   - [Phase 2: Universe — 分页拉取市场候选](#phase-2-universe--分页拉取市场候选)
   - [Phase 3: Eligibility — 结构化粗筛](#phase-3-eligibility--结构化粗筛)
   - [Phase 4: Streaming — WebSocket 实时报价采集](#phase-4-streaming--websocket-实时报价采集)
   - [Phase 5: Scoring — 实时推理与评分](#phase-5-scoring--实时推理与评分)
   - [Phase 6: Selection — 候选排序与分配](#phase-6-selection--候选排序与分配)
   - [Phase 7: Submission — 限价单构建与提交](#phase-7-submission--限价单构建与提交)
   - [Phase 8: Post-Submit Monitor — 提交后生命周期监控](#phase-8-post-submit-monitor--提交后生命周期监控)
5. [Stream Market Data 子命令](#5-stream-market-data-子命令)
6. [Monitor Orders 子命令](#6-monitor-orders-子命令)
7. [Exit 管理 (退出单)](#7-exit-管理退出单)
8. [Label Analysis 子命令](#8-label-analysis-子命令)
9. [Artifact Retention (产物压缩)](#9-artifact-retention-产物压缩)
10. [Runtime 层：状态、决策与风控](#10-runtime-层状态决策与风控)
11. [数据流与产物清单](#11-数据流与产物清单)
12. [离线产物依赖清单](#12-离线产物依赖清单)
13. [核心常量参考表](#13-核心常量参考表)
14. [配置参考表](#配置参考表)

---

## 1. 端到端流程总览

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│              execution_engine CLI (main.py)                                      │
│                                                                                 │
│  ┌────────────── 主交易管线: run-submit-window ────────────────────────────────┐ │
│  │                                                                             │ │
│  │  Phase 1  Prewarm (预热)                                                    │ │
│  │    加载 rules_frame, model_payload, serving_feature_bundle,                 │ │
│  │    feature_contract, rule_horizon_profile, fee_rate                          │ │
│  │     │                                                                       │ │
│  │     ▼                                                                       │ │
│  │  Phase 2  Universe — 分页拉取 Gamma API                                     │ │
│  │    fetch_event_page() × N 页                                                │ │
│  │     │                                                                       │ │
│  │     ▼ (每页进入以下流水线)                                                    │ │
│  │  Phase 3  Eligibility — 结构化粗筛                                           │ │
│  │    apply_structural_coarse_filter()                                          │ │
│  │     │                                                                       │ │
│  │     ▼ (direct_candidates 进入批次队列)                                       │ │
│  │  Phase 4  Streaming — 每批次 WebSocket 报价采集                              │ │
│  │    stream_market_data() → token_state                                       │ │
│  │     │                                                                       │ │
│  │     ▼                                                                       │ │
│  │  Phase 5  Scoring — 实时推理                                                │ │
│  │    run_live_inference()                                                      │ │
│  │    ├── apply_live_price_filter()                                            │ │
│  │    ├── _build_live_snapshot_rows()                                          │ │
│  │    ├── _build_market_feature_context()                                      │ │
│  │    ├── build_market_feature_cache()                                         │ │
│  │    ├── _build_live_rule_hits() + collapse + reasons                         │ │
│  │    ├── prepare_feature_inputs()                                             │ │
│  │    ├── _ensure_feature_contract()                                           │ │
│  │    ├── _predict_from_feature_inputs()                                       │ │
│  │    ├── compute_growth_and_direction()                                       │ │
│  │    └── 后处理: dedup + apply_earliest_market_dedup()                        │ │
│  │     │                                                                       │ │
│  │     ▼                                                                       │ │
│  │  Phase 6  Selection — 候选排序与 Kelly 分配                                  │ │
│  │    filter_candidates_by_growth_score()                                      │ │
│  │    allocate_candidates()                                                    │ │
│  │    build_selection_decisions()                                              │ │
│  │     │                                                                       │ │
│  │     ▼                                                                       │ │
│  │  Phase 7  Submission — 限价单提交                                            │ │
│  │    submit_selected_orders()                                                 │ │
│  │    ├── _wait_for_capacity()                                                 │ │
│  │    ├── get_live_quote()                                                     │ │
│  │    ├── build_submission_signal()                                            │ │
│  │    ├── check_price_and_liquidity()                                          │ │
│  │    ├── check_basic_risk()                                                   │ │
│  │    ├── build_decision_from_signal()                                         │ │
│  │    └── submit_order() → CLOB API                                           │ │
│  │     │                                                                       │ │
│  │     ▼ (所有页处理完毕后)                                                     │ │
│  │  Phase 8  Post-Submit Monitor                                               │ │
│  │    monitor_order_lifecycle() + manage_exit_lifecycle()                       │ │
│  │                                                                             │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  ┌────── 辅助子命令 ──────────────────────────────────────────────────────────┐ │
│  │  stream-market-data    — 独立 WebSocket 流式数据采集                        │ │
│  │  monitor-orders        — 独立订单生命周期监控与对账                          │ │
│  │  label-analysis-daily  — 每日标签对齐与 PnL 分析                            │ │
│  │  compact-run-artifacts — 历史运行产物压缩清理                               │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**主管线的逻辑关系**：

| 阶段 | 目的 | 关键模块 |
|------|------|---------|
| Prewarm | 一次性加载所有离线产物到内存，后续批次零加载延迟 | `pipeline/prewarm.py` |
| Universe | 从 Gamma API 分页拉取当前开放市场，解析为标准化候选表 | `universe/page_source.py` |
| Eligibility | 按时间窗、规则族覆盖、订单状态等维度快速筛除不合格市场 | `pipeline/eligibility.py` |
| Streaming | 对每批候选的 reference token 进行短时 WebSocket 订阅，获取实时盘口 | `streaming/manager.py` |
| Scoring | 构建决策时刻快照 → 市场特征上下文 → 特征缓存 → 规则匹配 → 特征工程 → 模型推理 → 计算 growth_score → 去重 | `scoring/live.py` |
| Selection | 按 growth_score 排序，Kelly 公式分配仓位，构建选单决策表 | `scoring/selection.py` |
| Submission | 将选中的候选转换为限价单，经容量等待、报价查询、多层风控校验后通过 CLOB API 提交 | `execution/submission.py` |
| Post-Submit | 等待订单 TTL，对账 fill/cancel，管理退出单和结算 | `execution/monitor.py` |

---

## 2. CLI 入口与子命令

> **脚本**: `execution_engine/app/cli/online/main.py`

### 2.1 子命令列表

| 子命令 | 描述 | 典型调度方式 |
|--------|------|-------------|
| `run-submit-window` | **主交易管线**：分页拉取 → 推理 → 提交 → 监控 | systemd timer (每 15 分钟) |
| `run-submit-window-post-submit` | 独立执行提交后监控阶段（异步拆分） | systemd transient unit |
| `stream-market-data` | 独立 WebSocket 数据流采集 | 手动或定时 |
| `monitor-orders` | 独立订单生命周期监控 | systemd timer |
| `label-analysis-daily` | 每日标签对齐与 PnL 分析 | systemd timer (每日) |
| `compact-run-artifacts` | 压缩历史运行产物 | 手动或定时 |

### 2.2 通用参数

- `--run-id`: 覆盖 `PEG_RUN_ID` 环境变量
- `--run-date`: 覆盖 `PEG_RUN_DATE` 环境变量（仅 submit-window 类）

### 2.3 环境变量传递

CLI 在调用各子命令前会设置 `PEG_RUN_MODE` 环境变量（如 `submit_window`、`market_stream`、`order_monitor`、`label_analysis`），供 `load_config()` 正确构建 `PegConfig`。

---

## 3. 运行时配置 (PegConfig)

> **脚本**: `execution_engine/runtime/config.py`

### 3.1 做什么

`PegConfig` 是一个 frozen dataclass，包含 **150+ 个配置字段**，覆盖目录结构、风控参数、CLOB 集成、规则引擎路径和在线管线行为。所有配置均可通过 `PEG_*` 环境变量覆盖。

### 3.2 目录结构

```
execution_engine/data/
  ├── shared/                    ← 跨 run 共享状态
  │   ├── universe/              ← 当前市场宇宙快照
  │   ├── positions/             ← 持仓缓存
  │   ├── orders_live/           ← 订单/成交/持仓的共享导出
  │   ├── state/                 ← StateStore 快照 (state_snapshot.json)
  │   ├── token_state/           ← WebSocket token 状态 (CSV + JSON)
  │   ├── ws_raw/                ← WebSocket 原始事件存档
  │   └── labels/                ← 已结算标签缓存
  ├── runs/                      ← 每次运行的产物
  │   └── {run_date}/
  │       └── {run_id}/
  │           ├── decisions/     ← 决策记录
  │           ├── orders/        ← 订单日志 (JSONL)
  │           ├── events/        ← 候选状态事件流
  │           ├── fills/         ← 成交记录
  │           ├── rejections/    ← 拒绝记录
  │           ├── snapshots/     ← 评分快照产物
  │           ├── submit/        ← 提交尝试与结果
  │           ├── exits/         ← 退出单产物
  │           ├── labels/        ← 标签分析产物
  │           ├── reports/       ← 延迟写入报告
  │           └── manifest.json  ← run manifest
  └── summary/                   ← 全局 dashboard & index
      ├── runs_index.jsonl
      └── dashboard.html
```

### 3.3 离线产物路径解析

`PegConfig` 通过 `_first_existing_path()` 自动探测离线产物的实际位置，支持以下候选路径（按优先级）：

| 产物 | 候选路径 |
|------|---------|
| `trading_rules.csv` | `rule_baseline/datasets/trading_rules.csv` → `rule_baseline/datasets/edge/trading_rules.csv` → `data/offline/edge/trading_rules.csv` |
| `q_model_bundle_deploy` | `rule_baseline/datasets/models/q_model_bundle_deploy` → `data/offline/models/q_model_bundle_deploy` |
| `group_serving_features.parquet` | `data/offline/edge/group_serving_features.parquet` |
| `fine_serving_features.parquet` | `data/offline/edge/fine_serving_features.parquet` |
| `serving_feature_defaults.json` | `data/offline/edge/serving_feature_defaults.json` |

### 3.4 关键配置分组

| 分组 | 代表字段 | 含义 |
|------|---------|------|
| 运行时 | `dry_run`, `run_id`, `run_date`, `run_mode` | 当前运行标识与模式 |
| 资金 | `initial_bankroll_usdc`, `max_trade_amount_usdc`, `order_usdc` | 资金管理 |
| 风控 | `max_notional`, `daily_loss_limit`, `max_daily_orders`, `max_position_per_market_usdc`, `max_net_exposure_usdc`, `max_exposure_per_category_usdc`, `fat_finger_high/low` | 风险限制 |
| 信号 TTL | `signal_ttl_sec_min`, `signal_ttl_sec_max`, `order_ttl_sec` | 信号和订单有效期 |
| 价格验证 | `price_dev_abs`, `price_dev_rel`, `price_dev_spread_k`, `max_spread`, `min_depth_usdc` | 价格偏离与流动性检查 |
| 在线管线 | `online_universe_window_hours`, `online_market_batch_size`, `online_stream_duration_sec`, `online_min_growth_score` | 管线行为控制 |
| CLOB | `clob_host`, `clob_chain_id`, `clob_private_key`, `clob_api_key/secret/passphrase` | CLOB 交易所集成 |

---

## 4. Submit Window 主管线

> **入口**: `execution_engine/online/pipeline/submit_window.py::run_submit_window()`  
> **CLI**: `main.py run-submit-window [--max-pages N]`  
> **调度**: systemd timer 每 15 分钟触发

### 4.0 整体流程

```
run_submit_window(cfg, max_pages)
  │
  ├── read_submit_phase(cfg)           ← 检查是否已有活跃提交阶段
  │     如果存在 → 写入 "skipped" manifest，直接返回
  │
  ├── acquire_submit_phase(cfg)        ← 获取提交阶段锁
  │
  ├── _run_submit_window_sync_impl(cfg, max_pages)  ← 核心实现
  │   │
  │   ├── build_runtime_container(cfg)     ← Phase 1: Prewarm
  │   │
  │   ├── 循环: fetch_event_page()         ← Phase 2: Universe
  │   │   │
  │   │   └── _process_page()
  │   │       ├── _filter_held_event_candidates()      ← 排除已持仓 event
  │   │       ├── apply_structural_coarse_filter()      ← Phase 3: Eligibility
  │   │       ├── DirectCandidateQueue.add_frame()      ← 分批
  │   │       └── _process_batch() × N 批
  │   │           ├── stream_market_data()              ← Phase 4: Streaming
  │   │           ├── run_live_inference()              ← Phase 5: Scoring
  │   │           ├── select_target_side()              ← Phase 6: Selection
  │   │           ├── filter_candidates_by_growth_score()
  │   │           ├── allocate_candidates()
  │   │           ├── build_selection_decisions()
  │   │           ├── submit_selected_orders()          ← Phase 7: Submission
  │   │           └── _persist_batch_training_artifacts()
  │   │
  │   ├── _run_post_submit_monitor()       ← Phase 8: Post-Submit
  │   │
  │   └── publish_run_summary()            ← 写入 manifest + summary
  │
  ├── (可选) _spawn_async_post_submit()   ← 异步提交后监控 (systemd-run)
  │
  └── _rewrite_submit_window_manifest()    ← 最终化清单
```

---

### Phase 1: Prewarm — 预热加载离线产物

> **脚本**: `execution_engine/online/pipeline/prewarm.py`

#### 做什么

在 submit-window 开始前，**一次性**将所有离线产物加载到内存，构建 `OnlineRuntimeContainer`。后续所有批次共享同一个 container，避免重复 IO。

#### 加载的产物

```
build_runtime_container(cfg)
  │
  ├── load_rules_frame(cfg)              → rules_frame: pd.DataFrame
  │     来源: trading_rules.csv
  │     缓存: _RULES_CACHE (进程级)
  │
  ├── load_model_payload(cfg)            → model_payload
  │     来源: q_model_bundle_deploy/
  │     包含: predictor, calibrator, runtime_manifest, feature_contract
  │     校验: target_mode 必须为 "q"
  │     缓存: _MODEL_PAYLOAD_CACHE (进程级)
  │
  ├── load_serving_feature_bundle(cfg)   → ServingFeatureBundle
  │     ├── fine_features: fine_serving_features.parquet
  │     ├── group_features: group_serving_features.parquet
  │     └── defaults_manifest: serving_feature_defaults.json
  │
  ├── load_rule_horizon_profile(cfg)     → RuleHorizonProfile
  │     从 rules 中提取唯一的 (h_min, h_max) 区间列表
  │
  ├── get_feature_contract(model_payload) → FeatureContract
  │     包含: feature_columns, numeric_columns, categorical_columns,
  │           required_critical_columns, required_noncritical_columns
  │
  ├── load_rule_runtime(cfg)             → RuleRuntime
  │     从 rule_baseline 导入:
  │     ├── match_rules()
  │     ├── predict_candidates()
  │     ├── compute_growth_and_direction()
  │     ├── preprocess_features()
  │     ├── build_market_feature_cache()
  │     ├── apply_earliest_market_dedup()
  │     └── ExecutionParityConfig()
  │
  └── load_fee_rate(cfg)                 → fee_rate: float
```

#### OnlineRuntimeContainer 结构

```python
@dataclass(frozen=True)
class OnlineRuntimeContainer:
    cfg: PegConfig
    rule_runtime: RuleRuntime            # rule_baseline 函数引用
    rules_frame: pd.DataFrame            # trading_rules.csv
    serving_feature_bundle: ServingFeatureBundle  # group/fine/defaults
    horizon_profile: RuleHorizonProfile  # 规则覆盖的 horizon 区间
    model_payload: Any                   # AutoGluon 模型 + 校准器
    feature_contract: FeatureContract    # 特征契约
    fee_rate: float                      # 交易手续费率
```

---

### Phase 2: Universe — 分页拉取市场候选

> **脚本**: `execution_engine/online/universe/page_source.py`

#### 做什么

从 Gamma API 分页拉取当前开放的市场事件，解析为标准化的候选 DataFrame。每页包含多个 event，每个 event 展开为多个 market。

#### 详细流程

```
fetch_event_page(cfg, offset, limit, seen_market_ids)
  │
  ├── GammaMarketProvider.fetch_open_events_page()
  │     API: GET {gamma_base_url}/events
  │     参数: limit, offset, order="endDate", ascending=True
  │     返回: List[Dict] (原始 event JSON)
  │
  ├── 逐 event → 逐 market 展开
  │     │
  │     ├── _build_binary_market_row()
  │     │     - 校验: 必须有 2 个 token (binary market)
  │     │     - 解析: outcomes, clobTokenIds, 时间字段
  │     │     - 计算: remaining_hours = (endDate - now) 的小时数
  │     │     - 选择 reference token: 默认选 outcome_0 (token_0_id)
  │     │     - 排除: negRisk 市场可配置排除
  │     │
  │     ├── 去重: 已见过的 market_id 跳过
  │     │     去重机制分为**两层**，确保同一市场不会被重复处理：
  │     │
  │     │     **层 1: 页内去重 (fetch_event_page 内部)**
  │     │     - `seen_market_ids` 作为参数传入 `fetch_event_page()`
  │     │     - 每解析出一个有效 market_id，先检查是否在 `seen` 集合中
  │     │     - 如果已存在 → 跳过该市场，计入 `exclusion_breakdown["duplicate_market_id"]`
  │     │     - 如果不存在 → `seen.add(market_id)`，加入本页结果
  │     │     - 这意味着同一 event 下如果有重复 market_id（API 返回异常），也会被去重
  │     │
  │     │     **层 2: 跨页累积去重 (submit_window 主循环)**
  │     │     - `_run_submit_window_sync_impl()` 维护一个全局 `seen_market_ids: set[str]`
  │     │     - 每次 `fetch_event_page()` 调用时传入该集合
  │     │     - 每页返回后，将该页所有 market_id 追加到全局集合：
  │     │       `for market_id in page.markets["market_id"]: seen_market_ids.add(market_id)`
  │     │     - 下一页拉取时，之前所有页已见的 market_id 都会被跳过
  │     │
  │     │     **为什么需要跨页去重？**
  │     │     Gamma API 按 endDate 排序分页，但同一 event 可能包含多个 market，
  │     │     且分页边界可能导致同一 event 出现在相邻两页中。跨页去重避免了
  │     │     同一市场被推理和提交两次，防止产生重复订单。
  │     │
  │     └── 排除原因记录: exclusion_breakdown
  │           structure_filtered, missing_market_id, duplicate_market_id
  │
  ├── apply_online_market_annotations(cfg, markets)
  │     - 调用 rule_baseline 的标注逻辑
  │     - 推断: domain, category, market_type, outcome_pattern
  │     - 标注归一化: 根据 normalization_manifest 将不在白名单中的 domain 映射为 "OTHER"
  │
  └── 按 remaining_hours 升序排序
      → EventPageResult(markets=DataFrame[EXECUTION_SOURCE_COLUMNS])
```

#### EXECUTION_SOURCE_COLUMNS (52 列)

包含: `market_id`, `question`, `description`, `resolution_source`, `game_id`, `remaining_hours`, `category`, `category_raw`, `category_parsed`, `category_override_flag`, `domain`, `domain_parsed`, `sub_domain`, `source_url`, `market_type`, `outcome_pattern`, `accepting_orders`, `volume`, `best_bid`, `best_ask`, `spread`, `last_trade_price`, `liquidity`, `token_0_id`, `token_1_id`, `selected_reference_token_id`, `uma_resolution_statuses`, `end_time_utc`, `first_seen_at_utc` 等。

#### 分页策略

- 页大小: `cfg.online_gamma_event_page_size` (默认 20)
- 排序: `endDate` 升序（优先处理即将结算的市场）
- 终止条件: `event_count == 0` 或 `has_more == False` 或达到 `max_pages`
- 全局去重: `seen_market_ids` 跨页累积

---

### Phase 3: Eligibility — 结构化粗筛

> **脚本**: `execution_engine/online/pipeline/eligibility.py`

#### 做什么

对每页拉取到的市场进行**两级快速过滤**，将不适合交易的市场尽早剔除：
1. **Event 级排除**: 已持仓的 event 下的所有市场直接排除
2. **结构级排除**: 按时间、状态、规则覆盖等维度过滤

#### 详细流程

```
Event 级排除 (_filter_held_event_candidates)
  - 加载 held_event_ids (来自 StateStore)
  - 已持仓 event 下的市场 → STATE_REJECT / EVENT_POSITION_EXISTS
      │
      ▼
apply_structural_coarse_filter(cfg, markets, rules_frame, excluded_market_ids)
  │
  ├── 时间过滤
  │   ├── missing_end_time:        end_time_utc 为空 → STRUCTURAL_REJECT
  │   ├── expired_market:          remaining_hours ≤ 0 → STRUCTURAL_REJECT
  │   └── outside_trading_horizon: remaining_hours > (window + slack) → STRUCTURAL_REJECT
  │
  ├── 状态过滤
  │   ├── accepting_orders_false:  接单状态为 false → STRUCTURAL_REJECT
  │   ├── open_or_pending_market:  market_id 在已有挂单/持仓中 → STATE_REJECT
  │   └── uma_resolution_status:   UMA 状态含 pending/proposed/resolved/disputed → STRUCTURAL_REJECT
  │
  └── 规则族覆盖过滤
      - 构建 (domain, category, market_type) 三元组
      - 与 rules_frame 的规则族集合做交集
      - rule_family_miss: 无匹配规则族 → STRUCTURAL_REJECT
      │
      ▼
输出: StructuralFilterResult
  ├── direct_candidates: 通过所有检查的候选市场
  └── rejected: 被拒绝的市场（含原因）
```

#### 过滤状态码

| 状态 | 含义 |
|------|------|
| `DIRECT_CANDIDATE` | 通过所有检查，进入推理管线 |
| `STRUCTURAL_REJECT` | 结构性拒绝（时间/状态/规则覆盖不满足） |
| `STATE_REJECT` | 状态性拒绝（已有挂单/持仓/已持仓 event） |

---

### Phase 4: Streaming — WebSocket 实时报价采集

> **脚本**: `execution_engine/online/streaming/manager.py`

#### 做什么

对每批候选的 reference token 建立 WebSocket 连接到 Polymarket CLOB，订阅实时市场数据，在短时间窗口（默认 5-10 秒）内收集盘口快照。

#### 详细流程

```
stream_market_data(cfg, asset_ids, duration_sec)
  │
  ├── resolve_stream_targets(cfg, asset_ids)
  │     将 token_id 列表转换为 TokenSubscriptionTarget 列表
  │
  ├── MarketStreamManager(cfg, targets)
  │     - 初始化每个 token 的初始状态: build_initial_token_state()
  │     - 创建 RawEventBuffer (可选的原始事件存档)
  │
  ├── manager.run(duration_sec)
  │     │
  │     ├── 分片: 按 max_tokens_per_connection 将 tokens 切分为多个 shard
  │     │
  │     ├── 每 shard 独立 WebSocket 连接
  │     │   ├── websockets.connect(ws_url)
  │     │   ├── send: {"type": "market", "assets_ids": [token_ids]}
  │     │   └── 循环接收消息:
  │     │       ├── 解析 JSON 事件
  │     │       ├── ingest_event() → 更新 state_by_token
  │     │       │   记录: best_bid, best_ask, mid_price, last_trade_price, spread
  │     │       │   计算: token_state_age_sec (距上次更新的时间)
  │     │       └── raw_writer.ingest() → 可选记录原始事件
  │     │
  │     ├── ping_loop: 定时发送 ping 保活
  │     ├── flush_loop: 定时写出 token state 和原始数据
  │     │
  │     └── 到达 duration_sec 后: 关闭连接, flush 所有状态
  │
  └── 输出: StreamRunResult
        ├── token_state_records: List[Dict] — 每个 token 的最终状态快照
        ├── shared_token_state_path — 写入 shared/token_state/
        ├── run_token_state_path — 写入 runs/{date}/{id}/
        └── event_counts, duration_sec 等统计
```

#### Token State 结构

每个 token 的状态包含：

| 字段 | 含义 |
|------|------|
| `token_id` | Token 唯一 ID |
| `best_bid` / `best_ask` | 最优买卖价 |
| `mid_price` | 中间价 (best_bid + best_ask) / 2 |
| `last_trade_price` | 最新成交价 |
| `spread` | 买卖价差 |
| `tick_size` | 最小价格步进 |
| `token_state_age_sec` | 最后更新距今的秒数 |
| `message_count` | 收到的消息数 |
| `last_updated_at_utc` | 最后更新时间 |

#### 连接管理

- **分片**: 每个 WebSocket 连接最多订阅 `max_tokens_per_connection` 个 token
- **重连**: 断线后等待 `reconnect_backoff_sec` 秒后自动重连
- **空闲超时**: `idle_timeout_sec` 内无消息则重连
- **保活**: 每 `ping_interval_sec` 秒发送 ping

---

### Phase 5: Scoring — 实时推理与评分

> **脚本**: `execution_engine/online/scoring/live.py`

#### 做什么

这是整个管线的**核心推理阶段**。将候选市场的实时报价与离线模型结合，生成交易信号。

#### 详细流程

```
run_live_inference(runtime, candidates, token_state)
  │
  ├── Step 1: 实时价格过滤 (apply_live_price_filter)
  │     - 将 token_state 按 token_id 合并到候选
  │     - 计算 live_mid_price = mid_price 或 (best_bid + best_ask)/2 或 last_trade_price
  │     - 过滤条件:
  │       ├── LIVE_STATE_MISSING:   token_state 中无该 token
  │       ├── LIVE_STATE_STALE:     token_state_age_sec 过大
  │       ├── LIVE_PRICE_MISS:      live_mid_price ≤ 0
  │       ├── LIVE_SPREAD_TOO_WIDE: spread 超过阈值
  │       └── INVALID_PRICE:        价格不在 [min_price, max_price] 范围
  │     → LiveFilterResult(eligible, rejected)
  │
  ├── Step 2: 构建决策时刻快照 (_build_live_snapshot_rows)
  │     - 对每个 eligible 候选:
  │       ├── 从 CLOB API 拉取价格历史 (ClobPriceHistoryClient)
  │       │     GET /prices-history?market={token_id}&fidelity=1
  │       │     返回 24h 内的 (timestamp, price) 序列
  │       │
  │       ├── 合并当前实时价格到历史序列末端
  │       │
  │       ├── build_quote_window_features(): 计算报价窗口特征
  │       │     offset_sec, local_gap_sec, points_in_window, stale_quote_flag
  │       │
  │       ├── build_historical_price_features(): 计算 term structure 特征
  │       │     path_price_mean/std/min/max/range, delta_p_1h_4h, delta_p_4h_24h,
  │       │     term_structure_slope, price_reversal_flag
  │       │
  │       └── build_decision_time_snapshot_row(): 调用 rule_baseline 的标准快照构建
  │             生成与离线训练一致的快照行
  │             包含: price, horizon_hours, market_id, snapshot_time, 附加字段
  │
  ├── Step 3: 构建 Market Feature Context (_build_market_feature_context)
  │     - 调用 rule_baseline.features.snapshot_semantics.build_market_context_projection()
  │     - 生成市场级上下文特征 (文本特征、情感特征、持续时间特征、text embedding 等)
  │
  ├── Step 4: 构建 Market Feature Cache
  │     - 调用 runtime.rule_runtime.build_market_feature_cache(market_context, annotations)
  │     - 将 Step 3 的上下文与 _build_market_annotations() 提取的标注合并
  │     - 生成完整的市场级特征缓存，供后续特征工程使用
  │
  ├── Step 5: 规则匹配 (_build_live_rule_hits)
  │     │
  │     ├── 精确匹配: runtime.rule_runtime.match_rules(snapshots, rules_frame)
  │     │     按 (domain, category, market_type, price_bin, horizon) 匹配
  │     │     匹配成功的标记 rule_match_priority = 1
  │     │
  │     └── 兜底匹配: build_group_default_rule_hits(remaining, serving_bundle)
  │           对未精确匹配的快照，使用 group 级默认规则
  │           生成 group_default 级别的 rule_hits
  │     │
  │     └── collapse_rule_hits() + add_rule_match_reasons(): 按 (market_id, snapshot_time) 去重
  │           优先保留 priority 高、rule_score 高的匹配，标注匹配原因
  │
  ├── Step 6: 特征工程 (prepare_feature_inputs)
  │     ├── 挂载 serving features: attach_serving_features()
  │     │     - 精确匹配: (group_key, price_bin, horizon_hours) → fine_feature_*
  │     │     - 兜底: 匹配不到时用 group_default_* 替代
  │     │     - 挂载: group_feature_* (组级历史统计)
  │     │
  │     └── preprocess_features(model_input, market_feature_cache)
  │           - merge 市场特征缓存
  │           - 派生交互/质量差值特征
  │           - NaN 填充、clip outliers
  │           - 删除 DROP_COLS（与离线训练一致）
  │
  ├── Step 7: 特征契约校验 (_ensure_feature_contract)
  │     - 对齐特征列与 feature_contract
  │     - missing critical columns → CriticalFeatureContractError (中止推理)
  │     - missing noncritical columns → 填充默认值并 warning
  │     - 输出: feature_contract_summary
  │
  ├── Step 8: 模型预测 (_predict_from_feature_inputs)
  │     - q_pred = model_payload.predict_q(feature_inputs)
  │       → AutoGluon 预测 + 校准器 (Isotonic/Platt/Beta)
  │     - trade_value_pred = model_payload.predict_trade_value(predicted, feature_inputs)
  │
  ├── Step 9: 计算增长方向与 Kelly 分数 (compute_growth_and_direction)
        │
        ├── edge_prob = q_pred - price
        ├── direction_model: q_pred > 0.5 → +1 (买 outcome_0), else -1 (买 outcome_1)
        ├── edge_final: 考虑方向后的 edge (总是正值)
        ├── f_star: Kelly 公式最优仓位比例 = edge / odds
        ├── f_exec: 实际执行仓位 = min(f_star, max_position_f)
        ├── g_net: 预期净增长率 (考虑手续费)
        └── growth_score: 最终评分 = g_net (用于排序和过滤)
  │
  └── Step 10: 后处理 — 去重与排序
        - 按 edge_final DESC 排序
        - 按 (market_id, snapshot_time) 去重，保留最优
        - apply_earliest_market_dedup(): 同一 market 只保留最早快照
        - 最终按 (batch_id, edge_final) 排序
        │
        ▼
输出: LiveInferenceResult
  ├── live_filter: LiveFilterResult
  ├── snapshots: 构建的决策时刻快照
  ├── rule_model: RuleModelResult
  │     ├── rule_hits: 规则匹配结果
  │     ├── feature_inputs: 特征输入
  │     ├── model_outputs: 模型预测 (含 q_pred, growth_score)
  │     └── viable_candidates: 通过可行性筛选的候选 (去重后)
  └── feature_contract_summary
```

#### 推理漏斗

```
候选市场 (batch.frame)
  → 实时价格过滤: live_eligible (排除 state_missing, stale, price_miss, spread_too_wide)
  → 快照构建: snapshots
  → 市场特征上下文 + 缓存: market_feature_cache
  → 规则匹配: rule_hits (精确匹配 + group_default 兜底 + collapse + reasons)
  → 特征工程: feature_inputs
  → 契约校验: aligned feature_inputs
  → 模型预测: model_outputs (q_pred, trade_value_pred)
  → 增长计算 + 后处理去重: viable_candidates (growth_score > 0, 去重)
```

---

### Phase 6: Selection — 候选排序与分配

> **脚本**: `execution_engine/online/scoring/selection.py`

#### 做什么

从 viable_candidates 中筛选最优交易机会，按 Kelly 公式分配仓位，生成最终的选单决策表。

#### 详细流程

```
viable_candidates (from Phase 5)
  │
  ├── select_target_side(viable_candidates)
  │     - direction_model > 0 → 选 token_0 (outcome_0)
  │     - direction_model ≤ 0 → 选 token_1 (outcome_1)
  │     - 翻转 price 和 q_pred (如果选 outcome_1)
  │     - 设置: selected_token_id, selected_outcome_label, position_side
  │
  ├── filter_candidates_by_growth_score()
  │     - 过滤: growth_score > min_growth_score (默认 0.2)
  │
  ├── allocate_candidates(candidates, cfg, state, bt_cfg)
  │     │
  │     ├── 获取可用现金
  │     │   ├── 实盘: balance_provider.get_available_usdc()
  │     │   └── 模拟: initial_bankroll - net_exposure
  │     │
  │     ├── 按 (snapshot_time ASC, edge_final DESC, market_id ASC) 排序
  │     │
  │     └── 逐行分配:
  │           - 跳过: 已持仓的 event (event_id in held_event_ids)
  │           - 跳过: 同一 event 已选过
  │           - 构建 settlement_key (从 closedTime 推导日期)
  │           - 构建 cluster_key (source_host|category|settlement_key)
  │           - stake = min(f_exec × bankroll, max_position_f × bankroll,
  │                         max_trade_amount_usdc, remaining_cash)
  │           - 扣减 remaining_cash
  │           - 分配结束条件: remaining_cash ≤ 0
  │
  └── build_selection_decisions(model_outputs, selected, cfg)
        │
        ├── 对 model_outputs 中的每一行:
        │   - 查找是否被 selected (按 (market_id, snapshot_time, rule_group_key, rule_leaf_id) 四元组匹配)
        │   - 标记 selection_reason:
        │     ├── "allocated": 被选中提交
        │     ├── "event_position_exists": event 已有持仓
        │     ├── "event_already_selected": event 已在本批次中选过
        │     ├── "growth_below_threshold": growth_score 低于阈值
        │     ├── "no_positive_growth": growth_score ≤ 0
        │     └── "not_allocated": 资金不足或排序靠后
        │
        └── 输出: 选单决策 DataFrame
              包含: market_id, event_id, selected_token_id, selected_for_submission,
                    selection_reason, stake_usdc, growth_score, edge_final,
                    q_pred, trade_value_pred, price, direction_model, position_side,
                    rule_group_key, rule_leaf_id, settlement_key, cluster_key, ...
```

---

### Phase 7: Submission — 限价单构建与提交

> **脚本**: `execution_engine/online/execution/submission.py`、`execution_engine/online/execution/pricing.py`

#### 做什么

将选中的候选转换为限价单（LIMIT order），经多层风控校验后通过 CLOB API 提交到 Polymarket。

#### 详细流程

```
submit_selected_orders(cfg, selection, token_state)
  │
  ├── 过滤 selected_for_submission == True 的行
  │
  └── 逐行提交 (while True 重试循环):
      │
      ├── Step 1: 容量等待 (_wait_for_capacity)
      │     - 检查: 市场曝光、类别曝光、净曝光、余额、持仓上限
      │     - 如果超限: sweep_expired_orders() + reconcile()
      │       + refresh_market_state_cache() + refresh_state_snapshot()
      │       + sleep + 重试
      │
      ├── Step 2: 实时报价查询 (get_live_quote)
      │     - 优先从 CLOB order book 获取最新盘口
      │     - fallback: 从 token_state 中获取
      │     - 两者均失败 → MISSING_LIVE_QUOTE 拒绝
      │     - 返回: {best_bid, best_ask, tick_size, mid, min_order_size, ...}
      │
      ├── Step 3: 早期价差检查 (_early_spread_reason)
      │     - (best_ask - best_bid) > EARLY_SPREAD_MAX (0.50) → SPREAD_TOO_WIDE 拒绝
      │
      ├── Step 4: 构建提交信号 (build_submission_signal)
      │     │
      │     ├── 计算 limit_price:
      │     │     limit_price = round_down_to_tick(
      │     │         best_bid + ticks_from_best_bid × tick_size,
      │     │         tick_size
      │     │     )
      │     │
      │     ├── 计算 price_cap:
      │     │     price_cap = max(q_pred - fee_rate - safety_buffer, 0.0)
      │     │     limit_price 不得超过 price_cap
      │     │
      │     ├── 计算 amount_usdc:
      │     │     required = min_order_size × limit_price
      │     │     amount = max(planned_stake, required)
      │     │
      │     ├── 校验:
      │     │     ├── MISSING_TOKEN_OR_MARKET_ID
      │     │     ├── BEST_BID_MISSING / BEST_ASK_MISSING
      │     │     ├── ABNORMAL_TOP_OF_BOOK (bid ≤ 0.01 且 ask ≥ 0.99 或 spread > 0.50)
      │     │     ├── INVALID_LIMIT_PRICE (≤ 0)
      │     │     ├── LIMIT_PRICE_OUTSIDE_RULE_RANGE
      │     │     ├── PRICE_CAP_NONPOSITIVE
      │     │     ├── LIMIT_PRICE_ABOVE_CAP
      │     │     ├── INVALID_ORDER_SIZE
      │     │     └── MIN_ORDER_SIZE_ABOVE_MAX_TRADE
      │     │
      │     └── 输出: SignalPayload (包含 40+ 字段的完整信号)
      │
      ├── Step 5: 价格与流动性校验 (check_price_and_liquidity)
      │     - 中间价偏离、价差、深度等多维度检查（详见 10.3 节）
      │
      ├── Step 6: 风控校验 (check_basic_risk)
      │     ├── ORDER_TYPE_NOT_ALLOWED: 仅允许 LIMIT
      │     ├── INVALID_ORDER_SIZE / MAX_TRADE_AMOUNT_BREACH / MAX_NOTIONAL_BREACH
      │     ├── FAT_FINGER: price_limit 超出 [fat_finger_low, fat_finger_high]
      │     ├── DAILY_LOSS_LIMIT / DAILY_ORDER_LIMIT
      │     ├── DUPLICATE_MARKET_ACTION / DUPLICATE_DECISION
      │     ├── MARKET_EXPOSURE_LIMIT / CATEGORY_EXPOSURE_LIMIT / NET_EXPOSURE_LIMIT
      │     ├── BALANCE_INSUFFICIENT / BALANCE_UNKNOWN
      │     └── ⚠️ 如果被容量相关原因拒绝 → 回到 Step 1 重试容量等待
      │
      ├── Step 7: 构建决策记录 (build_decision_from_signal)
      │     ├── 时间校验: signal 未过期、决策窗口未关闭、市场关闭时间充足
      │     └── 构建 DecisionRecord (包含 decision_id, market_id, 风控字段)
      │
      └── Step 8: 提交到 CLOB (record_decision_created + submit_order)
            ├── record_decision_created(): 记录决策事件
            ├── NonceManager 生成 nonce
            ├── ClobClient.create_order() → CLOB REST API
            ├── dry_run 模式: 不实际提交，状态 = DRY_RUN_SUBMITTED
            ├── 实盘模式: 提交并返回 order_id, status
            └── 记录产物:
                  ├── decisions.jsonl: 决策记录
                  ├── orders.jsonl: 订单记录
                  ├── orders_submitted.jsonl: 已提交订单
                  ├── rejections.jsonl: 被拒订单
                  └── submit_attempts.csv: 提交尝试
```

#### Pricing 核心公式

```
limit_price = round_down_to_tick(best_bid + ticks_from_best_bid × tick_size, tick_size)
price_cap = max(q_pred - fee_rate - safety_buffer, 0.0)
final_price = limit_price  (必须 ≤ price_cap)
amount_usdc = max(planned_stake, min_order_size × limit_price)
```

---

### Phase 8: Post-Submit Monitor — 提交后生命周期监控

> **脚本**: `execution_engine/online/execution/monitor.py`

#### 做什么

在所有订单提交完成后，等待一段时间（`submit_window_monitor_sleep_sec`），然后对订单进行生命周期对账：检查 fill/cancel 状态，更新持仓，管理退出单。

#### 详细流程

```
monitor_order_lifecycle(cfg, sleep_sec, publish_summary_enabled)
  │
  ├── sleep(sleep_sec)  ← 等待订单状态更新
  │
  ├── build_clob_client(cfg)  ← 创建 CLOB 客户端
  │
  ├── sweep_expired_orders(cfg, clob_client)  ← 清除过期订单
  │
  ├── reconcile(cfg, clob_client)  ← 对账订单状态 (CLOB API)
  │
  ├── manage_exit_lifecycle(cfg, clob_client)  ← 退出单管理
  │     ├── submit_pending_exit_orders()
  │     └── settle_resolved_positions()
  │
  ├── refresh_state_snapshot(cfg)  ← 重建 state_snapshot.json
  │
  ├── refresh_market_state_cache(cfg)  ← 刷新市场状态缓存
  │
  ├── load_open_position_rows(cfg)  ← 加载开仓持仓
  │
  ├── 构建生命周期导出:
  │     ├── _build_opened_position_events()  ← 检测新开仓事件
  │     ├── _build_batch_lifecycle_exports()  ← 每批次生命周期产物 (debug)
  │     └── _export_shared_orders_live()  ← 共享状态导出:
  │           ├── orders_live/latest_orders.jsonl
  │           ├── orders_live/fills.jsonl
  │           ├── orders_live/cancels.jsonl
  │           ├── orders_live/opened_positions.jsonl
  │           └── orders_live/opened_position_events.jsonl
  │
  ├── write_manifest()  ← 写入监控清单
  │
  └── publish_run_summary()  (如果 publish_summary_enabled)
      → OrderMonitorResult
```

---

## 5. Stream Market Data 子命令

> **CLI**: `main.py stream-market-data [--asset-id TOKEN_ID] [--duration-sec N]`

独立运行 WebSocket 数据流采集，不触发推理或提交。常用于：
- 预热 token state 缓存
- 测试 WebSocket 连接
- 持续采集市场数据用于分析

参数：
- `--asset-id`: 指定 token ID（可重复），空则从 universe 自动获取
- `--market-limit`: 限制订阅的市场数量
- `--market-offset`: 分页偏移量
- `--duration-sec`: 采集持续时间（默认 60 秒）
- `--print-head`: 印刷快照头部行数

---

## 6. Monitor Orders 子命令

> **CLI**: `main.py monitor-orders [--sleep-sec N]`

独立运行订单生命周期监控与对账。功能等同于 submit-window 的 Phase 8，但可在 submit-window 之外独立调度。

典型用途：
- 定时检查未结订单的 fill/cancel 状态
- 触发退出单提交
- 刷新持仓和状态快照

---

## 7. Exit 管理 (退出单)

> **脚本**: `execution_engine/online/exits/submit_exit.py`

### 7.1 做什么

对已入场且 TTL 已过期的持仓，自动提交**固定价格退出单**（SELL LIMIT at 0.99），锁定利润或等待结算。

### 7.2 详细流程

```
submit_pending_exit_orders(cfg)
  │
  ├── 加载所有历史订单 (orders.jsonl)
  │
  ├── 筛选退出候选:
  │     - 入场订单 (execution_phase = ENTRY, action = BUY)
  │     - 有对应的 open_position (通过 entry_order_attempt_id 关联)
  │     - 没有已存在的未终结退出单
  │     - TTL 已过期 (_ttl_elapsed)
  │
  ├── 对每个候选构建退出信号:
  │     - action = SELL
  │     - price_limit = EXIT_LIMIT_PRICE (0.99)
  │     - amount_usdc = filled_shares × 0.99
  │     - expiration_seconds = 7 天
  │     - execution_phase = EXIT
  │     - parent_order_attempt_id = 入场单 ID
  │
  └── 通过 CLOB API 提交
```

### 7.3 退出策略设计

当前的退出策略极为简单：以 0.99 的极高价格挂 SELL LIMIT 单。这意味着：
- 如果市场结算为该方向胜出 → 自动以 1.0 结算，退出单可能不会成交但持仓自动结算
- 如果价格达到 0.99 → 退出单成交，锁定利润
- 7 天 TTL → 长期挂单等待

### 7.4 Settlement（结算）

`execution_engine/online/exits/settlement.py` 处理市场结算后的持仓关闭：

```
settle_resolved_positions(cfg)
  │
  ├── 加载 open positions 和 resolved labels (CSV)
  │
  ├── 对每个已结算市场的持仓:
  │     ├── 计算 settlement_price:
  │     │     1.0 (持仓方向胜出) 或 0.0 (持仓方向失败)
  │     ├── close_amount_usdc = shares × settlement_price
  │     ├── PnL = realized_pnl_usdc(open_cost, close_amount)
  │     ├── 记录 fill 和 SETTLEMENT_CLOSE 事件
  │     └── 取消该持仓的未结终 EXIT 订单 (reason: MARKET_RESOLVED)
  │
  └── 输出: SettlementCloseResult
```

---

## 8. Label Analysis 子命令

> **CLI**: `main.py label-analysis-daily [--scope run|all]`  
> **脚本**: `execution_engine/online/analysis/labels.py`

### 8.1 做什么

每日从 Gamma API 拉取已结算市场的标签（resolution），与历史选单和订单记录进行对齐，计算**已执行交易**和**机会成本**的绩效分析。

### 8.2 详细流程

```
build_daily_label_analysis(cfg, scope)
  │
  ├── load_resolved_labels(cfg, scope)
  │     - 扫描历史订单中涉及的 market_id
  │     - 从 Gamma API 查询这些市场的结算结果
  │     - 输出: labels DataFrame (market_id, resolved, winning_outcome, ...)
  │
  ├── load_selection_history(cfg, scope)
  │     - 加载历史选单决策 (run_snapshot_selection_path)
  │     - scope="run": 仅当前 run; scope="all": 所有历史 run
  │
  ├── build_order_lifecycle(cfg, selections, scope)
  │     - 关联: 选单 ↔ 订单 ↔ 成交 ↔ 持仓
  │     - 计算: fill_rate, cancel_rate, avg_fill_latency
  │     - 输出: order_lifecycle DataFrame
  │
  ├── build_executed_analysis(labels, order_lifecycle)
  │     - 仅已成交的订单
  │     - 标注: predicted_correct (模型方向 vs 实际结果)
  │     - 计算: 胜率、PnL、edge 实现率
  │
  ├── build_opportunity_analysis(labels, selections, order_lifecycle)
  │     - 所有选单候选（无论是否成交）
  │     - 分类: matched_not_selected, selected_not_submitted, submitted_not_filled
  │     - 计算: 机会成本 = 如果执行了会赚多少
  │
  └── summary JSON:
        ├── executed_win_rate: 已执行交易的胜率
        ├── executed_performance: PnL 统计
        ├── opportunity_breakdown: 机会漏斗
        ├── executed_by_category/domain/rule_leaf: 分维度胜率
        ├── opportunity_edge_vs_realized_label: edge 桶 vs 实际结果
        └── opportunity_q_pred_calibration: q_pred 校准表
```

### 8.3 关键分析维度

| 维度 | 含义 |
|------|------|
| `executed_by_category` | 按市场大类统计已执行交易胜率 |
| `executed_by_domain` | 按数据源统计已执行交易胜率 |
| `executed_by_rule_leaf` | 按规则叶子节点统计胜率 |
| `opportunity_by_selection_reason` | 按选单原因统计机会分布 |
| `opportunity_by_execution_outcome` | 按执行结果统计 |
| `opportunity_edge_vs_realized_label` | 模型 edge 与实际结果的对照 |
| `opportunity_q_pred_calibration` | q_pred 分桶的校准分析 |

---

## 9. Artifact Retention (产物压缩)

> **CLI**: `main.py compact-run-artifacts [--full-days N] [--debug-days N]`  
> **脚本**: `execution_engine/online/reporting/artifact_retention.py`

### 做什么

扫描历史 run 目录，按三级保留策略删除过期产物，节省磁盘空间。

#### 三级保留策略

| 等级 | 包含文件 | 保留时间 |
|------|---------|----------|
| **DEBUG_ONLY** | `processed_markets.csv`, `raw_snapshot_inputs.jsonl`, `normalized_snapshots.csv`, `feature_inputs.csv`, `rule_hits.csv`, `model_outputs.csv`, `post_submit_model_features.csv` | `debug_retention_days` (默认 2 天) |
| **FULL_RETENTION_REMOVABLE** | `submission_attempts.csv`, `token_state.csv`, `current_universe.csv`, `events.jsonl`, `market_audit.csv` | `full_retention_days` (默认 7 天) |
| **CORE_RETAINED (永不删除)** | `run_summary.json`, `selection_decisions.csv`, `orders_submitted.jsonl`, `manifest.json`, `summary.json`, `resolved_labels.csv`, `order_lifecycle.csv`, `executed_analysis.csv`, `opportunity_analysis.csv` | **永久保留** |

- 超过保留窗口的运行目录中，对应等级的产物会被删除
- 删除后清理空目录
- 写入 `data_dir/retention/manifest.json` 记录压缩结果

---

## 10. Runtime 层：状态、决策与风控

### 10.1 StateStore — 运行时状态

> **脚本**: `execution_engine/runtime/state.py`

StateStore 是一个基于 JSON 快照的轻量状态存储，追踪：

| 状态字段 | 含义 |
|---------|------|
| `daily_order_count` | 今日订单计数 |
| `open_orders_count` | 当前未结订单数 |
| `net_exposure_usdc` | 净曝光（USDC） |
| `market_exposure_usdc` | 按 (market_id, outcome_index, action) 的市场曝光 |
| `category_exposure_usdc` | 按 category 的类别曝光 |
| `held_event_ids` | 已持仓的 event ID 集合 |
| `daily_pnl_usdc` | 今日 PnL |
| `decision_last_seen` | 决策 ID → 最后见到的时间戳 |
| `market_action_filled` | 已成交的 (market_id, outcome, action) 集合 |

`build_state_snapshot()` 通过扫描所有历史 `orders.jsonl` 和 `fills.jsonl` 重建完整状态。

### 10.2 Decision — 决策构建

> **脚本**: `execution_engine/runtime/decision.py`

`build_decision_from_signal()` 将 `SignalPayload` 转换为 `DecisionRecord`：
- 时间校验: signal 未过期、decision_window 未关闭、市场关闭时间充足
- 生成 decision_id（UUID）
- 保留所有信号字段用于审计

### 10.3 Validation — 风控校验

> **脚本**: `execution_engine/runtime/validation.py`

#### check_price_and_liquidity()

| 检查 | 条件 | 拒绝码 |
|------|------|--------|
| 价格绝对偏离 | `|mid_now - reference_mid|` > `price_dev_abs` | PRICE_DEVIATION |
| 价格相对偏离 | 偏离 > `price_dev_rel × reference_mid` | PRICE_DEVIATION_REL |
| 价差放大偏离 | 偏离 > `price_dev_spread_k × spread` | PRICE_DEVIATION_SPREAD |
| 价差过宽 | `spread` > `max_spread` | SPREAD_TOO_WIDE |
| 深度不足 | `depth_usdc` < `min_depth_usdc` | DEPTH_TOO_THIN |

#### check_basic_risk()

| 检查 | 拒绝码 |
|------|--------|
| 仅 LIMIT 单 | ORDER_TYPE_NOT_ALLOWED |
| 订单金额 > 0 | INVALID_ORDER_SIZE |
| 订单金额 ≤ max_trade_amount | MAX_TRADE_AMOUNT_BREACH |
| 订单金额 ≤ max_notional | MAX_NOTIONAL_BREACH |
| 价格不触发胖手指 | FAT_FINGER |
| 今日 PnL ≥ daily_loss_limit | DAILY_LOSS_LIMIT |
| 今日订单数 < max_daily_orders | DAILY_ORDER_LIMIT |
| 同一市场/方向未重复 | DUPLICATE_MARKET_ACTION |
| 决策 ID 未在 dup_window 内重复 | DUPLICATE_DECISION |
| 市场曝光未超限 | MARKET_EXPOSURE_LIMIT |
| 类别曝光未超限 | CATEGORY_EXPOSURE_LIMIT |
| 净曝光未超限 | NET_EXPOSURE_LIMIT |
| 余额充足 | BALANCE_INSUFFICIENT |

---

## 11. 数据流与产物清单

### 11.1 Submit Window 产物

| 产物路径 | 格式 | 生成阶段 | 含义 |
|---------|------|---------|------|
| `runs/{date}/{id}/manifest.json` | JSON | 全流程 | 运行元数据 |
| `runs/{date}/{id}/events/*.jsonl` | JSONL | 全流程 | 候选状态事件流 |
| `runs/{date}/{id}/decisions/*.jsonl` | JSONL | Phase 7 | 决策记录 |
| `runs/{date}/{id}/orders/*.jsonl` | JSONL | Phase 7 | 订单记录 |
| `runs/{date}/{id}/orders_submitted.jsonl` | JSONL | Phase 7 | 已提交订单 |
| `runs/{date}/{id}/fills/*.jsonl` | JSONL | Phase 8 | 成交记录 |
| `runs/{date}/{id}/rejections/*.jsonl` | JSONL | Phase 7 | 拒绝记录 |
| `runs/{date}/{id}/submit_attempts.csv` | CSV | Phase 7 | 提交尝试明细 |
| `runs/{date}/{id}/snapshots/selection_decisions.csv` | CSV | Phase 6 | 选单决策表 |
| `runs/{date}/{id}/snapshots/score_manifest.json` | JSON | Phase 5 | 评分清单 |
| `runs/{date}/{id}/snapshots/feature_semantics_manifest.json` | JSON | Phase 5 | 特征语义清单 |
| `runs/{date}/{id}/submit_window_manifest.json` | JSON | 全流程 | 提交窗口完整报告 |
| `summary/runs_index.jsonl` | JSONL | 全流程 | 全局 run 索引 |
| `summary/dashboard.html` | HTML | 全流程 | 仪表盘 |

### 11.2 Debug 模式额外产物 (artifact_policy=debug)

| 产物路径 | 格式 | 含义 |
|---------|------|------|
| `snapshots/processed_markets.csv` | CSV | 每批候选市场 |
| `snapshots/raw_snapshot_inputs.jsonl` | JSONL | 原始快照输入 |
| `snapshots/normalized_snapshots.csv` | CSV | 标准化快照 |
| `snapshots/rule_hits.csv` | CSV | 规则匹配结果 |
| `snapshots/feature_inputs.csv` | CSV | 特征输入 |
| `snapshots/model_outputs.csv` | CSV | 模型输出 |
| `submit/post_submit_features.csv` | CSV | 提交后特征审计 |

### 11.3 共享状态产物

| 产物路径 | 格式 | 更新频率 |
|---------|------|---------|
| `shared/state/state_snapshot.json` | JSON | 每次 monitor |
| `shared/positions/market_state_cache.json` | JSON | 每次 monitor |
| `shared/token_state/current.csv` | CSV | 每次 stream |
| `shared/token_state/current.json` | JSON | 每次 stream |
| `shared/orders_live/latest_orders.jsonl` | JSONL | 每次 monitor |
| `shared/orders_live/fills.jsonl` | JSONL | 每次 monitor |
| `shared/labels/resolved_labels.csv` | CSV | 每次 label analysis |

---

## 12. 离线产物依赖清单

Execution Engine 运行时依赖 `polymarket_rule_engine` 的以下离线产物：

| 产物 | 路径 | 来自步骤 | 用途 |
|------|------|---------|------|
| `trading_rules.csv` | `data/offline/edge/trading_rules.csv` | Step 4 | 规则匹配 |
| `q_model_bundle_deploy/` | `data/offline/models/q_model_bundle_deploy/` | Step 5 | 模型推理 |
| `group_serving_features.parquet` | `data/offline/edge/group_serving_features.parquet` | Step 4 | 组级 serving 特征查表 |
| `fine_serving_features.parquet` | `data/offline/edge/fine_serving_features.parquet` | Step 4 | 规则级 serving 特征查表 |
| `serving_feature_defaults.json` | `data/offline/edge/serving_feature_defaults.json` | Step 4 | fine feature fallback |
| `normalization_manifest.json` | 包含在 model bundle 中 | Step 5 | 域名标注归一化 |
| `feature_contract.json` | 包含在 model bundle 中 | Step 5 | 特征契约校验 |

> **注意**: `trading_rules.csv` 的探测优先级为：`rule_baseline/datasets/trading_rules.csv` → `rule_baseline/datasets/edge/trading_rules.csv` → `data/offline/edge/trading_rules.csv`（通过 `_first_existing_path()` 自动选择第一个存在的路径）。

此外，Execution Engine 在运行时 import `rule_baseline` 包中的以下模块：

| 模块 | 导入的函数/类 |
|------|-------------|
| `rule_baseline.backtesting.backtest_execution_parity` | `load_rules`, `match_rules`, `predict_candidates`, `compute_growth_and_direction`, `ExecutionParityConfig` |
| `rule_baseline.features` | `build_market_feature_cache`, `preprocess_features` |
| `rule_baseline.features.serving` | `attach_serving_features` |
| `rule_baseline.features.snapshot_semantics` | `build_decision_time_snapshot_row`, `build_market_context_projection`, `compute_contract_safe_defaults` |
| `rule_baseline.models` | `load_model_artifact` |
| `rule_baseline.datasets.snapshots` | `apply_earliest_market_dedup` |

---

## 13. 核心常量参考表

| 常量 | 值 | 定义位置 | 含义 |
|------|---|---------|------|
| `EXIT_LIMIT_PRICE` | 0.99 | `exits/submit_exit.py` | 退出单固定限价 |
| `EXIT_TTL_SECONDS` | 604800 (7天) | `exits/submit_exit.py` | 退出单 TTL |
| `EARLY_SPREAD_MAX` | 0.50 | `execution/submission.py` | 提交前价差检查阈值 |
| `MIN_EXECUTION_TICK_SIZE` | 0.01 | `execution/pricing.py` | 最小 tick size |
| `MIN_EXECUTION_ORDER_SHARES` | 5.0 | `execution/pricing.py` | 最小订单份数 |
| `ABNORMAL_BOOK_MIN_BID` | 0.01 | `execution/pricing.py` | 异常盘口最低 bid |
| `ABNORMAL_BOOK_MAX_ASK` | 0.99 | `execution/pricing.py` | 异常盘口最高 ask |
| `ABNORMAL_BOOK_MAX_SPREAD` | 0.50 | `execution/pricing.py` | 异常盘口最大 spread |

---

## 配置参考表

> 所有配置通过 `PEG_*` 环境变量设置，这里列出关键配置及默认值。

| 环境变量 | 默认值 | 含义 |
|---------|--------|------|
| `PEG_DRY_RUN` | `1` | 模拟模式（不实际提交订单） |
| `PEG_RUN_ID` | `manual` | 运行标识 |
| `PEG_RUN_DATE` | 今日(北京时间) | 运行日期 |
| `PEG_RUN_MODE` | `manual` | 运行模式 |
| `PEG_INITIAL_BANKROLL_USDC` | — | 初始资金 |
| `PEG_MAX_TRADE_AMOUNT_USDC` | — | 单笔最大交易金额 |
| `PEG_ORDER_TTL_SEC` | — | 订单有效期(秒) |
| `PEG_MAX_NOTIONAL` | — | 最大名义金额 |
| `PEG_DAILY_LOSS_LIMIT` | — | 日亏损限制 |
| `PEG_MAX_DAILY_ORDERS` | — | 日最大订单数 |
| `PEG_MAX_POSITION_PER_MARKET_USDC` | — | 单市场最大持仓 |
| `PEG_MAX_NET_EXPOSURE_USDC` | — | 净曝光上限 |
| `PEG_MAX_EXPOSURE_PER_CATEGORY_USDC` | — | 类别曝光上限 |
| `PEG_ONLINE_UNIVERSE_WINDOW_HOURS` | — | 市场交易窗口(小时) |
| `PEG_ONLINE_MARKET_BATCH_SIZE` | — | 每批候选数量 |
| `PEG_ONLINE_STREAM_DURATION_SEC` | — | WebSocket 采集时长(秒) |
| `PEG_ONLINE_MIN_GROWTH_SCORE` | `0.2` | growth_score 最低阈值 |
| `PEG_ONLINE_GAMMA_EVENT_PAGE_SIZE` | `20` | Gamma API 分页大小 |
| `PEG_CLOB_HOST` | — | CLOB 交易所地址 |
| `PEG_CLOB_ENABLED` | — | 是否启用 CLOB 集成 |
| `PEG_ARTIFACT_POLICY` | `minimal` | 产物策略 (minimal/debug) |
