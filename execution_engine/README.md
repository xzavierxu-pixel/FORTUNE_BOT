# Execution Engine — 在线执行管线工作流程与模块使用全景图

> **入口脚本**: `execution_engine/app/cli/online/main.py`
> **运行方式**: 统一 CLI，通过 6 个子命令调度在线交易管线
> **调度**: systemd timer 每 15 分钟触发 `run-submit-window`
> **配置**: 所有运行时行为通过 `PEG_*` 环境变量控制

---

## 1. 端到端工作流程总览

`main.py` 是唯一的 CLI 入口，注册 **6 个子命令**。核心子命令 `run-submit-window` 串行执行 8 个 Phase。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                main.py (unified CLI)                                        │
│                                                                             │
│  ┌──── 主交易管线: run-submit-window ──────────────────────────────────┐   │
│  │                                                                      │   │
│  │  Phase 1  Prewarm — 加载离线产物到 OnlineRuntimeContainer           │   │
│  │     │  prewarm.py → build_runtime_container()                        │   │
│  │     ▼                                                                │   │
│  │  Phase 2  Universe — 分页拉取 Gamma API 市场                        │   │
│  │     │  page_source.py → fetch_event_page()                           │   │
│  │     ▼                                                                │   │
│  │  Phase 3  Eligibility — 结构化粗筛 + 实时价格过滤                    │   │
│  │     │  eligibility.py → apply_structural_coarse_filter()             │   │
│  │     │                   apply_live_price_filter()                     │   │
│  │     ▼                                                                │   │
│  │  Phase 4  Streaming — WebSocket 实时报价采集                         │   │
│  │     │  manager.py → stream_market_data()                             │   │
│  │     ▼                                                                │   │
│  │  Phase 5  Scoring — 实时推理 (10 步)                                │   │
│  │     │  live.py → run_live_inference()                                │   │
│  │     ▼                                                                │   │
│  │  Phase 6  Selection — Kelly 分配 + 选单决策                         │   │
│  │     │  selection.py → allocate_candidates()                          │   │
│  │     ▼                                                                │   │
│  │  Phase 7  Submission — 限价单构建与提交                              │   │
│  │     │  submission.py → submit_selected_orders()                      │   │
│  │     ▼                                                                │   │
│  │  Phase 8  Post-Submit — 订单对账 + 退出单 + 结算                    │   │
│  │     monitor.py → monitor_order_lifecycle()                           │   │
│  │                                                                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌──── 辅助子命令 ─────────────────────────────────────────────────────┐   │
│  │  stream-market-data          独立 WebSocket 数据采集                 │   │
│  │  monitor-orders              独立订单生命周期监控                     │   │
│  │  label-analysis-daily        每日标签对齐与 PnL 分析                 │   │
│  │  run-submit-window-post-submit  独立执行提交后监控                   │   │
│  │  compact-run-artifacts       历史产物压缩清理                        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 子命令入口与调度方式

| 子命令 | 调度方式 | 设置 `PEG_RUN_MODE` | 参数 |
|--------|---------|---------------------|------|
| `run-submit-window` | systemd timer (每 15 分钟) | `submit_window` | `--run-id`, `--run-date`, `--max-pages` |
| `run-submit-window-post-submit` | systemd transient unit | `submit_window` | `--run-id`, `--run-date` |
| `stream-market-data` | 手动/定时 | `market_stream` | `--run-id`, `--asset-id` (×N), `--market-limit`, `--market-offset`, `--duration-sec`, `--print-head` |
| `monitor-orders` | systemd timer | `order_monitor` | `--run-id`, `--sleep-sec` |
| `label-analysis-daily` | systemd timer (每日) | `label_analysis` | `--run-id`, `--scope` (run\|all) |
| `compact-run-artifacts` | 手动/定时 | `artifact_retention` | `--run-id`, `--full-days`, `--debug-days` |

> **注意**: `--run-date` 仅 submit-window 类子命令支持。`--run-id` 在所有 6 个子命令上均存在（各自独立添加，非全局参数）。

---

## 3. 项目目录结构

```
execution_engine/
├── __init__.py
├── app/
│   ├── cli/online/main.py              ← 唯一 CLI 入口
│   └── scripts/
│       ├── linux/                       ← Shell 运维脚本 (10 个)
│       ├── online/                      ← PowerShell 运维脚本 (5 个)
│       ├── env/bootstrap_venv.ps1       ← 环境初始化
│       └── manual/                      ← 4 个手动诊断脚本
├── runtime/                             ← 配置、状态、决策、风控
│   ├── config.py                        ← PegConfig (156 字段 frozen dataclass)
│   ├── state.py                         ← StateStore + build_state_snapshot()
│   ├── decision.py                      ← build_decision_from_signal()
│   ├── validation.py                    ← check_basic_risk(), check_price_and_liquidity()
│   ├── models.py                        ← SignalPayload/DecisionRecord/OrderRecord/FillRecord TypedDicts
│   ├── exposure.py                      ← active_exposures() 5 维度曝光
│   └── run_state.py                     ← submit phase 文件锁 (fcntl)
├── integrations/
│   ├── providers/
│   │   ├── gamma_provider.py            ← GammaMarketProvider (Gamma REST API)
│   │   └── balance_provider.py          ← ClobBalanceProvider / FileBalanceProvider
│   └── trading/
│       ├── clob_client.py               ← ClobClient 抽象 + LiveClobClient + NullClobClient
│       ├── order_manager.py             ← submit_order(), reconcile(), sweep_expired_orders()
│       ├── nonce.py                     ← NonceManager (文件持久化)
│       └── state_machine.py             ← ORDER_STATES, TERMINAL_STATES, can_transition()
├── online/                              ← 在线管线，按职责分包
│   ├── pipeline/                        ← 跨阶段编排
│   │   ├── submit_window.py             ← run_submit_window() 主入口
│   │   ├── prewarm.py                   ← build_runtime_container(), OnlineRuntimeContainer
│   │   ├── eligibility.py               ← apply_structural_coarse_filter(), apply_live_price_filter()
│   │   ├── candidate_queue.py           ← DirectCandidateQueue, CandidateBatch
│   │   └── lifecycle.py                 ← record_candidate_state/frame/pass_complete()
│   ├── universe/                        ← 市场宇宙
│   │   ├── page_source.py              ← fetch_event_page(), iter_event_pages()
│   │   └── refresh.py                  ← refresh_current_universe()
│   ├── streaming/                       ← WebSocket 数据流
│   │   ├── manager.py                  ← MarketStreamManager, stream_market_data()
│   │   ├── state.py                    ← ingest_event() 事件处理
│   │   ├── token_state.py             ← TokenSubscriptionTarget, token state 读写
│   │   ├── utils.py                   ← chunked(), resolve_stream_targets()
│   │   └── io.py                      ← RawEventBuffer, write_stream_manifest()
│   ├── scoring/                         ← 推理与评分
│   │   ├── live.py                     ← run_live_inference() 核心推理
│   │   ├── selection.py                ← allocate_candidates(), build_selection_decisions()
│   │   ├── rules.py                    ← load_rules_frame(), ServingFeatureBundle, rule coverage
│   │   ├── rule_runtime.py             ← RuleRuntime, FeatureContract, prepare_feature_inputs()
│   │   ├── snapshot_builder.py         ← build_snapshot_inputs(), build_online_market_context()
│   │   ├── annotations.py             ← apply_online_market_annotations()
│   │   └── price_history.py            ← ClobPriceHistoryClient, quote/price 特征计算
│   ├── execution/                       ← 订单提交与监控
│   │   ├── submission.py               ← submit_selected_orders()
│   │   ├── pricing.py                  ← build_submission_signal(), round_down_to_tick()
│   │   ├── live_quote.py               ← get_live_quote() (CLOB → token_state fallback)
│   │   ├── monitor.py                  ← monitor_order_lifecycle()
│   │   ├── positions.py                ← refresh_market_state_cache(), load_open_position_rows()
│   │   └── submission_support.py       ← IO helpers, record_rejection/decision/order
│   ├── exits/                           ← 退出单与结算
│   │   ├── monitor_exit.py             ← manage_exit_lifecycle()
│   │   ├── submit_exit.py              ← submit_pending_exit_orders()
│   │   ├── settlement.py               ← settle_resolved_positions()
│   │   └── pnl.py                      ← realized_pnl_usdc()
│   ├── analysis/                        ← 标签分析
│   │   ├── labels.py                   ← build_daily_label_analysis()
│   │   ├── label_metrics.py            ← build_executed/opportunity_analysis()
│   │   ├── label_history.py            ← load_resolved_labels/selection_history()
│   │   ├── label_io.py                 ← IO 辅助函数
│   │   └── order_lifecycle.py          ← build_order_lifecycle()
│   └── reporting/                       ← 报告与产物管理
│       ├── run_summary.py              ← publish_run_summary()
│       ├── artifact_retention.py       ← compact_run_artifacts()
│       ├── dashboard.py                ← write_dashboard()
│       ├── dashboard_sections.py       ← HTML 片段渲染
│       ├── dashboard_template.py       ← build_dashboard_html()
│       ├── execution_audit.py          ← build_run_execution_audit()
│       ├── candidate_audit.py          ← build_candidate_audit()
│       ├── summary_metrics.py          ← build_counts/rejection_reasons/execution_metrics()
│       ├── summary_io.py               ← JSONL/JSON 读写辅助
│       └── deferred_writer.py          ← DeferredWriter (延迟产物写入)
├── shared/                              ← 跨模块基础设施
│   ├── io.py                           ← read_jsonl(), append_jsonl(), list_run_artifact_paths()
│   ├── time.py                         ← utc_now(), bj_now(), parse_utc(), to_iso()
│   ├── logger.py                       ← log_structured() (带北京时间戳)
│   ├── metrics.py                      ← increment_metric(), load/save_metrics()
│   └── alerts.py                       ← record_alert()
├── tests/                               ← 12 个测试文件
└── data/                                ← 运行时数据 (gitignored)
    ├── shared/                          ← 跨 run 共享状态
    ├── runs/{date}/{id}/                ← 每次运行产物
    └── summary/                         ← 全局索引与 dashboard
```

---

## 4. 核心模块职责与依赖关系

### 4.1 按管线阶段使用的模块

| Phase | 主模块 | 直接依赖的内部模块 | 依赖的 `rule_baseline` 模块 |
|-------|--------|-------------------|---------------------------|
| **1. Prewarm** | `pipeline/prewarm.py` | `scoring/rules.py`, `scoring/rule_runtime.py` | `backtesting.backtest_execution_parity`, `datasets.snapshots`, `features`, `features.serving`, `models` |
| **2. Universe** | `universe/page_source.py` | `scoring/annotations.py`, `universe/refresh.py`, `integrations/providers/gamma_provider.py` | `domain_extractor.market_annotations`, `features.annotation_normalization`, `data_collection.fetch_raw_events` |
| **3. Eligibility** | `pipeline/eligibility.py` | `scoring/rules.py` | — |
| **4. Streaming** | `streaming/manager.py` | `streaming/state.py`, `streaming/token_state.py`, `streaming/io.py`, `streaming/utils.py` | — |
| **5. Scoring** | `scoring/live.py` | `scoring/rule_runtime.py`, `scoring/rules.py`, `scoring/snapshot_builder.py`, `scoring/price_history.py`, `scoring/annotations.py`, `pipeline/eligibility.py` | `features.snapshot_semantics` |
| **6. Selection** | `scoring/selection.py` | `integrations/providers/balance_provider.py`, `runtime/state.py` | — |
| **7. Submission** | `execution/submission.py` | `execution/pricing.py`, `execution/live_quote.py`, `execution/positions.py`, `execution/submission_support.py`, `integrations/trading/*`, `runtime/decision.py`, `runtime/validation.py`, `runtime/state.py` | `utils.config` (fee rate) |
| **8. Post-Submit** | `execution/monitor.py` | `exits/*`, `execution/positions.py`, `integrations/trading/order_manager.py`, `reporting/run_summary.py`, `runtime/state.py` | — |

### 4.2 辅助子命令使用的模块

| 子命令 | 主模块 | 关键依赖 |
|--------|--------|---------|
| `stream-market-data` | `streaming/manager.py` | `streaming/*` |
| `monitor-orders` | `execution/monitor.py` | `exits/*`, `execution/positions.py`, `integrations/trading/*` |
| `label-analysis-daily` | `analysis/labels.py` | `analysis/*`, `integrations/providers/gamma_provider.py` |
| `compact-run-artifacts` | `reporting/artifact_retention.py` | `shared/time.py` |

---

## 5. 跨包依赖：`rule_baseline` 导入清单

所有 `rule_baseline` 导入均为 **lazy import**（在函数体内 `import`），确保 execution_engine 在 import 时不硬依赖 rule engine。

| `rule_baseline` 模块 | 导入位置 | 用途 |
|----------------------|---------|------|
| `backtesting.backtest_execution_parity` | `scoring/rule_runtime.py`, `scoring/rules.py` | `match_rules()`, `compute_growth_and_direction()`, `ExecutionParityConfig`, `load_rules()`, `predict_candidates()` |
| `datasets.snapshots` | `scoring/rule_runtime.py` | `apply_earliest_market_dedup()` |
| `features` (top-level) | `scoring/rule_runtime.py` | `build_market_feature_cache()`, `preprocess_features()` |
| `features.serving` | `scoring/rules.py`, `scoring/rule_runtime.py` | `attach_serving_features()`, `ServingFeatureBundle` |
| `features.snapshot_semantics` | `scoring/snapshot_builder.py`, `scoring/live.py` | `build_decision_time_snapshot_row()`, `build_market_context_projection()`, `compute_contract_safe_defaults()` |
| `features.annotation_normalization` | `scoring/annotations.py` | `build_normalization_manifest()`, `normalize_market_annotations()`, `merge_market_annotation_projection()` |
| `domain_extractor.market_annotations` | `scoring/annotations.py` | `build_market_annotations()`, `load_market_annotations()` |
| `data_collection.fetch_raw_events` | `universe/refresh.py` | `is_short_term_crypto_market()`, `resolve_category()` |
| `models` | `scoring/rule_runtime.py` | `load_model_artifact()` |
| `utils.config` | `scoring/annotations.py`, `scoring/price_history.py`, `execution/submission_support.py` | 路径常量、fee rate |

---

## 6. 离线产物依赖

Execution Engine 运行时从文件系统加载以下离线产物（由 `polymarket_rule_engine` offline pipeline 生成）：

| 产物 | 候选路径（`_first_existing_path` 按优先级探测） | 加载函数 |
|------|----------------------------------------------|---------|
| `trading_rules.csv` | `rule_baseline/datasets/trading_rules.csv` → `rule_baseline/datasets/edge/trading_rules.csv` → `data/offline/edge/trading_rules.csv` | `load_rules_frame()` |
| `q_model_bundle_deploy/` | `rule_baseline/datasets/models/q_model_bundle_deploy` → `data/offline/models/q_model_bundle_deploy` | `load_model_payload()` |
| `group_serving_features.parquet` | `data/offline/edge/group_serving_features.parquet` | `load_serving_feature_bundle()` |
| `fine_serving_features.parquet` | `data/offline/edge/fine_serving_features.parquet` | `load_serving_feature_bundle()` |
| `serving_feature_defaults.json` | `data/offline/edge/serving_feature_defaults.json` | `load_serving_feature_bundle()` |

Model bundle 内部包含：`runtime_manifest.json`, `feature_contract.json`, `normalization_manifest.json`, predictor + calibrator artifacts。

---

## 7. 关键约束与设计规则

### 7.1 架构约束

| 约束 | 说明 |
|------|------|
| **Lazy import** | 所有 `rule_baseline` 导入必须在函数体内执行，不能在模块顶层 import |
| **PegConfig frozen** | `@dataclass(frozen=True)`，所有 156 字段在构造后不可变 |
| **单一 CLI 入口** | 所有功能通过 `main.py` 子命令暴露，不存在分散的入口脚本 |
| **文件锁互斥** | `run_submit_window()` 通过 `fcntl.flock` 确保同一时刻只有一个 submit-window 实例运行 |
| **状态从产物重建** | `StateStore` 通过扫描所有 `orders.jsonl` + `fills.jsonl` 重建，无独立数据库 |
| **NullClobClient dry-run** | `PEG_DRY_RUN=1` 时使用 `NullClobClient`，所有 CLOB 调用返回空数据 |

### 7.2 数据流约束

| 约束 | 说明 |
|------|------|
| **分页去重** | `seen_market_ids` 跨页累积，同一 market 不会被重复推理/提交 |
| **每 event 最多一个持仓** | `held_event_ids` 检查确保同一 event 下不持有多个 market 方向 |
| **特征契约强制** | `_ensure_feature_contract()` 检查 critical/noncritical 列，critical 缺失直接中止推理 |
| **Model target_mode 校验** | `load_model_payload()` 要求 bundle 的 `target_mode == "q"`，否则拒绝加载 |
| **Batch 内 event 去重** | `allocate_candidates()` 在同一批次内每个 event 最多选一个 market |

### 7.3 风控约束

| 约束 | 检查位置 | 说明 |
|------|---------|------|
| 仅 LIMIT 单 | `check_basic_risk()` | `order_type != "LIMIT"` → 拒绝 |
| Fat finger | `check_basic_risk()` | `price ∉ [fat_finger_low, fat_finger_high]` → 拒绝 |
| 市场/类别/净曝光上限 | `check_basic_risk()` | 3 级曝光限制 |
| 日亏损/日订单上限 | `check_basic_risk()` | 日级风控 |
| 重复检测 | `check_basic_risk()` | 同市场方向重复 + 决策 ID 时间窗口重复 |
| 价格偏离 | `check_price_and_liquidity()` | 绝对/相对/spread-k 三种偏离检查 |
| 价差/深度 | `check_price_and_liquidity()` | spread 和 depth 阈值 |
| 容量重试 | `submission.py` | `check_basic_risk` 失败时可触发 `_wait_for_capacity` 循环重试 |

### 7.4 订单状态机

```
NEW → SENT → ACKED → PARTIALLY_FILLED → FILLED
                  ↘ DELAYED
                  ↘ CANCEL_REQUESTED → CANCELED
                  ↘ EXPIRED
                  ↘ REJECTED
                  ↘ ERROR

DRY_RUN_SUBMITTED (dry-run 专用终态)

终态: {CANCELED, REJECTED, EXPIRED, FILLED, ERROR}
```

状态转移由 `state_machine.py::can_transition()` 校验，非法转移被拒绝。

### 7.5 退出策略约束

| 约束 | 值 | 说明 |
|------|---|------|
| 退出单价格 | `EXIT_LIMIT_PRICE = 0.99` | 固定 SELL LIMIT 价格 |
| 退出单 TTL | `EXIT_TTL_SECONDS = 604800` (7 天) | 长期挂单 |
| 触发条件 | 入场单 TTL 过期 | `_ttl_elapsed()` 检查入场单创建时间 |
| 结算 | 市场结算后关闭持仓 | 胜出 → 1.0, 失败 → 0.0, 取消未结终退出单 |

### 7.6 产物保留策略

| 等级 | 保留时间 | 文件 |
|------|---------|------|
| DEBUG_ONLY | 2 天 | `processed_markets.csv`, `raw_snapshot_inputs.jsonl`, `feature_inputs.csv`, `rule_hits.csv`, `model_outputs.csv` 等 |
| FULL_RETENTION_REMOVABLE | 7 天 | `submission_attempts.csv`, `token_state.csv`, `events.jsonl` 等 |
| CORE_RETAINED | 永久 | `run_summary.json`, `selection_decisions.csv`, `orders_submitted.jsonl`, `manifest.json` 等 |

---

## 8. 模块依赖关系图

```
main.py
  │
  ├── run-submit-window
  │     └── pipeline/submit_window.py
  │           ├── pipeline/prewarm.py
  │           │     ├── scoring/rules.py ← rule_baseline.backtesting, features.serving
  │           │     └── scoring/rule_runtime.py ← rule_baseline.backtesting, features, models, datasets.snapshots
  │           │
  │           ├── universe/page_source.py
  │           │     ├── integrations/providers/gamma_provider.py
  │           │     └── scoring/annotations.py ← rule_baseline.domain_extractor, features.annotation_normalization
  │           │
  │           ├── pipeline/eligibility.py
  │           │     └── scoring/rules.py
  │           │
  │           ├── pipeline/candidate_queue.py (无外部依赖)
  │           │
  │           ├── streaming/manager.py
  │           │     ├── streaming/state.py
  │           │     ├── streaming/token_state.py
  │           │     ├── streaming/io.py
  │           │     └── streaming/utils.py
  │           │
  │           ├── scoring/live.py
  │           │     ├── scoring/snapshot_builder.py ← rule_baseline.features.snapshot_semantics
  │           │     ├── scoring/price_history.py
  │           │     ├── scoring/rule_runtime.py
  │           │     ├── scoring/rules.py
  │           │     └── pipeline/eligibility.py
  │           │
  │           ├── scoring/selection.py
  │           │     ├── integrations/providers/balance_provider.py
  │           │     └── runtime/state.py
  │           │
  │           ├── execution/submission.py
  │           │     ├── execution/pricing.py
  │           │     ├── execution/live_quote.py
  │           │     ├── execution/positions.py
  │           │     ├── execution/submission_support.py
  │           │     ├── integrations/trading/clob_client.py
  │           │     ├── integrations/trading/order_manager.py
  │           │     ├── integrations/trading/nonce.py
  │           │     ├── runtime/decision.py
  │           │     ├── runtime/validation.py
  │           │     └── runtime/state.py
  │           │
  │           ├── execution/monitor.py
  │           │     ├── exits/monitor_exit.py
  │           │     │     ├── exits/submit_exit.py
  │           │     │     └── exits/settlement.py → exits/pnl.py
  │           │     ├── execution/positions.py
  │           │     ├── integrations/trading/order_manager.py
  │           │     ├── reporting/run_summary.py
  │           │     └── reporting/execution_audit.py
  │           │
  │           ├── pipeline/lifecycle.py
  │           └── reporting/deferred_writer.py
  │
  ├── stream-market-data
  │     └── streaming/manager.py (同上)
  │
  ├── monitor-orders
  │     └── execution/monitor.py (同上)
  │
  ├── label-analysis-daily
  │     └── analysis/labels.py
  │           ├── analysis/label_history.py → integrations/providers/gamma_provider.py
  │           ├── analysis/label_metrics.py
  │           ├── analysis/order_lifecycle.py
  │           ├── analysis/label_io.py
  │           └── reporting/run_summary.py
  │
  ├── run-submit-window-post-submit
  │     └── pipeline/submit_window.py::complete_post_submit_monitor()
  │
  └── compact-run-artifacts
        └── reporting/artifact_retention.py
```

---

## 9. Standalone 脚本（不在子命令调用链中）

| 脚本 | 说明 | 分类 |
|------|------|------|
| `app/scripts/manual/compare_token_quote_sources.py` | 对比 Gamma/WS/CLOB 三种报价来源 | **手动诊断** |
| `app/scripts/manual/inspect_bid_ask_sources.py` | 检查提交尝试的 bid/ask 来源 | **手动诊断** |
| `app/scripts/manual/inspect_invalid_price_tokens.py` | 检查 INVALID_PRICE token 的 bid/ask 状态 | **手动诊断** |
| `app/scripts/manual/proxy_wallet_smoketest.py` | Polymarket 代理钱包冒烟测试 | **一次性测试** |
| `app/scripts/linux/*.sh` | 10 个 Shell 运维脚本 (start/stop/restart/clear) | **运维工具** |
| `app/scripts/online/*.ps1` | 5 个 PowerShell 运维脚本 | **运维工具** |

---

## 10. 测试清单

| 测试文件 | 覆盖模块 | 关键测试点 |
|---------|---------|-----------|
| `test_submit_pipeline_guards.py` | pricing, live_quote, live inference, eligibility, state_machine, validation | CLOB 报价经济最优价位、boundary top-of-book 拒绝、风控检查 |
| `test_submit_window_runtime.py` | submit_window, run_state | 文件锁竞争跳过、异步 post-submit 调度 |
| `test_live_snapshot_semantics.py` | live.py, rule_runtime | feature contract 裁剪/默认/critical 中止、quote window 特征 |
| `test_groupkey_rule_matching.py` | rules.py | 精确 horizon 匹配、连续 band 匹配、rule coverage 评分 |
| `test_online_annotations.py` | annotations.py | 标注输入帧构建、domain 白名单归一化 |
| `test_exit_order_lifecycle.py` | exits/* | 取消后重提交、结算关闭持仓 |
| `test_order_manager_fill_ledger.py` | order_manager | 即时成交记录、reconcile 生命周期 |
| `test_rule_runtime_bundle.py` | rule_runtime | bundle 加载、FeatureContract 字段、非 q target 拒绝 |
| `test_execution_audit_reporting.py` | execution_audit | 单 run 审计报告正确性 |
| `test_label_analysis_artifacts.py` | label_history, submit_window | 标签加载、selection 历史、训练产物持久化 |
| `test_storage_optimization.py` | streaming/io, reporting, monitor | WS raw 默认禁用、产物压缩、生命周期导出 |
| `test_autogluon_remaining_work.py` | (rule_baseline) | Beta 校准、blend 校准器、决策 parity |

---

## 11. 关键常量速查

| 常量 | 值 | 位置 |
|------|---|------|
| `EXIT_LIMIT_PRICE` | 0.99 | `exits/submit_exit.py` |
| `EXIT_TTL_SECONDS` | 604800 | `exits/submit_exit.py` |
| `EARLY_SPREAD_MAX` | 0.50 | `execution/submission.py` |
| `MIN_EXECUTION_TICK_SIZE` | 0.01 | `execution/pricing.py` |
| `MIN_EXECUTION_ORDER_SHARES` | 5.0 | `execution/pricing.py` |
| `ABNORMAL_BOOK_MIN_BID` | 0.01 | `execution/pricing.py` |
| `ABNORMAL_BOOK_MAX_ASK` | 0.99 | `execution/pricing.py` |
| `ABNORMAL_BOOK_MAX_SPREAD` | 0.50 | `execution/pricing.py` |
| `TERMINAL_STATES` | `{CANCELED, REJECTED, EXPIRED, FILLED, ERROR}` | `integrations/trading/state_machine.py` |
| `ORDER_STATES` | 12 种状态 | `integrations/trading/state_machine.py` |
| `EXECUTION_SOURCE_COLUMNS` | 52 列 | `universe/page_source.py` |
| `BEIJING_TZ` | `Asia/Shanghai` | `shared/time.py` |

---

## 12. 关键配置环境变量

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `PEG_DRY_RUN` | `1` | 模拟模式 |
| `PEG_RUN_ID` | `manual` | 运行标识 |
| `PEG_RUN_DATE` | 北京时间今日 | 运行日期 |
| `PEG_RUN_MODE` | `manual` | CLI 自动设置 |
| `PEG_INITIAL_BANKROLL_USDC` | `100.0` | 初始资金 |
| `PEG_MAX_TRADE_AMOUNT_USDC` | `4.0` | 单笔上限 |
| `PEG_ORDER_USDC` | `5.0` | 默认订单金额 |
| `PEG_ORDER_TTL_SEC` | `3600` | 订单 TTL |
| `PEG_MAX_NOTIONAL` | `5.0` | 名义上限 |
| `PEG_DAILY_LOSS_LIMIT` | `-500` | 日亏损限制 |
| `PEG_MAX_NET_EXPOSURE_USDC` | `100.0` | 净曝光上限 |
| `PEG_FAT_FINGER_HIGH` | `0.99` | 胖手指高价位 |
| `PEG_FAT_FINGER_LOW` | `0.01` | 胖手指低价位 |
| `PEG_ONLINE_MIN_GROWTH_SCORE` | `0.2` | growth_score 最低阈值 |
| `PEG_ONLINE_GAMMA_EVENT_PAGE_SIZE` | `20` | 分页大小 |
| `PEG_CLOB_ENABLED` | `False` | CLOB 集成开关 |
| `PEG_ARTIFACT_POLICY` | `minimal` | 产物策略 (minimal/debug) |

---

*基于 `execution_engine/` 全模块静态分析、CLI 入口链、systemd timer 配置和 12 个测试文件生成。*
