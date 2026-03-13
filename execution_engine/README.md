# Polymarket Execution Gateway (PEG)

PEG (Polymarket Execution Gateway) 是系统的核心执行引擎，负责接收来自规则模型 (Rule Baseline) 和大语言模型 (LLM) 的交易信号，进行融合、验证、风险检查，并最终将订单发送到 Polymarket CLOB (Central Limit Order Book) 或模拟执行。

## 目录结构

经过重构，PEG 采用了扁平化且职责清晰的架构：

```text
execution_engine/
├── cli/                # 命令行入口 (原 scripts)
│   ├── demo_run.py     # 演示/测试运行脚本
│   └── build_signals.py# 信号构建工具
├── core/               # 核心业务逻辑
│   ├── engine.py       # 引擎主入口 (Run Loop)
│   ├── decision.py     # 信号融合与决策逻辑
│   ├── validation.py   # 价格验证与风控检查
│   ├── state.py        # 运行时状态管理
│   ├── config.py       # 集中配置管理
│   └── models.py       # 数据模型定义
├── execution/          # 订单执行层
│   ├── order_manager.py# 订单生命周期管理
│   ├── clob_client.py  # Polymarket CLOB 客户端封装
│   ├── state_machine.py# 订单状态机
│   └── nonce.py        # Nonce 管理
├── connectors/         # 外部连接适配器
│   ├── llm_adapter.py  # LLM 信号适配
│   ├── rule_adapter.py # 规则信号适配
│   ├── price_provider.py # 价格数据源 (CLOB/File)
│   └── ...
└── utils/              # 通用工具库
    ├── time.py         # 时间处理
    ├── io.py           # 文件 I/O
    ├── logger.py       # 结构化日志
    └── ...
```

## 核心工作流

PEG 的单次运行 (`run_once`) 遵循以下流水线：

1.  **信号加载 (Ingestion)**:
    *   从 `data/execution_engine/rule_signals.jsonl` 加载规则信号。
    *   从 `data/execution_engine/llm_signals.jsonl` 加载 LLM 信号。
2.  **信号融合 (Fusion)**:
    *   在 `core/decision.py` 中，根据 Market ID 和 Outcome 匹配两种信号。
    *   执行融合策略（如：`HARD_AGREE`，要求方向一致，取更保守的价格）。
    *   生成 `DecisionRecord`。
3.  **验证与风控 (Validation & Risk)**:
    *   **价格检查 (`core/validation.py`)**: 验证当前盘口价格是否偏离参考价格过大，检查市场流动性。
    *   **风控检查 (`core/validation.py`)**: 检查资金余额、每日亏损限额、最大持仓、重复下单等。
4.  **执行 (Execution)**:
    *   通过 `execution/order_manager.py` 提交订单。
    *   如果是 `DRY_RUN` 模式，仅记录日志。
    *   如果是实盘模式，通过 `execution/clob_client.py` 签名并发送 HTTP 请求至 Polymarket。
5.  **对账 (Reconciliation)**:
    *   查询 CLOB 的 Open Orders 和 Fills。
    *   更新本地订单状态 (`FILLED`, `CANCELED` 等)。
    *   处理过期订单 (TTL)。

## 日常运维 (Daily Operations)

所有操作建议在项目根目录下通过 `python -m` 方式运行。

### 1. 生成信号 (Build Signals)

将上游的原始数据（CSV 或 LLM JSON）转换为 PEG 可识别的标准信号格式。

```bash
python -m execution_engine.cli.build_signals \
  --run-id <RUN_ID> \
  --rule-input data/raw/rule_candidates.csv \
  --llm-snapshot-dir tasks/snapshots
```

*   **输入**: 原始规则 CSV, LLM 推理结果目录。
*   **输出**: `data/execution_engine/rule_signals.jsonl`, `data/execution_engine/llm_signals.jsonl`。

### 2. 运行引擎 (Run Engine)

执行一次完整的交易循环（推荐用于 Cron Job）。

```bash
# 演示/测试运行 (会自动生成模拟数据)
python -m execution_engine.cli.demo_run --run-id TEST_001

# 实盘/生产运行 (需要配置环境变量)
# 通常由上层调度脚本 (ops/run.ps1) 调用 core/engine.py
python -m execution_engine.core.engine
```

### 3. 环境变量配置

在 `.env` 或 `execution_engine/core/config.py` 中管理配置。主要参数：

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `PEG_DRY_RUN` | `True` | 是否为模拟运行。生产环境设为 `False`。 |
| `PEG_ORDER_USDC` | `10.0` | 单笔订单金额 (USDC)。 |
| `PEG_CLOB_ENABLED` | `False` | 是否启用真实 CLOB 连接。 |
| `PEG_CLOB_API_KEY` | - | Polymarket API Key (实盘必填)。 |
| `PEG_PRICE_DEV_ABS` | `0.05` | 允许的最大价格绝对偏差 (5美分)。 |
| `PEG_DAILY_LOSS_LIMIT`| `-500` | 每日最大亏损额 (USDC)。 |

## 结果验证 (Verification)

运行后，所有输出文件位于项目根目录的 `data/execution_engine/` 下：

| 文件 | 内容 | 检查重点 |
|------|------|----------|
| `decisions.jsonl` | 生成的交易决策 | 检查 `fusion_mode` 和 `status`。 |
| `orders.jsonl` | 提交的订单记录 | 检查 `status` 是否为 `NEW`/`FILLED`。 |
| `rejections.jsonl` | 被拒绝的信号 | **最重要**。检查 `reason_code` (如 `PRICE_DEVIATION`, `RISK_LIMIT`)。 |
| `logs.jsonl` | 结构化运行日志 | 详细的调试信息。 |
| `metrics.json` | 累积统计指标 | `orders_sent`, `rejections_count` 等。 |

### 常见拒绝原因 (Rejection Codes)

*   `LLM_MISSING`: 只有规则信号，没有对应的 LLM 确认。
*   `ENGINE_DISAGREE`: 规则和 LLM 方向不一致。
*   `PRICE_DEVIATION`: 市场现价与模型预测价格偏差过大。
*   `BALANCE_INSUFFICIENT`: 余额不足。
*   `DUPLICATE_DECISION`: 短时间内对同一市场重复决策。

## 开发指南

*   **添加新规则**: 修改 `connectors/rule_adapter.py`。
*   **调整风控**: 修改 `core/validation.py` 中的 `check_basic_risk` 函数。
*   **对接新数据源**: 在 `connectors/` 下添加新的 Provider。
```