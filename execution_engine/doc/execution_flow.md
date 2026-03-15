# Execution Engine 完整流程图

该流程图描述了 `execution_engine` 从市场发现、数据采集到模型评分及最终执行订单的完整自动化交易生命周期。

```mermaid
graph TD
    subgraph "1. 市场发现 (Universe Refresh)"
        A1[Gamma API: 获取所有活跃市场] --> A2{过滤: 二元期权 & 24h内到期}
        A2 --> A3[生成 universe_v2.csv]
    end

    subgraph "2. 实时报价监控 (Streaming)"
        B1[CLOB WebSocket: 订阅订单簿] --> B2[维护实时价格状态]
        B2 --> B3[保存到 current_token_state.csv]
    end

    subgraph "3. 每小时交易循环 (Hourly Cycle)"
        C1[读取 universe_v2.csv] --> C2[加载已持有仓位]
        C2 --> C3{过滤: 排除已有持仓市场}
        C3 --> C4["按批次处理 (默认 20 个)"]
        
        subgraph "评分阶段 (Scoring)"
            D1[读取实时报价 B3] --> D2[构建 Snapshot Inputs]
            D2 --> D3[调用 polymarket_rule_engine]
            D3 --> D4[特征工程 & 模型推理]
            D4 --> D5[生成每个市场的评分/建议]
        end
        
        C4 --> D1
        D5 --> E1
        
        subgraph "执行阶段 (Execution)"
            E1[筛选高分建议] --> E2{风控检查: USDC 余额 & 敞口限制}
            E2 -- 通过 --> E3{流动性检查: 价格是否在有效范围}
            E3 -- 通过 --> E4[构建 Passive Limit Order]
            E4 --> E5[通过 CLOB Client 提交订单]
        end
    end

    subgraph "4. 订单生命周期管理 (Monitoring)"
        F1["订单状态对账 (CLOB Client)"] --> F2{订单是否成交?}
        F2 -- 是 --> F3[标记为 'Opened Position']
        F3 --> F4[更新持仓账本 positions.csv]
        F2 -- 否 --> F5[等待或撤单]
    end

    subgraph "5. 报告与分析 (Reporting)"
        G1[每日汇总运行数据] --> G2[生成 dashboard.html]
        G2 --> G3[更新 runs_index.jsonl]
    end

    E5 --> F1
    F4 -.-> C2
```

## 关键模块说明

- **Universe Refresh**: `execution_engine/online/universe/refresh.py`。负责维护可交易市场的“底池”。
- **Streaming Manager**: `execution_engine/online/streaming/manager.py`。通过 WebSocket 维持对订单簿的最佳买卖价监控。
- **Hourly Cycle**: `execution_engine/online/pipeline/cycle.py`。整个流水线的调度者。
- **Scoring**: `execution_engine/online/scoring/hourly.py`。负责将原始数据转换为模型输入并获取预测评分。
- **Execution Submission**: `execution_engine/online/execution/submission.py`。负责订单的合规性检查、定价（通常为 best_bid - 1 tick）和实际发送。
- **Order Monitoring**: `execution_engine/online/execution/monitor.py`。负责维护本地持仓状态与链上订单状态的同步。
