# Rule Baseline

Polymarket 预测市场量化交易策略的规则引擎与机器学习流水线。

> **重要**: 所有脚本必须从**项目根目录**运行，以确保 import 正确。

---

## 📋 目录

- [系统概述](#系统概述)
- [数据流架构](#数据流架构)
- [完整工作流程](#完整工作流程)
- [日常运维](#日常运维)
- [结果分析指南](#结果分析指南)
- [脚本详细说明](#脚本详细说明)
- [配置参数](#配置参数)
- [输出文件说明](#输出文件说明)
- [故障排查](#故障排查)

---

## 系统概述

### 核心理念

本系统通过以下方式在 Polymarket 预测市场中寻找 **edge (套利机会)**：

1. **规则挖掘**: 发现特定 (价格区间 × 类别 × 时间窗口) 组合下，市场价格系统性偏离真实概率的模式
2. **概率修正**: 使用 LightGBM 模型 (Q-Model) 对市场价格进行修正，输出更准确的概率估计
3. **组合管理**: 基于 Kelly Criterion 进行动态仓位管理，控制风险敞口

### 关键指标

| 指标 | 含义 | 目标 |
|------|------|------|
| `edge` | y - p (实际结果 - 市场价格) | > 0 表示买入有利 |
| `Brier Score` | (q - y)² 的均值 | 越低越好，< 0.25 为合理 |
| `Alpha Ratio` | 逆势正确率 | > 0.5 表示有真正 alpha |
| `Sharpe Ratio` | 风险调整后收益 | > 1.0 为良好 |

---

## 数据流架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Polymarket API                                                          │
│       │                                                                  │
│       ▼                                                                  │
│  fetch_raw_events.py ──────────► data/raw/raw_markets.csv               │
│       │                          (市场元数据: ID, 类别, 描述, 日期)       │
│       │                                                                  │
│       ▼                                                                  │
│  build_snapshots.py ───────────► data/processed/snapshots.csv           │
│       │                          (价格快照: 1h/2h/4h/6h/12h/24h horizons) │
│       │                                                                  │
├───────┼─────────────────────────────────────────────────────────────────┤
│       │                      TRAINING PIPELINE                           │
├───────┼─────────────────────────────────────────────────────────────────┤
│       │                                                                  │
│       ▼                                                                  │
│  train_rules_naive_output_rule.py ──► data/naive_rules/                 │
│       │                               ├── naive_trading_rules.csv       │
│       │                               └── naive_*.json (可视化)          │
│       │                                                                  │
│       ▼                                                                  │
│  train_snapshot_lgbm_v2.py ────────► data/models/lgbm_snapshot_q.pkl    │
│       │                              data/predictions/snapshots_with_q_v2.csv
│       │                              data/edge/trading_rules.csv         │
│       │                                                                  │
├───────┼─────────────────────────────────────────────────────────────────┤
│       │                    EVALUATION PIPELINE                           │
├───────┼─────────────────────────────────────────────────────────────────┤
│       │                                                                  │
│       ▼                                                                  │
│  analyze_q_model_calibration.py ──► 校准报告 (LogLoss, AUC, Brier)      │
│       │                                                                  │
│       ▼                                                                  │
│  analyze_alpha_quadrant.py ───────► data/analysis/alpha_*.csv           │
│       │                             (逆势/顺势四象限分析)                 │
│       │                                                                  │
│       ▼                                                                  │
│  backtest_execution_parity.py ────► data/backtesting/                   │
│                                     ├── backtest_equity_execution_parity.csv │
│                                     ├── backtest_trades_execution_parity.csv │
│                                     └── backtest_filter_breakdown_execution_parity.csv │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 完整工作流程

### 🚀 一次性初始化 (首次运行)

```powershell
# 0. 确保在项目根目录
cd /path/to/polymarket_strategy

# 1. 获取原始市场数据 (~5-10 分钟)
python rule_baseline/data_collection/fetch_raw_events.py

# 2. 构建市场注释
python rule_baseline/domain_extractor/build_market_annotations.py

# 3. 构建价格快照 (~30-60 分钟，取决于市场数量)
python rule_baseline/data_collection/build_snapshots.py

# 4. 挖掘朴素规则 (~2-5 分钟)
python rule_baseline/training/train_rules_naive_output_rule.py

# 5. 训练 LightGBM Q-Model (~5-10 分钟)
python rule_baseline/training/train_snapshot_lgbm_v2.py

# 5. 验证模型校准
python rule_baseline/analysis/analyze_q_model_calibration.py

# 6. 运行回测
python rule_baseline/backtesting/backtest_execution_parity.py

# 7. 分析 Alpha 四象限
python rule_baseline/analysis/analyze_alpha_quadrant.py
python rule_baseline/analysis/analyze_rules_alpha_quadrant.py
```

### 📊 完整流水线一键运行

```powershell
# 从数据收集到回测的完整流程
python rule_baseline/data_collection/fetch_raw_events.py && \
python rule_baseline/data_collection/build_snapshots.py && \
python rule_baseline/training/train_rules_naive_output_rule.py && \
python rule_baseline/training/train_snapshot_lgbm_v2.py && \
python rule_baseline/analysis/analyze_q_model_calibration.py && \
python rule_baseline/backtesting/backtest_execution_parity.py
```

---

## 日常运维

### 📅 每日任务

| 时间 | 任务 | 命令 | 目的 |
|------|------|------|------|
| 每日 | 更新市场数据 | `python rule_baseline/data_collection/fetch_raw_events.py` | 获取新市场 |
| 每日 | 更新快照 | `python rule_baseline/data_collection/build_snapshots.py` | 添加新价格数据 |

### 📅 每周任务

| 任务 | 命令 | 目的 |
|------|------|------|
| 重新训练规则 | `python rule_baseline/training/train_rules_naive_output_rule.py` | 适应市场变化 |
| 重新训练模型 | `python rule_baseline/training/train_snapshot_lgbm_v2.py` | 更新概率估计 |
| 验证校准 | `python rule_baseline/analysis/analyze_q_model_calibration.py` | 检查模型漂移 |
| 运行回测 | `python rule_baseline/backtesting/backtest_execution_parity.py` | 评估 execution parity 策略表现 |

### 📅 月度任务

| 任务 | 命令 | 目的 |
|------|------|------|
| Alpha 四象限分析 | `python rule_baseline/analysis/analyze_alpha_quadrant.py` | 深度分析模型价值 |
| 规则 Alpha 分析 | `python rule_baseline/analysis/analyze_rules_alpha_quadrant.py` | 识别高价值规则 |
| 调整日期分割 | 编辑 `rule_baseline/utils/config.py` | 更新训练/验证集 |

### ⚠️ 注意事项

1. **日期配置**: 定期更新 `config.py` 中的日期分割，确保验证集包含最新数据
2. **API 限流**: `build_snapshots.py` 会大量调用 API，建议在非高峰时段运行
3. **数据完整性**: 每次重新训练前检查 `snapshots.csv` 的数据质量

---

## 结果分析指南

### 1️⃣ 模型校准检查

运行 `analyze_q_model_calibration.py` 后，检查以下指标：

```
┌────────────────────────────────────────────────┐
│              校准指标解读                        │
├─────────────┬─────────────┬────────────────────┤
│ 指标        │ 良好范围     │ 警告阈值            │
├─────────────┼─────────────┼────────────────────┤
│ LogLoss     │ < 0.50      │ > 0.60 需要关注     │
│ AUC         │ > 0.55      │ < 0.52 模型无效     │
│ Brier Score │ < 0.22      │ > 0.25 校准差       │
└─────────────┴─────────────┴────────────────────┘
```

### 2️⃣ Alpha 四象限分析

运行 `analyze_alpha_quadrant.py` 后，解读四象限：

```
                    模型正确?
              ┌─────────┬─────────┐
              │   YES   │   NO    │
   ┌──────────┼─────────┼─────────┤
   │ 逆势     │ ⭐ Alpha │ ❌ Worst │  ← 关键区域
   │ Contrarian│  (最有价值) │ (最危险) │
   ├──────────┼─────────┼─────────┤
   │ 顺势     │ ✅ Follow│ ⚠️ Market│
   │ Consensus │  (跟随市场) │ (市场错误) │
   └──────────┴─────────┴─────────┘

关键指标:
- alpha_ratio > 0.5: 模型逆势时多数正确 ✅
- weighted_score > 0: 净价值为正 ✅
- contrarian_pct: 逆势比例，太高可能过度自信
```

### 3️⃣ 回测结果分析

运行 `backtest_execution_parity.py` 后，检查 `data/backtesting/` 下的文件：

**backtest_equity_execution_parity.csv** - 每日权益曲线
```python
# 分析脚本示例
import pandas as pd
equity = pd.read_csv('data/backtesting/backtest_equity_execution_parity.csv')

# 关键指标
total_return = (equity['equity'].iloc[-1] / equity['equity'].iloc[0] - 1) * 100
max_drawdown = (equity['equity'] / equity['equity'].cummax() - 1).min() * 100
sharpe = equity['daily_return'].mean() / equity['daily_return'].std() * (252**0.5)

print(f"Total Return: {total_return:.2f}%")
print(f"Max Drawdown: {max_drawdown:.2f}%")
print(f"Sharpe Ratio: {sharpe:.2f}")
```

**backtest_filter_breakdown_execution_parity.csv** - 筛选流量分解
```python
breakdown = pd.read_csv('data/backtesting/backtest_filter_breakdown_execution_parity.csv')
print(breakdown.T)
```

### 4️⃣ 分类别分析

```python
# 按类别分析模型表现
import pandas as pd

# 加载带四象限标签的预测
df = pd.read_csv('data/analysis/predictions_with_quadrant.csv')

# 按类别统计 alpha
category_alpha = df.groupby('category').agg({
    'quadrant': lambda x: (x == 'contrarian_correct').sum() / ((x == 'contrarian_correct').sum() + (x == 'contrarian_wrong').sum() + 1e-6)
}).rename(columns={'quadrant': 'alpha_ratio'})

print(category_alpha.sort_values('alpha_ratio', ascending=False))
```

---

## 脚本详细说明

### 📥 数据收集 (`data_collection/`)

| 脚本 | 功能 | 输入 | 输出 |
|------|------|------|------|
| `fetch_raw_events.py` | 从 Polymarket Gamma API 下载市场元数据 | API | `data/raw/raw_markets.csv` |
| `build_snapshots.py` | 构建多时间窗口价格快照 | `raw_markets.csv` + API | `data/processed/snapshots.csv` |

**build_snapshots.py 详解**:
- 对每个市场，在结算前 1h/2h/4h/6h/12h/24h 获取价格
- 记录实际结果 y (0 或 1)
- 计算 edge = y - price

### 🧠 训练 (`training/`)

| 脚本 | 功能 | 输入 | 输出 |
|------|------|------|------|
| `train_rules_naive_output_rule.py` | 朴素规则挖掘 (价格×类别×时间窗口分桶) | `snapshots.csv` | `data/naive_rules/` |
| `train_snapshot_lgbm_v2.py` | 训练 LightGBM Q-Model | `snapshots.csv` + rules | `data/models/`, `data/predictions/`, `data/edge/` |

**train_rules_naive_output_rule.py 参数**:
```python
PRICE_BIN_STEP = 0.03   # 价格分桶步长 (3%)
MIN_VALID_N = 10        # 最小验证集样本
EDGE_AB_THRESHOLD = 0.02 # 最小绝对 edge
```

**train_snapshot_lgbm_v2.py 输出**:
- `lgbm_snapshot_q.pkl`: 训练好的模型
- `snapshots_with_q_v2.csv`: 带模型预测的快照
- `trading_rules.csv`: 最终交易规则

### 📈 回测 (`backtesting/`)

| 脚本 | 功能 | 策略 |
|------|------|------|
| `backtest_execution_parity.py` | **[主要]** Execution parity 回测 | 对齐线上执行逻辑 + Kelly |
| `backtest_portfolio_qmodel.py` | Q-Model 组合回测 | 更严格的 rule / risk / portfolio 筛选 |

**backtest_execution_parity.py 关键参数**:
```python
INITIAL_BANKROLL = 10_000.0  # 初始资金
MAX_DAILY_TRADES = 300       # 每日最大交易数
MAX_POSITION_F = 0.02        # 单笔最大仓位 (2%)
MAX_DAILY_EXPOSURE_F = 1.0   # 每日最大敞口
KELLY_FRACTION = 0.25        # Kelly 分数 (保守)
FEE_RATE = 0.001             # 手续费 (来自 config.py)
```

### 📊 分析 (`analysis/`)

| 脚本 | 功能 | 输出 |
|------|------|------|
| `analyze_q_model_calibration.py` | 模型校准检查 | 终端报告 |
| `analyze_alpha_quadrant.py` | LGBM 模型 Alpha 四象限分析 | `data/analysis/alpha_*.csv` |
| `analyze_rules_alpha_quadrant.py` | 朴素规则 Alpha 四象限分析 | `data/analysis/rules_alpha_*.csv` |
| `analyze_qmodel_trades.py` | 回测交易深度分析 | 终端报告 |
| `analyze_snapshots.py` | 数据质量检查 | 终端报告 |
| `analyze_raw_markets.py` | 原始数据逻辑检查 | 终端报告 |

---

## 配置参数

### `utils/config.py` 关键配置

```python
# 时间窗口
HORIZONS = [1, 2, 4, 6, 12, 24]  # 小时

# 数据日期范围
DATE_START_STR = "2024-11-01"       # 数据起始
DATE_TRAIN_END_STR = "2025-11-26"   # 训练集结束
DATE_VALID_START_STR = "2025-11-27" # 验证集开始

# LGBM 专用日期 (可能不同)
DATE_TRAIN_END_LGBM_STR = "2025-11-10"
DATE_VALID_START_LGBM_STR = "2025-11-11"

# 回测参数
FEE_RATE = 0.001  # 0.1% 手续费 + 滑点

# 规则筛选
EDGE_THRESHOLD = 0.05  # 最小 5% edge
MIN_SAMPLES_LEAF = 200  # 最小样本数
```

---

## 输出文件说明

### `data/` 目录结构

```
data/
├── raw/
│   └── raw_markets.csv          # 原始市场数据
├── processed/
│   └── snapshots.csv            # 价格快照 (核心训练数据)
├── naive_rules/
│   ├── naive_trading_rules.csv  # 朴素规则
│   └── naive_*.json             # 可视化数据
├── models/
│   └── lgbm_snapshot_q.pkl      # 训练好的 Q-Model
├── predictions/
│   └── snapshots_with_q_v2.csv  # 带模型预测的快照
├── edge/
│   └── trading_rules.csv        # 最终交易规则
├── backtesting/
│   ├── backtest_equity_execution_parity.csv    # 权益曲线
│   ├── backtest_trades_execution_parity.csv    # 交易记录
│   └── backtest_filter_breakdown_execution_parity.csv   # 筛选分解
└── analysis/
    ├── alpha_quadrant_metrics.csv    # 四象限统计
    ├── alpha_by_category.csv         # 分类别 alpha
    ├── alpha_by_horizon.csv          # 分时间窗口 alpha
    └── predictions_with_quadrant.csv # 带四象限标签的预测
```

---

## 故障排查

### 常见问题

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| `ModuleNotFoundError: rule_baseline` | 未从项目根目录运行 | `cd` 到项目根目录 |
| `FileNotFoundError: snapshots.csv` | 未运行数据收集 | 先运行 `fetch_raw_events.py` 和 `build_snapshots.py` |
| 回测结果全部亏损 | 规则质量差或参数不当 | 检查 `alpha_ratio`，调整 `MIN_EDGE_TRADE` |
| API 请求超时 | Polymarket 限流 | 添加重试逻辑或降低请求频率 |
| 内存不足 | snapshots.csv 过大 | 分批处理或增加内存 |

### 日志检查

```powershell
# 检查数据完整性
python -c "import pandas as pd; df=pd.read_csv('data/processed/snapshots.csv'); print(df.info()); print(df['category'].value_counts())"

# 检查模型是否存在
ls data/models/lgbm_snapshot_q.pkl

# 检查规则数量
python -c "import pandas as pd; print(len(pd.read_csv('data/edge/trading_rules.csv')))"
```

---

## deprecated/ 文件夹

以下脚本已弃用，保留仅供参考：

| 脚本 | 原因 |
|------|------|
| `train_snapshot_lgbm_deprecated.py` | V1 版本，已被 V2 替代 |
| `train_rules_v3_deprecated.py` | 实验性贝叶斯平滑，效果不佳 |
| `train_rules_by_horizon_deprecated.py` | 分时间窗口训练，已合并到主流程 |
| `train_rules_naive_plot_deprecated.py` | 可视化脚本，功能已整合 |
| `backtest_rules_timeseries.py` | 期望 `market_ids` 字段但训练脚本不生成 |
