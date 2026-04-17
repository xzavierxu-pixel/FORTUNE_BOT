# Training Folder Refactor — Remaining Work

> 基于 `TRAINING_FOLDER_REFACTOR_plan.md` 审查，以下 3 项需求尚未完成或存在残留。
> 审查日期：2026-04-16

---

## 1. F3 残留：history window 旧引用清理

### 背景

代码层已完成：`HISTORY_WINDOWS` 仅保留 `expanding` 和 `recent_90days`，`recent_50` / `recent_200` 已从生产代码中移除。但测试和文档中仍有过期引用。

### 需要修改的文件

#### 1.1 `polymarket_rule_engine/tests/test_groupkey_feature_inventory.py`

测试数据中仍包含 `recent_200` 列名（如 `global_recent_200_bias_se`、`domain_category_recent_200_abs_bias_q75`）。需要将这些测试 fixture 更新为当前合同支持的 `recent_90days` 列名。

#### 1.2 `polymarket_rule_engine/OFFLINE_PIPELINE_WORKFLOW_AND_USAGE_MAP.md`

仍列出 `recent_50` / `recent_200` 作为历史窗口（约第 807、1158–1159 行区域）。需要更新为仅 `expanding` + `recent_90days`。

#### 1.3 `polymarket_rule_engine/OFFLINE_PIPELINE_STEP_BY_STEP_GUIDE.md`

提及旧窗口名称（约第 79 行区域）。虽然部分位置已注明"不再属于当前合同"，但仍应统一清理为仅描述当前窗口。

### 验收标准

- 整个代码库中 `recent_50` / `recent_200` 仅出现在 `TRAINING_FOLDER_REFACTOR_plan.md`（计划文档本身）中
- 测试 fixture 使用 `recent_90days` 列名并通过
- 文档仅描述 `expanding` 和 `recent_90days` 两个支持的窗口

---

## 2. F4 未实现：收紧 `build_group_serving_features()` Step A

### 背景

`train_rules_naive_output_rule.py` 中 `build_group_serving_features()` 的 Step A 应仅以去重的 `group_key` 作为起始骨架。当前实现在 Step A 之后立即从 `rules_df` 聚合并合入了 4 个禁止列。

### 当前问题代码

位于 `polymarket_rule_engine/rule_baseline/training/train_rules_naive_output_rule.py`，`build_group_serving_features()` 函数内部（约第 641–656 行区域）：

```python
group_metrics = (
    rules_df.groupby("group_key", observed=True)
    .agg(
        group_unique_markets=("group_unique_markets", "first"),
        group_snapshot_rows=("group_snapshot_rows", "first"),
        group_market_share_global=("group_market_share_global", "first"),
        group_median_logloss=("group_median_logloss", "first"),
        ...
    )
)
group_features = group_features.merge(group_metrics, on="group_key", how="left")
```

### 需要移除的列（禁止从 `rules_df` 直接注入 Step A）

- `group_unique_markets`
- `group_snapshot_rows`
- `group_market_share_global`
- `group_median_logloss`

### 允许的非 key 字段来源

Step A 完成后，`group_features` 的非 `group_key` 字段只能来自：

- `history_feature_frames` 合并
- 默认值 / fallback 逻辑

### 影响范围

移除这 4 列后，需要检查下游是否有依赖：

- `full_group_expanding_logloss_tail_x_market_share` 等派生特征（约第 721 行区域）使用了 `group_median_logloss` 和 `group_market_share_global`
- 如果这些派生特征仍需保留，其输入来源必须改为从 `history_feature_frames` 获取，而非从 `rules_df` 直接传递

### 验收标准

- `group_serving_features` 的非 key 内容不再从 `rules_df` 播种
- Step A 唯一的 carry-forward 字段是去重的 `group_key`
- 所有依赖这些列的派生特征要么改用 history 来源，要么随之移除
- 测试通过

---

## 3. H5/H6 残留：根目录 `workflow_and_usage_map.md` 过期引用

### 背景

`run_autogluon_round3_experiments.py` 文件已被删除，离线 pipeline 文档已正确标记为退役。但根目录 `workflow_and_usage_map.md` 仍将其列为活跃脚本。

### 需要修改的文件

`workflow_and_usage_map.md`（项目根目录）

### 需要修改的位置

1. **第 140 行区域**：分析阶段脚本表格中仍列出 `run_autogluon_round3_experiments.py` 作为活跃脚本 → 应标注为已删除或从表格中移除
2. **第 302 行区域**：统计表中"研究实验脚本"计数为 2，包含该脚本 → 应更新为 1，并移除该脚本引用
3. **第 310 行区域**：将该脚本列为清理候选项（措辞暗示文件仍存在）→ 应更新为已删除

### 验收标准

- 根目录 `workflow_and_usage_map.md` 不再将 `run_autogluon_round3_experiments.py` 呈现为存在的可运行脚本
- 统计数字与实际文件状态一致
