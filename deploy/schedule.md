# Submit Window 当前调度说明

这份文档描述当前仓库里 `fortune-bot-submit-window` 的已实现调度语义、部署前提和验证方式。

## 当前结论

当前主路径已经实现为：

1. `submit-window` 每 15 分钟触发一次
2. `submit phase` 与 `post-submit lifecycle` 已按运行语义解耦
3. 上一轮撤单、对账、exit handling 不应阻塞下一轮 submit
4. 只有上一轮 `submit phase` 还没结束时，下一轮 submit 才允许被跳过
5. execution engine 的 operator-facing 时间、manifest、summary、heartbeat 默认按北京时间展示

## 当前 systemd 配置

当前仓库中的 timer 文件是：

- `deploy/systemd/fortune-bot-submit-window.timer`

当前内容为：

```ini
[Unit]
Description=Run Fortune Bot submit-window every 15 minutes

[Timer]
OnCalendar=*:0/15
Persistent=true
Unit=fortune-bot-submit-window.service

[Install]
WantedBy=timers.target
```

含义是：

- 每小时的 `00`、`15`、`30`、`45` 分触发一次
- `Persistent=true` 保证错过触发点后恢复时可补跑

当前 service 仍为：

- `deploy/systemd/fortune-bot-submit-window.service`

它会执行 `run_submit_window.sh --max-pages 300`，所以单轮 submit phase 仍可能因为扫描 300 页而跨过下一个 15 分钟触发点。

## 当前运行语义

当前实现把一次 `submit-window` 运行分成两个阶段：

1. `submit phase`
2. `post-submit lifecycle`

### submit phase

`submit phase` 负责：

- 抓取页面
- 处理候选和 batch
- live filter / inference / selection
- order submission

### post-submit lifecycle

`post-submit lifecycle` 负责：

- 撤单
- 对账
- exit handling
- 共享状态刷新
- 订单生命周期导出

### 当前阻塞规则

当前代码已经实现的规则是：

- 如果上一轮仍在 `submit phase`，下一轮 submit 不启动
- 如果上一轮已经结束 `submit phase`，只是仍在 `post-submit lifecycle`，下一轮 submit 允许启动

换句话说：

- “上一轮还在撤单” 不是阻止下一轮 submit 的理由
- “上一轮 300 页还没处理完” 才是阻止下一轮 submit 的理由

## 当前实现方式

当前仓库中，`submit phase` 互斥是通过独立运行状态锁实现的，而不是把整轮 workflow 当成一个粗粒度 mutex。

对应实现点包括：

- `execution_engine/runtime/run_state.py`
- `execution_engine/online/pipeline/submit_window.py`
- `execution_engine/app/cli/online/main.py`

当前流程是：

1. submit-window 启动后先检查上一轮是否仍有活跃的 `submit phase`
2. 如果有活跃 `submit phase`，本轮会被标记为 skip
3. 如果没有活跃 `submit phase`，本轮进入新的 submit phase
4. submit phase 完成后释放 submit-phase 独占权
5. post-submit lifecycle 在释放 submit-phase 独占权之后执行

## 异步 post-submit 的现实前提

当前实现支持把 post-submit lifecycle 脱离当前 service，交给独立 transient unit 执行。

相关配置是：

- `PEG_SUBMIT_WINDOW_ASYNC_POST_SUBMIT=1`

现实前提是：

- 目标机器必须可用 `systemd-run`

如果机器上没有 `systemd-run`，代码会 fallback 到同步 post-submit。此时业务语义仍然正确，但 systemd 层面无法真正把 post-submit 从当前 service 生命周期中拆出去。

因此你要区分两层：

1. 应用层调度语义已经实现
2. 生产机上是否能做到真正的异步 post-submit，还取决于 `systemd-run`

## 当前时间语义

当前 execution engine 已实现以下时间约定：

- operator-facing 日志默认写北京时间
- manifest 默认带北京时间字段
- summary 默认带北京时间字段
- job heartbeat 默认带北京时间字段

推荐阅读字段：

- `*_bj`
- `logged_at_bj`
- `generated_at_bj`
- `last_start_bj`
- `last_end_bj`
- `last_success_bj`

如果保留 UTC 字段，字段名会显式带 `_utc`。

## 当前 deploy 侧状态

当前 deploy 目录已经收敛到以下生产 unit：

- `fortune-bot-submit-window.service`
- `fortune-bot-submit-window.timer`
- `fortune-bot-label-analysis.service`
- `fortune-bot-label-analysis.timer`
- `fortune-bot-healthcheck.service`
- `fortune-bot-healthcheck.timer`

旧的 `hourly-cycle` / `refresh-universe` 不再是当前 deploy 主模型的一部分。

## 当前健康检查阈值

当前示例 env 已按 15 分钟节奏更新：

```env
CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=1800
```

这表示：

- submit-window 采用 15 分钟调度
- 超过 30 分钟仍无新成功 heartbeat 时，再按 stale 处理

## 仍需关注的运行风险

虽然调度语义已经实现，但仍有两个现实风险需要持续观察：

1. 单次 `submit phase` 自身可能超过 15 分钟
2. 生产机如果没有 `systemd-run`，post-submit 不能真正脱离当前 service 生命周期

所以线上观察重点是：

- 300 页扫描是否经常跨过下一个触发点
- submit phase 是否稳定在 15 分钟以内
- post-submit 是否已在目标机器上真正异步化

## 上线后的验证方法

### 1. 验证 timer 生效

```bash
systemctl list-timers --all | grep fortune-bot-submit-window
systemctl status fortune-bot-submit-window.timer --no-pager
```

重点确认：

- `NEXT` 落在 `00/15/30/45`
- `Loaded` 指向 `/etc/systemd/system/fortune-bot-submit-window.timer`
- `Active` 为 `active (waiting)`

### 2. 验证 submit phase 互斥

```bash
journalctl -u fortune-bot-submit-window.service -n 200 --no-pager
```

重点确认：

- 不存在两个 run 的 `submit phase` 同时活跃
- 被跳过的 run 只发生在上一轮 `submit phase` 尚未结束时

### 3. 验证撤单不阻塞下一轮 submit

```bash
journalctl -u fortune-bot-submit-window.service --since "6 hours ago" --no-pager
```

理想现象是：

- run N 的 post-submit 仍在执行
- 同时 run N+1 的 submit phase 已开始

### 4. 验证 heartbeat 时间字段

```bash
cat /var/lib/fortune_bot/jobs/submit_window.json
```

重点确认：

- heartbeat 已更新
- 存在 `*_bj` 字段
- 若同时存在 UTC 字段，字段名带 `_utc`

## 一句话结论

当前仓库已经实现：

- 15 分钟 submit 调度
- submit phase 互斥
- 撤单与下一轮 submit 解耦
- 北京时间 operator-facing 时间语义

剩余需要在线上继续验证的，不是“语义有没有实现”，而是：

- submit phase 耗时是否足够短
- 目标机器是否具备 `systemd-run` 来真正异步化 post-submit
