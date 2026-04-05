# 最小重启 Runbook

这份文档给出 Ubuntu 上更新 `fortune-bot` 部署时的最小安全操作顺序。
目标是先停掉定时触发，再更新代码，最后恢复定时任务，并确认 submit-window 已按每 15 分钟调度生效。

## 1. 先停掉 Timer

先停 scheduler，避免你更新代码时又触发新任务。

```bash
sudo systemctl stop fortune-bot-submit-window.timer
sudo systemctl stop fortune-bot-label-analysis.timer
sudo systemctl stop fortune-bot-healthcheck.timer
```

## 2. 停掉当前正在跑的 Service

再停掉当前正在运行的任务。

```bash
sudo systemctl stop fortune-bot-submit-window.service
sudo systemctl stop fortune-bot-label-analysis.service
sudo systemctl stop fortune-bot-healthcheck.service
```

## 3. 更新代码

在服务器上拉取最新代码。

```bash
cd /opt/fortune_bot
git pull --ff-only
```

## 4. 修改 `/etc/fortune-bot/fortune_bot.env`

如果这次上线需要修改环境变量，建议按下面方式操作：

先备份当前 env 文件：

```bash
sudo cp /etc/fortune-bot/fortune_bot.env /etc/fortune-bot/fortune_bot.env.bak.$(date +%Y%m%d_%H%M%S)
```

再编辑正式配置：

```bash
sudo nano /etc/fortune-bot/fortune_bot.env
```

如果你只是想确认当前关键配置，可以先检查这些项：

```bash
grep -E 'PEG_SUBMIT_WINDOW_ASYNC_POST_SUBMIT|CHECK_SUBMIT_WINDOW_MAX_AGE_SEC|CHECK_REQUIRED_UNITS' /etc/fortune-bot/fortune_bot.env
```

当前 15 分钟 submit 调度建议至少确认：

```env
PEG_SUBMIT_WINDOW_ASYNC_POST_SUBMIT=1
CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=1800
CHECK_REQUIRED_UNITS=fortune-bot-submit-window.timer,fortune-bot-label-analysis.timer,fortune-bot-healthcheck.timer
```

修改完成后，建议快速检查文件里是否有明显拼写错误或重复键：

```bash
grep -n 'PEG_SUBMIT_WINDOW_ASYNC_POST_SUBMIT\|CHECK_SUBMIT_WINDOW_MAX_AGE_SEC\|CHECK_REQUIRED_UNITS' /etc/fortune-bot/fortune_bot.env
```

如果这次改动包含以下任一内容，还需要重新加载 systemd：

- `deploy/systemd/*.service`
- `deploy/systemd/*.timer`
- `/etc/fortune-bot/fortune_bot.env`

```bash
sudo systemctl daemon-reload
```

如果你这次修改了 submit-window timer 文件，还需要把仓库里的 timer 同步到 systemd 目录：

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-submit-window.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## 5. 再启动 Timer

正常运行时应该启动 timer，而不是直接常驻启动 service。

```bash
sudo systemctl start fortune-bot-submit-window.timer
sudo systemctl start fortune-bot-label-analysis.timer
sudo systemctl start fortune-bot-healthcheck.timer
```

## 6. 检查 Timer 是否已经恢复

先看 timer 是否都在。

```bash
systemctl list-timers --all | grep fortune-bot
```

再单独确认 submit-window 的 timer 状态。

```bash
systemctl status fortune-bot-submit-window.timer --no-pager
```

你要重点确认：

- `Loaded` 指向的是 `/etc/systemd/system/fortune-bot-submit-window.timer`
- `Active` 是 `active (waiting)`
- `Trigger` 或 `NEXT` 落在下一个 `00`、`15`、`30`、`45` 分钟点

## 7. 立即触发一次任务

只有你想立刻执行一次时，才直接启动 service。

```bash
sudo systemctl start fortune-bot-submit-window.service
sudo systemctl start fortune-bot-label-analysis.service
sudo systemctl start fortune-bot-healthcheck.service
```

## 8. 检查 Service 状态

```bash
sudo systemctl status fortune-bot-submit-window.service --no-pager
sudo systemctl status fortune-bot-label-analysis.service --no-pager
sudo systemctl status fortune-bot-healthcheck.service --no-pager
```

这些 service 是 `Type=oneshot`，所以执行完成后显示 `inactive (dead)` 是正常现象。

## 9. 检查日志

看最近日志是否正常。

```bash
journalctl -u fortune-bot-submit-window.service -n 100 --no-pager -l
journalctl -u fortune-bot-label-analysis.service -n 100 --no-pager -l
journalctl -u fortune-bot-healthcheck.service -n 100 --no-pager -l
```

如果你刚刚上线了“submit phase 与 post-submit 解耦”，还建议额外看最近几小时 submit-window 日志：

```bash
journalctl -u fortune-bot-submit-window.service --since "6 hours ago" --no-pager
```

重点确认：

- 下一轮 submit 是否仍按 `00/15/30/45` 分触发
- 上一轮 post-submit 仍在跑时，下一轮 submit 是否可以继续启动
- 只有上一轮 submit phase 本身还没结束时，新的 submit 才会被跳过

## 10. 检查 Heartbeat 文件

```bash
cat /var/lib/fortune_bot/jobs/submit_window.json
cat /var/lib/fortune_bot/jobs/label_analysis_daily.json
```

你要重点确认：

- `submit_window.json` 已更新到最新 run
- 时间字段同时包含 UTC 和北京时间时，默认排障优先看 `*_bj`
- 如果 healthcheck 用的是 15 分钟 submit 节奏，对应 stale 阈值应为 `CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=1800`

## 11. 常用数据目录命令

### 删除旧的 run 日期目录

下面这条命令会删除 `./execution_engine_data/runs` 下，目录名匹配 `2026-*` 且早于 `2026-03-30` 的一级日期目录：

```bash
sudo find ./execution_engine_data/runs -maxdepth 1 -type d -name "2026-*" ! -newermt "2026-03-30" -exec rm -rf {} +
```

执行前要确认当前路径正确，避免误删错误目录。

### 查看现有 run 日期目录

```bash
find ./execution_engine_data/runs -maxdepth 1 -type d -name "2026-*" | sort
```

### 从远端机器拉取 execution_engine_data

把远端 `aws-poly` 机器上的 execution data 拉到本地 `server_data/`：

```bash
scp -r aws-poly:/var/lib/fortune_bot/execution_engine_data server_data/
```

如果 `server_data/` 不存在，可以先创建：

```bash
mkdir -p server_data
```

## 12. 最小完整命令序列

```bash
sudo systemctl stop fortune-bot-submit-window.timer fortune-bot-label-analysis.timer fortune-bot-healthcheck.timer
sudo systemctl stop fortune-bot-submit-window.service fortune-bot-label-analysis.service fortune-bot-healthcheck.service
cd /opt/fortune_bot
git pull --ff-only
sudo cp /etc/fortune-bot/fortune_bot.env /etc/fortune-bot/fortune_bot.env.bak.$(date +%Y%m%d_%H%M%S)
sudo nano /etc/fortune-bot/fortune_bot.env
sudo cp deploy/systemd/fortune-bot-submit-window.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start fortune-bot-submit-window.timer fortune-bot-label-analysis.timer fortune-bot-healthcheck.timer
systemctl list-timers --all | grep fortune-bot
systemctl status fortune-bot-submit-window.timer --no-pager
```
