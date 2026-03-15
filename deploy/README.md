# Ubuntu 部署说明

这份文档只用于 Ubuntu 部署路径。

它不会替代你现在的 Windows 本地开发流程。
你本地现有的 `.ps1` 脚本保持不变。

## 1. `deploy` 目录里有什么

- `deploy/env/fortune_bot.env.example`
  - Ubuntu 环境变量模板。
- `deploy/systemd/*.service`
  - 定时任务对应的 `systemd` service。
- `deploy/systemd/*.timer`
  - 定时调度对应的 `systemd` timer。
- `deploy/monitor/job_status.py`
  - 写入任务心跳和成功/失败状态文件。
- `deploy/monitor/check_jobs.py`
  - 检查 timer 和心跳状态，并通过 SMTP 发邮件告警。
- `execution_engine/app/scripts/linux/*.sh`
  - Linux 专用的 execution 工作流启动脚本。

## 2. 当前 Ubuntu 运行形态

当前 Ubuntu 部署分成两部分：

- 由 `systemd` 管理的定时任务
  - `refresh-universe`
  - `hourly-cycle`
  - `label-analysis`
  - `healthcheck`
- 长驻的市场流进程
  - 目前用 `tmux` 手动启动
  - 这个仓库里暂时还没有单独的 `systemd` service

如果你要把 execution 整个跑起来，需要同时启动：

- `systemd` timers
- 长驻的 `stream-market-data`

## 3. 服务器依赖

你已经装好了：

- `python3-pip`
- `git`
- `tmux`
- `htop`

还建议补装这些：

```bash
sudo apt update
sudo apt install python3-venv build-essential -y
```

如果后面模型依赖安装失败，再补：

```bash
sudo apt install libgomp1 -y
```

## 4. 约定的目录

这套部署默认使用这些 Linux 路径：

- 仓库根目录：`/opt/fortune_bot`
- 虚拟环境：`/opt/fortune_bot/.venv-execution`
- 运行状态目录：`/var/lib/fortune_bot`
- execution 数据目录：`/var/lib/fortune_bot/execution_engine_data`
- 环境变量文件：`/etc/fortune-bot/fortune_bot.env`

先创建这些目录：

```bash
sudo mkdir -p /opt
sudo mkdir -p /var/lib/fortune_bot
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/shared
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/runs
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/summary
sudo mkdir -p /etc/fortune-bot
```

## 5. 把仓库放到服务器上

如果 Ubuntu 上还没有代码：

```bash
cd /opt
sudo git clone <your-repo-url> fortune_bot
sudo chown -R "$USER":"$USER" /opt/fortune_bot
cd /opt/fortune_bot
```

如果代码已经在服务器上，只需要拉最新：

```bash
cd /opt/fortune_bot
git pull
```

## 6. 创建 Ubuntu 环境变量文件

先复制模板：

```bash
cd /opt/fortune_bot
cp deploy/env/fortune_bot.env.example /tmp/fortune_bot.env
```

然后编辑：

```bash
nano /tmp/fortune_bot.env
```

至少要把这些变量填对：

- `FORTUNE_BOT_REPO_ROOT=/opt/fortune_bot`
- `FORTUNE_BOT_VENV=/opt/fortune_bot/.venv-execution`
- `FORTUNE_BOT_STATE_DIR=/var/lib/fortune_bot`
- `PEG_BASE_DATA_DIR=/var/lib/fortune_bot/execution_engine_data`
- `PEG_SHARED_DATA_DIR=/var/lib/fortune_bot/execution_engine_data/shared`
- `PEG_RUNS_ROOT_DIR=/var/lib/fortune_bot/execution_engine_data/runs`
- `PEG_SUMMARY_DIR=/var/lib/fortune_bot/execution_engine_data/summary`
- `PEG_BALANCES_PATH=/var/lib/fortune_bot/execution_engine_data/shared/balances.json`
- `PEG_DRY_RUN=0`
- `PEG_CLOB_ENABLED=1`
- `PEG_CLOB_PRIVATE_KEY=...`
- `PEG_CLOB_API_KEY=...`
- `PEG_CLOB_API_SECRET=...`
- `PEG_CLOB_API_PASSPHRASE=...`
- `SMTP_HOST=smtp.qq.com`
- `SMTP_PORT=465`
- `SMTP_USE_SSL=1`
- `SMTP_USERNAME=191611752@qq.com`
- `SMTP_PASSWORD=<QQ SMTP 授权码，不是邮箱登录密码>`
- `ALERT_EMAIL_FROM=191611752@qq.com`
- `ALERT_EMAIL_TO=191611752@qq.com`

编辑完成后安装到正式位置：

```bash
sudo mv /tmp/fortune_bot.env /etc/fortune-bot/fortune_bot.env
sudo chmod 600 /etc/fortune-bot/fortune_bot.env
```

## 7. 准备 `balances.json`

当前 execution 流程依赖余额文件。

文件路径是：

- `/var/lib/fortune_bot/execution_engine_data/shared/balances.json`

示例内容：

```json
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-15T00:00:00Z"
}
```

可以直接这样创建：

```bash
cat > /var/lib/fortune_bot/execution_engine_data/shared/balances.json <<'EOF'
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-15T00:00:00Z"
}
EOF
```

这里的金额要按你真实希望给 execution 使用的本金来填写。

## 8. 初始化 Python 虚拟环境

执行：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

这个脚本会做三件事：

- 创建 `/opt/fortune_bot/.venv-execution`
- 安装 `execution_engine/requirements-live.txt`
- 以 editable 模式安装本地 `py-clob-client`

可以这样验证 venv 是否正常：

```bash
/opt/fortune_bot/.venv-execution/bin/python --version
```

## 9. 在启用定时器之前先手动试跑

先把环境变量加载到当前 shell：

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
```

然后依次手动执行一次。

刷新 universe：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/refresh_universe.sh --max-markets 1000
```

执行一次 hourly cycle：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/run_hourly_cycle.sh --skip-refresh-universe
```

执行 label analysis：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/label_analysis_daily.sh --scope all
```

这三个命令如果都能跑通，说明定时任务路径基本可用了。

## 10. 用 `tmux` 启动市场流

当前仓库里还没有 `stream-market-data` 的独立 `systemd` service。
所以目前先用 `tmux` 跑。

先创建一个 `tmux` 会话：

```bash
tmux new -s fortune-stream
```

进入 `tmux` 之后执行：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
bash execution_engine/app/scripts/linux/stream_market_data.sh
```

退出但不关闭会话：

```bash
Ctrl+b d
```

后续常用的 `tmux` 命令：

```bash
tmux ls
tmux attach -t fortune-stream
tmux kill-session -t fortune-stream
```

## 11. 安装 `systemd` 单元

把 unit 文件复制到系统目录：

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
```

重新加载 `systemd`：

```bash
sudo systemctl daemon-reload
```

启用这些 timers：

```bash
sudo systemctl enable --now fortune-bot-refresh-universe.timer
sudo systemctl enable --now fortune-bot-hourly-cycle.timer
sudo systemctl enable --now fortune-bot-label-analysis.timer
sudo systemctl enable --now fortune-bot-healthcheck.timer
```

查看 timer 是否启动成功：

```bash
systemctl list-timers --all | grep fortune-bot
```

## 12. 每个 timer 是做什么的

- `fortune-bot-refresh-universe.timer`
  - 触发 `refresh_universe.sh`
- `fortune-bot-hourly-cycle.timer`
  - 触发 `run_hourly_cycle.sh --skip-refresh-universe`
- `fortune-bot-label-analysis.timer`
  - 触发 `label_analysis_daily.sh --scope all`
- `fortune-bot-healthcheck.timer`
  - 触发 `deploy/monitor/check_jobs.py`
  - 如果 timer 缺失、任务失败或心跳过期，会通过 SMTP 发邮件

## 13. 查看日志和任务状态

查看 service 日志：

```bash
journalctl -u fortune-bot-refresh-universe.service -n 100 --no-pager
journalctl -u fortune-bot-hourly-cycle.service -n 100 --no-pager
journalctl -u fortune-bot-label-analysis.service -n 100 --no-pager
journalctl -u fortune-bot-healthcheck.service -n 100 --no-pager
```

查看 service 状态：

```bash
systemctl status fortune-bot-refresh-universe.service
systemctl status fortune-bot-hourly-cycle.service
systemctl status fortune-bot-label-analysis.service
systemctl status fortune-bot-healthcheck.service
```

查看心跳文件：

```bash
find /var/lib/fortune_bot/jobs -maxdepth 1 -type f | sort
```

这些心跳和状态文件由下面这个脚本写入：

- `deploy/monitor/job_status.py`

这些文件会被下面这个脚本读取检查：

- `deploy/monitor/check_jobs.py`

## 14. 测试 SMTP 邮件告警

先手动执行一次健康检查：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
python3 deploy/monitor/check_jobs.py
```

如果系统健康，通常不会有明显输出。

如果你想强制测试邮件告警，可以先停掉一个 timer 再执行：

```bash
sudo systemctl stop fortune-bot-hourly-cycle.timer
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
python3 deploy/monitor/check_jobs.py
```

测试完记得恢复：

```bash
sudo systemctl start fortune-bot-hourly-cycle.timer
```

## 15. 日常更新和维护

拉最新代码：

```bash
cd /opt/fortune_bot
git pull
```

如果 Python 依赖有变化：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

如果 deploy 里的 `systemd` 文件有改动：

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

然后重启 timers：

```bash
sudo systemctl restart fortune-bot-refresh-universe.timer
sudo systemctl restart fortune-bot-hourly-cycle.timer
sudo systemctl restart fortune-bot-label-analysis.timer
sudo systemctl restart fortune-bot-healthcheck.timer
```

如果你更新了 stream 相关代码，还要手动重启 `tmux` 里的市场流进程。

## 16. 常见故障点

- `python3 -m venv` 找不到
  - 安装 `python3-venv`
- 依赖安装失败
  - 安装 `build-essential`
- 邮件发不出去
  - 确认 `SMTP_PASSWORD` 用的是 QQ 邮箱 SMTP 授权码
  - 确认 QQ 邮箱已经开启 SMTP
- timer 显示正常但任务没跑
  - 先看对应 `.service` 的 `journalctl` 日志
- `systemd` 下任务一启动就失败
  - 检查 `/etc/fortune-bot/fortune_bot.env`
  - 检查 `FORTUNE_BOT_REPO_ROOT=/opt/fortune_bot`
  - 检查服务器上代码是不是确实在这个目录
- execution 没有下单
  - 检查 `balances.json`
  - 检查 `PEG_DRY_RUN`
  - 检查候选市场是否真的生成了可交易信号

## 17. 最短上线顺序

如果这是台新的 Ubuntu 机器，推荐按这个顺序做：

1. 安装 apt 依赖。
2. 把仓库 clone 到 `/opt/fortune_bot`。
3. 创建 `/etc/fortune-bot/fortune_bot.env`。
4. 创建 `/var/lib/fortune_bot/execution_engine_data/shared/balances.json`。
5. 运行 `bootstrap_venv.sh`。
6. 手动测试 `refresh_universe.sh`。
7. 手动测试 `run_hourly_cycle.sh`。
8. 用 `tmux` 启动 `stream_market_data.sh`。
9. 安装并启用 `systemd` timers。
10. 手动运行一次 `check_jobs.py`，确认邮件告警能收到。

做完这些，Ubuntu 上的 execution 工作流就起来了。
