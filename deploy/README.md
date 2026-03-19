# Fortune Bot Ubuntu 部署说明

这份文档用于 AWS Ubuntu 服务器部署。
内容已经与当前仓库里的 `execution_engine` 生产路径对齐。

## 部署结论

当前 deploy 工作流与代码库现状是一致的。

当前生产工作流是：

1. `fortune-bot-submit-window.timer`
   运行基于直接翻页的 submit window 主流程。
   当 `PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER=1` 时，submit 流程内部会自动串联 post-submit order monitoring。
2. `fortune-bot-label-analysis.timer`
   运行每日标签分析和机会分析。
3. `fortune-bot-healthcheck.timer`
   检查 timer 状态和 job heartbeat 文件，并在需要时发送 SMTP 告警。

旧的 `refresh_universe` / `hourly_cycle` 定时任务已经不是当前有效部署模型的一部分，不应该再安装。

## 定时安排评估

当前定时安排对 AWS Ubuntu 是合理的：

- `submit-window`: `OnCalendar=hourly`
  这是主交易循环，和当前 direct submit-window 设计一致。
  当前部署的 `systemd` unit 会额外传入 `--max-pages 300`，所以每小时运行一次，且单次最多抓取 300 页。
- `label-analysis`: `OnCalendar=*-*-* 00:10:00`
  作为每日一次的分析任务是合理的。
  比整点晚 10 分钟，也避免和 submit-window 整点运行正面撞上。
- `healthcheck`: 每 5 分钟一次
  适合作 heartbeat 和 timer 监控，不会太重，也不会太慢。

推荐的服务器约定：

- 服务器时区保持为 `UTC`
- 上述 timer 按 `UTC` 理解
- 除非你明确希望所有任务整体平移，否则不要修改服务器时区

当前 heartbeat 阈值也合理：

- `CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=5400`
  给每小时任务留出执行和延迟缓冲
- `CHECK_LABEL_ANALYSIS_DAILY_MAX_AGE_SEC=93600`
  给每日任务留出约一天外加缓冲

## 服务器依赖

安装基础依赖：

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip build-essential htop
```

如果后续模型或运行时依赖报 OpenMP 相关缺失，再安装：

```bash
sudo apt install -y libgomp1
```

## 目录约定

这套部署默认使用：

- 仓库根目录：`/opt/fortune_bot`
- Python 虚拟环境：`/opt/fortune_bot/.venv-execution`
- 状态目录：`/var/lib/fortune_bot`
- execution 数据目录：`/var/lib/fortune_bot/execution_engine_data`
- 环境变量文件：`/etc/fortune-bot/fortune_bot.env`

创建这些目录：

```bash
sudo mkdir -p /opt
sudo mkdir -p /var/lib/fortune_bot
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/shared
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/runs
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/summary
sudo mkdir -p /etc/fortune-bot
```

## 克隆仓库

```bash
cd /opt
sudo git clone https://github.com/xzavierxu-pixel/FORTUNE_BOT.git fortune_bot
sudo chown -R "$USER":"$USER" /opt/fortune_bot
cd /opt/fortune_bot
```

如果服务器上已经有仓库：

```bash
cd /opt/fortune_bot
git pull --ff-only
```

## 环境变量文件

从模板开始：

```bash
cd /opt/fortune_bot
cp deploy/env/fortune_bot.env.example /tmp/fortune_bot.env
nano /tmp/fortune_bot.env
```

至少需要设置这些值：

```env
FORTUNE_BOT_REPO_ROOT=/opt/fortune_bot
FORTUNE_BOT_VENV=/opt/fortune_bot/.venv-execution
FORTUNE_BOT_STATE_DIR=/var/lib/fortune_bot

PEG_BASE_DATA_DIR=/var/lib/fortune_bot/execution_engine_data
PEG_SHARED_DATA_DIR=/var/lib/fortune_bot/execution_engine_data/shared
PEG_RUNS_ROOT_DIR=/var/lib/fortune_bot/execution_engine_data/runs
PEG_SUMMARY_DIR=/var/lib/fortune_bot/execution_engine_data/summary
PEG_BALANCES_PATH=/var/lib/fortune_bot/execution_engine_data/shared/balances.json

PEG_DRY_RUN=0
PEG_CLOB_ENABLED=1
PEG_CLOB_PRIVATE_KEY=replace_me
PEG_CLOB_FUNDER=replace_with_proxy_wallet_address
PEG_CLOB_SIGNATURE_TYPE=1
PEG_CLOB_API_KEY=replace_me
PEG_CLOB_API_SECRET=replace_me
PEG_CLOB_API_PASSPHRASE=replace_me

SMTP_HOST=replace_me
SMTP_PORT=465
SMTP_USE_SSL=1
SMTP_USERNAME=replace_me
SMTP_PASSWORD=replace_me
ALERT_EMAIL_FROM=replace_me
ALERT_EMAIL_TO=replace_me
ALERT_SUBJECT_PREFIX=[fortune-bot]
ALERT_COOLDOWN_SEC=3600

PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER=1
PEG_SUBMIT_WINDOW_MONITOR_SLEEP_SEC=0
PEG_SUBMIT_WINDOW_FAIL_ON_MONITOR_ERROR=0

CHECK_REQUIRED_UNITS=fortune-bot-submit-window.timer,fortune-bot-label-analysis.timer,fortune-bot-healthcheck.timer
CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=5400
CHECK_LABEL_ANALYSIS_DAILY_MAX_AGE_SEC=93600
```

把环境文件安装到正式位置：

```bash
sudo mv /tmp/fortune_bot.env /etc/fortune-bot/fortune_bot.env
sudo chmod 600 /etc/fortune-bot/fortune_bot.env
```

## balances.json

execution 层当前会读取一个本地余额文件：

- `/var/lib/fortune_bot/execution_engine_data/shared/balances.json`

它现在的作用是本地执行预算文件，不是链上余额自动同步结果。

示例：

```bash
cat > /var/lib/fortune_bot/execution_engine_data/shared/balances.json <<'EOF'
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-19T00:00:00Z"
}
EOF
```

如果你 Polymarket 账户里实际资金更多，但当前只想让策略使用 `100`，那这里就保持写 `100`。

## 初始化虚拟环境

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

验证：

```bash
/opt/fortune_bot/.venv-execution/bin/python --version
```

## 可选：先跑 Proxy Wallet Smoke Test

如果你想在启用 timer 前先验证凭据、allowance 和最小下单链路，可以先跑：

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

cd /opt/fortune_bot
/opt/fortune_bot/.venv-execution/bin/python execution_engine/app/scripts/manual/proxy_wallet_smoketest.py
```

这是最安全的联调方式，用来确认：

- `signature_type` 和 `funder` 配置正确
- API credentials 可以派生或可用
- allowance 设置正常
- 可以完成一次最小测试下单

## 手工端到端检查

在启用 timer 之前，建议先手工跑一遍完整流程。

先跑 submit window：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/run_submit_window.sh --run-id MANUAL_SUBMIT_001 --max-pages 1
```

再跑 label analysis：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/label_analysis_daily.sh --run-id MANUAL_LABEL_001 --scope all
```

如果这两个都能成功结束，说明部署主路径是通的。

## 安装 systemd Units

复制 unit 文件：

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

开机自启并立即启动：

```bash
sudo systemctl enable --now fortune-bot-submit-window.timer
sudo systemctl enable --now fortune-bot-label-analysis.timer
sudo systemctl enable --now fortune-bot-healthcheck.timer
```

验证：

```bash
systemctl list-timers --all | grep fortune-bot
systemctl status fortune-bot-submit-window.timer
systemctl status fortune-bot-label-analysis.timer
systemctl status fortune-bot-healthcheck.timer
```

## Pipeline 控制脚本

这些脚本和当前 deploy 模型是匹配的：

- `execution_engine/app/scripts/linux/start_pipeline.sh`
- `execution_engine/app/scripts/linux/stop_pipeline.sh`
- `execution_engine/app/scripts/linux/restart_pipeline.sh`

常用方式：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/start_pipeline.sh
```

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/restart_pipeline.sh
```

## 日志和健康状态

查看 service 日志：

```bash
journalctl -u fortune-bot-submit-window.service -n 100 --no-pager
journalctl -u fortune-bot-label-analysis.service -n 100 --no-pager
journalctl -u fortune-bot-healthcheck.service -n 100 --no-pager
```

查看 service 当前状态：

```bash
systemctl status fortune-bot-submit-window.service
systemctl status fortune-bot-label-analysis.service
systemctl status fortune-bot-healthcheck.service
```

查看 heartbeat 文件：

```bash
find /var/lib/fortune_bot/jobs -maxdepth 1 -type f | sort
```

## 手工测试 Healthcheck

手工执行一次 healthcheck：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

python3 deploy/monitor/check_jobs.py
```

如果 SMTP 配置正确，这个脚本只会在以下情况发送邮件：

- 必需的 timer 没有激活
- job heartbeat 文件缺失
- job 运行失败
- job 超时变 stale

## 推荐的 AWS 上线顺序

推荐按这个顺序在 Ubuntu 服务器上部署：

1. 克隆仓库并创建目录
2. 安装 `/etc/fortune-bot/fortune_bot.env`
3. 创建 `balances.json`
4. 初始化 execution venv
5. 可选：先跑 proxy wallet smoke test
6. 手工跑一次 `submit-window`
7. 手工跑一次 `label-analysis`
8. 安装并启用三个 timer
9. 用 `systemctl list-timers --all | grep fortune-bot` 确认调度生效

这就是当前仓库状态下正确的 Ubuntu / AWS 部署路径。
