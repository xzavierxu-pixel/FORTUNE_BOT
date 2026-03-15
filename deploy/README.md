# Ubuntu 部署说明

这份文档只用于 Ubuntu 部署路径。

它不会替代你现在的 Windows 本地开发流程。
你本地现有的 `.ps1` 脚本保持不变。

## 1. 现在这套真实交易工作流是什么

你当前要走的是：

- Polymarket 邮箱登录账户
- 账户下有一个 proxy wallet
- 资金在 proxy wallet 里
- API 签名按邮箱登录路径处理

当前正确流程是：

1. 用浏览器登录 Polymarket，让 proxy wallet 被部署。
2. 在 Polymarket UI 里抄下 proxy wallet 地址。
3. 把 `USDC.e` 转到这个 proxy wallet。
4. 代码里用：
   - `signature_type=1`
   - `funder=proxy wallet 地址`
5. 私钥使用这个邮箱登录 Polymarket 账户对应的导出私钥。
6. 执行 `create_or_derive_api_creds()`，拿到 CLOB API 凭证。
7. 做一次 approve / allowance 更新。
8. 下一个极小测试单。
9. 再查 `open orders / trades / balances`。
10. 验证通过后再启动 execution 的定时工作流。

仓库现在已经按这条路径改好了：

- 默认 `PEG_CLOB_SIGNATURE_TYPE=1`
- 已新增一次性测试脚本：
  - `execution_engine/app/scripts/manual/proxy_wallet_smoketest.py`

## 2. 你现在这个状态下，还需要存 POL 吗

如果你走的是这条邮箱登录 + proxy wallet 路径，一般不需要额外往 Polymarket 里存 `POL` 才能开始这套 API 交易流程。

你现在更关键的是这几件事：

- 确认 `USDC.e` 已经在 Polymarket 的 proxy wallet 里
- 确认 proxy wallet 地址已经拿到
- 确认服务器上填的是：
  - `PEG_CLOB_PRIVATE_KEY`
  - `PEG_CLOB_FUNDER`
  - `PEG_CLOB_SIGNATURE_TYPE=1`
- 先跑一次 smoke test，把 API creds 和 allowance 跑通

只有在你自己额外做需要链上 gas 的钱包操作时，才需要单独考虑 `POL`。

## 3. 你现在还没配完的关键配置

如果你已经把 USDC 存进 Polymarket，但还没跑通 API，下列值通常还没配完整：

- `PEG_CLOB_PRIVATE_KEY`
  - 这是邮箱登录 Polymarket 账户对应的导出私钥
  - 不是给它转钱的 MetaMask 私钥
- `PEG_CLOB_FUNDER`
  - 这里必须填 Polymarket UI 里看到的 proxy wallet 地址
- `PEG_CLOB_SIGNATURE_TYPE=1`
  - 邮箱登录路径应该用 1
- `PEG_CLOB_API_KEY`
- `PEG_CLOB_API_SECRET`
- `PEG_CLOB_API_PASSPHRASE`

上面最后这 3 个才是 CLOB API 凭证。

如果你现在还没生成过它们，那是正常的。
你应该先跑 smoke test，由脚本调用 `create_or_derive_api_creds()` 生成或取回它们，再把结果填回环境变量文件。

## 4. deploy 目录里有什么

- `deploy/env/fortune_bot.env.example`
  - Ubuntu 环境变量模板
- `deploy/systemd/*.service`
  - 定时任务对应的 `systemd` service
- `deploy/systemd/*.timer`
  - 定时调度对应的 `systemd` timer
- `deploy/monitor/job_status.py`
  - 写入任务心跳和成功/失败状态文件
- `deploy/monitor/check_jobs.py`
  - 检查 timer 和心跳状态，并通过 SMTP 发邮件告警
- `execution_engine/app/scripts/linux/*.sh`
  - Linux 专用的 execution 工作流启动脚本
- `execution_engine/app/scripts/manual/proxy_wallet_smoketest.py`
  - 邮箱登录 proxy wallet 一次性联调脚本

## 5. 服务器依赖

你已经装好了：

- `python3-pip`
- `git`
- `tmux`
- `htop`

还建议补装：

```bash
sudo apt update
sudo apt install python3-venv build-essential -y
```

如果后面模型依赖安装失败，再补：

```bash
sudo apt install libgomp1 -y
```

## 6. 约定目录

这套部署默认使用这些 Linux 路径：

- 仓库根目录：`/opt/fortune_bot`
- 虚拟环境：`/opt/fortune_bot/.venv-execution`
- 运行状态目录：`/var/lib/fortune_bot`
- execution 数据目录：`/var/lib/fortune_bot/execution_engine_data`
- 环境变量文件：`/etc/fortune-bot/fortune_bot.env`

创建目录：

```bash
sudo mkdir -p /opt
sudo mkdir -p /var/lib/fortune_bot
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/shared
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/runs
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/summary
sudo mkdir -p /etc/fortune-bot
```

## 7. 代码放到服务器

如果 Ubuntu 上还没有代码：

```bash
cd /opt
sudo git clone https://github.com/xzavierxu-pixel/FORTUNE_BOT.git fortune_bot
sudo chown -R "$USER":"$USER" /opt/fortune_bot
cd /opt/fortune_bot
```

如果代码已经在服务器上：

```bash
cd /opt/fortune_bot
git pull
```

## 8. 环境变量文件

先复制模板：

```bash
cd /opt/fortune_bot
cp deploy/env/fortune_bot.env.example /tmp/fortune_bot.env
```

然后编辑：

```bash
nano /tmp/fortune_bot.env
```

至少先填这些：

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
- `PEG_CLOB_PRIVATE_KEY=你的Polymarket邮箱账户导出私钥`
- `PEG_CLOB_FUNDER=你的Polymarket proxy wallet地址`
- `PEG_CLOB_SIGNATURE_TYPE=1`
- `PEG_CLOB_API_KEY=先留空或临时占位`
- `PEG_CLOB_API_SECRET=先留空或临时占位`
- `PEG_CLOB_API_PASSPHRASE=先留空或临时占位`
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

## 9. `balances.json` 是什么

当前 execution 还会读一个本地余额文件：

- `/var/lib/fortune_bot/execution_engine_data/shared/balances.json`

这个文件现在更适合被当成：

- 本地策略资金上限
- execution 的可用资金上限

它不是 Polymarket 真实余额的自动同步结果。

示例：

```json
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-15T00:00:00Z"
}
```

创建：

```bash
cat > /var/lib/fortune_bot/execution_engine_data/shared/balances.json <<'EOF'
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-15T00:00:00Z"
}
EOF
```

如果你真实存进 Polymarket 的资金大于 100，但你现在只想让策略先跑 100，这里就写 100。

## 10. 初始化 Python 虚拟环境

执行：

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

验证：

```bash
/opt/fortune_bot/.venv-execution/bin/python --version
```

## 11. 先跑邮箱登录 proxy wallet smoke test

这一步是现在最关键的。

先加载环境变量：

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
```

运行一次性测试脚本：

```bash
cd /opt/fortune_bot
/opt/fortune_bot/.venv-execution/bin/python execution_engine/app/scripts/manual/proxy_wallet_smoketest.py
```

这个脚本会做：

1. 用 `signature_type=1 + funder=proxy wallet` 初始化 client
2. 执行 `create_or_derive_api_creds()`
3. 查询 approve 前余额和 allowance
4. 执行 collateral / conditional allowance 更新
5. 下一个极小测试买单
6. 查询 `open orders / trades / balances`

脚本默认测试单参数是：

- token：
  - `83155705733555118569646804738526000527065734405672442364016752623981274522859`
- 价格：
  - `0.55`
- 数量：
  - `1`

如果你想调小：

```bash
/opt/fortune_bot/.venv-execution/bin/python execution_engine/app/scripts/manual/proxy_wallet_smoketest.py --size 0.1
```

## 12. 把脚本打印出来的 API creds 写回环境变量

smoke test 成功后，把脚本输出的这 3 个值抄回 `/etc/fortune-bot/fortune_bot.env`：

- `PEG_CLOB_API_KEY`
- `PEG_CLOB_API_SECRET`
- `PEG_CLOB_API_PASSPHRASE`

然后重新加载环境变量：

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
```

到这一步，Polymarket API 侧的必要凭证就配齐了。

## 13. 手动试跑 execution 工作流

先手动执行一次。

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

如果这些都能跑通，再开定时。

## 14. 用 `tmux` 启动市场流

当前仓库里还没有 `stream-market-data` 的独立 `systemd` service。
所以目前先用 `tmux` 跑。

创建会话：

```bash
tmux new -s fortune-stream
```

进入后执行：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
bash execution_engine/app/scripts/linux/stream_market_data.sh
```

退出但不关会话：

```bash
Ctrl+b d
```

常用命令：

```bash
tmux ls
tmux attach -t fortune-stream
tmux kill-session -t fortune-stream
```

## 15. 安装 `systemd` 单元

复制 unit 文件：

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
```

重新加载：

```bash
sudo systemctl daemon-reload
```

启用 timers：

```bash
sudo systemctl enable --now fortune-bot-refresh-universe.timer
sudo systemctl enable --now fortune-bot-hourly-cycle.timer
sudo systemctl enable --now fortune-bot-label-analysis.timer
sudo systemctl enable --now fortune-bot-healthcheck.timer
```

查看：

```bash
systemctl list-timers --all | grep fortune-bot
```

## 16. 查看日志和任务状态

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

## 17. 测试 SMTP 邮件告警

手动执行一次健康检查：

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
python3 deploy/monitor/check_jobs.py
```

如果想强制测试邮件告警：

```bash
sudo systemctl stop fortune-bot-hourly-cycle.timer
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a
python3 deploy/monitor/check_jobs.py
```

然后恢复：

```bash
sudo systemctl start fortune-bot-hourly-cycle.timer
```

## 18. 现在你接下来应该做什么

如果你已经把 USDC 存进 Polymarket，那么推荐顺序就是：

1. 去 Polymarket UI 再确认一次 proxy wallet 地址。
2. 去账户设置确认并导出邮箱登录账户对应的私钥。
3. 把 `PEG_CLOB_PRIVATE_KEY` 和 `PEG_CLOB_FUNDER` 写进 `/etc/fortune-bot/fortune_bot.env`。
4. 保持 `PEG_CLOB_SIGNATURE_TYPE=1`。
5. 先跑 `proxy_wallet_smoketest.py`。
6. 把脚本打印出来的 `PEG_CLOB_API_KEY / SECRET / PASSPHRASE` 回填到 env 文件。
7. 再手动试跑 `refresh_universe` 和 `hourly_cycle`。
8. 没问题后再启 `tmux` market stream 和 `systemd` timers。

这就是你当前最短、最稳的上线路径。
