# Fortune Bot Ubuntu Deployment

This document is the deployment path for an AWS Ubuntu server.
It is aligned with the current `execution_engine` production path in this repo.

## Deployment Verdict

The current deploy workflow is consistent with the codebase.

The production workflow is:

1. `fortune-bot-submit-window.timer`
   Runs the direct page-based submit window.
   The submit path already includes post-submit order monitoring when `PEG_SUBMIT_WINDOW_RUN_MONITOR_AFTER=1`.
2. `fortune-bot-label-analysis.timer`
   Runs daily label and opportunity analysis.
3. `fortune-bot-healthcheck.timer`
   Checks timer health and job heartbeat files, then sends SMTP alerts if needed.

Old `refresh_universe` / `hourly_cycle` timers are no longer part of the active deployment model and should not be installed.

## Schedule Review

The current schedule is reasonable for AWS Ubuntu:

- `submit-window`: `OnCalendar=hourly`
  This is the main trading loop and matches the current direct submit-window design.
  The deployed `systemd` unit runs it with `--max-pages 300`, so each hourly run has a hard page cap.
- `label-analysis`: `OnCalendar=*-*-* 00:10:00`
  Reasonable as a once-daily reconciliation job.
  The 10-minute offset avoids colliding with the top-of-hour submit run.
- `healthcheck`: every 5 minutes
  Reasonable for heartbeat and timer monitoring without being noisy.

Recommended server convention:

- Keep the server timezone on `UTC`.
- Interpret the above timer schedules in `UTC`.
- Do not change the server timezone unless you also want all timer behavior to shift.

The configured heartbeat thresholds are also reasonable:

- `CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=5400`
  Allows for an hourly run plus execution slippage.
- `CHECK_LABEL_ANALYSIS_DAILY_MAX_AGE_SEC=93600`
  Allows for a daily run plus a buffer.

## Server Prerequisites

Install base packages:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip build-essential htop
```

If model/runtime libraries later complain about missing OpenMP:

```bash
sudo apt install -y libgomp1
```

## Directory Layout

This deployment assumes:

- repo root: `/opt/fortune_bot`
- venv: `/opt/fortune_bot/.venv-execution`
- state dir: `/var/lib/fortune_bot`
- execution data: `/var/lib/fortune_bot/execution_engine_data`
- env file: `/etc/fortune-bot/fortune_bot.env`

Create them:

```bash
sudo mkdir -p /opt
sudo mkdir -p /var/lib/fortune_bot
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/shared
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/runs
sudo mkdir -p /var/lib/fortune_bot/execution_engine_data/summary
sudo mkdir -p /etc/fortune-bot
```

## Clone The Repo

```bash
cd /opt
sudo git clone https://github.com/xzavierxu-pixel/FORTUNE_BOT.git fortune_bot
sudo chown -R "$USER":"$USER" /opt/fortune_bot
cd /opt/fortune_bot
```

If the repo is already present:

```bash
cd /opt/fortune_bot
git pull --ff-only
```

## Environment File

Start from the template:

```bash
cd /opt/fortune_bot
cp deploy/env/fortune_bot.env.example /tmp/fortune_bot.env
nano /tmp/fortune_bot.env
```

At minimum, set:

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

Install the env file:

```bash
sudo mv /tmp/fortune_bot.env /etc/fortune-bot/fortune_bot.env
sudo chmod 600 /etc/fortune-bot/fortune_bot.env
```

## balances.json

The execution layer reads a local balance file:

- `/var/lib/fortune_bot/execution_engine_data/shared/balances.json`

This is currently a local execution budget file, not an automatic on-chain balance sync.

Example:

```bash
cat > /var/lib/fortune_bot/execution_engine_data/shared/balances.json <<'EOF'
{
  "available_usdc": 100.0,
  "total_usdc": 100.0,
  "updated_at_utc": "2026-03-19T00:00:00Z"
}
EOF
```

If your Polymarket account has more capital but you only want to let the bot use `100`, keep this file at `100`.

## Bootstrap The Venv

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

Verify:

```bash
/opt/fortune_bot/.venv-execution/bin/python --version
```

## Optional Proxy Wallet Smoke Test

If you want to validate credentials and allowance setup before enabling timers:

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

cd /opt/fortune_bot
/opt/fortune_bot/.venv-execution/bin/python execution_engine/app/scripts/manual/proxy_wallet_smoketest.py
```

This is the safest way to confirm:

- signature type and funder are correct
- API credentials can be derived or used
- allowance setup works
- a minimal test order can be created

## Manual End-To-End Checks

Before enabling timers, run the workflow manually once.

Run submit window:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/run_submit_window.sh --run-id MANUAL_SUBMIT_001 --max-pages 1
```

Run label analysis:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/label_analysis_daily.sh --run-id MANUAL_LABEL_001 --scope all
```

If both complete successfully, the deploy path is healthy.

## Install systemd Units

Copy the unit files:

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Enable timers at boot and start them now:

```bash
sudo systemctl enable --now fortune-bot-submit-window.timer
sudo systemctl enable --now fortune-bot-label-analysis.timer
sudo systemctl enable --now fortune-bot-healthcheck.timer
```

Verify:

```bash
systemctl list-timers --all | grep fortune-bot
systemctl status fortune-bot-submit-window.timer
systemctl status fortune-bot-label-analysis.timer
systemctl status fortune-bot-healthcheck.timer
```

## Pipeline Control Scripts

These helper scripts are aligned with the current deploy model:

- `execution_engine/app/scripts/linux/start_pipeline.sh`
- `execution_engine/app/scripts/linux/stop_pipeline.sh`
- `execution_engine/app/scripts/linux/restart_pipeline.sh`

Typical usage:

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/start_pipeline.sh
```

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/restart_pipeline.sh
```

## Logs And Health

Check service logs:

```bash
journalctl -u fortune-bot-submit-window.service -n 100 --no-pager
journalctl -u fortune-bot-label-analysis.service -n 100 --no-pager
journalctl -u fortune-bot-healthcheck.service -n 100 --no-pager
```

Check current service state:

```bash
systemctl status fortune-bot-submit-window.service
systemctl status fortune-bot-label-analysis.service
systemctl status fortune-bot-healthcheck.service
```

Check heartbeat files:

```bash
find /var/lib/fortune_bot/jobs -maxdepth 1 -type f | sort
```

## Healthcheck Test

Run the healthcheck manually:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

python3 deploy/monitor/check_jobs.py
```

If SMTP is configured correctly, this job will send mail only when:

- a required timer is inactive
- a job heartbeat file is missing
- a job has failed
- a job has gone stale

## Recommended AWS Rollout Order

Use this order on the Ubuntu server:

1. Clone repo and create directories.
2. Install `/etc/fortune-bot/fortune_bot.env`.
3. Create `balances.json`.
4. Bootstrap the execution venv.
5. Run the optional proxy wallet smoke test.
6. Run one manual `submit-window` test.
7. Run one manual `label-analysis` test.
8. Install and enable the three timers.
9. Confirm `systemctl list-timers --all | grep fortune-bot`.

That is the correct deploy path for the current repo state.
