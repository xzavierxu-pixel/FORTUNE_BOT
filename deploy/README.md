# Fortune Bot Server Deployment

This document is the current deployment guide for running the live `execution_engine` workflow on a new Amazon Linux server.

It matches the repository's current production model:

- `submit_window` is the primary trading workflow
- `label_analysis_daily` runs once per day
- `healthcheck` watches timers and job heartbeats
- Linux `systemd` units load `/etc/fortune-bot/fortune_bot.env`
- default order TTL is `900` seconds
- per-run submit artifacts now live under `submit_window/`, not `submit_hourly/`

## Current Workflow

The deployed workflow consists of three timers:

1. `fortune-bot-submit-window.timer`
   Runs the direct page-based online submit loop every 15 minutes.
2. `fortune-bot-label-analysis.timer`
   Runs the daily lifecycle and opportunity analysis job.
3. `fortune-bot-healthcheck.timer`
   Checks required timers and job heartbeat freshness, then sends SMTP alerts when needed.

The old `refresh_universe` and `hourly_cycle` deployment model is not part of the current production path and should not be installed.

## Scheduling

Current `systemd` schedules in this repo:

- `submit-window`: `OnCalendar=*:0/15`
- `label-analysis`: `OnCalendar=*-*-* 00:10:00`
- `healthcheck`: `OnCalendar=*:0/5`

The checked-in `fortune-bot-submit-window.service` runs:

```bash
bash "$FORTUNE_BOT_REPO_ROOT/execution_engine/app/scripts/linux/run_submit_window.sh" --max-pages 300
```

That means each scheduled submit run processes up to 300 Gamma pages unless you change the unit file.

Recommended server convention:

- keep the server timezone at `UTC`
- let `systemd` timers run in `UTC`
- keep operator-facing reporting timestamps in Beijing time where the app already does that

## Server Prerequisites

Install base packages. The live model bundle in `version3` was built on Python `3.13`, so the server runtime should also use Python `3.13`.

```bash
sudo dnf update -y
sudo dnf install -y git git-lfs python3.13 python3.13-pip python3.13-devel gcc gcc-c++
```

If runtime dependencies later complain about OpenMP, install:

```bash
sudo dnf install -y libgomp
```

Confirm `venv` support is available before bootstrapping:

```bash
python3.13 -m venv --help
```

If that command fails, install the matching Python development packages available on your Amazon Linux image, then retry.

On Amazon Linux, `/tmp` is often a small `tmpfs` mount. Large Python packages may fail to install there with `No space left on device` even when `/` still has free space.

Set a larger temp directory on the root volume before creating the venv and installing dependencies:

```bash
mkdir -p /opt/fortune_bot/.tmp
export TMPDIR=/opt/fortune_bot/.tmp
```

## Directory Layout

Recommended server layout:

- repo root: `/opt/fortune_bot`
- execution venv: `/opt/fortune_bot/.venv-execution`
- state root: `/opt/fortune_bot/execution_engine/runtime_state`
- execution data root: `/opt/fortune_bot/execution_engine/data`
- env file: `/etc/fortune-bot/fortune_bot.env`

Create the directories:

```bash
sudo mkdir -p /opt
sudo mkdir -p /etc/fortune-bot
```

## Clone or Update the Repo

Fresh clone:

```bash
cd /opt
sudo git clone --branch version3 --single-branch https://github.com/xzavierxu-pixel/FORTUNE_BOT.git fortune_bot
sudo chown -R "$USER":"$USER" /opt/fortune_bot
cd /opt/fortune_bot
```

Create the in-repo runtime directories after the repo exists:

```bash
mkdir -p /opt/fortune_bot/execution_engine/data/shared
mkdir -p /opt/fortune_bot/execution_engine/data/runs
mkdir -p /opt/fortune_bot/execution_engine/data/summary
mkdir -p /opt/fortune_bot/execution_engine/runtime_state
```

Update an existing checkout:

```bash
cd /opt/fortune_bot
git checkout version3
git pull --ff-only origin version3
```

## Environment File

The Linux `systemd` services in this repo load:

- `/etc/fortune-bot/fortune_bot.env`

Specifically, [fortune-bot-submit-window.service](C:\Users\ROG\Desktop\fortune_bot\deploy\systemd\fortune-bot-submit-window.service) uses:

```ini
EnvironmentFile=/etc/fortune-bot/fortune_bot.env
```

Create the env file from the deployment template, then fill in the values for the new server and the new Polymarket account:

```bash
cd /opt/fortune_bot
cp deploy/env/fortune_bot.env.example /tmp/fortune_bot.env
nano /tmp/fortune_bot.env
```

At minimum, set or verify:

```env
FORTUNE_BOT_REPO_ROOT=/opt/fortune_bot
FORTUNE_BOT_VENV=/opt/fortune_bot/.venv-execution
FORTUNE_BOT_STATE_DIR=/opt/fortune_bot/execution_engine/runtime_state

PEG_BASE_DATA_DIR=/opt/fortune_bot/execution_engine/data
PEG_SHARED_DATA_DIR=/opt/fortune_bot/execution_engine/data/shared
PEG_RUNS_ROOT_DIR=/opt/fortune_bot/execution_engine/data/runs
PEG_SUMMARY_DIR=/opt/fortune_bot/execution_engine/data/summary

PEG_DRY_RUN=0
PEG_CLOB_ENABLED=1
PEG_CLOB_PRIVATE_KEY=replace_me
PEG_CLOB_FUNDER=replace_me_if_proxy_wallet_is_used
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
PEG_SUBMIT_WINDOW_ASYNC_POST_SUBMIT=1

PEG_ORDER_TTL_SEC=900

CHECK_REQUIRED_UNITS=fortune-bot-submit-window.timer,fortune-bot-label-analysis.timer,fortune-bot-healthcheck.timer
CHECK_SUBMIT_WINDOW_MAX_AGE_SEC=1800
CHECK_LABEL_ANALYSIS_DAILY_MAX_AGE_SEC=93600
```

Install it into place:

```bash
sudo mv /tmp/fortune_bot.env /etc/fortune-bot/fortune_bot.env
sudo chmod 600 /etc/fortune-bot/fortune_bot.env
```

Important notes:

- do not reuse credentials from any existing account
- generate a fresh CLOB API key, secret, and passphrase for the new Polymarket account
- if you use a proxy wallet, make sure `PEG_CLOB_FUNDER` and `PEG_CLOB_SIGNATURE_TYPE` match that wallet mode

## Balance Behavior

The current live workflow reads available USDC from the online wallet through the CLOB client.

That means:

- live allocation no longer depends on a local `balances.json` seed file
- you do not need to create `/opt/fortune_bot/execution_engine/data/shared/balances.json` for the live path
- the wallet used by `PEG_CLOB_PRIVATE_KEY` and related CLOB credentials must actually hold the funds you intend to trade

## Bootstrap the Execution Environment

Create the isolated live-trading venv:

```bash
cd /opt/fortune_bot
mkdir -p /opt/fortune_bot/.tmp
export TMPDIR=/opt/fortune_bot/.tmp
export FORTUNE_BOT_PYTHON_BIN=python3.13
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

The bootstrap script installs `py-clob-client` from the official GitHub repository by default:

- [Polymarket/py-clob-client](https://github.com/Polymarket/py-clob-client)

If you want to pin a specific branch, tag, or commit for server deployment, set:

```bash
export FORTUNE_BOT_PY_CLOB_CLIENT_REF=<git-ref>
```

If you maintain your own fork, you can override the source URL:

```bash
export FORTUNE_BOT_PY_CLOB_CLIENT_GIT_URL=<git-url>
```

Verify:

```bash
/opt/fortune_bot/.venv-execution/bin/python --version
```

Expected output should be Python `3.13.x`.

## Recommended Preflight for a New Polymarket Account

Before enabling timers, validate the new account end-to-end:

```bash
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

cd /opt/fortune_bot
/opt/fortune_bot/.venv-execution/bin/python execution_engine/app/scripts/manual/proxy_wallet_smoketest.py
```

Use this to confirm:

- the wallet can derive or use API credentials successfully
- signature mode and funder settings are correct
- allowance reads and updates work
- the account can submit a minimal test order

## Manual End-to-End Checks

Before enabling timers, run the main jobs manually once.

Run `submit_window`:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/run_submit_window.sh --run-id MANUAL_SUBMIT_001 --max-pages 10
```

Run daily label analysis:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/label_analysis_daily.sh --run-id MANUAL_LABEL_001 --scope all
```

Optional standalone lifecycle monitoring:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

bash execution_engine/app/scripts/linux/monitor_orders.sh --run-id MANUAL_MONITOR_001 --sleep-sec 0
```

Expected artifact locations:

- `/opt/fortune_bot/execution_engine/data/runs/YYYY-MM-DD/<run_id>/submit_window/manifest.json`
- `/opt/fortune_bot/execution_engine/data/runs/YYYY-MM-DD/<run_id>/submit_window/submission_attempts.csv`
- `/opt/fortune_bot/execution_engine/data/runs/YYYY-MM-DD/<run_id>/submit_window/orders_submitted.jsonl`
- `/opt/fortune_bot/execution_engine/data/runs/YYYY-MM-DD/<run_id>/order_monitor/manifest.json`
- `/opt/fortune_bot/execution_engine/data/runs/YYYY-MM-DD/<run_id>/label_analysis/manifest.json`

If these jobs finish successfully, the deployment path is wired correctly.

## Install systemd Units

Copy the checked-in unit files:

```bash
cd /opt/fortune_bot
sudo cp deploy/systemd/fortune-bot-*.service /etc/systemd/system/
sudo cp deploy/systemd/fortune-bot-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

Enable and start the timers:

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

These Linux scripts match the current deployment model:

- `execution_engine/app/scripts/linux/start_pipeline.sh`
- `execution_engine/app/scripts/linux/stop_pipeline.sh`
- `execution_engine/app/scripts/linux/restart_pipeline.sh`

Common usage:

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/start_pipeline.sh
```

```bash
cd /opt/fortune_bot
bash execution_engine/app/scripts/linux/restart_pipeline.sh
```

## Updating a Running Server

If the workflow is already running and you need to deploy the latest `version3` code, use this order.

Stop timers first so new runs do not start while you update:

```bash
sudo systemctl stop fortune-bot-submit-window.timer
sudo systemctl stop fortune-bot-label-analysis.timer
sudo systemctl stop fortune-bot-healthcheck.timer
```

If a job is already in progress, stop the active services too:

```bash
sudo systemctl stop fortune-bot-submit-window.service
sudo systemctl stop fortune-bot-label-analysis.service
sudo systemctl stop fortune-bot-healthcheck.service
```

Confirm nothing is still running:

```bash
systemctl status fortune-bot-submit-window.service
systemctl status fortune-bot-label-analysis.service
systemctl status fortune-bot-healthcheck.service
```

Pull the latest code from `version3`:

```bash
cd /opt/fortune_bot
git fetch origin
git checkout version3
git pull --ff-only origin version3
```

If dependency or startup scripts changed, rebuild the venv before restarting:

```bash
cd /opt/fortune_bot
mkdir -p /opt/fortune_bot/.tmp
export TMPDIR=/opt/fortune_bot/.tmp
export FORTUNE_BOT_PYTHON_BIN=python3.13
bash execution_engine/app/scripts/linux/bootstrap_venv.sh
```

Reload units and restart the workflow:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fortune-bot-submit-window.timer
sudo systemctl enable --now fortune-bot-label-analysis.timer
sudo systemctl enable --now fortune-bot-healthcheck.timer
```

Verify the timers and watch the first submit run:

```bash
systemctl list-timers --all | grep fortune-bot
systemctl status fortune-bot-submit-window.timer
journalctl -u fortune-bot-submit-window.service -f
```

## Logs and Health

View service logs:

```bash
journalctl -u fortune-bot-submit-window.service -n 100 --no-pager
journalctl -u fortune-bot-label-analysis.service -n 100 --no-pager
journalctl -u fortune-bot-healthcheck.service -n 100 --no-pager
```

View current service state:

```bash
systemctl status fortune-bot-submit-window.service
systemctl status fortune-bot-label-analysis.service
systemctl status fortune-bot-healthcheck.service
```

Inspect heartbeat files:

```bash
find /opt/fortune_bot/execution_engine/runtime_state/jobs -maxdepth 1 -type f | sort
```

## Manual Healthcheck

Run a one-off healthcheck:

```bash
cd /opt/fortune_bot
set -a
source /etc/fortune-bot/fortune_bot.env
set +a

python3 deploy/monitor/check_jobs.py
```

If SMTP is configured correctly, this script only sends email when:

- a required timer is inactive
- a heartbeat file is missing
- a job failed
- a job is stale beyond its allowed freshness window

## Recommended Deployment Order

Use this order on a fresh Amazon Linux server:

1. Clone the repo and create the required directories.
2. Prepare `/etc/fortune-bot/fortune_bot.env` with the new server's credentials and paths.
3. Bootstrap `.venv-execution`.
4. Run the proxy wallet smoke test.
5. Manually run `submit_window`.
6. Manually run `label_analysis_daily`.
7. Install and enable the three `systemd` timers.
8. Verify the timer schedule with `systemctl list-timers --all | grep fortune-bot`.

That is the current deployment path for this repository on Amazon Linux.
