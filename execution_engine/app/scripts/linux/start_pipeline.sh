#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

ENV_FILE="${FORTUNE_BOT_ENV_FILE:-/etc/fortune-bot/fortune_bot.env}"
TMUX_SESSION="${FORTUNE_BOT_STREAM_TMUX_SESSION:-fortune-stream}"
STREAM_SCRIPT="$SCRIPT_DIR/stream_market_data.sh"
SERVICES=(
    "fortune-bot-refresh-universe.service"
    "fortune-bot-submit-window.service"
    "fortune-bot-label-analysis.service"
    "fortune-bot-healthcheck.service"
    "fortune-bot-hourly-cycle.service"
)
TIMERS=(
    "fortune-bot-refresh-universe.timer"
    "fortune-bot-submit-window.timer"
    "fortune-bot-label-analysis.timer"
    "fortune-bot-healthcheck.timer"
    "fortune-bot-hourly-cycle.timer"
)

SKIP_BOOTSTRAP=0

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-bootstrap)
                SKIP_BOOTSTRAP=1
                shift
                ;;
            *)
                echo "[ERROR] Unknown argument: $1" >&2
                exit 2
                ;;
        esac
    done
}

load_env_file() {
    if [[ -f "$ENV_FILE" ]]; then
        echo "[INFO] Loading env file: $ENV_FILE"
        set -a
        # shellcheck disable=SC1090
        source "$ENV_FILE"
        set +a
    else
        echo "[WARN] Env file not found: $ENV_FILE"
    fi
}

bootstrap_env() {
    if [[ "$SKIP_BOOTSTRAP" -eq 1 ]]; then
        echo "[INFO] Skipping virtualenv bootstrap."
        return 0
    fi
    echo "[INFO] Ensuring virtualenv is ready."
    bash "$SCRIPT_DIR/bootstrap_venv.sh"
}

clear_running_services() {
    local unit
    for unit in "${TIMERS[@]}"; do
        if systemctl is-active --quiet "$unit"; then
            echo "[INFO] Stopping timer before start: $unit"
            sudo systemctl stop "$unit" || true
        fi
    done
    for unit in "${SERVICES[@]}"; do
        if systemctl is-active --quiet "$unit"; then
            echo "[INFO] Waiting for service to stop: $unit"
            sudo systemctl stop "$unit" || true
        fi
    done
}

start_tmux_stream() {
    if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
        echo "[INFO] Replacing existing tmux session: $TMUX_SESSION"
        tmux kill-session -t "$TMUX_SESSION"
    fi
    echo "[INFO] Starting tmux session: $TMUX_SESSION"
    tmux new-session -d -s "$TMUX_SESSION" "bash -lc 'set -euo pipefail; cd \"$REPO_ROOT\"; if [[ -f \"$ENV_FILE\" ]]; then set -a; source \"$ENV_FILE\"; set +a; fi; exec bash \"$STREAM_SCRIPT\"'"
}

start_timers() {
    local unit
    echo "[INFO] Reloading systemd."
    sudo systemctl daemon-reload
    for unit in "${TIMERS[@]}"; do
        if systemctl list-unit-files "$unit" --no-legend 2>/dev/null | grep -q "^$unit"; then
            echo "[INFO] Starting timer: $unit"
            sudo systemctl start "$unit"
        else
            echo "[WARN] Timer not installed: $unit"
        fi
    done
}

main() {
    parse_args "$@"
    echo "[INFO] Repo root: $REPO_ROOT"
    load_env_file
    bootstrap_env
    clear_running_services
    start_tmux_stream
    start_timers
    echo "[INFO] Pipeline start sequence completed."
    echo "[INFO] Verify with: tmux ls && systemctl list-timers --all | grep fortune-bot"
}

main "$@"
