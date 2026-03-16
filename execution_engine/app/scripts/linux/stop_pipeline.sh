#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

TMUX_SESSION="${FORTUNE_BOT_STREAM_TMUX_SESSION:-fortune-stream}"
SERVICES=(
    "fortune-bot-refresh-universe.service"
    "fortune-bot-hourly-cycle.service"
    "fortune-bot-label-analysis.service"
    "fortune-bot-healthcheck.service"
)
TIMERS=(
    "fortune-bot-refresh-universe.timer"
    "fortune-bot-hourly-cycle.timer"
    "fortune-bot-label-analysis.timer"
    "fortune-bot-healthcheck.timer"
)

stop_tmux_stream() {
    if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
        echo "[INFO] Stopping tmux session: $TMUX_SESSION"
        tmux kill-session -t "$TMUX_SESSION"
    else
        echo "[INFO] Tmux session not running: $TMUX_SESSION"
    fi
}

stop_systemd_units() {
    local unit
    for unit in "${TIMERS[@]}"; do
        if systemctl list-unit-files "$unit" --no-legend 2>/dev/null | grep -q "^$unit"; then
            echo "[INFO] Stopping timer: $unit"
            sudo systemctl stop "$unit" || true
        fi
    done
    for unit in "${SERVICES[@]}"; do
        if systemctl list-unit-files "$unit" --no-legend 2>/dev/null | grep -q "^$unit"; then
            echo "[INFO] Stopping service: $unit"
            sudo systemctl stop "$unit" || true
        fi
    done
}

main() {
    echo "[INFO] Repo root: $REPO_ROOT"
    stop_tmux_stream
    stop_systemd_units
    echo "[INFO] Pipeline stop sequence completed."
}

main "$@"
