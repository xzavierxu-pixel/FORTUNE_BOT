#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

SERVICES=(
    "fortune-bot-submit-window.service"
    "fortune-bot-label-analysis.service"
    "fortune-bot-healthcheck.service"
)
TIMERS=(
    "fortune-bot-submit-window.timer"
    "fortune-bot-label-analysis.timer"
    "fortune-bot-healthcheck.timer"
)

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
    stop_systemd_units
    echo "[INFO] Pipeline stop sequence completed."
}

main "$@"
