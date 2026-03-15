#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
VENV_DIR="${FORTUNE_BOT_VENV:-$REPO_ROOT/.venv-execution}"
VENV_PYTHON="$VENV_DIR/bin/python"
STATUS_SCRIPT="$REPO_ROOT/deploy/monitor/job_status.py"

timestamp_run_id() {
    local prefix="$1"
    printf '%s_%s\n' "$prefix" "$(date -u +%Y%m%dT%H%M%SZ)"
}

extract_arg_value() {
    local target="$1"
    shift
    local prev=""
    for arg in "$@"; do
        if [[ "$prev" == "$target" ]]; then
            printf '%s\n' "$arg"
            return 0
        fi
        prev="$arg"
    done
    return 1
}

ensure_venv() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        "$SCRIPT_DIR/bootstrap_venv.sh"
    fi
}

mark_job_start() {
    local job_name="$1"
    local run_id="$2"
    /usr/bin/env python3 "$STATUS_SCRIPT" start --job "$job_name" --run-id "$run_id"
}

mark_job_finish() {
    local job_name="$1"
    local run_id="$2"
    local exit_code="$3"
    /usr/bin/env python3 "$STATUS_SCRIPT" finish --job "$job_name" --run-id "$run_id" --exit-code "$exit_code"
}

run_online_job() {
    local job_name="$1"
    local run_id="$2"
    local subcommand="$3"
    shift 3

    ensure_venv
    mark_job_start "$job_name" "$run_id"

    local exit_code=0
    if ! (
        cd "$REPO_ROOT"
        exec "$VENV_PYTHON" -m execution_engine.app.cli.online.main "$subcommand" "$@"
    ); then
        exit_code=$?
    fi

    mark_job_finish "$job_name" "$run_id" "$exit_code"
    return "$exit_code"
}
