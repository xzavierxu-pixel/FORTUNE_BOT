#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

RUN_ID="$(extract_arg_value --run-id "$@" || true)"
RUN_ID="${RUN_ID:-$(timestamp_run_id SUBMIT_WINDOW)}"

run_online_job "submit_window" "$RUN_ID" "run-submit-window" "$@"
