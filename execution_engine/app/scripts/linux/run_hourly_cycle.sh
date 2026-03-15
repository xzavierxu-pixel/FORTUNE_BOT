#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

RUN_ID="$(extract_arg_value --run-id "$@" || true)"
RUN_ID="${RUN_ID:-$(timestamp_run_id HOURLY_CYCLE)}"

run_online_job "hourly_cycle" "$RUN_ID" "run-hourly-cycle" "$@"
