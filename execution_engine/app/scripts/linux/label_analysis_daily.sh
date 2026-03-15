#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_common.sh"

RUN_ID="$(extract_arg_value --run-id "$@" || true)"
RUN_ID="${RUN_ID:-$(timestamp_run_id LABEL_ANALYSIS_DAILY)}"

run_online_job "label_analysis_daily" "$RUN_ID" "label-analysis-daily" "$@"
