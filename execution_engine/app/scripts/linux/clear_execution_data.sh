#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
DATA_DIR="${1:-$REPO_ROOT/execution_engine/data}"
EXPECTED_DATA_DIR="$REPO_ROOT/execution_engine/data"

if [[ "$(cd "$(dirname "$DATA_DIR")" && pwd)/$(basename "$DATA_DIR")" != "$EXPECTED_DATA_DIR" ]]; then
    echo "[ERROR] Refusing to clear unexpected path: $DATA_DIR" >&2
    exit 2
fi

mkdir -p "$EXPECTED_DATA_DIR"

echo "[INFO] Clearing execution data directory: $EXPECTED_DATA_DIR"
find "$EXPECTED_DATA_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +

mkdir -p \
    "$EXPECTED_DATA_DIR/runs" \
    "$EXPECTED_DATA_DIR/shared" \
    "$EXPECTED_DATA_DIR/summary" \
    "$EXPECTED_DATA_DIR/tests"

echo "[INFO] Execution data directory reset complete."
