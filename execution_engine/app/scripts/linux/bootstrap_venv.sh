#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
VENV_DIR="${FORTUNE_BOT_VENV:-$REPO_ROOT/.venv-execution}"
VENV_PYTHON="$VENV_DIR/bin/python"
REQUIREMENTS_PATH="$REPO_ROOT/execution_engine/requirements-live.txt"
CLOB_CLIENT_PATH="$REPO_ROOT/py-clob-client"

if [[ ! -d "$VENV_DIR" ]]; then
    python3 -m venv "$VENV_DIR"
fi

"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PYTHON" -m pip install -r "$REQUIREMENTS_PATH"
"$VENV_PYTHON" -m pip install -e "$CLOB_CLIENT_PATH"
