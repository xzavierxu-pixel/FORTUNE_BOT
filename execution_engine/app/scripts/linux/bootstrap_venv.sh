#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
VENV_DIR="${FORTUNE_BOT_VENV:-$REPO_ROOT/.venv-execution}"
VENV_PYTHON="$VENV_DIR/bin/python"
PYTHON_BIN="${FORTUNE_BOT_PYTHON_BIN:-python3.13}"
REQUIREMENTS_PATH="$REPO_ROOT/execution_engine/requirements-live.txt"
CLOB_CLIENT_GIT_URL="${FORTUNE_BOT_PY_CLOB_CLIENT_GIT_URL:-https://github.com/Polymarket/py-clob-client.git}"
CLOB_CLIENT_REF="${FORTUNE_BOT_PY_CLOB_CLIENT_REF:-}"
CLOB_CLIENT_PATH="$REPO_ROOT/py-clob-client"

install_clob_client() {
    if [[ -f "$CLOB_CLIENT_PATH/setup.py" || -f "$CLOB_CLIENT_PATH/pyproject.toml" ]]; then
        "$VENV_PYTHON" -m pip install -e "$CLOB_CLIENT_PATH"
        return
    fi

    if [[ -n "$CLOB_CLIENT_REF" ]]; then
        "$VENV_PYTHON" -m pip install "git+$CLOB_CLIENT_GIT_URL@$CLOB_CLIENT_REF"
    else
        "$VENV_PYTHON" -m pip install "git+$CLOB_CLIENT_GIT_URL"
    fi
}

if [[ ! -d "$VENV_DIR" ]]; then
    "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

"$VENV_PYTHON" -m pip install --upgrade pip
"$VENV_PYTHON" -m pip install -r "$REQUIREMENTS_PATH"
install_clob_client
