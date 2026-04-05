"""Balance providers for PEG."""

from __future__ import annotations

from pathlib import Path
import json
from typing import Optional

from execution_engine.integrations.trading.clob_client import ClobClient
from execution_engine.runtime.config import PegConfig


class FileBalanceProvider:
    def __init__(self, path: Path) -> None:
        self.path = path

    def get_available_usdc(self) -> Optional[float]:
        if not self.path.exists():
            return None
        # Accept UTF-8 files written by both Linux and Windows tooling.
        with self.path.open("r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
        value = data.get("available_usdc")
        return None if value is None else float(value)


class ClobBalanceProvider:
    def __init__(self, clob_client: ClobClient) -> None:
        self.clob_client = clob_client

    def get_available_usdc(self) -> Optional[float]:
        return self.clob_client.get_balance_usdc()


def build_balance_provider(cfg: PegConfig, clob_client: ClobClient | None = None) -> object:
    if not cfg.dry_run and cfg.clob_enabled:
        client = clob_client
        if client is None:
            from execution_engine.integrations.trading.clob_client import build_clob_client

            client = build_clob_client(cfg)
        return ClobBalanceProvider(client)
    return FileBalanceProvider(cfg.balances_path)
