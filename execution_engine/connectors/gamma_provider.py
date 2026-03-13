"""Gamma API helper for Polymarket market metadata."""

from __future__ import annotations

from typing import Any, Dict, List
import json
import urllib.parse
import urllib.request


class GammaMarketProvider:
    def __init__(self, base_url: str, timeout_sec: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec

    def fetch_markets_by_condition(self, condition_id: str) -> List[Dict[str, Any]]:
        params = urllib.parse.urlencode({"condition_ids": condition_id})
        url = f"{self.base_url}/markets?{params}"
        request = urllib.request.Request(url, headers={"User-Agent": "PEG/0.3"})
        with urllib.request.urlopen(request, timeout=self.timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"] or []
        return []
