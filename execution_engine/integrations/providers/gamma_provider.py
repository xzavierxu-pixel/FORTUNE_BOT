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

    def _fetch_paginated(self, endpoint: str, query: Dict[str, str], max_records: int) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        offset = 0
        limit = int(query.get("limit", "500") or 500)
        while len(records) < max_records:
            page_query = dict(query)
            page_query["offset"] = str(offset)
            params = urllib.parse.urlencode(page_query)
            url = f"{self.base_url}/{endpoint}?{params}"
            request = urllib.request.Request(url, headers={"User-Agent": "PEG/0.3"})
            with urllib.request.urlopen(request, timeout=self.timeout_sec) as resp:
                payload = resp.read().decode("utf-8")
            data = json.loads(payload)
            batch = data if isinstance(data, list) else data.get("data", []) if isinstance(data, dict) else []
            if not batch:
                break
            records.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        return records[:max_records]

    def _fetch_page(self, endpoint: str, query: Dict[str, str]) -> List[Dict[str, Any]]:
        params = urllib.parse.urlencode(query)
        url = f"{self.base_url}/{endpoint}?{params}"
        request = urllib.request.Request(url, headers={"User-Agent": "PEG/0.3"})
        with urllib.request.urlopen(request, timeout=self.timeout_sec) as resp:
            payload = resp.read().decode("utf-8")
        data = json.loads(payload)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "data" in data:
            return data["data"] or []
        return []

    def fetch_open_events_page(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        order: str = "endDate",
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, str] = {
            "active": "true",
            "closed": "false",
            "order": order,
            "ascending": "true" if ascending else "false",
            "limit": str(limit),
            "offset": str(max(offset, 0)),
        }
        return self._fetch_page("events", query)

    def fetch_open_events(
        self,
        limit: int = 500,
        max_events: int = 5000,
        *,
        order: str = "endDate",
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, str] = {
            "active": "true",
            "closed": "false",
            "order": order,
            "ascending": "true" if ascending else "false",
            "limit": str(limit),
        }
        return self._fetch_paginated("events", query, max_events)

    def fetch_open_markets(
        self,
        limit: int = 500,
        max_markets: int = 5000,
        *,
        order: str = "endDate",
        ascending: bool = True,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, str] = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "accepting_orders": "true",
            "order": order,
            "ascending": "true" if ascending else "false",
            "limit": str(limit),
        }
        if end_date_min:
            query["end_date_min"] = end_date_min
        if end_date_max:
            query["end_date_max"] = end_date_max
        return self._fetch_paginated("markets", query, max_markets)
