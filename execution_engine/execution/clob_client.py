"""CLOB client interface and live wrapper (optional)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import inspect

from ..core.config import PegConfig


def _import_py_clob_client() -> Dict[str, Any]:
    try:
        from py_clob_client.client import ClobClient as PyClobClient
        from py_clob_client.clob_types import (
            ApiCreds,
            AssetType,
            OpenOrderParams,
            OrderArgs,
            OrderType,
            Side,
        )
    except ImportError as exc:
        raise ImportError(
            "py-clob-client is required for live CLOB operations. "
            "Install it in your environment before running live."
        ) from exc

    return {
        "PyClobClient": PyClobClient,
        "ApiCreds": ApiCreds,
        "AssetType": AssetType,
        "OpenOrderParams": OpenOrderParams,
        "OrderArgs": OrderArgs,
        "OrderType": OrderType,
        "Side": Side,
    }


def _obj_get(obj: Any, names: List[str]) -> Optional[Any]:
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj[name]
    for name in names:
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _to_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "dict"):
        return obj.dict()  # type: ignore[no-any-return]
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return {"raw": str(obj)}


class ClobClient:
    def get_midpoint(self, token_id: str) -> Optional[float]:
        raise NotImplementedError

    def get_order_book(self, token_id: str) -> Dict[str, object]:
        raise NotImplementedError

    def get_balance_usdc(self) -> Optional[float]:
        raise NotImplementedError

    def get_open_orders(self) -> List[Dict[str, object]]:
        raise NotImplementedError

    def place_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> Dict[str, object]:
        raise NotImplementedError

    def get_fills(self) -> List[Dict[str, object]]:
        raise NotImplementedError


class NullClobClient(ClobClient):
    def get_midpoint(self, token_id: str) -> Optional[float]:
        _ = token_id
        return None

    def get_order_book(self, token_id: str) -> Dict[str, object]:
        _ = token_id
        return {}

    def get_balance_usdc(self) -> Optional[float]:
        return None

    def get_open_orders(self) -> List[Dict[str, object]]:
        return []

    def place_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        return {"status": "DRY_RUN", "payload": payload}

    def cancel_order(self, order_id: str) -> Dict[str, object]:
        return {"status": "DRY_RUN_CANCELED", "order_id": order_id}

    def get_fills(self) -> List[Dict[str, object]]:
        return []


class LiveClobClient(ClobClient):
    def __init__(self, cfg: PegConfig) -> None:
        self.cfg = cfg
        self._client = None
        self._types: Dict[str, Any] = {}

    def _ensure_client(self) -> None:
        if self._client is not None:
            return

        if not self.cfg.clob_private_key:
            raise ValueError("Missing PEG_CLOB_PRIVATE_KEY for live CLOB client")

        types = _import_py_clob_client()
        PyClobClient = types["PyClobClient"]

        kwargs: Dict[str, Any] = {}
        sig = inspect.signature(PyClobClient)
        if "host" in sig.parameters:
            kwargs["host"] = self.cfg.clob_host
        if "chain_id" in sig.parameters:
            kwargs["chain_id"] = self.cfg.clob_chain_id
        if "key" in sig.parameters:
            kwargs["key"] = self.cfg.clob_private_key
        if "signature_type" in sig.parameters:
            kwargs["signature_type"] = self.cfg.clob_signature_type
        if "funder" in sig.parameters and self.cfg.clob_funder:
            kwargs["funder"] = self.cfg.clob_funder

        try:
            client = PyClobClient(**kwargs)
        except TypeError:
            client = PyClobClient(self.cfg.clob_host, self.cfg.clob_chain_id, self.cfg.clob_private_key)

        if self.cfg.clob_api_key and self.cfg.clob_api_secret and self.cfg.clob_api_passphrase:
            creds = types["ApiCreds"](
                api_key=self.cfg.clob_api_key,
                api_secret=self.cfg.clob_api_secret,
                api_passphrase=self.cfg.clob_api_passphrase,
            )
            if hasattr(client, "set_api_creds"):
                client.set_api_creds(creds)
        elif self.cfg.clob_derive_api_key and hasattr(client, "create_or_derive_api_key"):
            creds = client.create_or_derive_api_key()
            if hasattr(client, "set_api_creds"):
                client.set_api_creds(creds)

        self._client = client
        self._types = types

    def get_midpoint(self, token_id: str) -> Optional[float]:
        self._ensure_client()
        mid = self._client.get_midpoint(token_id)  # type: ignore[union-attr]
        return None if mid is None else float(mid)

    def get_order_book(self, token_id: str) -> Dict[str, object]:
        self._ensure_client()
        book = self._client.get_order_book(token_id)  # type: ignore[union-attr]
        return _to_dict(book)

    def get_balance_usdc(self) -> Optional[float]:
        self._ensure_client()
        asset_type = getattr(self._types["AssetType"], "USDC", None) or getattr(
            self._types["AssetType"], "COLLATERAL", None
        )
        if asset_type is None:
            return None
        balance_obj = self._client.get_balance_allowance(asset_type)  # type: ignore[union-attr]
        balance_val = _obj_get(balance_obj, ["balance", "available", "value"])
        allowance_val = _obj_get(balance_obj, ["allowance", "available_allowance"])
        try:
            balance = float(balance_val) if balance_val is not None else None
            allowance = float(allowance_val) if allowance_val is not None else None
        except (TypeError, ValueError):
            return None
        if balance is not None and allowance is not None:
            return min(balance, allowance)
        return balance if balance is not None else allowance

    def get_open_orders(self) -> List[Dict[str, object]]:
        self._ensure_client()
        params = self._types["OpenOrderParams"]()
        orders = self._client.get_orders(params)  # type: ignore[union-attr]
        return [_to_dict(order) for order in orders]

    def place_order(self, payload: Dict[str, object]) -> Dict[str, object]:
        self._ensure_client()
        token_id = str(payload.get("token_id", ""))
        price = float(payload.get("price", 0.0))
        size = float(payload.get("size", 0.0))
        side_str = str(payload.get("side", "BUY")).upper()
        side = self._types["Side"].BUY if side_str == "BUY" else self._types["Side"].SELL

        order_args_kwargs: Dict[str, Any] = {
            "token_id": token_id,
            "price": price,
            "size": size,
            "side": side,
        }
        if payload.get("nonce") is not None:
            if "nonce" in inspect.signature(self._types["OrderArgs"]).parameters:
                order_args_kwargs["nonce"] = int(payload["nonce"])
        if payload.get("client_order_id") is not None:
            if "client_order_id" in inspect.signature(self._types["OrderArgs"]).parameters:
                order_args_kwargs["client_order_id"] = str(payload["client_order_id"])

        order_args = self._types["OrderArgs"](**order_args_kwargs)
        signed = self._client.create_order(order_args)  # type: ignore[union-attr]
        order_type = self._types["OrderType"].GTC
        response = self._client.post_order(signed, order_type)  # type: ignore[union-attr]
        data = _to_dict(response)
        order_id = _obj_get(data, ["orderID", "order_id", "id"])
        status = _obj_get(data, ["status", "state"]) or "SENT"
        return {
            "status": status,
            "order_id": order_id,
            "raw": data,
        }

    def cancel_order(self, order_id: str) -> Dict[str, object]:
        self._ensure_client()
        response = self._client.cancel(order_id)  # type: ignore[union-attr]
        return _to_dict(response)

    def get_fills(self) -> List[Dict[str, object]]:
        self._ensure_client()
        trades = self._client.get_trades()  # type: ignore[union-attr]
        return [_to_dict(trade) for trade in trades]


def build_clob_client(cfg: PegConfig) -> ClobClient:
    if cfg.dry_run or not cfg.clob_enabled:
        return NullClobClient()
    return LiveClobClient(cfg)
