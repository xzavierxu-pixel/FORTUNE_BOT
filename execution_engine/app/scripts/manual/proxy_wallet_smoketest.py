#!/usr/bin/env python3
"""One-off Polymarket proxy wallet smoke test."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any


DEFAULT_HOST = "https://clob.polymarket.com"
DEFAULT_CHAIN_ID = 137
DEFAULT_SIGNATURE_TYPE = 2
DEFAULT_TOKEN_ID = "83155705733555118569646804738526000527065734405672442364016752623981274522859"
DEFAULT_PRICE = 0.55
DEFAULT_SIZE = 1.0


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def import_py_clob_client() -> dict[str, Any]:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import (
            AssetType,
            BalanceAllowanceParams,
            OpenOrderParams,
            OrderArgs,
            OrderType,
            TradeParams,
        )
        from py_clob_client.order_builder.constants import BUY
    except ImportError:
        local_clone = repo_root() / "py-clob-client"
        if not local_clone.exists():
            raise
        sys.path.insert(0, str(local_clone))
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import (
            AssetType,
            BalanceAllowanceParams,
            OpenOrderParams,
            OrderArgs,
            OrderType,
            TradeParams,
        )
        from py_clob_client.order_builder.constants import BUY

    return {
        "ClobClient": ClobClient,
        "AssetType": AssetType,
        "BalanceAllowanceParams": BalanceAllowanceParams,
        "OpenOrderParams": OpenOrderParams,
        "OrderArgs": OrderArgs,
        "OrderType": OrderType,
        "TradeParams": TradeParams,
        "BUY": BUY,
    }


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if hasattr(value, "__dict__"):
        return {str(k): to_jsonable(v) for k, v in vars(value).items()}
    return value


def dump(label: str, value: Any) -> None:
    print(f"\n=== {label} ===")
    print(json.dumps(to_jsonable(value), ensure_ascii=True, indent=2, sort_keys=True))


def create_or_derive_api_key(client: Any) -> Any:
    if hasattr(client, "create_or_derive_api_key"):
        return client.create_or_derive_api_key()
    return client.create_or_derive_api_creds()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket proxy wallet one-off smoke test.")
    parser.add_argument("--host", default=env_or_default("PEG_CLOB_HOST", DEFAULT_HOST))
    parser.add_argument("--chain-id", type=int, default=int(env_or_default("PEG_CLOB_CHAIN_ID", str(DEFAULT_CHAIN_ID))))
    parser.add_argument("--private-key", default=os.getenv("PEG_CLOB_PRIVATE_KEY", ""))
    parser.add_argument("--funder", default=os.getenv("PEG_CLOB_FUNDER", ""))
    parser.add_argument(
        "--signature-type",
        type=int,
        default=int(env_or_default("PEG_CLOB_SIGNATURE_TYPE", str(DEFAULT_SIGNATURE_TYPE))),
    )
    parser.add_argument("--token-id", default=DEFAULT_TOKEN_ID)
    parser.add_argument("--price", type=float, default=DEFAULT_PRICE)
    parser.add_argument("--size", type=float, default=DEFAULT_SIZE)
    parser.add_argument("--order-type", choices=["GTC", "FOK", "FAK", "GTD"], default="GTC")
    parser.add_argument("--sleep-sec", type=float, default=2.0)
    return parser


def require(value: str, label: str) -> str:
    if value:
        return value
    raise SystemExit(f"Missing required value: {label}")


def main() -> None:
    args = build_parser().parse_args()
    private_key = require(args.private_key, "PEG_CLOB_PRIVATE_KEY or --private-key")
    funder = require(args.funder, "PEG_CLOB_FUNDER or --funder")

    if args.signature_type != 2:
        raise SystemExit("This smoke test is intended for proxy wallet trading. Use --signature-type 2.")

    types = import_py_clob_client()
    client = types["ClobClient"](
        args.host,
        key=private_key,
        chain_id=args.chain_id,
        signature_type=args.signature_type,
        funder=funder,
    )

    signer_address = client.get_address()
    dump(
        "client_setup",
        {
            "host": args.host,
            "chain_id": args.chain_id,
            "signature_type": args.signature_type,
            "signer_address": signer_address,
            "funder_address": funder,
            "token_id": args.token_id,
            "price": args.price,
            "size": args.size,
            "order_type": args.order_type,
        },
    )

    creds = create_or_derive_api_key(client)
    client.set_api_creds(creds)
    dump("api_creds", creds)

    collateral_params = types["BalanceAllowanceParams"](
        asset_type=types["AssetType"].COLLATERAL,
        signature_type=args.signature_type,
    )
    token_params = types["BalanceAllowanceParams"](
        asset_type=types["AssetType"].CONDITIONAL,
        token_id=args.token_id,
        signature_type=args.signature_type,
    )

    collateral_before = client.get_balance_allowance(collateral_params)
    token_before = client.get_balance_allowance(token_params)
    dump(
        "balances_before_approve",
        {
            "collateral": collateral_before,
            "conditional": token_before,
        },
    )

    collateral_approve = client.update_balance_allowance(collateral_params)
    token_approve = client.update_balance_allowance(token_params)
    dump(
        "approve_results",
        {
            "collateral": collateral_approve,
            "conditional": token_approve,
        },
    )

    collateral_after_approve = client.get_balance_allowance(collateral_params)
    token_after_approve = client.get_balance_allowance(token_params)
    dump(
        "balances_after_approve",
        {
            "collateral": collateral_after_approve,
            "conditional": token_after_approve,
        },
    )

    order_args = types["OrderArgs"](
        token_id=args.token_id,
        price=args.price,
        size=args.size,
        side=types["BUY"],
    )
    signed_order = client.create_order(order_args)
    order_type = getattr(types["OrderType"], args.order_type)
    order_response = client.post_order(signed_order, order_type)
    dump("order_response", order_response)

    time.sleep(max(args.sleep_sec, 0.0))

    open_orders = client.get_orders(types["OpenOrderParams"](asset_id=args.token_id))
    trades = client.get_trades(
        types["TradeParams"](
            maker_address=funder,
            asset_id=args.token_id,
        )
    )
    collateral_after_trade = client.get_balance_allowance(collateral_params)
    token_after_trade = client.get_balance_allowance(token_params)
    dump(
        "post_trade_state",
        {
            "open_orders": open_orders,
            "trades": trades,
            "collateral": collateral_after_trade,
            "conditional": token_after_trade,
        },
    )


if __name__ == "__main__":
    main()
