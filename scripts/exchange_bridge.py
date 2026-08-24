#!/usr/bin/env python3
"""交易所实盘桥接：Lighter（lighter-python）+ SoDEX（Go sodex_bridge + 官方 SDK）。

Rust 通过 stdin 传入 JSON：
  {"cmd":"account|place|cancel", "venue_yaml":"...", "params":{...}}

依赖：
  pip install -r scripts/requirements-exchange.txt
  cd scripts/sodex_bridge && go build -o sodex_bridge.exe .
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------


def respond(ok: bool, data: Any = None, error: str = "") -> None:
    out = {"ok": ok, "data": data or {}, "error": error}
    print(json.dumps(out, ensure_ascii=False))
    sys.stdout.flush()


def load_venue(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    with p.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def read_request() -> dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("empty stdin")
    return json.loads(raw)


# ---------------------------------------------------------------------------
# Lighter
# ---------------------------------------------------------------------------


def _lighter_base_url(rest: str) -> str:
    return rest.rstrip("/").removesuffix("/api/v1")


async def lighter_account(venue: dict[str, Any]) -> dict[str, Any]:
    import lighter

    base = _lighter_base_url(venue["rest"])
    pk = venue.get("api_key_private_key") or venue.get("private_key")
    account_index = int(venue.get("account_index", 0))
    api_key_index = int(venue.get("api_key_index", 0))
    client = lighter.SignerClient(
        url=base,
        api_private_keys={api_key_index: pk},
        account_index=account_index,
    )
    api = lighter.ApiClient(configuration=lighter.Configuration(host=base))
    account_api = lighter.AccountApi(api)
    order_api = lighter.OrderApi(api)

    resp = await account_api.account(by="index", value=str(account_index))
    balances: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    if resp.accounts:
        acc = resp.accounts[0]
        avail = Decimal(str(getattr(acc, "available_balance", "0")))
        collateral = Decimal(str(getattr(acc, "collateral", "0")))
        if collateral > 0:
            balances.append(
                {
                    "asset": "USDC",
                    "available": str(avail),
                    "total": str(collateral),
                }
            )
        for pos in getattr(acc, "positions", []) or []:
            sym = getattr(pos, "symbol", "") or str(getattr(pos, "market_id", ""))
            size = Decimal(str(getattr(pos, "position", getattr(pos, "size", "0"))))
            if size == 0:
                continue
            entry = getattr(pos, "avg_entry_price", None)
            positions.append(
                {
                    "symbol": sym,
                    "qty": str(size),
                    "entry_price": str(entry) if entry is not None else None,
                }
            )

    # 补充活跃持仓（部分账户结构在 positions 字段为空）
    try:
        auth = client.create_auth_token_with_expiry(600)
        active = await order_api.account_active_orders(
            account_index=account_index, market_id=255, auth=auth
        )
        _ = active
    except Exception:
        pass

    await api.close()
    return {"balances": balances, "positions": positions}


async def lighter_place(venue: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    import lighter

    base = _lighter_base_url(venue["rest"])
    pk = venue.get("api_key_private_key") or venue.get("private_key")
    account_index = int(venue.get("account_index", 0))
    api_key_index = int(venue.get("api_key_index", 0))
    client = lighter.SignerClient(
        url=base,
        api_private_keys={api_key_index: pk},
        account_index=account_index,
    )

    market_index = int(params["market_index"])
    qty = Decimal(params["qty"])
    is_buy = bool(params["is_buy"])
    reduce_only = bool(params.get("reduce_only", False))
    style = params.get("style", "market")
    limit_price = params.get("limit_price")
    client_order_id = params.get("client_order_id") or str(int(time.time() * 1000))

    # 市场精度：从 orderBooks 拉 size decimals
    api = lighter.ApiClient(configuration=lighter.Configuration(host=base))
    ob_api = lighter.OrderApi(api)
    books = await ob_api.order_books(filter="perp")
    size_dec = 4
    price_dec = 2
    for b in books.order_books or []:
        if int(b.market_id) == market_index:
            size_dec = int(getattr(b, "supported_size_decimals", 4))
            price_dec = int(getattr(b, "supported_price_decimals", 2))
            break
    await api.close()

    base_amount = int(qty * (10**size_dec))
    if base_amount <= 0:
        raise ValueError(f"qty too small after scale: {qty}")

    is_ask = not is_buy
    coid = int(client_order_id) if str(client_order_id).isdigit() else int(time.time() * 1000)

    if style == "limit":
        if not limit_price:
            raise ValueError("limit_price required for limit order")
        price = int(Decimal(limit_price) * (10**price_dec))
        tx, tx_hash, err = await client.create_order(
            market_index=market_index,
            client_order_index=coid,
            base_amount=base_amount,
            price=price,
            is_ask=is_ask,
            order_type=client.ORDER_TYPE_LIMIT,
            time_in_force=client.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=reduce_only,
            order_expiry=client.DEFAULT_28_DAY_ORDER_EXPIRY,
        )
    else:
        # 市价：用 IOC + 保护价（参考项目 slippage 逻辑简化版 +5%）
        api = lighter.ApiClient(configuration=lighter.Configuration(host=base))
        candle = lighter.OrderApi(api)
        ob = await candle.order_book_details(market_id=market_index)
        await api.close()
        detail = ob.order_book_details[0] if ob.order_book_details else None
        if detail is None:
            raise RuntimeError("no order book detail")
        last = Decimal(str(getattr(detail, "last_trade_price", "0")))
        if last <= 0:
            raise RuntimeError("invalid last trade price")
        slip = Decimal("1.05") if is_buy else Decimal("0.95")
        prot = last * slip
        price = int(prot * (10**price_dec))
        tx, tx_hash, err = await client.create_order(
            market_index=market_index,
            client_order_index=coid,
            base_amount=base_amount,
            price=price,
            is_ask=is_ask,
            order_type=client.ORDER_TYPE_MARKET,
            time_in_force=client.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            reduce_only=reduce_only,
            order_expiry=client.DEFAULT_IOC_EXPIRY,
        )

    if err:
        raise RuntimeError(str(err))
    if style == "limit":
        return {
            "order_id": str(coid),
            "client_order_id": str(coid),
            "filled_qty": "0",
            "status": "accepted",
            "avg_price": limit_price,
        }
    oid = str(tx_hash or coid)
    return {
        "order_id": oid,
        "client_order_id": str(coid),
        "filled_qty": params["qty"],
        "status": "filled",
        "avg_price": limit_price,
    }


async def lighter_order_status(venue: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    import lighter

    base = _lighter_base_url(venue["rest"])
    pk = venue.get("api_key_private_key") or venue.get("private_key")
    account_index = int(venue.get("account_index", 0))
    api_key_index = int(venue.get("api_key_index", 0))
    client = lighter.SignerClient(
        url=base,
        api_private_keys={api_key_index: pk},
        account_index=account_index,
    )
    market_index = int(params["market_index"])
    order_id = str(params["order_id"])
    auth = client.create_auth_token_with_expiry(600)
    api = lighter.ApiClient(configuration=lighter.Configuration(host=base))
    order_api = lighter.OrderApi(api)
    active = await order_api.account_active_orders(
        account_index=account_index, market_id=market_index, auth=auth
    )
    await api.close()
    orders = getattr(active, "orders", None) or []
    for o in orders:
        coid = str(getattr(o, "client_order_index", getattr(o, "client_order_id", "")))
        if coid == order_id or str(getattr(o, "order_index", "")) == order_id:
            filled = Decimal(str(getattr(o, "filled_base_amount", "0")))
            status = "accepted" if filled <= 0 else ("filled" if filled >= Decimal(params.get("qty", "0") or "0") else "partial")
            price = getattr(o, "price", None)
            return {
                "order_id": order_id,
                "filled_qty": str(filled),
                "status": status,
                "avg_price": str(price) if price is not None else None,
            }
    # 不在 active 列表：视为已成交或已撤
    return {
        "order_id": order_id,
        "filled_qty": params.get("qty", "0"),
        "status": "filled",
        "avg_price": None,
    }


async def lighter_cancel(venue: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    import lighter

    base = _lighter_base_url(venue["rest"])
    pk = venue.get("api_key_private_key") or venue.get("private_key")
    account_index = int(venue.get("account_index", 0))
    api_key_index = int(venue.get("api_key_index", 0))
    client = lighter.SignerClient(
        url=base,
        api_private_keys={api_key_index: pk},
        account_index=account_index,
    )
    market_index = int(params["market_index"])
    order_id = params["order_id"]
    # 对齐 internal/exchange/lighter：撤单 Index = ClientOrderIndex（下单时传入的 coid）
    order_index = int(order_id) if str(order_id).isdigit() else 0
    if order_index <= 0:
        raise ValueError(f"lighter cancel requires numeric client_order_index, got {order_id!r}")
    tx, tx_hash, err = await client.cancel_order(
        market_index=market_index,
        order_index=order_index,
        api_key_index=api_key_index,
    )
    if err:
        raise RuntimeError(str(err))
    return {"order_id": str(tx_hash or order_id), "status": "canceled"}


# ---------------------------------------------------------------------------
# SoDEX（委托 Go sodex_bridge，对齐 internal/exchange/sodex.go）
# ---------------------------------------------------------------------------

SODEX_BRIDGE_DIR = Path(__file__).resolve().parent / "sodex_bridge"


def _sodex_bridge_bin() -> Path:
    for name in ("sodex_bridge.exe", "sodex_bridge"):
        candidate = SODEX_BRIDGE_DIR / name
        if candidate.exists():
            return candidate
    raise RuntimeError(
        "sodex_bridge 未找到；请先构建：cd scripts/sodex_bridge && go build -o sodex_bridge.exe ."
    )


def sodex_dispatch(req: dict[str, Any]) -> Any:
    bridge = _sodex_bridge_bin()
    proc = subprocess.run(
        [str(bridge)],
        input=json.dumps(req, ensure_ascii=False),
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(stderr or stdout or f"sodex_bridge exit {proc.returncode}")
    if not stdout:
        raise RuntimeError(stderr or "sodex_bridge returned empty stdout")
    try:
        out = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"sodex_bridge invalid json: {stdout[:500]}") from exc
    if not out.get("ok"):
        raise RuntimeError(out.get("error") or "sodex_bridge failed")
    return out.get("data")


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


async def dispatch(req: dict[str, Any]) -> Any:
    venue = load_venue(req["venue_yaml"])
    cmd = req["cmd"]
    params = req.get("params") or {}
    vid = venue.get("id", "")

    if vid in ("lighter", "lighter_rh"):
        if cmd == "account":
            return await lighter_account(venue)
        if cmd == "place":
            return await lighter_place(venue, params)
        if cmd == "cancel":
            return await lighter_cancel(venue, params)
        if cmd == "order_status":
            return await lighter_order_status(venue, params)
        raise ValueError(f"unknown cmd {cmd}")

    if vid == "sodex":
        return sodex_dispatch(req)

    raise ValueError(f"unsupported venue {vid}")


def main() -> None:
    try:
        req = read_request()
        data = asyncio.run(dispatch(req))
        respond(True, data)
    except Exception as exc:
        respond(False, error=str(exc))


if __name__ == "__main__":
    main()
