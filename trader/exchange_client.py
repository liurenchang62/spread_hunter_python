"""
各交易所 REST 下单客户端（测试网 / 主网双模式）。

统一接口：
    result = await client.place_order(symbol, side, target_qty, ref_price, symbol_info)

target_qty：base coin 数量（目标币，统一单位）
symbol_info：来自 market_info，包含 native_ct_val 供各所转换张数

side: "buy" | "sell"
"""

import base64
import hashlib
import hmac
import json
import logging
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import aiohttp

from clients import TESTNET_REST_BASE, REST_BASE

logger = logging.getLogger("trader.client")


# ─── 下单结果 ─────────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    success:    bool
    order_id:   str   = ""
    fill_price: float = 0.0
    fill_size:  float = 0.0   # base coin 数量
    fee_usdt:   float = 0.0
    error:      str   = ""


# ─── API Key 加载 ────────────────────────────────────────────────────────────

def _read_key(val: str) -> str:
    p = Path(val.strip())
    try:
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return val.strip()


def _load_keys() -> dict[str, dict]:
    keys: dict[str, dict] = {}
    api_keys_path = Path(__file__).resolve().parent.parent / "clients" / "api_keys.py"
    if not api_keys_path.exists():
        logger.warning("clients/api_keys.py 不存在，交易将使用空密钥")
        return keys

    raw: dict[str, str] = {}
    for line in api_keys_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            raw[k.strip()] = _read_key(v.strip())

    keys["binance"] = {"key": raw.get("BINANCE_TESTNET_API_KEY", ""),
                       "secret": raw.get("BINANCE_TESTNET_SECRET_KEY", "")}
    keys["okx"]     = {"key": raw.get("OKX_DEMO_API_KEY", ""),
                       "secret": raw.get("OKX_DEMO_SECRET_KEY", ""),
                       "passphrase": raw.get("OKX_DEMO_PASSPHRASE", "")}
    keys["gate"]    = {"key": raw.get("GATE_TESTNET_API_KEY", ""),
                       "secret": raw.get("GATE_TESTNET_SECRET_KEY", "")}
    keys["bitget"]  = {"key": raw.get("BITGET_DEMO_API_KEY", ""),
                       "secret": raw.get("BITGET_DEMO_SECRET_KEY", ""),
                       "passphrase": raw.get("BITGET_DEMO_PASSPHRASE", "")}
    return keys


# ─── 工具：base coin → 各所合约张数 ────────────────────────────────────────

def _to_contracts(target_qty: float, ct_val: float) -> int:
    """base coin 数量 → 合约张数（向下取整）。"""
    if ct_val <= 0:
        return 0
    return max(1, math.floor(target_qty / ct_val))


# ─── 基类 ─────────────────────────────────────────────────────────────────────

class BaseClient:
    exchange: str = ""

    def __init__(self, live: bool, keys: dict):
        self.live  = live
        self.keys  = keys
        self.base  = REST_BASE[self.exchange] if live else TESTNET_REST_BASE[self.exchange]
        self._session: Optional[aiohttp.ClientSession] = None

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def place_order(
        self,
        symbol: str,
        side: str,
        target_qty: float,
        ref_price: float,
        symbol_info=None,    # SymbolInfo | None
    ) -> OrderResult:
        raise NotImplementedError


# ─── Binance ─────────────────────────────────────────────────────────────────

class BinanceClient(BaseClient):
    exchange = "binance"

    def _sign(self, params: dict) -> tuple[dict, dict]:
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(self.keys["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params, {"X-MBX-APIKEY": self.keys["key"]}

    async def place_order(
        self, symbol: str, side: str, target_qty: float,
        ref_price: float, symbol_info=None,
    ) -> OrderResult:
        # Binance USDT-M：qty 直接是 base coin，用 stepSize 取整
        step = symbol_info.qty_step if symbol_info and symbol_info.qty_step > 0 else 0.001
        qty  = math.floor(target_qty / step) * step
        qty  = round(qty, 8)
        if qty <= 0:
            return OrderResult(success=False, error="qty=0")

        params, headers = self._sign({
            "symbol": symbol.upper(), "side": side.upper(),
            "type": "MARKET", "quantity": str(qty),
        })
        try:
            sess = await self._sess()
            async with sess.post(
                f"{self.base}/fapi/v1/order",
                params=params, headers=headers, ssl=False,
            ) as r:
                data = await r.json()
            if r.status == 200:
                fill     = float(data.get("avgPrice") or data.get("price") or ref_price)
                fill_qty = float(data.get("executedQty", qty))
                return OrderResult(
                    success=True, order_id=str(data.get("orderId", "")),
                    fill_price=fill, fill_size=fill_qty,
                    fee_usdt=fill_qty * fill * 0.0005,
                )
            return OrderResult(success=False, error=str(data))
        except Exception as e:
            return OrderResult(success=False, error=str(e))


# ─── OKX ─────────────────────────────────────────────────────────────────────

class OKXClient(BaseClient):
    exchange = "okx"

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        ts  = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        sig = base64.b64encode(
            hmac.new(self.keys["secret"].encode(),
                     (ts + method.upper() + path + body).encode(),
                     hashlib.sha256).digest()
        ).decode()
        h = {
            "OK-ACCESS-KEY": self.keys["key"], "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": self.keys["passphrase"],
            "Content-Type": "application/json",
        }
        if not self.live:
            h["x-simulated-trading"] = "1"
        return h

    async def place_order(
        self, symbol: str, side: str, target_qty: float,
        ref_price: float, symbol_info=None,
    ) -> OrderResult:
        # OKX：sz 单位为合约张数，1张 = ct_val base coins
        ct_val = symbol_info.native_ct_val if symbol_info else 0.01
        sz     = _to_contracts(target_qty, ct_val)
        if sz <= 0:
            return OrderResult(success=False, error="sz=0")

        body_d = {"instId": symbol, "tdMode": "cross",
                  "side": side.lower(), "ordType": "market", "sz": str(sz)}
        body   = json.dumps(body_d)
        path   = "/api/v5/trade/order"
        try:
            sess = await self._sess()
            async with sess.post(
                f"{self.base}{path}", headers=self._sign("POST", path, body),
                data=body, ssl=False,
            ) as r:
                data = await r.json()
            if data.get("code") == "0":
                order_id  = data["data"][0].get("ordId", "")
                fill_size = sz * ct_val
                fill_price = await self._query_fill_price(symbol, order_id, ref_price)
                return OrderResult(
                    success=True, order_id=order_id,
                    fill_price=fill_price, fill_size=fill_size,
                    fee_usdt=fill_size * fill_price * 0.0005,
                )
            return OrderResult(success=False, error=str(data))
        except Exception as e:
            return OrderResult(success=False, error=str(e))

    async def _query_fill_price(self, inst_id: str, order_id: str, fallback: float) -> float:
        """查询 OKX 已成交订单的平均成交价，超时或失败时回退到 fallback。"""
        if not order_id:
            return fallback
        path = f"/api/v5/trade/order?instId={inst_id}&ordId={order_id}"
        try:
            sess = await self._sess()
            async with sess.get(
                f"{self.base}{path}", headers=self._sign("GET", path),
                ssl=False, timeout=aiohttp.ClientTimeout(total=2),
            ) as r:
                data = await r.json()
            if data.get("code") == "0" and data.get("data"):
                avg_px = float(data["data"][0].get("avgPx") or 0)
                if avg_px > 0:
                    return avg_px
        except Exception:
            pass
        return fallback


# ─── Gate ─────────────────────────────────────────────────────────────────────

class GateClient(BaseClient):
    exchange = "gate"

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        ts        = str(int(time.time()))
        body_hash = hashlib.sha512(body.encode() if body else b"").hexdigest()
        msg       = f"{method.upper()}\n{path}\n\n{body_hash}\n{ts}"
        sig       = hmac.new(self.keys["secret"].encode(), msg.encode(), hashlib.sha512).hexdigest()
        return {"KEY": self.keys["key"], "SIGN": sig,
                "Timestamp": ts, "Content-Type": "application/json"}

    async def place_order(
        self, symbol: str, side: str, target_qty: float,
        ref_price: float, symbol_info=None,
    ) -> OrderResult:
        # Gate 线性永续：size 单位为合约张数，1张 = ct_val(quanto_multiplier) base coins
        ct_val = symbol_info.native_ct_val if symbol_info and symbol_info.native_ct_val > 0 else (
            1.0 / ref_price if ref_price > 0 else 0.0001  # 1 USD per contract fallback
        )
        sz = _to_contracts(target_qty, ct_val)
        if sz <= 0:
            return OrderResult(success=False, error="sz=0")
        if side == "sell":
            sz = -sz   # Gate 用负数表示做空

        body_d = {"contract": symbol, "size": sz, "price": "0", "tif": "ioc"}
        body   = json.dumps(body_d)
        path   = "/api/v4/futures/usdt/orders"
        try:
            sess = await self._sess()
            async with sess.post(
                f"{self.base}{path}", headers=self._sign("POST", path, body),
                data=body, ssl=False,
            ) as r:
                data = await r.json()
            if r.status in (200, 201):
                fill      = float(data.get("fill_price") or data.get("price") or ref_price)
                fill_size = abs(sz) * ct_val
                return OrderResult(
                    success=True, order_id=str(data.get("id", "")),
                    fill_price=fill, fill_size=fill_size,
                    fee_usdt=fill_size * fill * 0.00075,
                )
            return OrderResult(success=False, error=str(data))
        except Exception as e:
            return OrderResult(success=False, error=str(e))


# ─── Bitget ───────────────────────────────────────────────────────────────────

class BitgetClient(BaseClient):
    exchange = "bitget"

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        ts  = str(int(time.time() * 1000))
        sig = base64.b64encode(
            hmac.new(self.keys["secret"].encode(),
                     (ts + method.upper() + path + body).encode(),
                     hashlib.sha256).digest()
        ).decode()
        h = {"ACCESS-KEY": self.keys["key"], "ACCESS-SIGN": sig,
             "ACCESS-TIMESTAMP": ts, "ACCESS-PASSPHRASE": self.keys["passphrase"],
             "Content-Type": "application/json"}
        if not self.live:
            h["paptrading"] = "1"
        return h

    async def place_order(
        self, symbol: str, side: str, target_qty: float,
        ref_price: float, symbol_info=None,
    ) -> OrderResult:
        # Bitget：size 单位直接是 base coin（native_ct_val=1.0）
        step = symbol_info.qty_step if symbol_info and symbol_info.qty_step > 0 else 0.001
        qty  = math.floor(target_qty / step) * step
        qty  = round(max(qty, step), 8)
        if qty <= 0:
            return OrderResult(success=False, error="qty=0")

        body_d = {
            "symbol": symbol, "productType": "USDT-FUTURES",
            "marginMode": "crossed", "marginCoin": "USDT",
            "size": str(qty), "side": side.lower(),
            "tradeSide": "open", "orderType": "market",
        }
        body = json.dumps(body_d)
        path = "/api/v2/mix/order/place-order"
        try:
            sess = await self._sess()
            async with sess.post(
                f"{self.base}{path}", headers=self._sign("POST", path, body),
                data=body, ssl=False,
            ) as r:
                data = await r.json()
            if str(data.get("code", "")) == "00000":
                order_id   = str(data.get("data", {}).get("orderId", ""))
                fill_price = await self._query_fill_price(symbol, order_id, ref_price)
                return OrderResult(
                    success=True, order_id=order_id,
                    fill_price=fill_price, fill_size=qty,
                    fee_usdt=qty * fill_price * 0.0006,
                )
            return OrderResult(success=False, error=str(data))
        except Exception as e:
            return OrderResult(success=False, error=str(e))

    async def _query_fill_price(self, symbol: str, order_id: str, fallback: float) -> float:
        """查询 Bitget 已成交订单的平均成交价，超时或失败时回退到 fallback。"""
        if not order_id:
            return fallback
        path = f"/api/v2/mix/order/detail?symbol={symbol}&productType=USDT-FUTURES&orderId={order_id}"
        try:
            ts  = str(int(time.time() * 1000))
            sig = base64.b64encode(
                hmac.new(self.keys["secret"].encode(),
                         (ts + "GET" + path).encode(),
                         hashlib.sha256).digest()
            ).decode()
            headers = {"ACCESS-KEY": self.keys["key"], "ACCESS-SIGN": sig,
                       "ACCESS-TIMESTAMP": ts, "ACCESS-PASSPHRASE": self.keys["passphrase"]}
            if not self.live:
                headers["paptrading"] = "1"
            sess = await self._sess()
            async with sess.get(
                f"{self.base}{path}", headers=headers, ssl=False,
                timeout=aiohttp.ClientTimeout(total=2),
            ) as r:
                data = await r.json()
            if str(data.get("code", "")) == "00000" and data.get("data"):
                avg_px = float(data["data"].get("priceAvg") or 0)
                if avg_px > 0:
                    return avg_px
        except Exception:
            pass
        return fallback


# ─── 工厂函数 ─────────────────────────────────────────────────────────────────

def build_clients(live: bool) -> dict[str, BaseClient]:
    keys    = _load_keys()
    clients = {}
    for exchange, cls in [("binance", BinanceClient), ("okx", OKXClient),
                           ("gate", GateClient), ("bitget", BitgetClient)]:
        k = keys.get(exchange, {})
        if not k.get("key"):
            logger.warning(f"[{exchange}] 未配置 API key，跳过")
            continue
        clients[exchange] = cls(live=live, keys=k)
        logger.info(f"[{exchange}] client 初始化 ({'主网' if live else '测试网'})")
    return clients
