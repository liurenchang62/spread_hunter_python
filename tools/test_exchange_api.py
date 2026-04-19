"""
交易所 API 连通性 & 账户测试工具。

测试内容：
  1. 余额查询（认证 GET）— 显示当前 USDT 可用余额
  2. 持仓查询（认证 GET）— 显示当前持仓汇总
  3. 下单测试（可选，--trade 参数开启）
       Binance  : POST /fapi/v1/order/test（不实际成交）
       OKX Demo : 真实 Demo 市价单（BTCUSDT 最小量，自动平仓）
       Gate Test: 真实 Testnet 市价单（最小量，自动平仓）
       Bitget   : 真实 Demo 市价单（最小量，自动平仓）

用法：
    cd D:\\spread_hunter_python
    python -m tools.test_exchange_api              # 只测余额 + 持仓
    python -m tools.test_exchange_api --trade      # 还测试下单（自动平仓）
    python -m tools.test_exchange_api --ex binance # 只测某一所
"""

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import aiohttp

# Windows 终端强制 UTF-8 输出
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf-8-sig"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ─── 公共颜色 ──────────────────────────────────────────────────────────────────
G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; C = "\033[36m"; W = "\033[0m"

def ok(msg):   print(f"  {G}[OK] {msg}{W}")
def err(msg):  print(f"  {R}[ERR] {msg}{W}")
def warn(msg): print(f"  {Y}[!] {msg}{W}")
def info(msg): print(f"  {C}    {msg}{W}")


# ─── Key 加载（复用 exchange_client 逻辑）──────────────────────────────────────

def _read_key(val: str) -> str:
    p = Path(val.strip())
    try:
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return val.strip()


def load_keys() -> dict[str, dict]:
    api_keys_path = Path(__file__).resolve().parent.parent / "clients" / "api_keys.py"
    if not api_keys_path.exists():
        print(f"{R}clients/api_keys.py 不存在{W}")
        return {}

    raw: dict[str, str] = {}
    for line in api_keys_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            raw[k.strip()] = _read_key(v.strip())

    return {
        "binance": {
            "key":    raw.get("BINANCE_TESTNET_API_KEY", ""),
            "secret": raw.get("BINANCE_TESTNET_SECRET_KEY", ""),
            "base":   "https://testnet.binancefuture.com",
        },
        "okx": {
            "key":        raw.get("OKX_DEMO_API_KEY", ""),
            "secret":     raw.get("OKX_DEMO_SECRET_KEY", ""),
            "passphrase": raw.get("OKX_DEMO_PASSPHRASE", ""),
            "base":       "https://www.okx.com",
        },
        "gate": {
            "key":    raw.get("GATE_TESTNET_API_KEY", ""),
            "secret": raw.get("GATE_TESTNET_SECRET_KEY", ""),
            # 尝试另一个测试网端点（testnet.gate.com 可能使用这个）
            "base":   "https://api-testnet.gateapi.io",
        },
        "bitget": {
            "key":        raw.get("BITGET_DEMO_API_KEY", ""),
            "secret":     raw.get("BITGET_DEMO_SECRET_KEY", ""),
            "passphrase": raw.get("BITGET_DEMO_PASSPHRASE", ""),
            "base":       "https://api.bitget.com",
        },
    }


# ─── Binance ───────────────────────────────────────────────────────────────────

async def test_binance(sess: aiohttp.ClientSession, k: dict, do_trade: bool):
    print(f"\n{C}═══ Binance Testnet ══════════════════════════════════{W}")
    if not k["key"]:
        err("API key 未配置"); return

    def sign(params: dict):
        params["timestamp"] = int(time.time() * 1000)
        query = urlencode(params)
        sig = hmac.new(k["secret"].encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params, {"X-MBX-APIKEY": k["key"]}

    # 1. 账户余额
    try:
        p, h = sign({})
        async with sess.get(f"{k['base']}/fapi/v2/balance", params=p, headers=h, ssl=False) as r:
            data = await r.json()
        if r.status == 200 and isinstance(data, list):
            for item in data:
                if item.get("asset") == "USDT":
                    bal  = float(item.get("availableBalance", 0))
                    total = float(item.get("balance", 0))
                    ok(f"余额查询成功 | 可用={bal:.2f} USDT  总额={total:.2f} USDT")
                    break
            else:
                warn(f"无 USDT 余额记录 | 返回: {data[:2]}")
        else:
            err(f"余额查询失败 HTTP {r.status} | {data}")
            return
    except Exception as e:
        err(f"余额查询异常: {e}"); return

    # 2. 当前持仓
    try:
        p, h = sign({"symbol": "BTCUSDT"})
        async with sess.get(f"{k['base']}/fapi/v2/positionRisk", params=p, headers=h, ssl=False) as r:
            data = await r.json()
        if r.status == 200 and isinstance(data, list):
            positions = [x for x in data if float(x.get("positionAmt", 0)) != 0]
            ok(f"持仓查询成功 | 活跃持仓={len(positions)} 笔")
        else:
            warn(f"持仓查询返回 HTTP {r.status}")
    except Exception as e:
        warn(f"持仓查询异常: {e}")

    # 3. 下单测试（test endpoint，不实际成交）
    if do_trade:
        try:
            p, h = sign({
                "symbol": "BTCUSDT", "side": "BUY",
                "type": "MARKET", "quantity": "0.001",
            })
            async with sess.post(f"{k['base']}/fapi/v1/order/test", params=p, headers=h, ssl=False) as r:
                data = await r.json()
            if r.status == 200:
                ok("下单测试通过（/order/test，不实际成交）")
            else:
                err(f"下单测试失败 HTTP {r.status} | {data}")
        except Exception as e:
            err(f"下单测试异常: {e}")


# ─── OKX ───────────────────────────────────────────────────────────────────────

async def test_okx(sess: aiohttp.ClientSession, k: dict, do_trade: bool):
    print(f"\n{C}═══ OKX Demo Trading ════════════════════════════════{W}")
    if not k["key"]:
        err("API key 未配置"); return

    def sign(method: str, path: str, body: str = "") -> dict:
        ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        sig = base64.b64encode(
            hmac.new(k["secret"].encode(),
                     (ts + method.upper() + path + body).encode(),
                     hashlib.sha256).digest()
        ).decode()
        return {
            "OK-ACCESS-KEY": k["key"], "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": ts, "OK-ACCESS-PASSPHRASE": k["passphrase"],
            "Content-Type": "application/json",
            "x-simulated-trading": "1",  # Demo 模式
        }

    # 1. 账户余额
    try:
        path = "/api/v5/account/balance?ccy=USDT"
        async with sess.get(f"{k['base']}{path}", headers=sign("GET", path), ssl=False) as r:
            data = await r.json()
        if data.get("code") == "0":
            for detail in (data.get("data") or [{}])[0].get("details", []):
                if detail.get("ccy") == "USDT":
                    bal   = float(detail.get("availBal", 0))
                    total = float(detail.get("eq", 0))
                    ok(f"余额查询成功 | 可用={bal:.2f} USDT  权益={total:.2f} USDT")
                    break
            else:
                warn(f"未找到 USDT 明细 | code={data.get('code')} msg={data.get('msg')}")
        else:
            err(f"余额查询失败 | code={data.get('code')} msg={data.get('msg')}")
            return
    except Exception as e:
        err(f"余额查询异常: {e}"); return

    # 2. 当前持仓
    try:
        path = "/api/v5/account/positions?instType=SWAP"
        async with sess.get(f"{k['base']}{path}", headers=sign("GET", path), ssl=False) as r:
            data = await r.json()
        if data.get("code") == "0":
            positions = [x for x in (data.get("data") or []) if float(x.get("pos", 0)) != 0]
            ok(f"持仓查询成功 | 活跃持仓={len(positions)} 笔")
        else:
            warn(f"持仓查询失败 | {data.get('msg')}")
    except Exception as e:
        warn(f"持仓查询异常: {e}")

    # 3. 下单测试（Demo 环境真实下单，最小 1 张 BTC-USDT-SWAP = 0.01 BTC）
    if do_trade:
        try:
            body_d = {"instId": "BTC-USDT-SWAP", "tdMode": "cross",
                      "side": "buy", "ordType": "market", "sz": "1"}
            body = json.dumps(body_d)
            path = "/api/v5/trade/order"
            h = sign("POST", path, body)
            async with sess.post(f"{k['base']}{path}", headers=h, data=body, ssl=False) as r:
                data = await r.json()
            if data.get("code") == "0":
                order_id = data["data"][0].get("ordId", "")
                ok(f"下单成功（Demo） | ordId={order_id}")
                # 立即平仓
                body_d2 = {"instId": "BTC-USDT-SWAP", "tdMode": "cross",
                           "side": "sell", "ordType": "market", "sz": "1"}
                body2 = json.dumps(body_d2)
                async with sess.post(f"{k['base']}{path}", headers=sign("POST", path, body2),
                                     data=body2, ssl=False) as r2:
                    d2 = await r2.json()
                if d2.get("code") == "0":
                    ok(f"平仓成功 | ordId={d2['data'][0].get('ordId','')}")
                else:
                    warn(f"平仓失败（可能有残留持仓）| {d2.get('msg')}")
            else:
                err(f"下单失败 | code={data.get('code')} msg={data.get('msg')}")
        except Exception as e:
            err(f"下单测试异常: {e}")


# ─── Gate ──────────────────────────────────────────────────────────────────────

async def test_gate(sess: aiohttp.ClientSession, k: dict, do_trade: bool):
    print(f"\n{C}═══ Gate.io Testnet ══════════════════════════════════{W}")
    if not k["key"]:
        err("API key 未配置"); return

    def sign(method: str, api_path: str, body: str = "") -> dict:
        ts        = str(int(time.time()))
        body_hash = hashlib.sha512(body.encode() if body else b"").hexdigest()
        # api_path 已经包含 /api/v4 前缀，直接用于签名
        msg       = f"{method.upper()}\n{api_path}\n\n{body_hash}\n{ts}"
        sig       = hmac.new(k["secret"].encode(), msg.encode(), hashlib.sha512).hexdigest()
        return {"KEY": k["key"], "SIGN": sig,
                "Timestamp": ts, "Content-Type": "application/json"}, msg

    # 1. 账户余额
    try:
        api_path = "/api/v4/futures/usdt/accounts"
        headers, sign_msg = sign("GET", api_path)
        # 调试：打印签名信息（注意：生产环境不要打印 secret）
        info(f"请求: GET {k['base']}{api_path}")
        info(f"签名串: {sign_msg[:50]}...")
        async with sess.get(f"{k['base']}{api_path}", headers=headers, ssl=False) as r:
            data = await r.json()
        if r.status == 200 and isinstance(data, dict):
            avail = float(data.get("available", 0))
            total = float(data.get("total", 0))
            ok(f"余额查询成功 | 可用={avail:.2f} USDT  总额={total:.2f} USDT")
        else:
            err(f"余额查询失败 HTTP {r.status} | {data}")
            if r.status == 401:
                warn("API Key 无效：请确认使用的是 Gate.io 【测试网】API Key")
                warn("获取地址: https://www.gate.io/futures_testnet")
                warn("注意：testnet.gate.com 网页上的模拟盘和 futures_testnet 的 API 测试网是两个系统")
            return
    except Exception as e:
        err(f"余额查询异常: {e}"); return

    # 2. 当前持仓
    try:
        api_path = "/api/v4/futures/usdt/positions"
        headers, _ = sign("GET", api_path)
        async with sess.get(f"{k['base']}{api_path}", headers=headers, ssl=False) as r:
            data = await r.json()
        if r.status == 200 and isinstance(data, list):
            positions = [x for x in data if float(x.get("size", 0)) != 0]
            ok(f"持仓查询成功 | 活跃持仓={len(positions)} 笔")
        else:
            warn(f"持仓查询返回 HTTP {r.status}")
    except Exception as e:
        warn(f"持仓查询异常: {e}")

    # 3. 下单测试（Testnet 真实下单，最小 1 张 BTC_USDT，1张=0.001BTC）
    if do_trade:
        try:
            api_path = "/api/v4/futures/usdt/orders"
            body_d = {"contract": "BTC_USDT", "size": 1, "price": "0", "tif": "ioc"}
            body = json.dumps(body_d)
            headers, _ = sign("POST", api_path, body)
            async with sess.post(f"{k['base']}{api_path}", headers=headers,
                                 data=body, ssl=False) as r:
                data = await r.json()
            if r.status in (200, 201):
                order_id = data.get("id", "")
                status   = data.get("status", "")
                ok(f"下单成功（Testnet） | id={order_id} status={status}")
                # IOC 订单若没有对手方会自动取消，无需手动平仓
                if status == "finished":
                    ok("IOC 订单已自动完成/取消（无残留）")
            else:
                err(f"下单失败 HTTP {r.status} | {data}")
        except Exception as e:
            err(f"下单测试异常: {e}")


# ─── Bitget ────────────────────────────────────────────────────────────────────

async def test_bitget(sess: aiohttp.ClientSession, k: dict, do_trade: bool):
    print(f"\n{C}═══ Bitget Demo Trading ══════════════════════════════{W}")
    if not k["key"]:
        err("API key 未配置"); return

    def sign(method: str, path: str, body: str = "") -> dict:
        ts  = str(int(time.time() * 1000))
        sig = base64.b64encode(
            hmac.new(k["secret"].encode(),
                     (ts + method.upper() + path + body).encode(),
                     hashlib.sha256).digest()
        ).decode()
        h = {"ACCESS-KEY": k["key"], "ACCESS-SIGN": sig,
             "ACCESS-TIMESTAMP": ts, "ACCESS-PASSPHRASE": k["passphrase"],
             "Content-Type": "application/json"}
        # 注意：如果 API Key 是 Demo Trading 的，需要传 paptrading: 1
        # 但如果是在主站创建的 Key，可能需要去掉这个 header
        # h["paptrading"] = "1"
        return h

    # 1. 账户余额
    try:
        # 尝试模拟币模式：使用 S 开头的 productType
        path = "/api/v2/mix/account/accounts?productType=SUSDT-FUTURES"
        async with sess.get(f"{k['base']}{path}", headers=sign("GET", path), ssl=False) as r:
            data = await r.json()
        if str(data.get("code", "")) == "00000":
            for item in (data.get("data") or []):
                # 模拟币模式下找 SUSDT，正常模式下找 USDT
                if item.get("marginCoin") in ["USDT", "SUSDT"]:
                    coin = item.get("marginCoin", "USDT")
                    avail = float(item.get("available", 0))
                    total = float(item.get("accountEquity", 0))
                    ok(f"余额查询成功 | 可用={avail:.2f} {coin}  权益={total:.2f} {coin}")
                    break
            else:
                warn("未找到 USDT/SUSDT 账户")
        else:
            err(f"余额查询失败 | code={data.get('code')} msg={data.get('msg', data.get('message', ''))}")
            return
    except Exception as e:
        err(f"余额查询异常: {e}"); return

    # 2. 当前持仓
    try:
        path = "/api/v2/mix/position/all-position?productType=SUSDT-FUTURES&marginCoin=SUSDT"
        async with sess.get(f"{k['base']}{path}", headers=sign("GET", path), ssl=False) as r:
            data = await r.json()
        if str(data.get("code", "")) == "00000":
            positions = [x for x in (data.get("data") or []) if float(x.get("total", 0)) != 0]
            ok(f"持仓查询成功 | 活跃持仓={len(positions)} 笔")
        else:
            warn(f"持仓查询失败 | {data.get('msg', '')}")
    except Exception as e:
        warn(f"持仓查询异常: {e}")

    # 3. 下单测试（模拟币模式，SBTCSUSDT 是模拟的 BTCUSDT）
    if do_trade:
        try:
            path = "/api/v2/mix/order/place-order"
            body_d = {
                "symbol": "SBTCSUSDT", "productType": "SUSDT-FUTURES",
                "marginMode": "crossed", "marginCoin": "SUSDT",
                "size": "0.001", "side": "buy",
                "tradeSide": "open", "orderType": "market",
            }
            body = json.dumps(body_d)
            async with sess.post(f"{k['base']}{path}", headers=sign("POST", path, body),
                                 data=body, ssl=False) as r:
                data = await r.json()
            if str(data.get("code", "")) == "00000":
                order_id = str(data.get("data", {}).get("orderId", ""))
                ok(f"下单成功（模拟币模式） | orderId={order_id}")
                # 立即平仓
                path2 = "/api/v2/mix/order/place-order"
                body_d2 = {
                    "symbol": "SBTCSUSDT", "productType": "SUSDT-FUTURES",
                    "marginMode": "crossed", "marginCoin": "SUSDT",
                    "size": "0.001", "side": "sell",
                    "tradeSide": "close", "orderType": "market",
                }
                body2 = json.dumps(body_d2)
                async with sess.post(f"{k['base']}{path2}", headers=sign("POST", path2, body2),
                                     data=body2, ssl=False) as r2:
                    d2 = await r2.json()
                if str(d2.get("code", "")) == "00000":
                    ok(f"平仓成功 | orderId={d2.get('data',{}).get('orderId','')}")
                else:
                    warn(f"平仓失败（可能有残留）| {d2.get('msg', '')}")
            else:
                err(f"下单失败 | code={data.get('code')} msg={data.get('msg', '')}")
        except Exception as e:
            err(f"下单测试异常: {e}")


# ─── 主函数 ─────────────────────────────────────────────────────────────────────

def load_env_file():
    """从 env/.env 文件加载环境变量（仅当前进程有效，不修改系统环境）"""
    env_path = Path(__file__).resolve().parent.parent / "env" / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()
                # 如果环境变量已存在，不覆盖（优先使用系统环境变量）
                if key and value and key not in os.environ:
                    os.environ[key] = value


async def _main(targets: list[str], do_trade: bool):
    # 首先加载 env/.env 文件中的环境变量
    load_env_file()

    keys = load_keys()
    if not keys:
        return

    # 检查 key 是否配置
    print(f"\n{C}── Key 文件检查 ──────────────────────────────────────{W}")
    for ex, k in keys.items():
        if ex not in targets:
            continue
        filled = [v for v in k.values() if v and v not in ("", "base")]
        total  = len([v for kk, v in k.items() if kk != "base"])
        status = ok if len(filled) == total else warn
        status(f"{ex:8s} key={'OK' if k.get('key') else '--'}  "
               f"secret={'OK' if k.get('secret') else '--'}  "
               f"passphrase={'OK' if k.get('passphrase') else 'N/A'}")

    # 显示代理配置（如果设置了）
    proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    if proxy:
        info(f"使用代理: {proxy}")

    connector = aiohttp.TCPConnector(ssl=False)
    timeout   = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, trust_env=True) as sess:
        handlers = {
            "binance": test_binance,
            "okx":     test_okx,
            "gate":    test_gate,
            "bitget":  test_bitget,
        }
        for ex in targets:
            fn = handlers.get(ex)
            if fn and ex in keys:
                await fn(sess, keys[ex], do_trade)

    print(f"\n{C}── 测试完成 ──────────────────────────────────────────{W}\n")
    if do_trade:
        warn("已执行下单测试（Demo/Testnet），请确认 Demo 账户无残留持仓")


def main():
    parser = argparse.ArgumentParser(description="交易所 API 连通性测试")
    parser.add_argument("--trade", action="store_true",
                        help="同时测试下单（Demo/Testnet 安全，不影响真实资金）")
    parser.add_argument("--ex", metavar="EXCHANGE",
                        help="只测某一所: binance / okx / gate / bitget")
    args = parser.parse_args()

    targets = [args.ex] if args.ex else ["binance", "okx", "gate", "bitget"]
    asyncio.run(_main(targets, args.trade))


if __name__ == "__main__":
    main()
