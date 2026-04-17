"""
标的筛选：在5个交易所都有的 USDT-M 永续合约，按24h成交额排序取前N个。

流程：
  1. 从 Binance FAPI 拉所有 USDT-M 合约 + 24h 成交额
  2. 从其余4所分别拉合约列表，构建各所可用标的集合
  3. 取5所的交集，按 Binance 成交额排序，返回前 TOP_N 个标的
  4. 每 SYMBOL_REFRESH_H 小时刷新一次

返回的标的格式：内部格式 BTCUSDT（Binance 风格，大写，无分隔符）
"""

import asyncio
import logging
import time
import warnings
from typing import Optional

import requests
import urllib3

# Windows 上本地 CA 缺失导致 SSL 验证失败很常见，统一关掉警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .config import REST_BASE, TOP_N_SYMBOLS, MIN_VOLUME_USDT, SYMBOL_REFRESH_H

logger = logging.getLogger("tracker.symbols")


# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None, timeout: int = 10) -> Optional[dict | list]:
    """同步 HTTP GET，封装异常。供 asyncio.to_thread 调用。
    verify=False 绕过 Windows 常见的 SSL 本地证书问题。
    """
    try:
        r = requests.get(url, params=params, timeout=timeout, verify=False)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"GET {url} 失败: {e}")
        return None


# ─── 各交易所标的列表获取 ─────────────────────────────────────────────────────

def _binance_symbols_with_volume() -> dict[str, float]:
    """
    返回 {BTCUSDT: 24h_quote_volume, ...}
    只保留 USDT 永续合约且成交额 >= MIN_VOLUME_USDT 的标的。
    """
    data = _get(f"{REST_BASE['binance']}/fapi/v1/ticker/24hr")
    if not data:
        return {}
    result = {}
    for item in data:
        sym = item.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        try:
            vol = float(item.get("quoteVolume", 0))
        except (TypeError, ValueError):
            continue
        if vol >= MIN_VOLUME_USDT:
            result[sym] = vol
    return result


def _okx_symbols() -> set[str]:
    """返回 OKX 可交易的 USDT 永续合约，内部格式如 BTCUSDT。"""
    data = _get(f"{REST_BASE['okx']}/api/v5/public/instruments", {"instType": "SWAP"})
    if not data or data.get("code") != "0":
        return set()
    result = set()
    for item in data.get("data", []):
        inst_id = item.get("instId", "")   # 格式: BTC-USDT-SWAP
        state   = item.get("state", "")
        if inst_id.endswith("-USDT-SWAP") and state == "live":
            base = inst_id.replace("-USDT-SWAP", "")
            result.add(f"{base}USDT")
    return result


def _gate_symbols() -> set[str]:
    """返回 Gate USDT 永续合约，内部格式如 BTCUSDT。"""
    data = _get(f"{REST_BASE['gate']}/api/v4/futures/usdt/contracts")
    if not data:
        return set()
    result = set()
    for item in data:
        name = item.get("name", "")        # 格式: BTC_USDT
        in_delisting = item.get("in_delisting", True)
        if name.endswith("_USDT") and not in_delisting:
            base = name.replace("_USDT", "")
            result.add(f"{base}USDT")
    return result


def _bitget_symbols() -> set[str]:
    """返回 Bitget USDT-M 合约标的，内部格式如 BTCUSDT。"""
    data = _get(
        f"{REST_BASE['bitget']}/api/v2/mix/market/contracts",
        {"productType": "USDT-FUTURES"},
    )
    if not data or str(data.get("code", "")) != "00000":
        return set()
    result = set()
    for item in data.get("data", []):
        sym = item.get("symbol", "")      # 格式: BTCUSDT
        status = item.get("symbolStatus", "")
        if sym.endswith("USDT") and status == "normal":
            result.add(sym)
    return result


def _htx_symbols() -> set[str]:
    """返回 HTX 线性永续合约标的，内部格式如 BTCUSDT。"""
    data = _get(f"{REST_BASE['htx']}/linear-swap-api/v1/swap_contract_info")
    if not data or data.get("status") != "ok":
        return set()
    result = set()
    for item in data.get("data", []):
        code   = item.get("contract_code", "")   # 格式: BTC-USDT
        status = item.get("contract_status", 0)  # 1 = 正常
        if code.endswith("-USDT") and status == 1:
            base = code.replace("-USDT", "")
            result.add(f"{base}USDT")
    return result


# ─── 主逻辑 ───────────────────────────────────────────────────────────────────

async def fetch_common_symbols() -> list[str]:
    """
    异步调度，返回5所都有的标的，按 Binance 24h 成交额从大到小排序，
    最多返回 TOP_N_SYMBOLS 个。
    """
    logger.info("开始拉取各交易所标的列表…")

    # 并发调用（都是 IO 操作）
    (bn_vol, okx_set, gate_set, bitget_set, htx_set) = await asyncio.gather(
        asyncio.to_thread(_binance_symbols_with_volume),
        asyncio.to_thread(_okx_symbols),
        asyncio.to_thread(_gate_symbols),
        asyncio.to_thread(_bitget_symbols),
        asyncio.to_thread(_htx_symbols),
    )

    logger.info(
        f"各所标的数: Binance={len(bn_vol)} OKX={len(okx_set)} "
        f"Gate={len(gate_set)} Bitget={len(bitget_set)} HTX={len(htx_set)}"
    )

    # 5所交集
    common = set(bn_vol.keys()) & okx_set & gate_set & bitget_set & htx_set

    # 按 Binance 成交额排序，取前 N
    ranked = sorted(common, key=lambda s: bn_vol.get(s, 0), reverse=True)
    selected = ranked[:TOP_N_SYMBOLS]

    logger.info(f"筛选完成：5所共同标的 {len(common)} 个，选用 {len(selected)} 个")
    if selected:
        preview = ", ".join(selected[:10])
        logger.info(f"前10: {preview}{'…' if len(selected) > 10 else ''}")

    return selected


class SymbolSelector:
    """
    持有当前的标的列表，并定期刷新。
    tracker.py 持有一个实例，启动时调用 start()，之后访问 .symbols。
    """

    def __init__(self):
        self.symbols: list[str] = []
        self._refresh_interval = SYMBOL_REFRESH_H * 3600
        self._last_refresh: float = 0.0

    async def start(self) -> list[str]:
        """首次加载。失败时返回空列表（后续可重试）。"""
        self.symbols = await fetch_common_symbols()
        self._last_refresh = time.time()
        return self.symbols

    async def maybe_refresh(self) -> bool:
        """如果距上次刷新超过 SYMBOL_REFRESH_H 小时，则重新拉取。返回是否发生了刷新。"""
        if time.time() - self._last_refresh < self._refresh_interval:
            return False
        new = await fetch_common_symbols()
        if new:
            self.symbols = new
            self._last_refresh = time.time()
            return True
        return False
