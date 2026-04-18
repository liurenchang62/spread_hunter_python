"""
交易所客户端配置：集中管理所有交易所的连接参数、分级和格式转换。

如需新增交易所：
  1. 在 WS_URLS / REST_BASE / TESTNET_WS_URLS / TESTNET_REST_BASE 中添加 URL
  2. 在 EXCHANGE_TIERS 中定义分级（"big" 或 "small"）
  3. 在 to_exchange_fmt / from_raw_symbol 中添加格式转换逻辑
"""

from typing import Optional

# ─── 交易所分级 ───────────────────────────────────────────────────────────────
EXCHANGE_TIERS = {
    "binance": "big",
    "okx":     "big",
    "gate":    "small",
    "bitget":  "small",
    "htx":     "small",
}

BIG_EXCHANGES:   list[str] = [ex for ex, tier in EXCHANGE_TIERS.items() if tier == "big"]
SMALL_EXCHANGES: list[str] = [ex for ex, tier in EXCHANGE_TIERS.items() if tier == "small"]
ALL_EXCHANGES:   list[str] = list(EXCHANGE_TIERS.keys())

# ─── WebSocket 地址（主网 / 永续合约）────────────────────────────────────────
WS_URLS: dict[str, str] = {
    "binance": "wss://fstream.binance.com/stream",      # USDT-M 永续 combined
    "okx":     "wss://ws.okx.com:8443/ws/v5/public",    # OKX 公共频道
    "gate":    "wss://fx-ws.gateio.ws/v4/ws/usdt",      # Gate USDT 永续
    "bitget":  "wss://ws.bitget.com/v2/ws/public",      # Bitget USDT-M
    "htx":     "wss://api.hbdm.com/linear-swap-ws",     # HTX 线性永续
}

# ─── REST 地址（主网）─────────────────────────────────────────────────────────
REST_BASE: dict[str, str] = {
    "binance": "https://fapi.binance.com",
    "okx":     "https://www.okx.com",
    "gate":    "https://api.gateio.ws",
    "bitget":  "https://api.bitget.com",
    "htx":     "https://api.hbdm.com",
}

# ─── 测试网 WebSocket 地址 ───────────────────────────────────────────────────
TESTNET_WS_URLS: dict[str, str] = {
    "binance": "wss://stream.testnet.binance.vision/stream",  # Binance Spot Testnet
    "okx":     "wss://ws.okx.com:8443/ws/v5/public",        # OKX Demo 用 isDemo flag
    "gate":    "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt",   # Gate Testnet
    "bitget":  "wss://ws.bitget.com/v2/ws/public",           # Bitget Demo 用 header
    # "htx":   "",  # HTX 无明确 testnet
}

# ─── 测试网 REST 地址 ────────────────────────────────────────────────────────
TESTNET_REST_BASE: dict[str, str] = {
    "binance": "https://testnet.binance.vision",
    "okx":     "https://www.okx.com",  # Demo trading 用 test-api 路径或 isDemo flag
    "gate":    "https://fx-api-testnet.gateio.ws",
    "bitget":  "https://api.bitget.com",  # Demo 用 PAPTRADING header
    # "htx":   "",
}

# ─── 标的格式转换 ────────────────────────────────────────────────────────────

def to_exchange_fmt(symbol: str, exchange: str) -> str:
    """
    内部格式 BTCUSDT → 各交易所的合约代码格式。
    
    Args:
        symbol: 内部格式，如 "BTCUSDT"
        exchange: 交易所 ID
    
    Returns:
        交易所特定格式的合约代码
    """
    base = symbol[:-4]  # 去掉末尾 USDT
    
    if exchange == "binance":
        return symbol.lower()                       # btcusdt
    elif exchange == "okx":
        return f"{base}-USDT-SWAP"                  # BTC-USDT-SWAP
    elif exchange == "gate":
        return f"{base}_USDT"                       # BTC_USDT
    elif exchange == "bitget":
        return symbol                               # BTCUSDT
    elif exchange == "htx":
        return f"{base}-USDT"                       # BTC-USDT
    return symbol


def from_raw_symbol(raw: str, exchange: str) -> Optional[str]:
    """
    各交易所原始代码 → 内部 BTCUSDT 格式。
    
    Args:
        raw: 交易所返回的原始 symbol
        exchange: 交易所 ID
    
    Returns:
        内部格式如 "BTCUSDT"，不认识则返回 None
    """
    r = raw.upper()
    
    if exchange == "binance":
        return r if r.endswith("USDT") else None
    elif exchange == "okx":
        if r.endswith("-USDT-SWAP"):
            return r.replace("-USDT-SWAP", "") + "USDT"
    elif exchange == "gate":
        if r.endswith("_USDT"):
            return r.replace("_USDT", "") + "USDT"
    elif exchange == "bitget":
        return r if r.endswith("USDT") else None
    elif exchange == "htx":
        if r.endswith("-USDT"):
            return r.replace("-USDT", "") + "USDT"
    return None
