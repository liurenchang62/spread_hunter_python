"""
Clients 模块：交易所客户端配置管理。

集中管理所有交易所的连接参数、分级定义和标的格式转换。

对外暴露：
    from clients import BIG_EXCHANGES, SMALL_EXCHANGES, ALL_EXCHANGES
    from clients import WS_URLS, REST_BASE
    from clients import TESTNET_WS_URLS, TESTNET_REST_BASE
    from clients import to_exchange_fmt, from_raw_symbol

新增交易所步骤：
    1. 在 clients/config.py 中添加 URL 和分级
    2. 在 to_exchange_fmt / from_raw_symbol 中添加格式转换
"""

from .config import (
    # 交易所分级
    EXCHANGE_TIERS,
    ACTIVE_EXCHANGES,
    BIG_EXCHANGES,
    SMALL_EXCHANGES,
    ALL_EXCHANGES,
    # 主网地址
    WS_URLS,
    REST_BASE,
    # 测试网地址
    TESTNET_WS_URLS,
    TESTNET_REST_BASE,
    # 格式转换
    to_exchange_fmt,
    from_raw_symbol,
)

__all__ = [
    "EXCHANGE_TIERS",
    "ACTIVE_EXCHANGES",
    "BIG_EXCHANGES",
    "SMALL_EXCHANGES",
    "ALL_EXCHANGES",
    "WS_URLS",
    "REST_BASE",
    "TESTNET_WS_URLS",
    "TESTNET_REST_BASE",
    "to_exchange_fmt",
    "from_raw_symbol",
]
