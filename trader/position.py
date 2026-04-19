"""
仓位数据结构。

每一笔套利仓位由两条腿组成：
  - small_leg：小所（主力腿，跟随大所方向）
  - big_leg：大所（对冲腿，反向）
"""

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Leg:
    """单腿成交记录。"""
    exchange:    str
    symbol:      str
    side:        str         # "buy" | "sell"
    order_id:    str
    entry_price: float       # 实际成交均价
    size_usdt:   float       # 名义价值
    size_base:   float       # 基础货币数量（如 0.001 BTC）
    fee_usdt:    float = 0.0


@dataclass
class Position:
    """
    一笔完整的套利仓位（双腿）。

    open 状态：两腿都已开仓，等待价差收敛
    closing 状态：已发出平仓指令，等待确认
    closed 状态：已平仓，pnl 有值
    """
    id:            str   = field(default_factory=lambda: uuid.uuid4().hex[:8])
    symbol:        str   = ""
    big_exchange:  str   = ""
    small_exchange: str  = ""
    direction:     str   = ""   # "long"：小所做多；"short"：小所做空

    small_leg: Optional[Leg] = None
    big_leg:   Optional[Leg] = None

    open_anomaly_pct: float = 0.0   # 开仓时的异常价差（%）
    open_time:        float = field(default_factory=time.time)

    status: str = "open"            # "open" | "closing" | "closed"

    # 平仓信息
    close_anomaly_pct: float = 0.0
    close_time:        float = 0.0
    close_reason:      str   = ""   # "convergence" | "stop_loss" | "timeout"
    pnl_usdt:          float = 0.0  # 净盈亏（含手续费）

    @property
    def hold_seconds(self) -> float:
        return time.time() - self.open_time

    @property
    def is_open(self) -> bool:
        return self.status == "open"

    def calc_pnl(self) -> float:
        """根据两腿成交价计算理论 PnL（不含已记录手续费以外的滑点）。"""
        if not self.small_leg or not self.big_leg:
            return 0.0
        # small leg PnL
        if self.direction == "long":
            small_pnl = (self.small_leg.entry_price - self.small_leg.entry_price) * self.small_leg.size_base  # 未平仓时为0
        else:
            small_pnl = 0.0
        return small_pnl - self.small_leg.fee_usdt - self.big_leg.fee_usdt
