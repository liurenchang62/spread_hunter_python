"""
Spread Hunter — Tracker + Trader 联合启动入口。

用法（在项目根目录 spread_hunter_python 下）：
    python run.py

Tracker 负责：行情采集、信号检测、signal_queue 输出
Trader  负责：消费 signal_queue、管理仓位、执行开/平仓

LIVE_TRADING_ON 开关在 trader/config.py：
    False → 测试网/Demo（当前默认）
    True  → 主网实盘（谨慎！）
"""

import asyncio
import logging
import signal
import sys

# Windows 终端强制 UTF-8
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from tracker.tracker import Tracker
from trader.trader import Trader


async def _async_main():
    tracker = Tracker()
    trader  = Trader(tracker)

    loop = asyncio.get_running_loop()

    def _handle_signal():
        print("\n收到退出信号，正在清理…")
        tracker.stop()
        trader.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass   # Windows 不支持 add_signal_handler

    await asyncio.gather(
        tracker.start(),
        trader.start(),
    )


def main():
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
