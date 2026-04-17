"""
主控制器。入口：python -m tracker.tracker

运行流程：
  1. 拉取标的列表（5所交集，按成交额排序）
  2. 启动5所 WebSocket
  3. 每个 tick：更新基准 → 检测信号 → 写日志
  4. 每 CONSOLE_STAT_S 秒打印一次终端统计
  5. 每 SYMBOL_REFRESH_H 小时后台刷新标的列表

终止：Ctrl+C 优雅退出，自动 flush 日志。
"""

import asyncio
import logging
import signal
import sys
import time
from datetime import datetime

# Windows 终端强制 UTF-8，避免中文/特殊字符编码崩溃
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from .config import CONSOLE_STAT_S, LOGS_DIR, BIG_EXCHANGES, SMALL_EXCHANGES
from .models import Tick, MarketEvent
from .symbol_selector import SymbolSelector
from .ws_feed import WSFeed
from .baseline import BaselineTracker
from .signal_detector import SignalDetector
from .spread_logger import SpreadLogger

# ─── 日志配置 ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "tracker.log", encoding="utf-8", errors="replace"),
    ],
)
logger = logging.getLogger("tracker")


class Tracker:
    """
    跟踪器主类。负责协调 symbol_selector / ws_feed / baseline / signal_detector / spread_logger。
    对外暴露 signal_queue：asyncio.Queue[Signal]，trader 模块消费这个队列即可。
    """

    def __init__(self):
        self.symbol_sel  = SymbolSelector()
        self.baseline    = BaselineTracker()
        self.detector    = SignalDetector()
        self.log         = SpreadLogger()
        self.feed: WSFeed | None = None

        # 实时行情快照：latest[symbol][exchange] = Tick
        self.latest: dict[str, dict[str, Tick]] = {}

        # 对外输出队列（trader 模块消费 MarketEvent）
        self.signal_queue: asyncio.Queue[MarketEvent] = asyncio.Queue()

        # trader 写、tracker 读：当前持仓对集合
        # 元素格式：(big_exchange, small_exchange, symbol)
        # trader 开仓时 add，平仓时 discard
        self.active_positions: set[tuple[str, str, str]] = set()

        # 统计
        self._tick_count  = 0
        self._t0          = time.time()
        self._last_stat   = time.time()
        self._stop        = asyncio.Event()

    # ─── Tick 处理（同步，被 WS 回调调用）────────────────────────────────────

    def _on_tick(self, tick: Tick):
        sym = tick.symbol
        if sym not in self.latest:
            self.latest[sym] = {}
        self.latest[sym][tick.exchange] = tick
        self._tick_count += 1

        # 更新基准
        self.baseline.update(tick, self.latest)

        # 热身完成后检测信号
        if self.baseline.warmed_up:
            for sig in self.detector.check(tick, self.latest, self.baseline):
                self.log.log_signal(sig)
                SpreadLogger.print_signal(sig)
                self.signal_queue.put_nowait(sig)

        # 价差快照日志（每对每秒一次）
        self.log.maybe_snap(tick, self.latest, self.baseline)

    # ─── 主循环 ───────────────────────────────────────────────────────────────

    async def start(self):
        print(_banner())
        logger.info("正在拉取标的列表…")

        symbols = await self.symbol_sel.start()
        if not symbols:
            logger.error("标的列表为空，请检查网络或 REST 接口")
            return

        logger.info(f"监控 {len(symbols)} 个标的: {', '.join(symbols[:8])}{'…' if len(symbols)>8 else ''}")

        # 启动 WS
        self.feed = WSFeed(symbols, self._on_tick)
        await self.feed.start()

        # 等待 WS 稳定连接（最多10s）
        await self._wait_connections()

        # 主循环 + update 推送（并发运行）
        try:
            await asyncio.gather(
                self._main_loop(),
                self._update_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    async def _wait_connections(self):
        """等待至少1所连上，最多等 15s。"""
        deadline = time.time() + 15
        while time.time() < deadline and not self._stop.is_set():
            if self.feed and self.feed.n_connected > 0:
                break
            await asyncio.sleep(0.5)
        n = self.feed.n_connected if self.feed else 0
        logger.info(f"已连接 {n}/5 所 ({', '.join(sorted(self.feed.connected)) if self.feed else ''})")

    async def _main_loop(self):
        """后台维护循环：定期打印统计、刷新标的列表。"""
        while not self._stop.is_set():
            await asyncio.sleep(1.0)

            now = time.time()

            # 热身状态变化提示
            if self.baseline.warmed_up and not getattr(self, "_warmup_printed", False):
                self._warmup_printed = True
                bs = self.baseline.summary()
                logger.info(
                    f"\033[32m✓ 热身完成\033[0m "
                    f"pair基准={bs['pair_baselines']} "
                    f"ba基准={bs['ba_baselines']}"
                )

            # 定期终端统计
            if now - self._last_stat >= CONSOLE_STAT_S:
                self._print_stats()
                self._last_stat = now

            # 标的列表刷新（每8小时）
            refreshed = await self.symbol_sel.maybe_refresh()
            if refreshed and self.feed:
                self.feed.update_symbols(self.symbol_sel.symbols)
                # 更新 latest 字典，加入新标的
                for s in self.symbol_sel.symbols:
                    if s not in self.latest:
                        self.latest[s] = {}

    async def _update_loop(self):
        """
        每 100ms 扫一次 active_positions，对每个持仓对推送当前异常值。
        trader 消费这些 position_update 事件来判断是否平仓。
        """
        while not self._stop.is_set():
            await asyncio.sleep(0.1)

            if not self.active_positions or not self.baseline.warmed_up:
                continue

            now_ns  = time.monotonic_ns()
            wall_ms = time.time() * 1000

            for (big, small, sym) in list(self.active_positions):
                sym_map = self.latest.get(sym, {})
                bt = sym_map.get(big)
                st = sym_map.get(small)
                if not bt or not st or st.mid <= 0:
                    continue

                anomaly = self.baseline.get_pair_anomaly(big, small, sym, bt.mid, st.mid)
                if anomaly is None:
                    continue

                event = MarketEvent(
                    event_type     = "position_update",
                    symbol         = sym,
                    big_exchange   = big,
                    small_exchange = small,
                    anomaly_bps    = anomaly,
                    baseline_bps   = self.baseline.get_pair_baseline(big, small, sym),
                    big_mid        = bt.mid,
                    small_bid      = st.bid,
                    small_ask      = st.ask,
                    small_mid      = st.mid,
                    ts_ns          = now_ns,
                    wall_ms        = wall_ms,
                )
                self.signal_queue.put_nowait(event)

    async def _shutdown(self):
        logger.info("正在退出…")
        if self.feed:
            await self.feed.stop()
        self.log.close()
        self._print_stats()
        logger.info("已退出")

    def stop(self):
        self._stop.set()

    # ─── 统计打印 ─────────────────────────────────────────────────────────────

    def _print_stats(self):
        elapsed = time.time() - self._t0
        tps = self._tick_count / elapsed if elapsed > 0 else 0
        bs  = self.baseline.summary()
        n_connected = self.feed.n_connected if self.feed else 0
        print(
            f"\n\033[90m──────────── 统计 [{datetime.now().strftime('%H:%M:%S')}] "
            f"────────────\033[0m"
        )
        print(f"  运行时长: {elapsed/60:.1f} 分钟")
        print(f"  Tick总数: {self._tick_count:,}  ({tps:.0f}/s)")
        print(f"  已连接:   {n_connected}/5 所 ({', '.join(sorted(self.feed.connected)) if self.feed else ''})")
        print(f"  热身状态: {'[完成]' if bs['warmed_up'] else '[进行中]'}")
        print(f"  价差基准: {bs['pair_baselines']} 对  盘口基准: {bs['ba_baselines']} 个")
        print(f"  信号数:   {self.detector.total_signals}")
        print(f"  快照行数: {self.log.snap_count:,}")

        # 当前所有大-小对的价差概览（最多显示10个）
        shown = 0
        for sym, ex_map in self.latest.items():
            for big in BIG_EXCHANGES:
                if shown >= 10:
                    break
                bt = ex_map.get(big)
                if not bt:
                    continue
                for small in SMALL_EXCHANGES:
                    if shown >= 10:
                        break
                    st = ex_map.get(small)
                    if not st or st.mid <= 0:
                        continue
                    anomaly = self.baseline.get_pair_anomaly(big, small, sym, bt.mid, st.mid)
                    base    = self.baseline.get_pair_baseline(big, small, sym)
                    if anomaly is None:
                        continue
                    spread = (bt.mid - st.mid) / st.mid * 10000
                    print(
                        f"    {sym:12s} {big}→{small}: "
                        f"spread={spread:+.1f}bps  base={base:.1f}  anom={anomaly:+.1f}"
                    )
                    shown += 1
        print()


# ─── Banner ──────────────────────────────────────────────────────────────────

def _banner() -> str:
    return """
╔══════════════════════════════════════════════════════════════╗
║  Spread Tracker v1  —  跨所价差跟踪（仅观测，不交易）        ║
║  策略A: 大所异动 → 小所延迟 → 记录信号和价差数据            ║
║  交易所: Binance / OKX (大) + Gate / Bitget / HTX (小)      ║
║  输出: logs/spread_snapshots.csv  logs/signals.csv           ║
╚══════════════════════════════════════════════════════════════╝"""


# ─── 入口 ─────────────────────────────────────────────────────────────────────

async def _async_main():
    tracker = Tracker()

    # 优雅处理 Ctrl+C / SIGTERM
    loop = asyncio.get_running_loop()

    def _handle_signal():
        print("\n收到退出信号，正在清理…")
        tracker.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass   # Windows 不支持 add_signal_handler

    await tracker.start()


def main():
    try:
        asyncio.run(_async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
