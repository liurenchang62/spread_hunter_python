"""
全局配置。修改此文件调整所有行为，核心逻辑不用动。
"""

from pathlib import Path

# ─── 项目目录 ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR     = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ─── 交易所分级 ───────────────────────────────────────────────────────────────
BIG_EXCHANGES:   list[str] = ["binance", "okx"]
SMALL_EXCHANGES: list[str] = ["gate", "bitget", "htx"]
ALL_EXCHANGES:   list[str] = BIG_EXCHANGES + SMALL_EXCHANGES

# ─── WebSocket 地址（全部是永续合约/USDT-M） ─────────────────────────────────
WS_URLS: dict[str, str] = {
    "binance": "wss://fstream.binance.com/stream",      # USDT-M 永续 combined stream
    "okx":     "wss://ws.okx.com:8443/ws/v5/public",    # OKX 公共频道
    "gate":    "wss://fx-ws.gateio.ws/v4/ws/usdt",      # Gate USDT 永续
    "bitget":  "wss://ws.bitget.com/v2/ws/public",      # Bitget USDT-M
    "htx":     "wss://api.hbdm.com/linear-swap-ws",     # HTX 线性永续（非现货）
}

# ─── REST 地址（仅用于启动时拉标的列表，不用于行情） ──────────────────────────
REST_BASE: dict[str, str] = {
    "binance": "https://fapi.binance.com",
    "okx":     "https://www.okx.com",
    "gate":    "https://api.gateio.ws",
    "bitget":  "https://api.bitget.com",
    "htx":     "https://api.hbdm.com",
}

# ─── 标的筛选 ─────────────────────────────────────────────────────────────────
TOP_N_SYMBOLS       = 50           # 最多监控几个标的
SYMBOL_REFRESH_H    = 8            # 多少小时刷新一次标的列表
MIN_VOLUME_USDT     = 10_000_000   # 24h 成交额过滤（USDT），过滤掉流动性极差的小币

# ─── 基准追踪 ─────────────────────────────────────────────────────────────────
BASELINE_WARMUP_S   = 60           # 热身时间（秒）。热身期间只收集数据，不触发信号
BASELINE_WINDOW     = 2000         # 滚动窗口大小（存多少个 tick 算中位数）
BASELINE_UPDATE_MS  = 50           # 每隔多少毫秒才更新一次基准（节省 CPU）

# ─── 信号检测（大所带动 + 小所滞后）──────────────────────────────────────────
LEADER_WINDOW_MS  = 1000         # 检测大所 N 毫秒内的价格变动
LEADER_MOVE_PCT   = 0.3          # 大所触发阈值：变动超过多少 % 才算异动（例：0.3 = 0.3%）
ANOMALY_MIN_PCT   = 0.5          # 小所相对基准的异常价差最小值（%，例：0.5 = 0.5%）
CONVERGENCE_PCT   = 0.2          # 收敛阈值：|异常%| <= 0.2% 认为价差已回归
COOLDOWN_MS       = 2000         # 同标的同方向信号冷却时间（毫秒）
MIN_SMALL_MID     = 1e-6         # 小所价格低于此值时跳过（防除零）

# ─── 日志输出 ─────────────────────────────────────────────────────────────────
SPREAD_SNAP_INTERVAL_S = 1.0       # 每隔多少秒快照一次所有价差（写入 CSV）
SIGNAL_LOG      = LOGS_DIR / "signals.csv"
SPREAD_LOG      = LOGS_DIR / "spread_snapshots.csv"
CONSOLE_STAT_S  = 30               # 每隔多少秒在终端打印一次统计摘要
