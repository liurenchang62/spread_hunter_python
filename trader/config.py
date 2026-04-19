"""
Trader 配置。

LIVE_TRADING_ON = False  → 测试网/Demo 模拟下单
LIVE_TRADING_ON = True   → 主网实盘（谨慎！）
"""

from pathlib import Path

# ─── 主开关 ───────────────────────────────────────────────────────────────────
LIVE_TRADING_ON = False   # False = 测试网模拟；True = 主网实盘

# 测试网支持的交易所（HTX 无测试网，暂不参与交易）
TESTNET_EXCHANGES = {"binance", "okx", "gate", "bitget"}

# ─── 资金结构 ─────────────────────────────────────────────────────────────────
# 每个套利对（big-small-symbol）两腿合计最大资金（USDT）
# 单腿 = PAIR_CAPITAL_USDT / 2
# 后续接入账户余额查询后改为动态计算（取4所最小余额的固定比例）
PAIR_CAPITAL_USDT = 100.0

MAX_POSITIONS_PER_PAIR   = 3    # 同一套利对同时最多 N 笔
MAX_POSITIONS_PER_SYMBOL = 15   # 同一合约跨所有套利对最多 N 笔

# ─── 开仓条件 ─────────────────────────────────────────────────────────────────
MIN_ANOMALY_TO_OPEN_PCT = 0.5  # 开仓最低异常阈值（%），建议 >= tracker 的 ANOMALY_MIN_PCT
MIN_NET_ROI             = 0.001  # 最低净 ROI（相对每腿资金），0.001 = 0.1%
MIN_PROFIT_USDT         = 0.05   # 最低净利润绝对值（USDT），防止为几分钱交易

# ─── 成本模型参数 ─────────────────────────────────────────────────────────────
HOLD_ESTIMATE_S     = 60.0   # 预估持仓时长（秒），用于资金费率估算
SLIPPAGE_MULTIPLIER = 1.5    # 滑点保守系数：BBO 价差 × 此系数

# ─── 平仓条件 ─────────────────────────────────────────────────────────────────
CONVERGENCE_PCT  = 0.15   # |anomaly| <= 此值认为价差收敛 → 止盈平仓（%）
STOP_LOSS_PCT    = 0.8    # anomaly 反向超过此值 → 止损平仓（%）
MAX_HOLD_SECONDS = 300    # 超过此时间强制平仓（秒）

# ─── 市场信息刷新 ─────────────────────────────────────────────────────────────
MARKET_INFO_REFRESH_H = 4  # 合约规格 / 费率 / 资金费 刷新周期（小时）

# ─── 风控参数 ──────────────────────────────────────────────────────────────────
# 日止损：当日净盈亏跌至日初余额的 X% 时，触发日止损停机（关闭所有仓位后退出）
DAILY_HALT_PCT        = 0.70   # 亏损超过日初余额的 30%（剩余 70%）时触发

# 单所余额下降超过 X% 时打印警告（不停机，提醒人工补充或调仓）
REBALANCE_WARN_PCT    = 0.30   # 某所余额相比日初下降超过 30% 发出警告

# 所有仓位名义价值（两腿之和）不超过账户总余额的 X%
MAX_EXPOSURE_PCT      = 0.50   # 最大总敞口：账户总余额 × 50%

# 连续下单失败 N 次后冷却（进入 exposure 类型暂停，自动恢复）
MAX_CONSECUTIVE_FAILS = 3
FAILURE_COOLDOWN_S    = 300    # 冷却时长（秒）

# 单所每分钟最大下单次数（防止触发交易所频率限制）
MAX_ORDERS_PER_MIN    = 10

# 账户余额后台刷新周期（秒）
BALANCE_REFRESH_S     = 300

# ─── 日志 ─────────────────────────────────────────────────────────────────────
LOGS_DIR  = Path(__file__).resolve().parent.parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
TRADE_LOG = LOGS_DIR / "trades.csv"
