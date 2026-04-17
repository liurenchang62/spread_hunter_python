"""
信号统计工具：读取 logs/signals.csv，按 大所-小所-合约 聚合分析。

运行：
    python tools/signal_stats.py
    python tools/signal_stats.py --file logs/signals.csv --top 20
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


def load_signals(path: Path) -> list[dict]:
    if not path.exists():
        print(f"文件不存在: {path}")
        sys.exit(1)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def stats(rows: list[dict], top_n: int = 20):
    # ── 聚合维度：(big_ex, small_ex, symbol, direction) ──────────────────────
    groups: dict[tuple, list[float]] = defaultdict(list)
    for r in rows:
        key = (r["big_ex"], r["small_ex"], r["symbol"], r["direction"])
        try:
            groups[key].append(float(r["anomaly_bps"]))
        except (ValueError, KeyError):
            continue

    if not groups:
        print("没有有效数据")
        return

    # ── 计算统计量 ────────────────────────────────────────────────────────────
    results = []
    for (big, small, sym, direction), anomalies in groups.items():
        n = len(anomalies)
        avg = sum(anomalies) / n
        mx  = max(anomalies)
        mn  = min(anomalies)
        # 粗略估计：超过 10bps 的信号占比（可能覆盖手续费）
        pct_10bps = sum(1 for a in anomalies if abs(a) >= 10) / n * 100
        results.append({
            "big":        big,
            "small":      small,
            "symbol":     sym,
            "direction":  direction,
            "count":      n,
            "avg_anom":   avg,
            "max_anom":   mx,
            "min_anom":   mn,
            "pct_ge10bps": pct_10bps,
        })

    # 按信号数降序
    results.sort(key=lambda x: x["count"], reverse=True)

    # ── 打印 ─────────────────────────────────────────────────────────────────
    header = f"{'大所':8s} {'小所':8s} {'合约':16s} {'方向':6s} {'次数':>6s} {'均值bps':>8s} {'最大bps':>8s} {'>=10bps%':>9s}"
    print("\n" + "=" * len(header))
    print(f"信号统计  (共 {len(rows)} 条，{len(groups)} 个唯一交易对)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for r in results[:top_n]:
        print(
            f"{r['big']:8s} {r['small']:8s} {r['symbol']:16s} {r['direction']:6s} "
            f"{r['count']:>6d} {r['avg_anom']:>8.1f} {r['max_anom']:>8.1f} {r['pct_ge10bps']:>8.1f}%"
        )

    # ── 全局汇总 ─────────────────────────────────────────────────────────────
    total = len(rows)
    all_anom = [float(r["anomaly_bps"]) for r in rows if r.get("anomaly_bps")]
    print("-" * len(header))
    print(f"总计: {total} 条信号  |  异常均值: {sum(all_anom)/len(all_anom):.1f}bps  |  "
          f"最大: {max(all_anom):.1f}bps")

    # ── 按标的汇总（不区分方向和交易所对） ────────────────────────────────────
    by_sym: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        try:
            by_sym[r["symbol"]].append(float(r["anomaly_bps"]))
        except (ValueError, KeyError):
            pass
    sym_results = sorted(by_sym.items(), key=lambda x: len(x[1]), reverse=True)

    print(f"\n{'─'*40}")
    print("按标的汇总（前10）")
    print(f"{'合约':16s} {'信号数':>6s} {'均值bps':>8s} {'最大bps':>8s}")
    print(f"{'─'*40}")
    for sym, anomalies in sym_results[:10]:
        print(f"{sym:16s} {len(anomalies):>6d} {sum(anomalies)/len(anomalies):>8.1f} {max(anomalies):>8.1f}")
    print()


def main():
    parser = argparse.ArgumentParser(description="价差信号统计分析")
    parser.add_argument("--file", default="logs/signals.csv", help="信号CSV文件路径")
    parser.add_argument("--top",  type=int, default=20,       help="显示前N个交易对")
    args = parser.parse_args()

    rows = load_signals(Path(args.file))
    stats(rows, top_n=args.top)


if __name__ == "__main__":
    main()
