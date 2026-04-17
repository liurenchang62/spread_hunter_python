"""
信号收敛分析：跟踪每个信号从触发到价差回归基准的过程。

运行：
    python tools/signal_convergence.py
    python tools/signal_convergence.py --threshold 2.0 --max-seconds 60

输出：tools/out/signal_convergence.csv
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

# 默认收敛阈值：|anomaly_bps| <= 2.0 认为价差已收敛到可接受范围
DEFAULT_CONVERGENCE_THRESHOLD = 2.0
DEFAULT_MAX_SECONDS = 60  # 最多观察 60 秒


def load_snapshots(path: Path) -> dict[tuple, list[dict]]:
    """
    读取 spread_snapshots.csv，按 (big_ex, small_ex, symbol) 索引。
    返回：索引 -> 按 wall_ms 排序的记录列表。
    """
    if not path.exists():
        print(f"快照文件不存在: {path}")
        sys.exit(1)

    index = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                key = (row["big_ex"], row["small_ex"], row["symbol"])
                index[key].append({
                    "wall_ms": float(row["wall_ms"]),
                    "anomaly_bps": float(row["anomaly_bps"]),
                    "spread_bps": float(row["spread_bps"]),
                    "baseline_bps": float(row["baseline_bps"]),
                    "big_mid": float(row["big_mid"]),
                    "small_mid": float(row["small_mid"]),
                })
            except (ValueError, KeyError):
                continue

    # 按时间排序
    for key in index:
        index[key].sort(key=lambda x: x["wall_ms"])

    return index


def find_convergence(
    snapshots: list[dict],
    signal_time_ms: float,
    threshold: float,
    max_seconds: float,
    initial_anomaly: float,
) -> dict:
    """
    在快照列表中查找信号时间之后的收敛点。
    
    返回：{
        "converged": bool,
        "converge_time_ms": Optional[float],
        "duration_ms": Optional[float],
        "max_anomaly": float,
        "final_anomaly": float,
        "observations": int,
    }
    """
    max_observed_anomaly = abs(initial_anomaly)
    final_anomaly = initial_anomaly
    observations = 0
    converge_time_ms: Optional[float] = None

    cutoff_ms = signal_time_ms + max_seconds * 1000

    for snap in snapshots:
        if snap["wall_ms"] <= signal_time_ms:
            continue
        if snap["wall_ms"] > cutoff_ms:
            break

        observations += 1
        anom = snap["anomaly_bps"]
        final_anomaly = anom
        max_observed_anomaly = max(max_observed_anomaly, abs(anom))

        # 收敛判断：|anomaly| <= threshold
        if abs(anom) <= threshold and converge_time_ms is None:
            converge_time_ms = snap["wall_ms"]
            # 继续扫描，记录最大异常和最终异常，但已标记收敛时间

    result = {
        "converged": converge_time_ms is not None,
        "converge_time_ms": converge_time_ms,
        "duration_ms": (converge_time_ms - signal_time_ms) if converge_time_ms else None,
        "max_anomaly": max_observed_anomaly,
        "final_anomaly": final_anomaly,
        "observations": observations,
    }
    return result


def analyze(signals_path: Path, snapshots_path: Path, threshold: float, max_seconds: float):
    """主分析逻辑。"""
    if not signals_path.exists():
        print(f"信号文件不存在: {signals_path}")
        sys.exit(1)

    print(f"正在加载快照数据（可能较慢）...")
    snapshot_index = load_snapshots(snapshots_path)
    print(f"快照索引建立完成：{len(snapshot_index)} 个唯一交易对")

    # 读取信号
    signals = []
    with open(signals_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                signals.append({
                    "wall_ms": float(row["wall_ms"]),
                    "symbol": row["symbol"],
                    "direction": row["direction"],
                    "big_ex": row["big_ex"],
                    "small_ex": row["small_ex"],
                    "initial_anomaly": float(row["anomaly_bps"]),
                    "baseline": float(row["baseline_bps"]),
                    "big_move": float(row["big_move_bps"]),
                })
            except (ValueError, KeyError):
                continue

    print(f"信号总数: {len(signals)}")

    # 逐个分析收敛
    results = []
    for sig in signals:
        key = (sig["big_ex"], sig["small_ex"], sig["symbol"])
        snaps = snapshot_index.get(key, [])

        if not snaps:
            # 无对应快照数据
            results.append({
                **sig,
                "converged": False,
                "converge_time_ms": None,
                "duration_ms": None,
                "max_anomaly": abs(sig["initial_anomaly"]),
                "final_anomaly": sig["initial_anomaly"],
                "observations": 0,
            })
            continue

        conv = find_convergence(
            snaps,
            sig["wall_ms"],
            threshold,
            max_seconds,
            sig["initial_anomaly"],
        )

        results.append({
            **sig,
            **conv,
        })

    return results


def write_output(results: list[dict], out_path: Path, threshold: float):
    """写入 CSV 结果。"""
    header = [
        "signal_time_ms", "symbol", "direction", "big_ex", "small_ex",
        "initial_anomaly_bps", "baseline_bps", "big_move_bps",
        "converged", "converge_time_ms", "duration_ms",
        "max_abs_anomaly_bps", "final_anomaly_bps", "observations",
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "signal_time_ms": f"{r['wall_ms']:.0f}",
                "symbol": r["symbol"],
                "direction": r["direction"],
                "big_ex": r["big_ex"],
                "small_ex": r["small_ex"],
                "initial_anomaly_bps": f"{r['initial_anomaly']:.3f}",
                "baseline_bps": f"{r['baseline']:.3f}",
                "big_move_bps": f"{r['big_move']:.3f}",
                "converged": "1" if r["converged"] else "0",
                "converge_time_ms": f"{r['converge_time_ms']:.0f}" if r["converge_time_ms"] else "",
                "duration_ms": f"{r['duration_ms']:.0f}" if r["duration_ms"] else "",
                "max_abs_anomaly_bps": f"{r['max_anomaly']:.3f}",
                "final_anomaly_bps": f"{r['final_anomaly']:.3f}",
                "observations": r["observations"],
            })

    # 打印汇总
    total = len(results)
    converged = sum(1 for r in results if r["converged"])
    durations = [r["duration_ms"] for r in results if r["converged"] and r["duration_ms"]]
    avg_duration = sum(durations) / len(durations) if durations else 0

    print(f"\n分析完成（收敛阈值 |anomaly| <= {threshold} bps）:")
    print(f"  总信号数: {total}")
    print(f"  收敛信号: {converged} ({converged/total*100:.1f}%)")
    print(f"  平均收敛时间: {avg_duration:.0f} ms ({avg_duration/1000:.1f} 秒)")
    print(f"  输出文件: {out_path}")


def main():
    parser = argparse.ArgumentParser(description="信号收敛过程分析")
    parser.add_argument("--signals", default="logs/signals.csv", help="信号 CSV 路径")
    parser.add_argument("--snapshots", default="logs/spread_snapshots.csv", help="快照 CSV 路径")
    parser.add_argument("--threshold", type=float, default=DEFAULT_CONVERGENCE_THRESHOLD,
                        help=f"收敛阈值 |anomaly| (bps)，默认 {DEFAULT_CONVERGENCE_THRESHOLD}")
    parser.add_argument("--max-seconds", type=float, default=DEFAULT_MAX_SECONDS,
                        help=f"最大观察时间（秒），默认 {DEFAULT_MAX_SECONDS}")
    parser.add_argument("--out", default="tools/out/signal_convergence.csv", help="输出路径")
    args = parser.parse_args()

    results = analyze(
        Path(args.signals),
        Path(args.snapshots),
        args.threshold,
        args.max_seconds,
    )

    write_output(results, Path(args.out), args.threshold)


if __name__ == "__main__":
    main()
