"""
信号收敛分析：跟踪每个信号从触发到价差回归基准的过程。

运行：
    python tools/signal_convergence.py
    python tools/signal_convergence.py --threshold 0.2 --max-seconds 60

输出：tools/out/signal_convergence.csv
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# 默认收敛阈值：|anomaly| <= 0.2% 认为价差已收敛到可接受范围
DEFAULT_CONVERGENCE_THRESHOLD = 0.2
DEFAULT_MAX_SECONDS = 60  # 最多观察 60 秒


def load_params(path: Path) -> dict:
    """读取 tracker 运行时的参数配置。"""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def get_snapshot_time_range(path: Path) -> tuple[float, float]:
    """返回 snapshot 的最早和最晚 wall_ms。"""
    if not path.exists():
        return 0.0, 0.0
    min_ts = float("inf")
    max_ts = 0.0
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = float(row["wall_ms"])
                min_ts = min(min_ts, ts)
                max_ts = max(max_ts, ts)
            except (ValueError, KeyError):
                continue
    return (min_ts, max_ts) if min_ts != float("inf") else (0.0, 0.0)


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
                    "anomaly_pct": float(row["anomaly_pct"]),
                    "spread_pct": float(row["spread_pct"]),
                    "baseline_pct": float(row["baseline_pct"]),
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
        anom = snap["anomaly_pct"]
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
    # 读取 tracker 参数
    params = load_params(Path("logs/params.json"))
    
    # 读取 snapshot 时间范围
    snap_start, snap_end = get_snapshot_time_range(snapshots_path)
    snap_duration_ms = snap_end - snap_start if snap_end > snap_start else 0
    
    # 打印参数配置（整齐格式）
    print("=" * 60)
    print("【分析参数配置】")
    print("-" * 60)
    if params:
        print(f"  大所移动阈值:     {params.get('LEADER_MOVE_PCT', '-'):>6}%")
        print(f"  异常价差阈值:     {params.get('ANOMALY_MIN_PCT', '-'):>6}%")
        print(f"  收敛阈值:         {params.get('CONVERGENCE_PCT', threshold):>6}%")
        print(f"  观察窗口:         {params.get('LEADER_WINDOW_MS', '-'):>6} ms")
        print(f"  冷却时间:         {params.get('COOLDOWN_MS', '-'):>6} ms")
        print(f"  基准热身:         {params.get('BASELINE_WARMUP_S', '-'):>6} s")
        print(f"  监控标的数:       {params.get('TOP_N_SYMBOLS', '-'):>6} 个")
    else:
        print(f"  收敛阈值:         {threshold:>6}% (命令行指定)")
    print(f"  最大观察时间:     {max_seconds:>6} s")
    print("-" * 60)
    print("【数据时间范围】")
    print(f"  快照起始: {datetime.fromtimestamp(snap_start/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  快照结束: {datetime.fromtimestamp(snap_end/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  总时长:   {snap_duration_ms/1000/60:.1f} 分钟 ({snap_duration_ms/1000:.0f} 秒)")
    print("=" * 60)
    print()
    
    if not signals_path.exists():
        print(f"信号文件不存在: {signals_path}")
        sys.exit(1)

    print(f"正在加载快照数据...")
    snapshot_index = load_snapshots(snapshots_path)
    print(f"  快照索引: {len(snapshot_index)} 个唯一交易对")

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
                    "initial_anomaly": float(row["anomaly_pct"]),
                    "baseline": float(row["baseline_pct"]),
                    "big_move": float(row["big_move_pct"]),
                })
            except (ValueError, KeyError):
                continue

    print(f"  信号总数: {len(signals)} 个")

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
        "initial_anomaly_pct", "baseline_pct", "big_move_pct",
        "converged", "converge_time_ms", "duration_ms",
        "max_abs_anomaly_pct", "final_anomaly_pct", "observations",
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
                "initial_anomaly_pct": f"{r['initial_anomaly']:.3f}",
                "baseline_pct": f"{r['baseline']:.3f}",
                "big_move_pct": f"{r['big_move']:.3f}",
                "converged": "1" if r["converged"] else "0",
                "converge_time_ms": f"{r['converge_time_ms']:.0f}" if r["converge_time_ms"] else "",
                "duration_ms": f"{r['duration_ms']:.0f}" if r["duration_ms"] else "",
                "max_abs_anomaly_pct": f"{r['max_anomaly']:.3f}",
                "final_anomaly_pct": f"{r['final_anomaly']:.3f}",
                "observations": r["observations"],
            })

    # 打印汇总
    total = len(results)
    converged = sum(1 for r in results if r["converged"])
    durations = [r["duration_ms"] for r in results if r["converged"] and r["duration_ms"]]
    avg_duration = sum(durations) / len(durations) if durations else 0
    median_duration = sorted(durations)[len(durations)//2] if durations else 0

    print("\n" + "=" * 60)
    print("【分析结果汇总】")
    print("-" * 60)
    print(f"  总信号数:         {total:>8} 个")
    print(f"  收敛信号数:       {converged:>8} 个 ({converged/total*100:>5.1f}%)")
    print(f"  未收敛信号数:     {total-converged:>8} 个 ({(total-converged)/total*100:>5.1f}%)")
    print("-" * 60)
    print(f"  平均收敛时间:     {avg_duration:>8.0f} ms ({avg_duration/1000:>5.2f} s)")
    print(f"  中位数收敛时间:   {median_duration:>8.0f} ms ({median_duration/1000:>5.2f} s)")
    print(f"  最短收敛时间:     {min(durations):>8.0f} ms ({min(durations)/1000:>5.2f} s)" if durations else "  最短收敛时间:     N/A")
    print(f"  最长收敛时间:     {max(durations):>8.0f} ms ({max(durations)/1000:>5.2f} s)" if durations else "  最长收敛时间:     N/A")
    print("-" * 60)
    print(f"  输出文件:         {out_path}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="信号收敛过程分析")
    parser.add_argument("--signals", default="logs/signals.csv", help="信号 CSV 路径")
    parser.add_argument("--snapshots", default="logs/spread_snapshots.csv", help="快照 CSV 路径")
    parser.add_argument("--threshold", type=float, default=DEFAULT_CONVERGENCE_THRESHOLD,
                        help=f"收敛阈值 |anomaly| (%%)，默认 {DEFAULT_CONVERGENCE_THRESHOLD}%")
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
