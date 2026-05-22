#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FA-iForest 统一分析脚本 / Unified analysis script for FA-iForest experiments

支持 4 种分析模式 / Supports 4 analysis modes:
  - drift       : 单次漂移数据集详细分析 (按版本/分段/延迟/滑动窗口)
  - stationary  : 批处理 N 次 stationary 实验, 输出 median+std
  - throughput  : 从 Prometheus 拉吞吐 + 端到端业务延迟分析
  - scalability : 合并多个 parallelism 的实验结果, 画扩展性曲线

使用示例 / Usage examples:

  # 1. 单次漂移分析
  python analyze.py --mode drift \\
      --scores scores-sudden-old.jsonl \\
      --window 2000 \\
      --driftspec synth_abrupt.driftspec.json \\
      --out drift_report.json

  # 2. 批处理 30 次 stationary 实验
  python analyze.py --mode stationary \\
      --scores-dir runs/donors/ \\
      --out donors_summary.json \\
      --out-csv donors_summary.csv

  # 3. 吞吐 + 业务延迟
  python analyze.py --mode throughput \\
      --prometheus http://master:9090 \\
      --start "2026-05-21T09:00:00Z" \\
      --end "2026-05-21T11:00:00Z" \\
      --scores scores.jsonl \\
      --out throughput_report.json

  # 4. 可扩展性
  python analyze.py --mode scalability \\
      --runs-dir runs/scalability/ \\
      --out scalability.png \\
      --out-csv scalability.csv
"""
import argparse
import json
import os
import sys
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd

# matplotlib 仅在 scalability 模式需要; 延迟导入避免无 X11 环境出错
def _lazy_import_matplotlib():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    return plt


# ============================================================
# 通用工具函数 / Common utilities
# ============================================================

def load_scores(path):
    """加载 scores.jsonl, 返回按 originalSequence 排序的 DataFrame.
    
    每条 jsonl 记录字段(由 ScoreResult 序列化):
      seq, id, score, label, forestVersion, phase, 
      (可选) ingestionTime, scoreTime  -- 用于业务延迟
    """
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    df = pd.DataFrame(records)
    if "seq" not in df.columns:
        raise ValueError(f"{path}: missing required field 'seq'")
    df = df.sort_values("seq").reset_index(drop=True)
    return df


def compute_auc(y_true, scores):
    """计算 ROC AUC (Mann-Whitney U 公式, O(n log n)).
    
    Args:
        y_true: 0/1 标签数组
        scores: 异常分数, 越大越异常
    
    Returns:
        AUC, 数据不足或全同类返回 None
    """
    y = np.asarray(y_true)
    s = np.asarray(scores)
    n = len(y)
    if n < 2:
        return None
    n_pos = int(y.sum())
    n_neg = n - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    # 按 score 升序排, 求 ranks, 用 Mann-Whitney 公式
    order = np.argsort(s, kind="mergesort")
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(1, n + 1)
    # 处理 ties: 同分给平均 rank
    _, inv, counts = np.unique(s, return_inverse=True, return_counts=True)
    # 对每个 tie 组用 mean rank
    sums = np.zeros_like(counts, dtype=np.float64)
    np.add.at(sums, inv, ranks)
    mean_ranks = sums / counts
    ranks = mean_ranks[inv]
    sum_ranks_pos = ranks[y == 1].sum()
    u = sum_ranks_pos - n_pos * (n_pos + 1) / 2
    auc = u / (n_pos * n_neg)
    return float(auc)


def compute_fpr_fnr(y_true, scores, threshold):
    """阈值 threshold 时的 FPR (假正率) 和 FNR (假负率)."""
    y = np.asarray(y_true)
    pred = (np.asarray(scores) >= threshold).astype(int)
    n_neg = int((y == 0).sum())
    n_pos = int((y == 1).sum())
    if n_neg == 0:
        fpr = None
    else:
        fp = int(((pred == 1) & (y == 0)).sum())
        fpr = fp / n_neg
    if n_pos == 0:
        fnr = None
    else:
        fn = int(((pred == 0) & (y == 1)).sum())
        fnr = fn / n_pos
    return fpr, fnr


def compute_precision_recall(y_true, scores, threshold):
    """阈值 threshold 时的 precision (= TP/(TP+FP)) 和 recall (= TP/(TP+FN))."""
    y = np.asarray(y_true)
    pred = (np.asarray(scores) >= threshold).astype(int)
    tp = int(((pred == 1) & (y == 1)).sum())
    fp = int(((pred == 1) & (y == 0)).sum())
    fn = int(((pred == 0) & (y == 1)).sum())
    prec = tp / (tp + fp) if (tp + fp) > 0 else None
    rec = tp / (tp + fn) if (tp + fn) > 0 else None
    return prec, rec


def find_threshold_at_fpr(y_true, scores, target_fpr):
    """二分查找在 target_fpr 时的阈值. 返回 (阈值, 实际 FPR, FNR)."""
    y = np.asarray(y_true)
    s = np.asarray(scores)
    if (y == 1).sum() == 0 or (y == 0).sum() == 0:
        return None, None, None
    # 排序所有分数, 用作候选阈值
    candidates = np.sort(np.unique(s))
    # 二分: 在阈值升序中找 FPR 最接近 target 的
    best_t = None
    best_diff = float("inf")
    for t in candidates:
        fpr, fnr = compute_fpr_fnr(y, s, t)
        if fpr is None:
            continue
        diff = abs(fpr - target_fpr)
        if diff < best_diff:
            best_diff = diff
            best_t = t
            best_fpr = fpr
            best_fnr = fnr
        if fpr <= target_fpr:
            break  # 已经低于目标, 后续阈值更高 FPR 只会更低
    return (best_t, best_fpr, best_fnr) if best_t is not None else (None, None, None)


def find_optimal_threshold(y_true, scores):
    """用 Youden's J statistic (TPR - FPR) 找最优阈值."""
    y = np.asarray(y_true)
    s = np.asarray(scores)
    if (y == 1).sum() == 0 or (y == 0).sum() == 0:
        return None, None
    candidates = np.linspace(s.min(), s.max(), 100)
    best_t = None
    best_j = -1
    for t in candidates:
        fpr, fnr = compute_fpr_fnr(y, s, t)
        if fpr is None or fnr is None:
            continue
        tpr = 1 - fnr
        j = tpr - fpr
        if j > best_j:
            best_j = j
            best_t = t
            best_fpr = fpr
    return (best_t, best_fpr) if best_t is not None else (None, None)


# ============================================================
# Mode 1: drift -- 单次漂移数据集详细分析
# ============================================================

def mode_drift(args):
    """单次漂移数据集详细分析."""
    df = load_scores(args.scores)
    n = len(df)
    
    print("=" * 64)
    print(f"模式 drift: {args.scores}")
    print("=" * 64)
    print(f"\n[1] 整体 AUC: {compute_auc(df['label'], df['score']):.4f}  (n={n})")
    
    result = {
        "mode": "drift",
        "scores_file": str(args.scores),
        "n": n,
        "overall_auc": compute_auc(df["label"], df["score"]),
    }
    
    # [2] 各版本指标 (阈值=0.5)
    print(f"\n[2] 各版本指标 (阈值=0.5)")
    print(f"    {'ver':>6} {'count':>8} {'AUC':>8} {'FPR':>8} {'FNR':>8} "
          f"{'prec':>8} {'recall':>8}")
    version_stats = {}
    for ver in sorted(df["forestVersion"].unique()):
        sub = df[df["forestVersion"] == ver]
        auc = compute_auc(sub["label"], sub["score"])
        fpr, fnr = compute_fpr_fnr(sub["label"], sub["score"], 0.5)
        prec, rec = compute_precision_recall(sub["label"], sub["score"], 0.5)
        print(f"    v{int(ver):<5} {len(sub):>8} "
              f"{auc:>8.4f} {fpr*100:>7.1f}% {fnr*100:>7.1f}% "
              f"{prec:>8.4f} {rec:>8.4f}")
        version_stats[f"v{int(ver)}"] = {
            "count": len(sub), "auc": auc,
            "fpr_at_0.5": fpr, "fnr_at_0.5": fnr,
            "precision_at_0.5": prec, "recall_at_0.5": rec,
        }
    result["per_version"] = version_stats
    
    # [3] 漂移点前后对比 (从 driftspec.json 读漂移点)
    drift_point = None
    drift_events = []
    if args.driftspec and Path(args.driftspec).exists():
        with open(args.driftspec) as f:
            spec = json.load(f)
        drift_events = spec.get("drift_events", [])
        if drift_events:
            drift_point = drift_events[0]["start_line"]
    else:
        drift_point = args.drift_point
    
    if drift_point is not None:
        print(f"\n[3] 漂移点 seq={drift_point} 前后对比")
        print(f"    {'segment':<30} {'n':>6} {'AUC':>8} {'FPR':>8} {'FNR':>8} "
              f"{'opt_t':>6} {'opt_FPR':>8}")
        
        segments = {}
        # 漂移前 v1
        pre_v1 = df[(df["seq"] < drift_point) & (df["forestVersion"] == 1)]
        # 漂移后 v1 (无响应基线)
        post_v1 = df[(df["seq"] >= drift_point) & (df["forestVersion"] == 1)]
        # 漂移后最终版本 (响应后)
        max_ver = int(df["forestVersion"].max())
        post_final = df[(df["seq"] >= drift_point) & (df["forestVersion"] == max_ver)]
        
        for label, sub in [("漂移前 v1", pre_v1),
                          ("漂移后 v1 (旧森林)", post_v1),
                          (f"漂移后 v{max_ver} (最终版本)", post_final)]:
            if len(sub) == 0:
                continue
            auc = compute_auc(sub["label"], sub["score"])
            fpr, fnr = compute_fpr_fnr(sub["label"], sub["score"], 0.5)
            opt_t, opt_fpr = find_optimal_threshold(sub["label"], sub["score"])
            print(f"    {label:<30} {len(sub):>6} "
                  f"{auc:>8.4f} {fpr*100 if fpr else 0:>7.1f}% {fnr*100 if fnr else 0:>7.1f}% "
                  f"{opt_t if opt_t else 0:>6.3f} {opt_fpr*100 if opt_fpr else 0:>7.1f}%")
            segments[label] = {
                "n": len(sub), "auc": auc,
                "fpr_at_0.5": fpr, "fnr_at_0.5": fnr,
                "optimal_threshold": opt_t, "optimal_fpr": opt_fpr,
            }
        result["drift_point"] = drift_point
        result["segments"] = segments
        
        # [4] FPR <= 5% 阈值 (业务视角)
        print(f"\n[4] FPR <= 5% 时的阈值 (业务视角)")
        threshold_at_5pct = {}
        for label, sub in [("漂移前 v1", pre_v1),
                          ("漂移后 v1 (旧森林)", post_v1),
                          (f"漂移后 v{max_ver}", post_final)]:
            if len(sub) == 0:
                continue
            t, fpr, fnr = find_threshold_at_fpr(sub["label"], sub["score"], 0.05)
            if t is None:
                print(f"    {label:<30} (n={len(sub)} 太少)")
                continue
            print(f"    {label:<30} 阈值={t:.3f}, FPR={fpr*100:.1f}%, FNR={fnr*100:.1f}%")
            threshold_at_5pct[label] = {"threshold": t, "fpr": fpr, "fnr": fnr}
        result["threshold_at_5pct_fpr"] = threshold_at_5pct
        
        # [5] 漂移响应延迟
        print(f"\n[5] 漂移响应延迟 (从 seq={drift_point})")
        delays = {}
        
        # 定义 1: 首次新版本出现
        non_v1 = df[(df["seq"] >= drift_point) & (df["forestVersion"] > 1)]
        if len(non_v1) > 0:
            first_new = int(non_v1["seq"].min())
            delay_1 = first_new - drift_point
            print(f"    定义1 (首次新版本出现)")
            print(f"      首次非 v1 数据 @ seq={first_new}")
            print(f"      -> 延迟: {delay_1} 条数据")
            delays["definition_1_first_new_version"] = {
                "first_new_version_seq": first_new,
                "delay_records": delay_1,
            }
        else:
            print(f"    定义1: 漂移后未产生新版本 (HDDM 未触发或 COOLDOWN 未完成)")
            delays["definition_1_first_new_version"] = None
        
        # 定义 2: AUC 回到漂移前 95%
        # 用 1000 条滑动窗口看 AUC 恢复
        win = 1000
        if len(pre_v1) > 0:
            baseline_auc = compute_auc(pre_v1["label"], pre_v1["score"])
            target = baseline_auc * 0.95
            print(f"    定义2 (AUC 回到漂移前 95%)")
            print(f"      漂移前 AUC 基线: {baseline_auc:.4f}")
            print(f"      目标: {target:.4f}")
            recovery_seq = None
            for start in range(drift_point, n - win, 100):
                window_df = df[(df["seq"] >= start) & (df["seq"] < start + win)]
                if len(window_df) < 100:
                    continue
                w_auc = compute_auc(window_df["label"], window_df["score"])
                if w_auc and w_auc >= target:
                    recovery_seq = start
                    break
            if recovery_seq:
                delay_2 = recovery_seq - drift_point
                print(f"      恢复 @ seq={recovery_seq}")
                print(f"      -> 延迟: {delay_2} 条数据")
                delays["definition_2_auc_recovery"] = {
                    "baseline_auc": baseline_auc, "target_auc": target,
                    "recovery_seq": recovery_seq, "delay_records": delay_2,
                }
            else:
                print(f"      未恢复到 95% 基线")
                delays["definition_2_auc_recovery"] = None
        
        # 定义 3: FPR <= 10%
        print(f"    定义3 (FPR @ 阈值 0.5 <= 10%)")
        if len(pre_v1) > 0:
            pre_fpr, _ = compute_fpr_fnr(pre_v1["label"], pre_v1["score"], 0.5)
            print(f"      漂移前 FPR 基线: {pre_fpr*100:.1f}%")
            peak_fpr = 0
            stable_seq = None
            for start in range(drift_point, n - win, 100):
                window_df = df[(df["seq"] >= start) & (df["seq"] < start + win)]
                if len(window_df) < 100:
                    continue
                fpr, _ = compute_fpr_fnr(window_df["label"], window_df["score"], 0.5)
                if fpr is None:
                    continue
                peak_fpr = max(peak_fpr, fpr)
                if fpr <= 0.10 and stable_seq is None:
                    stable_seq = start
                    break
            print(f"      漂移期峰值 FPR: {peak_fpr*100:.1f}%")
            if stable_seq:
                delay_3 = stable_seq - drift_point
                print(f"      稳定 @ seq={stable_seq}")
                print(f"      -> 延迟: {delay_3} 条数据")
                delays["definition_3_fpr_stable"] = {
                    "peak_fpr": peak_fpr, "stable_seq": stable_seq,
                    "delay_records": delay_3,
                }
            else:
                print(f"      数据末尾仍未稳定到 <= 10%")
                delays["definition_3_fpr_stable"] = None
        result["delays"] = delays
    
    # [6] 滑动窗口导出 (供画图)
    if args.window_csv:
        sliding = []
        for start in range(0, n - args.window, args.window // 4):
            window_df = df[(df["seq"] >= start) & (df["seq"] < start + args.window)]
            if len(window_df) < 100:
                continue
            auc = compute_auc(window_df["label"], window_df["score"])
            fpr, _ = compute_fpr_fnr(window_df["label"], window_df["score"], 0.5)
            sliding.append({
                "window_start": start,
                "window_center": start + args.window // 2,
                "n": len(window_df),
                "auc": auc, "fpr_at_0.5": fpr,
            })
        pd.DataFrame(sliding).to_csv(args.window_csv, index=False)
        print(f"\n[6] 滑动窗口 CSV 已导出到 {args.window_csv}")
        print(f"    {len(sliding)} 个数据点, window={args.window}")
    
    # 写 JSON
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n报告写入: {args.out}")
    
    return result


# ============================================================
# Mode 2: stationary -- 批处理 N 次实验
# ============================================================

def mode_stationary(args):
    """批处理 N 次 stationary 实验, 输出 median+std."""
    scores_dir = Path(args.scores_dir)
    files = sorted(scores_dir.glob("*.jsonl"))
    if not files:
        print(f"ERROR: 在 {scores_dir} 找不到 .jsonl 文件")
        return None
    
    print("=" * 64)
    print(f"模式 stationary: 批处理 {len(files)} 个实验")
    print("=" * 64)
    
    runs = []
    for f in files:
        df = load_scores(f)
        n = len(df)
        auc = compute_auc(df["label"], df["score"])
        fpr, fnr = compute_fpr_fnr(df["label"], df["score"], 0.5)
        # 触发次数 = 数据集中出现的不同版本数 - 1
        versions = sorted(df["forestVersion"].unique())
        n_triggers = len(versions) - 1  # v1 是初始版本不算触发
        runs.append({
            "file": str(f.name),
            "n": int(n),
            "auc": float(auc) if auc is not None else None,
            "fpr_at_0.5": float(fpr) if fpr is not None else None,
            "fnr_at_0.5": float(fnr) if fnr is not None else None,
            "n_triggers": int(n_triggers),
            "versions": [int(v) for v in versions],
        })
        print(f"  {f.name}: AUC={auc:.4f}, FPR={fpr*100:.2f}%, triggers={n_triggers}")
    
    runs_df = pd.DataFrame(runs)
    
    # 汇总统计
    print(f"\n汇总 ({len(runs)} runs):")
    summary = {
        "n_runs": len(runs),
        "auc": {
            "median": float(runs_df["auc"].median()),
            "mean": float(runs_df["auc"].mean()),
            "std": float(runs_df["auc"].std()),
            "min": float(runs_df["auc"].min()),
            "max": float(runs_df["auc"].max()),
        },
        "fpr": {
            "median": float(runs_df["fpr_at_0.5"].median()),
            "mean": float(runs_df["fpr_at_0.5"].mean()),
            "std": float(runs_df["fpr_at_0.5"].std()),
        },
        "fnr": {
            "median": float(runs_df["fnr_at_0.5"].median()),
            "mean": float(runs_df["fnr_at_0.5"].mean()),
            "std": float(runs_df["fnr_at_0.5"].std()),
        },
        "trigger_count": {
            "n_runs_triggered": int((runs_df["n_triggers"] > 0).sum()),
            "trigger_rate": float((runs_df["n_triggers"] > 0).mean()),
            "median_triggers": int(runs_df["n_triggers"].median()),
        },
    }
    
    print(f"  AUC : median={summary['auc']['median']:.4f}, "
          f"mean={summary['auc']['mean']:.4f}, std={summary['auc']['std']:.4f}")
    print(f"  FPR : median={summary['fpr']['median']*100:.2f}%, "
          f"std={summary['fpr']['std']*100:.2f}%")
    print(f"  FNR : median={summary['fnr']['median']*100:.2f}%, "
          f"std={summary['fnr']['std']*100:.2f}%")
    print(f"  触发: {summary['trigger_count']['n_runs_triggered']}/{len(runs)} runs 触发, "
          f"trigger_rate={summary['trigger_count']['trigger_rate']*100:.1f}%")
    
    if args.out:
        out = {"mode": "stationary", "summary": summary, "runs": runs}
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2)
        print(f"\nJSON 写入: {args.out}")
    
    if args.out_csv:
        runs_df.to_csv(args.out_csv, index=False)
        # 加一行汇总
        with open(args.out_csv, "a") as f:
            f.write(f"\n# SUMMARY\n")
            f.write(f"# AUC median={summary['auc']['median']:.4f} std={summary['auc']['std']:.4f}\n")
            f.write(f"# FPR median={summary['fpr']['median']*100:.2f}% std={summary['fpr']['std']*100:.2f}%\n")
            f.write(f"# trigger_rate={summary['trigger_count']['trigger_rate']*100:.1f}%\n")
        print(f"CSV 写入: {args.out_csv}")
    
    return summary


# ============================================================
# Mode 3: throughput -- Prometheus + 业务延迟
# ============================================================

def query_prometheus(prom_url, query, start_ts, end_ts, step="15s"):
    """从 Prometheus 拉时间序列数据."""
    import requests
    url = f"{prom_url.rstrip('/')}/api/v1/query_range"
    params = {
        "query": query,
        "start": start_ts,
        "end": end_ts,
        "step": step,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus error: {data}")
    return data["data"]["result"]


def mode_throughput(args):
    """从 Prometheus 拉吞吐 + 端到端业务延迟."""
    print("=" * 64)
    print(f"模式 throughput")
    print("=" * 64)
    
    result = {"mode": "throughput"}
    
    # Part 1: Prometheus 吞吐量
    if args.prometheus:
        print(f"\n[1] 拉取 Prometheus 吞吐数据 ({args.start} -> {args.end})")
        # LocalProcessor numRecordsIn (Flink 1.13 默认 metric)
        query = ('rate(flink_taskmanager_job_task_operator_numRecordsIn'
                 '{operator_name=~".*Local Processor.*"}[1m])')
        try:
            series = query_prometheus(args.prometheus, query, args.start, args.end)
            print(f"  {len(series)} 个时间序列")
            
            # 合并所有 subtask 的 numRecordsIn (平均, 因为 keyBy 均匀分发)
            # 取每个时间点所有 subtask 之和 = 系统总吞吐
            all_points = defaultdict(list)
            for s in series:
                for t, v in s["values"]:
                    all_points[float(t)].append(float(v))
            
            # 每个时间点求和 (4 subtasks 总吞吐)
            timestamps = sorted(all_points.keys())
            throughput = [sum(all_points[t]) for t in timestamps]
            
            stats = {
                "avg_throughput_rps": float(np.mean(throughput)),
                "median_throughput_rps": float(np.median(throughput)),
                "p95_throughput_rps": float(np.percentile(throughput, 95)),
                "min_throughput_rps": float(np.min(throughput)),
                "max_throughput_rps": float(np.max(throughput)),
                "duration_seconds": float(timestamps[-1] - timestamps[0]) if len(timestamps) > 1 else 0,
                "n_samples": len(throughput),
            }
            print(f"  avg throughput: {stats['avg_throughput_rps']:.1f} records/sec")
            print(f"  p95 throughput: {stats['p95_throughput_rps']:.1f} records/sec")
            print(f"  duration: {stats['duration_seconds']:.1f} seconds")
            result["throughput"] = stats
        except Exception as e:
            print(f"  ERROR: {e}")
            result["throughput"] = {"error": str(e)}
    
    # Part 2: 端到端业务延迟 (从 scores.jsonl 的 ingestionTime/scoreTime)
    if args.scores:
        print(f"\n[2] 端到端业务延迟 (从 {args.scores})")
        df = load_scores(args.scores)
        
        # 检查是否有时间戳字段
        if "ingestionTime" not in df.columns or "scoreTime" not in df.columns:
            print(f"  WARN: scores.jsonl 缺少 ingestionTime/scoreTime 字段 - 跳过业务延迟分析")
            print(f"  提示: 在 ScoreResult 中加 ingestionTime (KafkaRecord timestamp) 和 scoreTime (打分时刻) 字段")
            result["latency"] = None
        else:
            latencies_ms = (df["scoreTime"] - df["ingestionTime"]).astype(float)
            # 过滤明显错误 (负值、时钟漂移)
            latencies_ms = latencies_ms[(latencies_ms >= 0) & (latencies_ms < 60000)]
            
            stats = {
                "n_records": len(latencies_ms),
                "mean_ms": float(latencies_ms.mean()),
                "median_ms": float(latencies_ms.median()),
                "p50_ms": float(np.percentile(latencies_ms, 50)),
                "p95_ms": float(np.percentile(latencies_ms, 95)),
                "p99_ms": float(np.percentile(latencies_ms, 99)),
                "p999_ms": float(np.percentile(latencies_ms, 99.9)),
                "max_ms": float(latencies_ms.max()),
            }
            print(f"  n: {stats['n_records']}")
            print(f"  median: {stats['median_ms']:.2f} ms")
            print(f"  P95:    {stats['p95_ms']:.2f} ms")
            print(f"  P99:    {stats['p99_ms']:.2f} ms")
            print(f"  P99.9:  {stats['p999_ms']:.2f} ms")
            print(f"  max:    {stats['max_ms']:.2f} ms")
            result["latency"] = stats
    
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"\n报告写入: {args.out}")
    
    return result


# ============================================================
# Mode 4: scalability -- 合并多个 parallelism 的结果
# ============================================================

def mode_scalability(args):
    """合并多个 parallelism 的实验结果, 画扩展性曲线."""
    runs_dir = Path(args.runs_dir)
    
    print("=" * 64)
    print(f"模式 scalability: {runs_dir}")
    print("=" * 64)
    
    # 期望目录结构:
    #   runs/scalability/
    #     p1/throughput.json
    #     p2/throughput.json
    #     p4/throughput.json
    #     p6/throughput.json
    
    rows = []
    for p_dir in sorted(runs_dir.iterdir()):
        if not p_dir.is_dir() or not p_dir.name.startswith("p"):
            continue
        try:
            parallelism = int(p_dir.name[1:])
        except ValueError:
            print(f"  跳过 {p_dir.name} (无法解析 parallelism)")
            continue
        
        tp_file = p_dir / "throughput.json"
        if not tp_file.exists():
            print(f"  WARN: {tp_file} 不存在")
            continue
        with open(tp_file) as f:
            data = json.load(f)
        
        tp = data.get("throughput", {})
        lat = data.get("latency", {})
        
        row = {
            "parallelism": parallelism,
            "avg_throughput_rps": tp.get("avg_throughput_rps"),
            "p95_throughput_rps": tp.get("p95_throughput_rps"),
            "median_latency_ms": lat.get("median_ms") if lat else None,
            "p95_latency_ms": lat.get("p95_ms") if lat else None,
            "p99_latency_ms": lat.get("p99_ms") if lat else None,
        }
        rows.append(row)
        print(f"  p{parallelism}: throughput={row['avg_throughput_rps']:.1f} rps")
    
    if not rows:
        print(f"ERROR: 没有找到任何 parallelism 数据")
        return None
    
    df = pd.DataFrame(rows).sort_values("parallelism")
    
    # 计算扩展比 (相对 p=1)
    base = df[df["parallelism"] == 1]
    if len(base) > 0:
        base_tp = base["avg_throughput_rps"].iloc[0]
        df["speedup"] = df["avg_throughput_rps"] / base_tp
        df["efficiency"] = df["speedup"] / df["parallelism"]
        print(f"\n扩展性 (相对 p=1):")
        for _, row in df.iterrows():
            print(f"  p{row['parallelism']}: throughput={row['avg_throughput_rps']:.1f} rps, "
                  f"speedup={row['speedup']:.2f}x, efficiency={row['efficiency']*100:.1f}%")
    
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)
        print(f"\nCSV 写入: {args.out_csv}")
    
    if args.out and args.out.endswith((".png", ".pdf", ".svg")):
        plt = _lazy_import_matplotlib()
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        
        # 子图 1: 吞吐量
        ax1.plot(df["parallelism"], df["avg_throughput_rps"], 
                marker="o", linewidth=2, label="实测")
        if len(base) > 0:
            ideal = [base_tp * p for p in df["parallelism"]]
            ax1.plot(df["parallelism"], ideal, 
                    linestyle="--", color="gray", label="理想线性扩展")
        ax1.set_xlabel("Parallelism")
        ax1.set_ylabel("Throughput (records/sec)")
        ax1.set_title("吞吐量随并行度扩展")
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 子图 2: 效率
        if "efficiency" in df.columns:
            ax2.plot(df["parallelism"], df["efficiency"] * 100, 
                    marker="s", color="orange", linewidth=2)
            ax2.axhline(100, linestyle="--", color="gray", alpha=0.5, label="理想 100%")
            ax2.set_xlabel("Parallelism")
            ax2.set_ylabel("Efficiency (%)")
            ax2.set_title("并行效率 (speedup/parallelism)")
            ax2.set_ylim(0, 110)
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(args.out, dpi=150, bbox_inches="tight")
        print(f"图表写入: {args.out}")
    
    return df.to_dict(orient="records")


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", required=True,
                       choices=["drift", "stationary", "throughput", "scalability"],
                       help="分析模式")
    
    # drift 模式参数
    parser.add_argument("--scores", help="[drift/throughput] scores.jsonl 路径")
    parser.add_argument("--window", type=int, default=2000,
                       help="[drift] 滑动窗口大小 (default 2000)")
    parser.add_argument("--driftspec", help="[drift] driftspec.json 路径 (SEA 数据集)")
    parser.add_argument("--drift-point", type=int, 
                       help="[drift] 手动指定漂移点 (无 driftspec 时)")
    parser.add_argument("--window-csv", help="[drift] 滑动窗口数据导出 CSV")
    
    # stationary 模式参数
    parser.add_argument("--scores-dir", help="[stationary] N 次实验的 scores 目录")
    
    # throughput 模式参数
    parser.add_argument("--prometheus", help="[throughput] Prometheus URL (e.g. http://master:9090)")
    parser.add_argument("--start", help="[throughput] 起始时间 (ISO 8601, e.g. 2026-05-21T09:00:00Z)")
    parser.add_argument("--end", help="[throughput] 结束时间")
    
    # scalability 模式参数
    parser.add_argument("--runs-dir", help="[scalability] 包含 p1/p2/p4/... 的目录")
    
    # 通用输出参数
    parser.add_argument("--out", help="JSON 输出路径")
    parser.add_argument("--out-csv", help="CSV 输出路径 (stationary/scalability)")
    
    args = parser.parse_args()
    
    if args.mode == "drift":
        if not args.scores:
            parser.error("--mode drift 需要 --scores")
        return mode_drift(args)
    elif args.mode == "stationary":
        if not args.scores_dir:
            parser.error("--mode stationary 需要 --scores-dir")
        return mode_stationary(args)
    elif args.mode == "throughput":
        if not args.prometheus and not args.scores:
            parser.error("--mode throughput 至少需要 --prometheus 或 --scores")
        return mode_throughput(args)
    elif args.mode == "scalability":
        if not args.runs_dir:
            parser.error("--mode scalability 需要 --runs-dir")
        return mode_scalability(args)


if __name__ == "__main__":
    main()
