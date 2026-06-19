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
  python analyze_old.py --mode drift \\
      --scores scores-sudden-old.jsonl \\
      --window 2000 \\
      --driftspec synth_abrupt.driftspec.json \\
      --out drift_report.json

  # 2. 批处理 30 次 stationary 实验
  python analyze_old.py --mode stationary \\
      --scores-dir runs/donors/ \\
      --out donors_summary.json \\
      --out-csv donors_summary.csv

  # 3. 吞吐 + 业务延迟
  python analyze_old.py --mode throughput \\
      --prometheus http://master:9090 \\
      --start "2026-05-21T09:00:00Z" \\
      --end "2026-05-21T11:00:00Z" \\
      --scores scores.jsonl \\
      --out throughput_report.json

  # 4. 可扩展性
  python analyze_old.py --mode scalability \\
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
    """找在 target_fpr 时的阈值. 返回 (阈值, 实际 FPR, FNR).

    向量化版 (一次排序 + searchsorted), 等价于原"升序遍历候选阈值"循环:
    原循环升序扫候选, 记 |fpr-target| 最小者, 一旦 fpr<=target 即 break.
    这里用 searchsorted 一次算出所有候选的 fpr/fnr, 再在"首个 fpr<=target"
    截断范围内取 |fpr-target| 最小. 数值与原逻辑一致, 复杂度 O(n log n).
    """
    y = np.asarray(y_true)
    s = np.asarray(scores)
    n_pos = int((y == 1).sum())
    n_neg = int((y == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return None, None, None
    candidates = np.sort(np.unique(s))         # 升序候选阈值 (同原)
    neg_sorted = np.sort(s[y == 0])
    pos_sorted = np.sort(s[y == 1])
    # compute_fpr_fnr 用 pred = (s >= t): FPR = #(neg >= t)/n_neg
    #   #(neg >= t) = n_neg - #(neg < t);  searchsorted side="left" = 严格小于
    fpr_arr = (n_neg - np.searchsorted(neg_sorted, candidates, side="left")) / n_neg
    # FNR = #(pos < t)/n_pos  (pred=0 即 s<t 的正样本)
    fnr_arr = np.searchsorted(pos_sorted, candidates, side="left") / n_pos
    # 原逻辑: 升序遇到首个 fpr<=target 就 break → 在 [0, cutoff] 内找最优
    le = np.where(fpr_arr <= target_fpr)[0]
    end = (le[0] + 1) if len(le) > 0 else len(candidates)
    diffs = np.abs(fpr_arr[:end] - target_fpr)
    if len(diffs) == 0:
        return None, None, None
    k = int(np.argmin(diffs))                  # 同原: 第一个达到最小 diff 的位置
    return float(candidates[k]), float(fpr_arr[k]), float(fnr_arr[k])


def find_optimal_threshold(y_true, scores):
    """用 Youden's J statistic (TPR - FPR) 找最优阈值. 向量化版."""
    y = np.asarray(y_true)
    s = np.asarray(scores)
    n_pos = int((y == 1).sum())
    n_neg = int((y == 0).sum())
    if n_pos == 0 or n_neg == 0:
        return None, None
    candidates = np.linspace(s.min(), s.max(), 100)   # 同原: 100 个等距候选
    neg_sorted = np.sort(s[y == 0])
    pos_sorted = np.sort(s[y == 1])
    fpr_arr = (n_neg - np.searchsorted(neg_sorted, candidates, side="left")) / n_neg
    fnr_arr = np.searchsorted(pos_sorted, candidates, side="left") / n_pos
    tpr_arr = 1 - fnr_arr
    j_arr = tpr_arr - fpr_arr
    # 原循环: j > best_j 才更新 → 取第一个最大 J (argmax 返回首个最大值索引, 一致)
    k = int(np.argmax(j_arr))
    return float(candidates[k]), float(fpr_arr[k])


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
        if len(drift_events) > 1:
            print(f"\n[3] 漂移点 seq={drift_point} 前后对比 (单点参考: 仅反映第 1 次漂移; "
                  f"完整多漂移分析见 [5])")
        else:
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
        
        # [5] 漂移响应延迟 (多漂移点支持: 对 drift_events 中每个点分别算 def1/2/3)
        # 单漂移退化: drift_events 只有 1 项时, 每个 list 仅含 1 个元素, 语义与旧版等价
        # 注: pre_v1 (forestVersion==1 且 seq<drift_events[0]) 作为所有漂移点共用的全局
        #     基线 — 即系统初始稳定段, 用于 def2 的 AUC 基线和 def3 的 FPR 基线。
        # 注: 每个漂移点 i 的滑窗扫描上界 = 下一个漂移点 (最后一个扫到 n), 避免扫过
        #     下一个漂移导致指标被污染。
        delays = {
            "definition_1_first_new_version": [],
            "definition_2_auc_recovery": [],
            "definition_3_fpr_stable": [],
        }

        # ===== 向量化预处理 (一次性, 供 def2/3 所有漂移点的滑窗用) =====
        # seq 在 Flink 并行下无序, searchsorted 需有序; AUC/FPR 不依赖行序, sort 不改数值
        _sorted = df.sort_values("seq")
        _seq = _sorted["seq"].to_numpy()
        _label = _sorted["label"].to_numpy()
        _score = _sorted["score"].to_numpy()
        win = 1000  # 滑窗大小

        # 漂移点列表 (与 drift_events 一一对应) + 每个漂移点的扫描上界
        dp_list = [int(e["start_line"]) for e in drift_events] if drift_events else [drift_point]
        dp_upper = [dp_list[i+1] if i+1 < len(dp_list) else n for i in range(len(dp_list))]

        # 漂移前基线 (全局, 所有漂移点共用): forestVersion==1 且 seq < 第一个漂移点
        baseline_auc = None
        baseline_fpr = None
        if len(pre_v1) > 0:
            baseline_auc = compute_auc(pre_v1["label"], pre_v1["score"])
            baseline_fpr, _ = compute_fpr_fnr(pre_v1["label"], pre_v1["score"], 0.5)

        print(f"\n[5] 漂移响应延迟 (共 {len(dp_list)} 个漂移点)")
        if baseline_auc is not None:
            print(f"    全局基线 AUC: {baseline_auc:.4f}, 全局基线 FPR@0.5: {baseline_fpr*100:.1f}%")

        # 漂移#i 之前(即 seq < dp_i 范围内)的最大 forestVersion, 用于 def1 判定"新版本"
        # 单漂移下: dp_0 之前全是 v1, 该值=1, 等价于原 forestVersion>1 判据
        for i, dp in enumerate(dp_list):
            upper = dp_upper[i]
            ver_before = int(df[df["seq"] < dp]["forestVersion"].max()) if (df["seq"] < dp).any() else 1
            print(f"\n  --- 漂移 #{i} @ seq={dp} (扫描上界 seq={upper}, ver_before={ver_before}) ---")

            # 定义 1: 漂移区间 [dp, upper) 内首次出现比 ver_before 更新的森林版本
            seg_new_ver = df[(df["seq"] >= dp) & (df["seq"] < upper) & (df["forestVersion"] > ver_before)]
            if len(seg_new_ver) > 0:
                first_new = int(seg_new_ver["seq"].min())
                delay_1 = first_new - dp
                print(f"    定义1 (首次出现 ver>{ver_before}): @ seq={first_new}, 延迟 {delay_1} 条")
                delays["definition_1_first_new_version"].append({
                    "drift_idx": i, "drift_point": dp,
                    "first_new_version_seq": first_new, "delay_records": delay_1,
                })
            else:
                print(f"    定义1: 区间内未产生新版本 (检测面未触发或 COOLDOWN 未完成)")
                delays["definition_1_first_new_version"].append({
                    "drift_idx": i, "drift_point": dp, "first_new_version_seq": None, "delay_records": None,
                })

            # 定义 2: AUC 回到全局基线 95% (仅在 [dp, upper) 内扫)
            if baseline_auc is not None:
                target = baseline_auc * 0.95
                recovery_seq = None
                for start in range(dp, upper - win, 100):
                    lo = np.searchsorted(_seq, start, side="left")
                    hi = np.searchsorted(_seq, start + win, side="left")
                    if hi - lo < 100:
                        continue
                    w_auc = compute_auc(_label[lo:hi], _score[lo:hi])
                    if w_auc and w_auc >= target:
                        recovery_seq = start
                        break
                if recovery_seq:
                    delay_2 = recovery_seq - dp
                    print(f"    定义2 (AUC>={target:.4f}): @ seq={recovery_seq}, 延迟 {delay_2} 条")
                    delays["definition_2_auc_recovery"].append({
                        "drift_idx": i, "drift_point": dp,
                        "baseline_auc": baseline_auc, "target_auc": target,
                        "recovery_seq": recovery_seq, "delay_records": delay_2,
                    })
                else:
                    print(f"    定义2: 区间内未恢复到 95% 基线")
                    delays["definition_2_auc_recovery"].append({
                        "drift_idx": i, "drift_point": dp,
                        "baseline_auc": baseline_auc, "target_auc": target,
                        "recovery_seq": None, "delay_records": None,
                    })

            # 定义 3: FPR <= 10% (仅在 [dp, upper) 内扫)
            if baseline_fpr is not None:
                peak_fpr = 0
                stable_seq = None
                for start in range(dp, upper - win, 100):
                    lo = np.searchsorted(_seq, start, side="left")
                    hi = np.searchsorted(_seq, start + win, side="left")
                    if hi - lo < 100:
                        continue
                    fpr, _ = compute_fpr_fnr(_label[lo:hi], _score[lo:hi], 0.5)
                    if fpr is None:
                        continue
                    peak_fpr = max(peak_fpr, fpr)
                    if fpr <= 0.10 and stable_seq is None:
                        stable_seq = start
                        break
                if stable_seq:
                    delay_3 = stable_seq - dp
                    print(f"    定义3 (FPR<=10%): @ seq={stable_seq}, 延迟 {delay_3} 条, 峰值 FPR {peak_fpr*100:.1f}%")
                    delays["definition_3_fpr_stable"].append({
                        "drift_idx": i, "drift_point": dp,
                        "peak_fpr": peak_fpr, "stable_seq": stable_seq, "delay_records": delay_3,
                    })
                else:
                    print(f"    定义3: 区间内未稳定到 <=10%, 峰值 FPR {peak_fpr*100:.1f}%")
                    delays["definition_3_fpr_stable"].append({
                        "drift_idx": i, "drift_point": dp,
                        "peak_fpr": peak_fpr, "stable_seq": None, "delay_records": None,
                    })

        result["delays"] = delays

        # [5.5] EXP1 顶级 headline 字段 (方向二(a) Phase 3 附录 D)
        # 把已算好的散落字段提升到顶层,让 analyze-all.sh / dilution-summary 直接读
        # n_retrains = 重训次数 = 出现的 forestVersion 数 - 1 (初始 v1 不算触发)
        # delta_post_auc > 0 → 重训改善 stale-baseline;≤ 0 → 池子拖后腿 (Q3 试金石)
        result["n_retrains"] = max_ver - 1
        if "漂移后 v1 (旧森林)" in segments:
            result["post_v1_auc"] = segments["漂移后 v1 (旧森林)"]["auc"]
        if f"漂移后 v{max_ver} (最终版本)" in segments:
            result["post_final_auc"] = segments[f"漂移后 v{max_ver} (最终版本)"]["auc"]
        if result.get("post_v1_auc") is not None and result.get("post_final_auc") is not None:
            result["delta_post_auc"] = result["post_final_auc"] - result["post_v1_auc"]
        # recovery_latency:第一个漂移点的 def2 (AUC>=95% 基线) 延迟;None 表未恢复
        d2 = delays.get("definition_2_auc_recovery", [])
        if d2:
            result["recovery_latency"] = d2[0].get("delay_records")
            result["recovered"] = d2[0].get("recovery_seq") is not None
    
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


# 扩展性三口径算子吞吐(handover v1.0-exp3 §0.3):
#   source    端到端吞吐,受 source 单分区单线限制 → 暴露端到端瓶颈
#   detection 检测面算子吞吐(Per-Feature IKS / HDDM_W),随并行度上升
#   scoring   打分面算子吞吐(Local Processor),随并行度上升
# 重要:operator_name label 实际值可能经 Prometheus 转义/截断,过滤式须在集群
# Prometheus UI 实测校准(查 flink_taskmanager_job_task_operator_numRecordsIn 的
# operator_name 取值),不可仅凭 LocalProcessor.java 的算子名假设(handover §3)。
THROUGHPUT_OPERATOR_FILTERS = {
    "source":    ".*Kafka Source.*",
    "detection": ".*Per-Feature.*",
    "scoring":   ".*Local Processor.*",
}


def _aggregate_throughput(series):
    """把某算子 numRecordsIn rate 的多 subtask 序列按时间点求和 = 该算子总吞吐。
    返回 stats dict;无匹配序列时返回 None(供上层标注"过滤式未命中")。"""
    if not series:
        return None
    all_points = defaultdict(list)
    for s in series:
        for t, v in s["values"]:
            all_points[float(t)].append(float(v))
    if not all_points:
        return None
    timestamps = sorted(all_points.keys())
    throughput = [sum(all_points[t]) for t in timestamps]
    return {
        "avg_throughput_rps": float(np.mean(throughput)),
        "median_throughput_rps": float(np.median(throughput)),
        "p95_throughput_rps": float(np.percentile(throughput, 95)),
        "min_throughput_rps": float(np.min(throughput)),
        "max_throughput_rps": float(np.max(throughput)),
        "duration_seconds": float(timestamps[-1] - timestamps[0]) if len(timestamps) > 1 else 0,
        "n_samples": len(throughput),
        "n_series": len(series),
    }


def mode_throughput(args):
    """从 Prometheus 拉吞吐 + 端到端业务延迟."""
    print("=" * 64)
    print(f"模式 throughput")
    print("=" * 64)
    
    result = {"mode": "throughput"}
    
    # Part 1: Prometheus 吞吐量
    if args.prometheus:
        print(f"\n[1] 拉取 Prometheus 三口径吞吐 ({args.start} -> {args.end})")
        # 三口径(source 端到端 / detection 检测面 / scoring 打分面)各拉一条 numRecordsIn rate;
        # 每口径把所有 subtask 序列按时间点求和 = 该算子总吞吐。
        tp_by_operator = {}
        for kpi, opfilter in THROUGHPUT_OPERATOR_FILTERS.items():
            query = ('rate(flink_taskmanager_job_task_operator_numRecordsIn'
                     '{operator_name=~"' + opfilter + '"}[1m])')
            try:
                series = query_prometheus(args.prometheus, query, args.start, args.end)
                stats = _aggregate_throughput(series)
                if stats is None:
                    print(f"  [{kpi:9}] 无匹配序列 — 校准 operator_name 过滤式: {opfilter}")
                    tp_by_operator[kpi] = {"error": "no series matched", "filter": opfilter}
                else:
                    print(f"  [{kpi:9}] avg={stats['avg_throughput_rps']:.1f} rps, "
                          f"p95={stats['p95_throughput_rps']:.1f} rps "
                          f"({stats['n_series']} series, {stats['duration_seconds']:.0f}s)")
                    tp_by_operator[kpi] = stats
            except Exception as e:
                print(f"  [{kpi:9}] ERROR: {e}")
                tp_by_operator[kpi] = {"error": str(e), "filter": opfilter}
        result["throughput"] = tp_by_operator
    
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
    
    KPIS = ["source", "detection", "scoring"]

    def _kpi_avg(tp, kpi):
        """取某口径 avg_throughput_rps;兼容旧单口径(flat)throughput.json → 视作 scoring。"""
        if isinstance(tp, dict) and "avg_throughput_rps" in tp:
            return tp.get("avg_throughput_rps") if kpi == "scoring" else None
        return ((tp or {}).get(kpi, {}) or {}).get("avg_throughput_rps")

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

        row = {"parallelism": parallelism}
        for kpi in KPIS:
            row[f"{kpi}_avg_rps"] = _kpi_avg(tp, kpi)
        row["median_latency_ms"] = lat.get("median_ms") if lat else None
        row["p95_latency_ms"] = lat.get("p95_ms") if lat else None
        row["p99_latency_ms"] = lat.get("p99_ms") if lat else None
        rows.append(row)
        tp_str = ", ".join(
            f"{kpi}={row[f'{kpi}_avg_rps']:.1f}" if row[f"{kpi}_avg_rps"] is not None else f"{kpi}=NA"
            for kpi in KPIS)
        print(f"  p{parallelism}: {tp_str} rps")

    if not rows:
        print(f"ERROR: 没有找到任何 parallelism 数据")
        return None

    df = pd.DataFrame(rows).sort_values("parallelism")

    # 每口径相对 p=1 的 speedup(source 受单线限制预期接近持平;detection/scoring 随 P 上升)
    base = df[df["parallelism"] == 1]
    if len(base) > 0:
        for kpi in KPIS:
            col = f"{kpi}_avg_rps"
            base_v = base[col].iloc[0]
            df[f"{kpi}_speedup"] = (df[col] / base_v) if (base_v and not pd.isna(base_v)) else float("nan")
        print(f"\n扩展性 speedup (相对 p=1,三口径):")
        for _, r in df.iterrows():
            parts = ", ".join(
                f"{kpi} {r[f'{kpi}_speedup']:.2f}x" if not pd.isna(r.get(f"{kpi}_speedup", float('nan'))) else f"{kpi} NA"
                for kpi in KPIS)
            print(f"  p{int(r['parallelism'])}: {parts}")

    if args.out_csv:
        df.to_csv(args.out_csv, index=False)
        print(f"\nCSV 写入: {args.out_csv}")

    if args.out and args.out.endswith((".png", ".pdf", ".svg")):
        plt = _lazy_import_matplotlib()
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))
        markers = {"source": "^", "detection": "o", "scoring": "s"}

        # 子图 1: 三口径吞吐
        for kpi in KPIS:
            col = f"{kpi}_avg_rps"
            if df[col].notna().any():
                ax1.plot(df["parallelism"], df[col], marker=markers[kpi], linewidth=2, label=kpi)
        ax1.set_xlabel("Parallelism")
        ax1.set_ylabel("Throughput (records/sec)")
        ax1.set_title("三口径吞吐随并行度")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 子图 2: 三口径 speedup + 理想线性
        if len(base) > 0:
            for kpi in KPIS:
                col = f"{kpi}_speedup"
                if col in df.columns and df[col].notna().any():
                    ax2.plot(df["parallelism"], df[col], marker=markers[kpi], linewidth=2, label=kpi)
            ax2.plot(df["parallelism"], df["parallelism"], linestyle="--", color="gray", label="理想线性")
            ax2.set_xlabel("Parallelism")
            ax2.set_ylabel("Speedup (× over p=1)")
            ax2.set_title("三口径 speedup")
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
