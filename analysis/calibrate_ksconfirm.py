#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
离线标定 ksConfirm(峰值-KS 确认门)—— 本地跑,不碰部署系统。
Offline calibration of the peak-KS confirmation gate.

方法 / Method
------------
对每个特征跑 per-feature IKSSW(与 de-risk / 部署检测面同一套,W=2000, p=0.001 → ca≈1.858)。
模拟"改进后的门":KS 一旦过 ca,**先不 rebase**,在确认窗 C 内继续滑动、记录峰值 KS;
确认窗结束时,峰值 ≥ ksConfirm 才算该特征"确认漂移"(否则丢弃),无论确认/丢弃都 rebase。
聚合:aggWin 内有 ≥ K 个特征确认 → 一次 COMMITTED(之后 refractory R 去抖)。

关键效率:rebase 时机只由 C 决定、与 ksConfirm 无关 → 固定 C 重放一次记下每次过阈峰值,
再廉价扫 ksConfirm。

输出 / Output
------------
ksconfirm_sweep.csv:每个 (C, ksConfirm) 的 recall / precision / n_committed。
并打印推荐值:在 recall==1.0(5 个标注漂移全保)前提下、n_committed 最小的最大 ksConfirm。

依赖:同目录需有 IKS.py / IKSSW.py / Treap.py(项目里的参考实现)。
运行:python3 calibrate_ksconfirm.py
"""
import csv
import os
import sys
import numpy as np
from multiprocessing import Pool

# IKSSW 参考实现需在 sys.path 上(与本脚本同目录,或 PYTHONPATH 指向)
from IKS import IKS
from IKSSW import IKSSW

# ======================= 配置(只改这块) =======================
DATA_CSV       = "/Users/lijing/developer/FA-IForest/data/insects/INSECTS_abrupt_imbalanced_transformed.csv"  # ← INSECTS 特征 CSV 路径
LABEL_COL      = -1            # 标签列索引(将被排除,不参与检测);若无标签列设 None
FEATURE_COLS   = None         # None=除 LABEL_COL 外全部列;或给显式列表如 list(range(33))
HAS_HEADER     = True        # CSV 是否有表头

DRIFT_POINTS   = [51255, 74381, 100944, 142291, 152207]  # 5 个标注漂移点(seq)
W              = 2000         # IKS 窗口
P_VALUE        = 0.001        # ca = sqrt(-0.5 ln p) ≈ 1.858
K              = 2            # aggK:同时确认的特征数门
AGG_WIN        = 2000         # 聚合窗:多少 seq 内的多特征确认算"共发"
REFRACTORY     = 4000         # 去抖:一次 COMMITTED 后多少 seq 内不再发(≈重训周期)

C_GRID         = [500]        # 确认窗:先固定 1 个标定 ksConfirm(扫 C 再加,每多 1 个慢一倍)
KSCONFIRM_GRID = [round(x, 3) for x in np.arange(0.00, 0.26, 0.01)]  # 峰值门候选(扫这个不增加重放成本)
NUM_WORKERS    = max(1, (os.cpu_count() or 4))  # 并行进程数:33 个特征的 IKSSW 互相独立,可并行

MATCH_BEFORE   = W            # 命中判定:漂移点前 W 也算(检测略早)
MATCH_AFTER    = 2 * W        # 漂移点后 2W 内算命中(检测有滞后)
MAKE_PLOT      = True         # 有 matplotlib 则画 recall/n_committed vs ksConfirm
# ===============================================================


def load_features(path):
    """读 CSV → (N, d) 特征矩阵(排除标签列)。"""
    rows = []
    with open(path, newline="") as f:
        rdr = csv.reader(f)
        if HAS_HEADER:
            next(rdr, None)
        for r in rdr:
            if r:
                rows.append(r)
    arr = np.array(rows, dtype=float)
    ncol = arr.shape[1]
    if FEATURE_COLS is not None:
        cols = FEATURE_COLS
    else:
        lc = (LABEL_COL % ncol) if LABEL_COL is not None else None
        cols = [c for c in range(ncol) if c != lc]
    return arr[:, cols]


def replay_feature(col, ca, C):
    """单特征重放,返回该特征所有'过阈事件'的 (crossing_seq, peak_KS)。
    模拟:过 ca → 进确认窗(不 rebase)C 步,记峰值;窗满 → 记录事件 + rebase。"""
    n = len(col)
    if n <= W:
        return []
    ikssw = IKSSW(list(col[:W]))
    events = []
    state_confirming = False
    peak = 0.0
    cc = 0
    cross_seq = 0
    for t in range(W, n):
        ikssw.Increment(float(col[t]))
        d = ikssw.KS()
        if not state_confirming:
            if ikssw.Test(ca):           # KS 过阈,进确认窗
                state_confirming = True
                peak = d
                cc = 0
                cross_seq = t
        else:
            if d > peak:
                peak = d
                cc += 1
            else:
                cc += 1
            if cc >= C:                  # 确认窗满 → 记录峰值 + rebase
                events.append((cross_seq, peak))
                ikssw.Update()           # rebase:参考窗 ← 当前滑窗
                state_confirming = False
                peak = 0.0
    return events


def aggregate(confirmed_seqs, K, agg_win, refractory):
    """confirmed_seqs: 已排序的确认事件 seq 列表(跨特征,同一 seq 可重复=不同特征)。
    返回 COMMITTED 的 seq 列表:agg_win 内有 ≥K 个确认 → 发,之后 refractory 去抖。"""
    confirmed_seqs = sorted(confirmed_seqs)
    committed = []
    last_fire = -10 ** 18
    i = 0
    n = len(confirmed_seqs)
    for j in range(n):
        s = confirmed_seqs[j]
        while confirmed_seqs[i] < s - agg_win:
            i += 1
        if (j - i + 1) >= K and s - last_fire > refractory:
            committed.append(s)
            last_fire = s
    return committed


def score(committed, drift_points, before, after):
    """recall=被命中的漂移点比例;precision=命中某漂移点的 COMMITTED 比例。"""
    hit_drift = 0
    for dp in drift_points:
        if any(dp - before <= c <= dp + after for c in committed):
            hit_drift += 1
    recall = hit_drift / len(drift_points) if drift_points else 0.0
    if committed:
        good = sum(1 for c in committed
                   if any(dp - before <= c <= dp + after for dp in drift_points))
        precision = good / len(committed)
    else:
        precision = 0.0
    return recall, precision


def main():
    print(f"[load] {DATA_CSV}")
    X = load_features(DATA_CSV)
    N, d = X.shape
    ca = IKS.CAForPValue(P_VALUE)
    print(f"[info] N={N}, features={d}, ca={ca:.4f}, drift_points={DRIFT_POINTS}")

    results = []  # (C, ksConfirm, recall, precision, n_committed)
    for C in C_GRID:
        print(f"[replay] C={C} —— {d} 个特征并行重放 IKSSW({NUM_WORKERS} 进程)...")
        # 每特征过阈事件:(crossing_seq, peak_KS);各特征独立,并行
        with Pool(NUM_WORKERS) as pool:
            per_feat = pool.starmap(
                replay_feature, [(X[:, fi], ca, C) for fi in range(d)])
        all_events = []  # (seq, peak)
        for ev in per_feat:
            all_events.extend(ev)
        # 廉价扫 ksConfirm
        for ksc in KSCONFIRM_GRID:
            confirmed = [seq for (seq, peak) in all_events if peak >= ksc]
            committed = aggregate(confirmed, K, AGG_WIN, REFRACTORY)
            recall, precision = score(committed, DRIFT_POINTS, MATCH_BEFORE, MATCH_AFTER)
            results.append((C, ksc, recall, precision, len(committed)))

    # 写 CSV
    out = "ksconfirm_sweep.csv"
    with open(out, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["C", "ksConfirm", "recall", "precision", "n_committed"])
        wr.writerows(results)
    print(f"[out] {out}")

    # 推荐:每个 C 下,recall==1.0 前提中 n_committed 最小、ksConfirm 最大者
    print("\n=== 推荐(每个 C:保全 5 漂移前提下的最优 ksConfirm)===")
    print(f"{'C':>6} {'ksConfirm':>10} {'recall':>7} {'precision':>10} {'n_committed':>12}")
    for C in C_GRID:
        cand = [r for r in results if r[0] == C and r[2] >= 0.999]
        if cand:
            # n_committed 最小;并列取 ksConfirm 最大(门越高越稳)
            best = sorted(cand, key=lambda r: (r[4], -r[1]))[0]
            print(f"{best[0]:>6} {best[1]:>10.3f} {best[2]:>7.2f} {best[3]:>10.3f} {best[4]:>12}")
        else:
            print(f"{C:>6} {'(无 recall=1.0 的阈值,放宽 ksConfirm 或检查数据)':>10}")

    if MAKE_PLOT:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            fig, axes = plt.subplots(1, len(C_GRID), figsize=(5 * len(C_GRID), 4), sharey=False)
            if len(C_GRID) == 1:
                axes = [axes]
            for ax, C in zip(axes, C_GRID):
                rs = [r for r in results if r[0] == C]
                xs = [r[1] for r in rs]
                rec = [r[2] for r in rs]
                ncom = [r[4] for r in rs]
                ax.plot(xs, rec, "o-", label="recall", color="tab:blue")
                ax.set_xlabel("ksConfirm"); ax.set_ylabel("recall", color="tab:blue")
                ax.set_ylim(-0.05, 1.05); ax.set_title(f"C={C}")
                ax2 = ax.twinx()
                ax2.plot(xs, ncom, "s--", label="n_committed", color="tab:red")
                ax2.set_ylabel("n_committed", color="tab:red")
                ax.axhline(1.0, color="gray", lw=0.6, ls=":")
            fig.tight_layout()
            fig.savefig("ksconfirm_sweep.png", dpi=130)
            print("[out] ksconfirm_sweep.png")
        except Exception as e:
            print(f"[plot] 跳过绘图({e})")


if __name__ == "__main__":
    main()
