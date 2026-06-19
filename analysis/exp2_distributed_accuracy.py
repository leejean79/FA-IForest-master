#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EXP2 分布式准确性分析 — stationary AUC × 并行度,对照 Leveni 2025(Online Isolation Forest)。

论文角色(核心贡献②"分布式异常检测算法"):
    证明 iForest 分布式化(列并行检测 + 分布式打分/重训)后,stationary 准确性随并行度
    保持。P=1 是"准单机基线",P=2/4 是分布式;若 AUC 随 P 不显著下降 → 分布式不牺牲准确性。
    P=1 的 AUC 直接对照 Leveni 2025 Table 4 的单机/在线 iForest 数字(数据集同源,口径可比)。

EXP2 配置:datasets=[donors,forestcover,fraud],detector=IKS,pauseMode=BACKLOG,
          P∈{1,2,4},10 shuffle;iForest totalTrees=100/subsample=256。

分析:
    1. 主表:每 (数据集 × 并行度) 的 stationary AUC(mean±std over 10 shuffle)。
    2. AUC vs 并行度:看 AUC 随 P 是否保持(分布式保持准确性的直接证据)。
    3. 对照 Leveni Table 4:把本系统 P=1 AUC 与 oIFOR/asdIFOR/RRCF/HST/LODA 并排。
    4. 次要:误触发率(forestVersion 变化数,沿用 stationary 无漂移理想=0 的口径)。

口径说明(写入论文):
    - AUC = 全流 label vs score 的 ROC AUC(与 Leveni 全流 ROC AUC 口径一致)。
    - 对照定位为"量级参考"(选项 A):本系统 100 树 vs Leveni oIFOR 32 树,树数不同,
      故不作严格 head-to-head,只论"落在同一 competitive 区间"。数据集同源使对照更可信。
    - 误触发口径 = distinct(forestVersion)−1(scores.jsonl 间接测),非 drift 事件直接计数。

CLI:
    python3 analysis/exp2_distributed_accuracy.py --results-dir results-local
    python3 analysis/exp2_distributed_accuracy.py --results-dir results-local --datasets donors fraud

输出(analysis/out/):
    exp2_accuracy_summary.csv   每 (数据集,并行度) 一行:auc_mean/std、误触发均值、run 数
    exp2_vs_leveni.csv          本系统 P=1 AUC 与 Leveni Table 4 五算法并排
    exp2_auc_vs_parallelism.csv AUC-vs-P(画曲线用)
    控制台 打印对照表
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"

DEFAULT_DATASETS = ["donors", "forestcover", "fraud"]

# Leveni 2025 (Online Isolation Forest) Table 4 — stationary ROC AUC。
# 仅录入 EXP2 三数据集;数值来自论文 Table 4(供对照,量级参考)。
LEVENI_TABLE4 = {
    "donors":      {"oIFOR": 0.795, "asdIFOR": 0.769, "HST": 0.715, "RRCF": 0.637, "LODA": 0.554},
    "forestcover": {"oIFOR": 0.887, "asdIFOR": 0.861, "HST": 0.722, "RRCF": 0.917, "LODA": 0.500},
    "fraud":       {"oIFOR": 0.936, "asdIFOR": 0.946, "HST": 0.910, "RRCF": 0.951, "LODA": 0.722},
}

# 合理区间(口径对齐自检:本系统 P=1 AUC 应落在各数据集的算法分布范围内)
SANITY_RANGE = {
    "donors": (0.55, 0.85), "forestcover": (0.50, 0.92), "fraud": (0.70, 0.96),
}

# exp_id 形如 donors_BACKLOG_THEN_NEW_FOREST_default_p2_r3_iksWindowSize-2000_...
EXP2_PAT = re.compile(r"^(?P<ds>.+?)_BACKLOG_THEN_NEW_FOREST_default_p(?P<p>\d+)_r(?P<r>\d+)")


def compute_auc(labels: np.ndarray, scores: np.ndarray) -> Optional[float]:
    """全流 ROC AUC(向量化 np.searchsorted,与项目 analyze.py 一致口径)。"""
    pos = scores[labels == 1]
    neg = scores[labels == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    neg_sorted = np.sort(neg)
    # 每个正样本 score 大于多少个负样本(含一半相等)
    left = np.searchsorted(neg_sorted, pos, side="left")
    right = np.searchsorted(neg_sorted, pos, side="right")
    wins = left + (right - left) * 0.5
    return float(wins.sum() / (len(pos) * len(neg)))


def parse_run(path: Path) -> Optional[Dict]:
    rows = []  # (seq, label, score, version)
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rows.append((
                    int(r.get("seq", len(rows))),
                    int(r.get("label", 0)),
                    float(r.get("score", 0.0)),
                    r.get("forestVersion"),
                ))
    except FileNotFoundError:
        return None
    if not rows:
        return None
    rows.sort(key=lambda x: x[0])  # 按 seq
    labels = np.array([r[1] for r in rows])
    scores = np.array([r[2] for r in rows])
    versions = [r[3] for r in rows]
    distinct_vers = sorted({v for v in versions if v is not None})
    auc = compute_auc(labels, scores)

    # per-version AUC:每个 forestVersion 段内的 AUC
    per_version = {}
    for v in distinct_vers:
        idx = np.array([i for i, vv in enumerate(versions) if vv == v])
        if idx.size == 0:
            continue
        va = compute_auc(labels[idx], scores[idx])
        per_version[v] = dict(count=int(idx.size), auc=va)

    # 重训前后 AUC 增量:每次版本切换(v→v+1),比较切换前一段 vs 后一段的 AUC。
    # 无标注漂移点,故"恢复"定义为"重训后段 AUC 相对重训前段是否维持/提升"。
    retrain_deltas = []
    if len(distinct_vers) >= 2:
        for k in range(len(distinct_vers) - 1):
            v_pre, v_post = distinct_vers[k], distinct_vers[k + 1]
            pre = per_version.get(v_pre, {}).get("auc")
            post = per_version.get(v_post, {}).get("auc")
            if pre is not None and post is not None:
                retrain_deltas.append(dict(
                    from_v=v_pre, to_v=v_post,
                    auc_pre=pre, auc_post=post, delta=post - pre,
                ))

    return dict(
        auc=auc, n=len(rows),
        n_false_retrains=max(len(distinct_vers) - 1, 0),
        n_versions=len(distinct_vers),
        per_version=per_version,
        retrain_deltas=retrain_deltas,
    )


def main():
    ap = argparse.ArgumentParser(description="EXP2 分布式准确性分析 + Leveni 对照")
    ap.add_argument("--results-dir", default="results-local")
    ap.add_argument("--datasets", nargs="*", default=DEFAULT_DATASETS)
    args = ap.parse_args()

    root = (PROJECT_ROOT / args.results_dir if not Path(args.results_dir).is_absolute()
            else Path(args.results_dir))
    if not root.exists():
        print(f"[ERROR] 结果目录不存在: {root}")
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 收集:groups[(ds, p)] = [run stats...]
    groups: Dict[Tuple[str, int], List[Dict]] = {}
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        m = EXP2_PAT.match(d.name)
        if not m:
            continue
        ds = m.group("ds")
        if ds not in args.datasets:
            continue
        p = int(m.group("p"))
        st = parse_run(d / "scores.jsonl")
        if st is None or st["auc"] is None:
            print(f"[WARN] {d.name}: 无有效 AUC,跳过")
            continue
        st["exp_id"] = d.name
        groups.setdefault((ds, p), []).append(st)

    if not groups:
        print("[WARN] 未找到 EXP2 run(donors/forestcover/fraud × BACKLOG × default)。"
              "确认已拉取结果且命名匹配。")
        return

    # ---- 主表 + AUC-vs-P ----
    summ = OUT_DIR / "exp2_accuracy_summary.csv"
    avp = OUT_DIR / "exp2_auc_vs_parallelism.csv"
    parallelisms = sorted({p for (_, p) in groups})
    with open(summ, "w") as fs, open(avp, "w") as fa:
        fs.write("dataset,parallelism,n_runs,auc_mean,auc_std,auc_min,auc_max,false_retrains_mean\n")
        fa.write("dataset," + ",".join(f"P{p}_auc_mean" for p in parallelisms) + "\n")
        print(f"\n{'='*64}\n主表:stationary AUC × 并行度\n{'-'*64}")
        print(f"{'dataset':<14}{'P':>3}{'runs':>5}{'auc_mean':>10}{'auc_std':>9}{'fa_retr':>9}")
        avp_rows: Dict[str, Dict[int, float]] = {}
        for ds in args.datasets:
            row_means = {}
            for p in parallelisms:
                runs = groups.get((ds, p), [])
                if not runs:
                    continue
                aucs = [r["auc"] for r in runs]
                retr = [r["n_false_retrains"] for r in runs]
                am = statistics.mean(aucs)
                ast = statistics.stdev(aucs) if len(aucs) > 1 else 0.0
                row_means[p] = am
                print(f"{ds:<14}{p:>3}{len(runs):>5}{am:>10.4f}{ast:>9.4f}"
                      f"{statistics.mean(retr):>9.2f}")
                fs.write(f"{ds},{p},{len(runs)},{am:.4f},{ast:.4f},"
                         f"{min(aucs):.4f},{max(aucs):.4f},{statistics.mean(retr):.4f}\n")
            avp_rows[ds] = row_means
            fa.write(ds + "," + ",".join(
                f"{row_means.get(p, ''):.4f}" if p in row_means else ""
                for p in parallelisms) + "\n")

    # ---- AUC 随 P 是否保持(分布式准确性结论)----
    print(f"\n{'='*64}\nAUC 随并行度的保持性(分布式不牺牲准确性?)\n{'-'*64}")
    for ds in args.datasets:
        rm = avp_rows.get(ds, {})
        if 1 in rm and rm:
            base = rm[1]
            drops = {p: rm[p] - base for p in sorted(rm) if p != 1}
            drop_str = ", ".join(f"P{p}:{d:+.4f}" for p, d in drops.items())
            verdict = "保持" if all(abs(d) < 0.02 for d in drops.values()) else "有变化(>0.02)"
            print(f"{ds:<14} P1基线={base:.4f}  相对P1: {drop_str}  → {verdict}")
        else:
            print(f"{ds:<14} 缺 P=1 基线,无法判保持性")

    # ---- 重训前后 AUC 增量(在线适应有效性,核心证据)----
    # 论点:stationary 数据集本身含内在分布波动(故被在线异常检测文献选用);
    # 系统的模型更新是对此的在线适应(同源 oIFOR/asdIFOR),非误触发。
    # 判据:重训后段 AUC 相对重训前段维持/提升(delta≥0)→ 有效适应;持续显著下降 → 噪声过拟合。
    rd_csv = OUT_DIR / "exp2_retrain_delta.csv"
    print(f"\n{'='*64}\n重训前后 AUC 增量(在线适应有效性)\n{'-'*64}")
    print(f"{'dataset':<14}{'P':>3}{'n_retrains':>11}{'delta_mean':>11}{'delta>=0占比':>13}")
    with open(rd_csv, "w") as f:
        f.write("dataset,parallelism,n_retrain_events,delta_mean,delta_std,frac_nonneg,"
                "auc_pre_mean,auc_post_mean\n")
        for ds in args.datasets:
            for p in parallelisms:
                runs = groups.get((ds, p), [])
                if not runs:
                    continue
                deltas, pres, posts = [], [], []
                for r in runs:
                    for ev in r.get("retrain_deltas", []):
                        deltas.append(ev["delta"]); pres.append(ev["auc_pre"]); posts.append(ev["auc_post"])
                if not deltas:
                    print(f"{ds:<14}{p:>3}{0:>11}  (无重训事件)")
                    f.write(f"{ds},{p},0,,,,,\n")
                    continue
                dm = statistics.mean(deltas)
                dstd = statistics.stdev(deltas) if len(deltas) > 1 else 0.0
                frac_nonneg = sum(1 for d in deltas if d >= -0.005) / len(deltas)  # 容差 0.005
                print(f"{ds:<14}{p:>3}{len(deltas):>11}{dm:>+11.4f}{frac_nonneg:>13.2f}")
                f.write(f"{ds},{p},{len(deltas)},{dm:.4f},{dstd:.4f},{frac_nonneg:.4f},"
                        f"{statistics.mean(pres):.4f},{statistics.mean(posts):.4f}\n")

    # ---- per-version AUC(每次重训换进的森林质量,逐 run 明细)----
    pv_csv = OUT_DIR / "exp2_per_version_auc.csv"
    with open(pv_csv, "w") as f:
        f.write("dataset,parallelism,exp_id,version,count,auc\n")
        for (ds, p), runs in sorted(groups.items()):
            for r in runs:
                eid = r.get("exp_id", "")
                for v, st in sorted(r.get("per_version", {}).items()):
                    av = "" if st["auc"] is None else f"{st['auc']:.4f}"
                    f.write(f"{ds},{p},{eid},{v},{st['count']},{av}\n")

    # ---- 对照 Leveni Table 4(用 P=1 准单机)----
    vs = OUT_DIR / "exp2_vs_leveni.csv"
    print(f"\n{'='*64}\n对照 Leveni 2025 Table 4(本系统 P=1 vs 五算法,量级参考)\n{'-'*64}")
    algos = ["oIFOR", "asdIFOR", "HST", "RRCF", "LODA"]
    print(f"{'dataset':<14}{'ours_P1':>9}" + "".join(f"{a:>9}" for a in algos) + f"{'sanity':>8}")
    with open(vs, "w") as f:
        f.write("dataset,ours_p1_auc," + ",".join(a.lower() for a in algos) + ",in_range\n")
        for ds in args.datasets:
            ours = avp_rows.get(ds, {}).get(1)
            base = LEVENI_TABLE4.get(ds, {})
            lo, hi = SANITY_RANGE.get(ds, (0, 1))
            in_range = (ours is not None and lo <= ours <= hi)
            ours_s = f"{ours:.4f}" if ours is not None else "—"
            print(f"{ds:<14}{ours_s:>9}" +
                  "".join(f"{base.get(a, float('nan')):>9.3f}" for a in algos) +
                  f"{'ok' if in_range else 'CHECK':>8}")
            f.write(f"{ds},{ours_s}," + ",".join(f"{base.get(a,'')}" for a in algos) +
                    f",{int(in_range)}\n")
    print("=" * 64)
    print(f"[OK] {summ}\n[OK] {avp}\n[OK] {vs}")
    print("\n判读:① AUC 随 P 保持(|ΔP1|<0.02)= 分布式不牺牲准确性(贡献②核心);"
          "② P=1 落在 Leveni 五算法区间(sanity=ok)= 口径对齐、competitive;"
          "③ 对照为量级参考(本系统 100 树 vs Leveni oIFOR 32 树),不作严格 head-to-head。")


if __name__ == "__main__":
    main()
