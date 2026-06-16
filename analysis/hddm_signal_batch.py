#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDDM_W per-feature 信号离线批量检验 — feature/per-feature-hddm_w 分支。
Reference: HANDOVER_per_feature_hddm_w_devspec.md §7, EXP4_detector_morphology_skeleton.md §3.

用途:
    在多个数据集 × 多个 λ 上批量运行 hddm_signal_sanity 的信号与 HDDM_W 复刻逻辑,
    汇总成一张跨数据集、跨漂移形态(abrupt / gradual / incremental / reoccurring)
    的对比矩阵。用于在上集群前判断:HDDM_W 的可触发性是否随漂移形态系统性变化
    (持续平台利于 EWMA 累积,脉冲式不利),并为 EXP4 提供扩样本的离线证据。

    本脚本不替代集群 EXP1;所有结论均为离线必要条件,最终判据为集群 overall_auc。

复用:
    直接 import hddm_signal_sanity 的 compute_signal / run_hddm_on_signal /
    rank_features_by_lift / window_stats,保证信号逻辑与单数据集脚本逐字一致。
    数据加载复用 derisk_proxy 的 yml 驱动 loader。

漂移形态来源:
    datasets.yml 的 drift_type 字段;无该字段时按 driftspec 的 drift_type 推断,
    再无则标 unknown。每个数据集的全部标注漂移点都参与命中统计。

命中(detected)定义:
    某 (特征 × 漂移点) 在窗口 [start, start + warmup] 内有 HDDM_W 触发 DRIFT。
    触发率 = 有至少一次触发的 (特征×漂移点) 占比;
    命中率 = 触发点落在标注窗内的 (特征×漂移点) 占比(更严格)。

CLI:
    # 默认:全部 synth + insects 数据集 × λ∈{0.002,0.005,0.01},每集 top-8 特征
    python3 analysis/hddm_signal_batch.py

    python3 analysis/hddm_signal_batch.py --datasets synth_abrupt,synth_gradual,synth_incremental
    python3 analysis/hddm_signal_batch.py --lambdas 0.005,0.01
    python3 analysis/hddm_signal_batch.py --top 12 --scale-mode p99
    python3 analysis/hddm_signal_batch.py --warmup 2000

输出(analysis/out/):
    hddm_batch_matrix.csv   每行 = 数据集 × λ:drift_type, n_features, n_events,
                            fire_rate, hit_rate, median_lift, median_first_delay
    hddm_batch_matrix.md    同上,Markdown 表(便于直接贴入 EXP4 文档)
    控制台 同时打印按 drift_type 分组的汇总
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"
sys.path.insert(0, str(THIS_FILE.parent))

# 复用单数据集脚本的信号逻辑与 yml loader
from derisk_proxy import all_dataset_specs, load_dataset  # noqa: E402
import hddm_signal_sanity as sig  # noqa: E402


DEFAULT_DATASETS = [
    "synth_abrupt", "synth_gradual", "synth_incremental", "synth_reoccurring",
    "insects_abrupt_imbalanced", "insects_gradual_imbalanced",
]
DEFAULT_LAMBDAS = [0.002, 0.005, 0.01]


def _drift_type_of(spec, yml_meta: Dict) -> str:
    """从 yml meta / driftspec 推断漂移形态;取不到标 unknown。"""
    dt = yml_meta.get("drift_type")
    if dt:
        return str(dt)
    # driftspec 里可能也有
    name = spec.name
    if "abrupt" in name:
        return "abrupt"
    if "gradual" in name:
        return "gradual"
    if "incremental" in name:
        return "incremental"
    if "reoccurring" in name:
        return "reoccurring"
    return "unknown"


def _load_yml_meta() -> Dict[str, Dict]:
    import yaml
    with open(PROJECT_ROOT / "deploy" / "datasets.yml") as f:
        y = yaml.safe_load(f) or {}
    return y.get("datasets", {}) or {}


def evaluate_one(spec, X: np.ndarray, drift_starts: List[int], *,
                 feats: List[int], warmup: int, scale_mode: str,
                 lam: float, drift_conf: float, warn_conf: float
                 ) -> Dict:
    """
    对一个数据集在单个 λ 下评估。返回该 (数据集×λ) 的汇总指标。
    遍历 feats × drift_starts 的每个单元,统计触发率 / 命中率 / lift / 首触发延迟。
    """
    n_units = 0
    n_fire_units = 0       # 至少触发一次
    n_hit_units = 0        # 触发点落在标注窗内
    lifts: List[float] = []
    first_delays: List[int] = []  # 命中单元的 (first_fire - start),仅正延迟计入

    for d in feats:
        s, ref_mean, scale, _ = sig.compute_signal(X[:, d], warmup, scale_mode)
        fires = sig.run_hddm_on_signal(s, warmup, lam, drift_conf, warn_conf)
        for ep in drift_starts:
            n_units += 1
            pre_m, post_m, _ = sig.window_stats(s, ep, warmup)
            if not (math.isnan(pre_m) or math.isnan(post_m)):
                lifts.append(post_m - pre_m)
            in_win = [f for f in fires if ep <= f < ep + warmup]
            any_fire = len(fires) > 0
            if any_fire:
                n_fire_units += 1
            if in_win:
                n_hit_units += 1
                first_delays.append(min(in_win) - ep)

    return dict(
        dataset=spec.name,
        n_features=len(feats),
        n_events=len(drift_starts),
        n_units=n_units,
        fire_rate=(n_fire_units / n_units if n_units else float("nan")),
        hit_rate=(n_hit_units / n_units if n_units else float("nan")),
        median_lift=(float(np.median(lifts)) if lifts else float("nan")),
        median_first_delay=(float(np.median(first_delays)) if first_delays else float("nan")),
    )


def main():
    ap = argparse.ArgumentParser(description="HDDM_W per-feature 信号离线批量检验矩阵")
    ap.add_argument("--datasets", default=",".join(DEFAULT_DATASETS),
                    help="逗号分隔数据集名(默认全部 synth + insects)")
    ap.add_argument("--lambdas", default=",".join(str(x) for x in DEFAULT_LAMBDAS),
                    help="逗号分隔 λ 值(默认 0.002,0.005,0.01)")
    ap.add_argument("--top", type=int, default=8, help="每数据集自动选 top-N 特征(默认 8)")
    ap.add_argument("--warmup", type=int, default=2000, help="warm-up 样本数(默认 2000)")
    ap.add_argument("--scale-mode", default="p99", choices=["maxdev", "p99"],
                    help="scale 估法(默认 p99,与 devspec 一致)")
    ap.add_argument("--drift-conf", type=float, default=0.001)
    ap.add_argument("--warn-conf", type=float, default=0.005)
    args = ap.parse_args()

    want_datasets = [d.strip() for d in args.datasets.split(",") if d.strip()]
    lambdas = [float(x) for x in args.lambdas.split(",") if x.strip()]

    specs = {s.name: s for s in all_dataset_specs()}
    yml_meta = _load_yml_meta()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    rows: List[Dict] = []
    for name in want_datasets:
        if name not in specs:
            print(f"[WARN] {name} 不在 datasets.yml,跳过")
            continue
        spec = specs[name]
        if not spec.csv_path.exists():
            print(f"[WARN] {name} CSV 不存在({spec.csv_path}),跳过")
            continue
        drift_type = _drift_type_of(spec, yml_meta.get(name, {}))
        print(f"[INFO] 加载 {name} (drift_type={drift_type}) ...")
        X, y = load_dataset(spec)
        drift_starts = sorted(spec.drift_starts)
        if not drift_starts:
            print(f"[WARN] {name} 无标注漂移点,跳过")
            continue

        # 特征选择:按首个漂移点的 lift 排名,取 top-N(与单数据集脚本同口径)
        ranked = sig.rank_features_by_lift(X, drift_starts, args.warmup, args.scale_mode)
        feats = [d for d, _ in ranked[:args.top]]
        print(f"       n={X.shape[0]}, D={X.shape[1]}, events={drift_starts}, "
              f"top-{args.top} feats={feats}")

        for lam in lambdas:
            r = evaluate_one(spec, X, drift_starts, feats=feats, warmup=args.warmup,
                             scale_mode=args.scale_mode, lam=lam,
                             drift_conf=args.drift_conf, warn_conf=args.warn_conf)
            r["drift_type"] = drift_type
            r["lambda"] = lam
            rows.append(r)
            print(f"       λ={lam:<6} fire_rate={r['fire_rate']:.2f} "
                  f"hit_rate={r['hit_rate']:.2f} median_lift={r['median_lift']:+.3f} "
                  f"median_delay={r['median_first_delay']}")

    if not rows:
        print("[ERROR] 没有任何数据集可评估(检查 CSV 是否在本地)")
        sys.exit(2)

    # --- 写 CSV ---
    cols = ["dataset", "drift_type", "lambda", "n_features", "n_events", "n_units",
            "fire_rate", "hit_rate", "median_lift", "median_first_delay"]
    csv_path = OUT_DIR / "hddm_batch_matrix.csv"
    with open(csv_path, "w") as f:
        f.write(",".join(cols) + "\n")
        for r in rows:
            f.write(",".join(_fmt(r.get(c)) for c in cols) + "\n")
    print(f"\n[OK] 写出 {csv_path}")

    # --- 写 Markdown 表(便于贴入 EXP4 文档)---
    md_path = OUT_DIR / "hddm_batch_matrix.md"
    with open(md_path, "w") as f:
        f.write("# HDDM_W per-feature 离线批量检验矩阵\n\n")
        f.write(f"warmup={args.warmup}, scale_mode={args.scale_mode}, "
                f"driftConf={args.drift_conf}, warnConf={args.warn_conf}, "
                f"每数据集 top-{args.top} 特征\n\n")
        f.write("| 数据集 | 漂移形态 | λ | 触发率 | 命中率 | 中位 lift | 中位首触发延迟 |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in rows:
            f.write(f"| {r['dataset']} | {r['drift_type']} | {r['lambda']} | "
                    f"{r['fire_rate']:.2f} | {r['hit_rate']:.2f} | "
                    f"{r['median_lift']:+.3f} | {_fmt(r['median_first_delay'])} |\n")
        f.write("\n> 触发率=至少触发一次的(特征×漂移点)占比;命中率=触发点落在 "
                "[start, start+warmup] 窗内的占比;延迟单位为样本数。\n")
        f.write("> 离线必要条件,最终判据以集群 EXP1 overall_auc 为准。\n")
    print(f"[OK] 写出 {md_path}")

    # --- 按漂移形态分组的控制台汇总 ---
    print("\n=== 按漂移形态分组(各 λ 的命中率均值)===")
    by_type: Dict[str, Dict[float, List[float]]] = {}
    for r in rows:
        by_type.setdefault(r["drift_type"], {}).setdefault(r["lambda"], []).append(r["hit_rate"])
    lambdas_sorted = sorted(lambdas)
    header = f"{'drift_type':>14} | " + " | ".join(f"λ={l}" for l in lambdas_sorted)
    print(header)
    print("-" * len(header))
    for dt in sorted(by_type):
        cells = []
        for l in lambdas_sorted:
            vals = by_type[dt].get(l, [])
            cells.append(f"{np.mean(vals):.2f}" if vals else "  - ")
        print(f"{dt:>14} | " + " | ".join(cells))

    print("\n[判读] 若 gradual/incremental 的命中率系统性高于 abrupt,"
          "即支持'持续平台利于 EWMA 累积'的机制结论(EXP4 §2.2)。")
    print("[提醒] 离线必要条件;最终判定以集群 EXP1 overall_auc 为准。")


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return "nan"
        return f"{v:.4g}"
    return str(v)


if __name__ == "__main__":
    main()
