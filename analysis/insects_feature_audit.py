#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INSECTS 33 特征量纲与决策 B 信号健康度普查 — feature/per-feature-hddm_w 分支。
Reference: dataset_suitability_probe.md §5.

动机:
    探查中发现 f0/f2 等特征原始值在数百~上千量级,决策 B 的 p99 归一化 scale 仍达
    669/1306,signal 被压扁失真。本脚本对全部 33 特征逐一体检,直接回答每个特征在
    决策 B 信号(冻结 refMean + p99 scale 的 |x−refMean|/scale)下是否有效,
    而非只报原始量纲统计。

普查对每个特征报告:
    原始量纲:   min / max / mean / std / 近常数判定
    决策 B 信号: warmup 段 refMean / scale(p99 与 maxdev 对比)/ 检测期 signal 的
                 均值与饱和率(signal==1.0 的占比,过高=被 clamp 压扁)
    漂移响应:   各标注漂移点的 lift(post_mean − pre_mean)
    健康度判定: ok / saturated(饱和率过高)/ near_constant / weak_signal(漂移处 lift 过低)

判定阈值(可调):
    saturated:    检测期 signal 饱和率 > 20%(被 clamp 到 1.0 过多 → scale 偏小或重尾)
    near_constant: 原始 std / |mean| < 1e-3(特征几乎不变 → 无信息)
    weak_signal:  全部漂移点 lift 均 < 0.02(漂移处无可见偏移)

> 注:决策 B 的 |x−refMean|/scale 理论上对任何量纲都归一化到可比范围;失真根因通常
> 不是"量纲大"本身,而是 scale 估计被极端值影响或特征分布病态。本脚本据实测区分。

数据加载复用 derisk_proxy 的 yml loader;信号逻辑复用 hddm_signal_sanity.compute_signal。

CLI:
    python3 analysis/insects_feature_audit.py                              # 默认全部 insects 数据集
    python3 analysis/insects_feature_audit.py --dataset insects_abrupt_imbalanced
    python3 analysis/insects_feature_audit.py --warmup 2000 --sat-thresh 0.2

输出(analysis/out/):
    feature_audit_<dataset>.csv   每特征一行的完整体检
    控制台 打印问题特征清单(saturated / near_constant / weak_signal)与建议
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"
sys.path.insert(0, str(THIS_FILE.parent))

from derisk_proxy import all_dataset_specs, load_dataset  # noqa: E402
import hddm_signal_sanity as sig  # noqa: E402

DEFAULT_DATASETS = [
    "insects_abrupt_imbalanced", "insects_gradual_imbalanced",
    "insects_reoccurring_imbalanced",
]


def audit_feature(x: np.ndarray, drift_starts: List[int], warmup: int,
                  sat_thresh: float, weak_thresh: float) -> Dict:
    """对单特征做量纲 + 决策 B 信号 + 漂移响应体检。"""
    n = len(x)
    raw_min, raw_max = float(np.min(x)), float(np.max(x))
    raw_mean, raw_std = float(np.mean(x)), float(np.std(x))
    # 近常数:std 相对 mean 量级极小(用 max(|mean|,|max-min|) 防 mean≈0)
    denom = max(abs(raw_mean), abs(raw_max - raw_min), 1e-12)
    near_const = (raw_std / denom) < 1e-3

    # 决策 B 信号(p99 与 maxdev 两种 scale 对比,看是否被离群撑大)
    sig_p99, ref_p99, scale_p99, _ = sig.compute_signal(x, warmup, "p99")
    _, _, scale_maxdev, _ = sig.compute_signal(x, warmup, "maxdev")
    det = sig_p99[warmup:]
    det_valid = det[~np.isnan(det)]
    sig_mean = float(np.nanmean(det)) if det_valid.size else float("nan")
    # 饱和率:signal 恰为 1.0(被 clamp)的占比
    sat_rate = float(np.mean(det_valid >= 0.999)) if det_valid.size else float("nan")
    scale_ratio = (scale_maxdev / scale_p99) if scale_p99 > 0 else float("inf")

    # 漂移响应:各漂移点 lift
    lifts = []
    for ep in drift_starts:
        pre_m, post_m, _ = sig.window_stats(sig_p99, ep, warmup)
        if not (math.isnan(pre_m) or math.isnan(post_m)):
            lifts.append(post_m - pre_m)
    max_lift = max(lifts) if lifts else float("nan")
    weak = bool(lifts) and (max([abs(l) for l in lifts]) < weak_thresh)

    # 健康度判定(可多标签,优先级:near_constant > saturated > weak > ok)
    flags = []
    if near_const:
        flags.append("near_constant")
    if not math.isnan(sat_rate) and sat_rate > sat_thresh:
        flags.append("saturated")
    if weak:
        flags.append("weak_signal")
    verdict = "ok" if not flags else "|".join(flags)

    return dict(
        raw_min=raw_min, raw_max=raw_max, raw_mean=raw_mean, raw_std=raw_std,
        ref_mean=ref_p99, scale_p99=scale_p99, scale_maxdev=scale_maxdev,
        scale_ratio=scale_ratio, sig_mean=sig_mean, sat_rate=sat_rate,
        max_lift=max_lift, verdict=verdict,
    )


def main():
    ap = argparse.ArgumentParser(description="INSECTS 33 特征量纲与决策 B 信号健康度普查")
    ap.add_argument("--dataset", default=None,
                    help="指定单个数据集;默认普查全部 insects")
    ap.add_argument("--warmup", type=int, default=2000)
    ap.add_argument("--sat-thresh", type=float, default=0.2,
                    help="饱和率阈值,超过则标 saturated(默认 0.2)")
    ap.add_argument("--weak-thresh", type=float, default=0.02,
                    help="漂移 lift 阈值,全部漂移点低于则标 weak_signal(默认 0.02)")
    args = ap.parse_args()

    specs = {s.name: s for s in all_dataset_specs()}
    targets = [args.dataset] if args.dataset else DEFAULT_DATASETS
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for name in targets:
        if name not in specs:
            print(f"[WARN] {name} 不在 datasets.yml,跳过")
            continue
        spec = specs[name]
        if not spec.csv_path.exists():
            print(f"[WARN] {name} CSV 不存在({spec.csv_path}),跳过")
            continue
        print(f"\n{'='*70}\n[INFO] 普查 {name}  ({spec.csv_path})")
        X, y = load_dataset(spec)
        n, D = X.shape
        drift_starts = sorted(spec.drift_starts)
        print(f"[INFO] n={n}, D={D}, drift_starts={drift_starts}")

        rows = []
        for d in range(D):
            r = audit_feature(X[:, d], drift_starts, args.warmup,
                              args.sat_thresh, args.weak_thresh)
            r["feature_id"] = d
            rows.append(r)

        # 写 CSV
        cols = ["feature_id", "raw_min", "raw_max", "raw_mean", "raw_std",
                "ref_mean", "scale_p99", "scale_maxdev", "scale_ratio",
                "sig_mean", "sat_rate", "max_lift", "verdict"]
        csv_path = OUT_DIR / f"feature_audit_{name}.csv"
        with open(csv_path, "w") as f:
            f.write(",".join(cols) + "\n")
            for r in rows:
                f.write(",".join(_fmt(r.get(c)) for c in cols) + "\n")
        print(f"[OK] 写出 {csv_path}")

        # 控制台:量纲跨度总览
        mags = [abs(r["raw_max"]) for r in rows] + [abs(r["raw_min"]) for r in rows]
        big = [r["feature_id"] for r in rows if max(abs(r["raw_max"]), abs(r["raw_min"])) > 10]
        print(f"[量纲] 原始值绝对量级 > 10 的特征: {big if big else '无'}")
        print(f"       全体特征 |值| 范围: [{min(mags):.3g}, {max(mags):.3g}]")

        # 控制台:问题特征清单
        for tag in ["near_constant", "saturated", "weak_signal"]:
            hit = [r["feature_id"] for r in rows if tag in r["verdict"]]
            if hit:
                print(f"[{tag}] 特征: {hit}")
        ok = [r["feature_id"] for r in rows if r["verdict"] == "ok"]
        print(f"[ok] 健康特征 ({len(ok)}/{D}): {ok}")

        # 重点:scale_ratio 大 = maxdev 远大于 p99 = 该特征有离群,p99 是对的;
        #       但若 p99 本身就很大(scale_p99 >> sig 有效范围),说明特征重尾/大量纲
        big_scale = [(r["feature_id"], round(r["scale_p99"], 1))
                     for r in rows if r["scale_p99"] > 10]
        if big_scale:
            print(f"[关注] p99 scale > 10 的特征(大量纲/重尾,signal 可能失真): {big_scale}")
            print("       建议:对这些特征在 FeatureSplitFlatMap 前先做 per-feature 标准化,"
                  "或在决策 B 信号里改用相对尺度(如 MAD)。")

    print(f"\n[提醒] 本普查为离线诊断;归一化方案的最终有效性以集群 EXP1 为准。")


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return "nan"
        if math.isinf(v):
            return "inf"
        return f"{v:.5g}"
    return str(v)


if __name__ == "__main__":
    main()
