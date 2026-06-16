#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HDDM_W per-feature 信号离线 sanity 检查 — feature/per-feature-hddm_w 分支 §3。
Reference: HANDOVER_per_feature_hddm_w_devspec.md §3.

科学问题 / Scientific question:
    决策 B 的被监控信号 signal = min(|x - refMean| / scale, 1.0) 在 INSECTS 真实
    特征上,是否在标注漂移点附近出现可见的均值上升?maxdev 估出的 scale 是否会
    把漂移后 signal 压到检测不到?

    这是上集群前的离线 sanity check。结论仅为必要条件(信号可见 → 值得上集群);
    最终判定以集群 EXP1 overall_auc 为准(离线↔部署系统性差距,既定教训)。

镜像 Java 端 PerFeatureHDDMFunction §1.3 的信号逻辑,逐字对应:
    - warm-up 期(前 W 样本)只累积 refMean / scale,不检测;
    - warm-up 末冻结 refMean = mean(warmup 段),scale = maxdev 或 p99;
    - 检测期 signal = min(|x - refMean| / scale, 1.0);
    - (可选)用纯 Python HDDM_W 复刻跑 update(signal),标 DRIFT 点 + reset 重 warm-up,
      与 Java drift/HDDM_W.java 的 EWMA Hoeffding 界一致。

注意:本脚本只验证"信号是否可见"与"HDDM_W 是否在漂移点附近 fire",
不替代集群 AUC 评估。

数据加载复用 derisk_proxy 的 yml 驱动 loader(尊重 hasHeader/hasId/hasLabel/
labelPosition/anomalyLabel/dimensions),无硬编码列名。

CLI:
    # 默认:insects_abrupt_imbalanced,自动选偏移最大的若干特征,出图 + CSV
    python3 analysis/hddm_signal_sanity.py

    python3 analysis/hddm_signal_sanity.py --dataset insects_gradual_imbalanced
    python3 analysis/hddm_signal_sanity.py --features 0,3,7,12     # 指定特征
    python3 analysis/hddm_signal_sanity.py --top 6                 # 自动选 top-6
    python3 analysis/hddm_signal_sanity.py --scale-mode p99
    python3 analysis/hddm_signal_sanity.py --lambda 0.1 --run-hddm # 跑 HDDM_W 标 fire
    python3 analysis/hddm_signal_sanity.py --warmup 2000

输出(analysis/out/):
    hddm_signal_<dataset>.png    每特征 signal 滑动均值曲线 + 漂移点竖线 (+ fire 标记)
    hddm_signal_<dataset>.csv    feature_id, drift_event, pre_mean, post_mean, lift, peak,
                                 scale, n_fire, first_fire_seq, detected
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 复用 derisk_proxy 的 yml 驱动数据加载,保证与既有 harness 完全同一套数据约定
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"
sys.path.insert(0, str(THIS_FILE.parent))
from derisk_proxy import all_dataset_specs, load_dataset, DatasetSpec  # noqa: E402


# ===================== 信号:镜像 Java §1.3 =====================

def compute_signal(values: np.ndarray, warmup: int, scale_mode: str
                   ) -> Tuple[np.ndarray, float, float, int]:
    """
    单特征值序列 → signal 序列,逐字镜像 PerFeatureHDDMFunction 的 warm-up + 归一化。

    返回 (signal[n], refMean, scale, warmup_end_index)。
    warm-up 期 signal 记为 np.nan(检测器在这段不工作)。
    单遍近似:warm-up 期用运行均值 runMean 估临时参考算偏离,warm-up 末把
    refMean 冻结为整段算术均值(Java 端同此近似,见 devspec §1.2 注)。
    """
    n = len(values)
    signal = np.full(n, np.nan, dtype=float)
    if n <= warmup:
        # 数据不足一个 warm-up 段:无法检测,全 nan
        return signal, float("nan"), float("nan"), n

    warm = values[:warmup]
    ref_mean = float(np.mean(warm))

    # warm-up 段偏离(用 runMean 近似,与 Java 单遍一致):
    # 这里直接用冻结 ref_mean 算偏离的最大/分位,作为 scale。
    # (runMean 与最终 mean 在 warmup=2000 时差异可忽略,devspec §1.2 已述)
    warm_dev = np.abs(warm - ref_mean)
    if scale_mode == "maxdev":
        scale = float(np.max(warm_dev))
    elif scale_mode == "p99":
        scale = float(np.percentile(warm_dev, 99))
    else:
        raise ValueError(f"unknown scale_mode: {scale_mode!r}")
    scale = max(scale, 1e-12)  # EPS 防除零(Java 端同)

    dev = np.abs(values[warmup:] - ref_mean) / scale
    signal[warmup:] = np.minimum(dev, 1.0)  # clamp [0,1],双向
    return signal, ref_mean, scale, warmup


# ===================== 纯 Python HDDM_W 复刻(可选,--run-hddm) =====================
# 逐字镜像 drift/HDDM_W.java 的 update():EWMA + 单样本 Hoeffding 界 + best 跟踪 + 双阈值。
# 用于在离线信号上标注 HDDM_W 会在哪 fire(STABLE/WARN/DRIFT)。

class HDDM_W_Py:
    def __init__(self, lam: float, drift_conf: float, warn_conf: float):
        if not (0.0 < lam <= 1.0):
            raise ValueError("lambda must be in (0,1]")
        if not (0.0 < drift_conf < warn_conf < 1.0):
            raise ValueError("require 0 < driftConf < warnConf < 1")
        self.lam = lam
        self.drift_conf = drift_conf
        self.warn_conf = warn_conf
        self.reset()

    def reset(self):
        self.ewma = 0.0
        self.best_mean = 0.0
        self.best_bound = 0.0
        self.n = 0

    def update(self, value: float) -> str:
        self.n += 1
        if self.n == 1:
            self.ewma = value
        else:
            self.ewma = (1.0 - self.lam) * self.ewma + self.lam * value
        mean = self.ewma
        # EWMA 单样本 Hoeffding 界(Frías-Blanco 2015 Example 7),数据范围 [0,1] → b-a=1
        epsilon = math.sqrt(self.lam * math.log(1.0 / self.drift_conf) /
                            (2.0 * (2.0 - self.lam)))
        if self.n == 1 or mean + epsilon < self.best_mean + self.best_bound:
            self.best_mean = mean
            self.best_bound = epsilon
        drift_bound = self.best_bound * math.sqrt(
            math.log(1.0 / self.drift_conf) / math.log(1.0 / self.warn_conf))
        warn_bound = self.best_bound * math.sqrt(
            math.log(1.0 / self.warn_conf) / math.log(1.0 / self.drift_conf))
        if (mean - epsilon) > self.best_mean + drift_bound:
            return "DRIFT"
        if (mean - epsilon) > self.best_mean + warn_bound:
            return "WARN"
        return "STABLE"


def run_hddm_on_signal(signal: np.ndarray, warmup: int, lam: float,
                       drift_conf: float, warn_conf: float) -> List[int]:
    """
    在 signal 序列上跑 HDDM_W,返回 DRIFT 触发的样本下标列表(全局下标)。
    DRIFT 后 reset 并重新 warm-up:即跳过接下来 warmup 个样本不喂(因为 Java 端
    DRIFT 后重置 refMean/scale 重 warm-up;离线我们用同一 signal,简化为 reset 检测器
    并冷却 warmup 个样本不喂,近似"重新建立参考"的节流效果)。
    """
    det = HDDM_W_Py(lam, drift_conf, warn_conf)
    fires: List[int] = []
    cooldown = 0
    for i in range(warmup, len(signal)):
        s = signal[i]
        if np.isnan(s):
            continue
        if cooldown > 0:
            cooldown -= 1
            continue
        status = det.update(float(s))
        if status == "DRIFT":
            fires.append(i)
            det.reset()
            cooldown = warmup  # 重新 warm-up 的节流近似
    return fires


# ===================== 特征选择 + 度量 =====================

def rank_features_by_lift(X: np.ndarray, drift_starts: List[int], warmup: int,
                          scale_mode: str) -> List[Tuple[int, float]]:
    """
    对每个特征算 signal,用"第一个漂移点前后各一窗的 signal 均值差"作 lift 排名。
    窗长取 warmup。返回 [(feature_id, lift)] 按 lift 降序。
    """
    if not drift_starts:
        # 无漂移点:退化为按 signal 总体方差排名(只为出图,不下结论)
        ranked = []
        for d in range(X.shape[1]):
            sig, *_ = compute_signal(X[:, d], warmup, scale_mode)
            ranked.append((d, float(np.nanvar(sig))))
        return sorted(ranked, key=lambda t: -t[1])

    ep = drift_starts[0]
    w = warmup
    ranked = []
    for d in range(X.shape[1]):
        sig, *_ = compute_signal(X[:, d], warmup, scale_mode)
        pre = sig[max(warmup, ep - w):ep]
        post = sig[ep:ep + w]
        pre_m = float(np.nanmean(pre)) if pre.size and not np.all(np.isnan(pre)) else 0.0
        post_m = float(np.nanmean(post)) if post.size and not np.all(np.isnan(post)) else 0.0
        ranked.append((d, post_m - pre_m))
    return sorted(ranked, key=lambda t: -t[1])


def window_stats(sig: np.ndarray, ep: int, w: int) -> Tuple[float, float, float]:
    """漂移点 ep 前后各一窗的 signal 均值 + post 窗峰值。返回 (pre_mean, post_mean, post_peak)。"""
    pre = sig[max(0, ep - w):ep]
    post = sig[ep:ep + w]
    pre_m = float(np.nanmean(pre)) if pre.size and not np.all(np.isnan(pre)) else float("nan")
    post_m = float(np.nanmean(post)) if post.size and not np.all(np.isnan(post)) else float("nan")
    post_pk = float(np.nanmax(post)) if post.size and not np.all(np.isnan(post)) else float("nan")
    return pre_m, post_m, post_pk


def moving_mean(sig: np.ndarray, win: int) -> np.ndarray:
    """nan-aware 滑动均值,仅用于画图平滑。"""
    out = np.full_like(sig, np.nan, dtype=float)
    valid = ~np.isnan(sig)
    if valid.sum() == 0:
        return out
    filled = np.where(valid, sig, 0.0)
    csum = np.cumsum(np.insert(filled, 0, 0.0))
    ccnt = np.cumsum(np.insert(valid.astype(float), 0, 0.0))
    for i in range(len(sig)):
        a = max(0, i - win + 1)
        s = csum[i + 1] - csum[a]
        c = ccnt[i + 1] - ccnt[a]
        out[i] = s / c if c > 0 else np.nan
    return out


# ===================== 主流程 =====================

def main():
    ap = argparse.ArgumentParser(description="HDDM_W per-feature 信号离线 sanity 检查 (§3)")
    ap.add_argument("--dataset", default="insects_abrupt_imbalanced",
                    help="datasets.yml 中的数据集名 (默认 insects_abrupt_imbalanced)")
    ap.add_argument("--features", default=None,
                    help="逗号分隔的特征下标 (如 0,3,7);不给则自动按 lift 选 top")
    ap.add_argument("--top", type=int, default=6, help="自动选 top-N 特征 (默认 6)")
    ap.add_argument("--warmup", type=int, default=2000, help="warm-up 样本数 W (默认 2000)")
    ap.add_argument("--scale-mode", default="maxdev", choices=["maxdev", "p99"],
                    help="scale 估法 (默认 maxdev)")
    ap.add_argument("--smooth", type=int, default=500, help="画图滑动均值窗 (默认 500)")
    ap.add_argument("--run-hddm", action="store_true",
                    help="额外跑纯 Python HDDM_W 复刻,标 DRIFT fire 点")
    ap.add_argument("--lambda", dest="lam", type=float, default=0.1, help="HDDM_W λ (默认 0.1)")
    ap.add_argument("--drift-conf", type=float, default=0.001, help="driftConfidence (默认 0.001)")
    ap.add_argument("--warn-conf", type=float, default=0.005, help="warnConfidence (默认 0.005)")
    args = ap.parse_args()

    # --- 取 spec + 加载 ---
    specs = {s.name: s for s in all_dataset_specs()}
    if args.dataset not in specs:
        print(f"[ERROR] dataset {args.dataset!r} 不在 datasets.yml;可选:{sorted(specs)}")
        sys.exit(2)
    spec = specs[args.dataset]
    if not spec.csv_path.exists():
        print(f"[ERROR] CSV 不存在:{spec.csv_path}(请在 leejean 本地有数据的机器上跑)")
        sys.exit(2)

    print(f"[INFO] 加载 {spec.name}  ({spec.csv_path})")
    X, y = load_dataset(spec)
    n, D = X.shape
    drift_starts = sorted(spec.drift_starts)
    print(f"[INFO] n={n}, D={D}, anomaly_ratio={y.mean():.4f}, drift_starts={drift_starts}")
    print(f"[INFO] warmup={args.warmup}, scale_mode={args.scale_mode}, "
          f"run_hddm={args.run_hddm}, lambda={args.lam}")

    # --- 选特征 ---
    if args.features:
        feats = [int(t) for t in args.features.split(",") if t.strip() != ""]
        for f in feats:
            if not (0 <= f < D):
                print(f"[ERROR] feature {f} 越界 [0,{D})"); sys.exit(2)
    else:
        ranked = rank_features_by_lift(X, drift_starts, args.warmup, args.scale_mode)
        feats = [d for d, _ in ranked[:args.top]]
        print(f"[INFO] 自动选 top-{args.top} (按首个漂移点 lift): {feats}")
        print("       lift 排名前列: " +
              ", ".join(f"f{d}:{lift:+.3f}" for d, lift in ranked[:args.top]))

    # --- 逐特征算 signal + 度量 + (可选) HDDM fire ---
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_rows = []
    sig_cache = {}
    fire_cache = {}
    for d in feats:
        sig, ref_mean, scale, _ = compute_signal(X[:, d], args.warmup, args.scale_mode)
        sig_cache[d] = sig
        fires = []
        if args.run_hddm:
            fires = run_hddm_on_signal(sig, args.warmup, args.lam,
                                       args.drift_conf, args.warn_conf)
        fire_cache[d] = fires

        # 对每个漂移点记录前后窗统计
        events = drift_starts if drift_starts else [None]
        for ep in events:
            if ep is None:
                csv_rows.append(dict(
                    feature_id=d, drift_event="", pre_mean="", post_mean="",
                    lift="", post_peak=f"{np.nanmax(sig):.4f}",
                    scale=f"{scale:.6g}", ref_mean=f"{ref_mean:.6g}",
                    n_fire=len(fires), first_fire_seq=(fires[0] if fires else ""),
                    detected=""))
                continue
            pre_m, post_m, post_pk = window_stats(sig, ep, args.warmup)
            lift = (post_m - pre_m) if (not math.isnan(pre_m) and not math.isnan(post_m)) else float("nan")
            # detected:该漂移点 [ep, ep+warmup] 窗内是否有 HDDM DRIFT
            det_here = any(ep <= fi < ep + args.warmup for fi in fires) if args.run_hddm else ""
            csv_rows.append(dict(
                feature_id=d, drift_event=ep,
                pre_mean=f"{pre_m:.4f}", post_mean=f"{post_m:.4f}",
                lift=f"{lift:+.4f}", post_peak=f"{post_pk:.4f}",
                scale=f"{scale:.6g}", ref_mean=f"{ref_mean:.6g}",
                n_fire=len(fires),
                first_fire_seq=(fires[0] if fires else ""),
                detected=det_here))

    # --- 写 CSV ---
    csv_path = OUT_DIR / f"hddm_signal_{spec.name}.csv"
    cols = ["feature_id", "drift_event", "pre_mean", "post_mean", "lift",
            "post_peak", "scale", "ref_mean", "n_fire", "first_fire_seq", "detected"]
    with open(csv_path, "w") as f:
        f.write(",".join(cols) + "\n")
        for r in csv_rows:
            f.write(",".join(str(r.get(c, "")) for c in cols) + "\n")
    print(f"[OK] 写出 {csv_path}")

    # --- 画图:每特征一行 signal 滑动均值曲线 ---
    k = len(feats)
    fig, axes = plt.subplots(k, 1, figsize=(12, 2.2 * k), sharex=True, squeeze=False)
    for ax_i, d in enumerate(feats):
        ax = axes[ax_i][0]
        sig = sig_cache[d]
        sm = moving_mean(sig, args.smooth)
        xs = np.arange(n)
        ax.plot(xs, sm, lw=0.8, color="#1f77b4", label=f"f{d} signal (MA{args.smooth})")
        ax.axvspan(0, args.warmup, color="0.85", alpha=0.5)  # warm-up 灰带
        for ep in drift_starts:
            ax.axvline(ep, color="red", lw=1.0, ls="--", alpha=0.8)
        if args.run_hddm:
            for fi in fire_cache[d]:
                ax.axvline(fi, color="green", lw=0.8, ls=":", alpha=0.7)
        ax.set_ylabel(f"f{d}")
        ax.set_ylim(-0.02, 1.02)
        ax.legend(loc="upper right", fontsize=7)
    axes[-1][0].set_xlabel("sample seq")
    title = f"HDDM_W per-feature signal — {spec.name} (warmup={args.warmup}, scale={args.scale_mode})"
    if args.run_hddm:
        title += f", HDDM λ={args.lam} (green=DRIFT)"
    fig.suptitle(title, fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.98])
    png_path = OUT_DIR / f"hddm_signal_{spec.name}.png"
    fig.savefig(png_path, dpi=110)
    print(f"[OK] 写出 {png_path}")

    # --- 终端摘要 ---
    print("\n=== 信号可见性摘要(每特征 × 每漂移点的 lift = post_mean − pre_mean)===")
    print(f"{'feat':>4} {'event':>8} {'pre':>7} {'post':>7} {'lift':>8} {'peak':>6}"
          + (f" {'fire?':>6}" if args.run_hddm else ""))
    for r in csv_rows:
        if r["drift_event"] == "":
            continue
        line = (f"{r['feature_id']:>4} {str(r['drift_event']):>8} "
                f"{r['pre_mean']:>7} {r['post_mean']:>7} {r['lift']:>8} {r['post_peak']:>6}")
        if args.run_hddm:
            line += f" {str(r['detected']):>6}"
        print(line)

    # --- 判读提示(不下最终结论)---
    lifts = [float(r["lift"]) for r in csv_rows
             if r["drift_event"] != "" and r["lift"] not in ("", "nan")
             and not math.isnan(float(r["lift"]))]
    if lifts:
        pos = sum(1 for v in lifts if v > 0.02)
        print(f"\n[判读] {pos}/{len(lifts)} 个 (特征×漂移点) 的 signal lift > 0.02 "
              f"(中位 lift={np.median(lifts):+.3f})。")
        print("       lift 明显为正 = 漂移后偏离上升、信号可见,值得上集群 EXP1。")
        print("       若多数 lift≈0 或为负:scale 可能被 warm-up 离群点撑大(试 --scale-mode p99),")
        print("       或该数据集漂移在这些特征上不体现为均值偏移。")
    if args.run_hddm:
        det_cnt = sum(1 for r in csv_rows if r.get("detected") is True)
        ev_cnt = sum(1 for r in csv_rows if r["drift_event"] != "")
        print(f"[判读] HDDM_W 在 {det_cnt}/{ev_cnt} 个 (特征×漂移点) 窗内 fire。")
    print("\n[提醒] 这是离线必要条件检查;最终判定以集群 EXP1 overall_auc 为准"
          "(IKS 基线中位 0.760)。")


if __name__ == "__main__":
    main()
