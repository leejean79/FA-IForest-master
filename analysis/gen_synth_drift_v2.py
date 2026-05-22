#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合成漂移流生成器 v2 / Synthetic drift stream generator v2

为漂移检测延迟测量(detection latency)生成 4 个数据集:
  - synth_abrupt:        突变, 单时刻切换
  - synth_gradual:       渐变, 两个概念按概率混合(Souza 2020 标准定义)
  - synth_incremental:   增量, 持续平移
  - synth_reoccurring:   复现, 多次回切

v2 改动:
  1. gradual 改为概率混合(p(t) 随时间从 0 到 1, 决定每条点属于 C1 或 C2)
  2. 漂移幅度: normal 类 mu 0 → 4 (与 ANOMALY_MU 重合, AUC 漂移前 ~1.0 漂移后 ~0.65)
  3. 异常类 ANOMALY_MU=4, SIGMA=1.5 (与漂移后 normal 类重合, 漂移影响明显)
  4. n=100000 (与 INSECTS 数据量级对齐)
  5. reoccurring 修正切换位置, 与 n=100000 匹配

每个数据集:
  - CSV 文件   : id,f0,f1,f2,f3,f4,label   (与 CsvToDataPointFunction 兼容)
  - 配套 JSON  : 漂移事件时间戳列表        (后续 detection latency 计算用)

约束 / Constraints:
  - 漂移只作用于 normal 类 (label=0) 的分布参数
  - anomaly 类 (label=1) 在整条流上分布不变, 保持 iForest "few and different" 假设
  - 异常占比 5% (与 INSECTS / Online-iForest 论文设置接近)
"""
import argparse
import json
import os
import numpy as np
from pathlib import Path


# ---------------- 配置 ----------------
DEFAULT_N = 100000          # v2: 每个数据集总点数 (50000 → 100000)
DEFAULT_DIM = 5             # 特征维度
ANOMALY_RATIO = 0.05        # 异常占比
SEED = 42                   # 可重复性

# v2: 漂移后 normal 类 mu 从 0 → 4 (与 ANOMALY_MU 重合)
# 经实证, mu_after=4 让漂移后 AUC 从 ~1.0 降到 ~0.65, 漂移影响明显但不"病态翻转"
NORMAL_MU_BEFORE = 0.0
NORMAL_MU_AFTER = 4.0
NORMAL_SIGMA = 1.0

# v2: 异常类 mu 从 8 → 4, sigma 从 2 → 1.5
ANOMALY_MU = 4.0
ANOMALY_SIGMA = 1.5


# ---------------- 各漂移类型的 normal 类均值生成器 ----------------
def normal_means_abrupt(n, dim, drift_at=50000):
    """突变: 第 drift_at 条之前 mu=before, 之后 mu=after"""
    mus = np.full((n, dim), NORMAL_MU_BEFORE)
    mus[drift_at:] = NORMAL_MU_AFTER
    drift_events = [{"type": "abrupt", "start_line": drift_at, "end_line": drift_at + 1}]
    return mus, drift_events


def normal_means_gradual(n, dim, start=40000, end=60000, seed=SEED):
    """
    Gradual (Souza 2020 标准定义): 在 [start, end] 期间, 每条点以概率 p(t)=(t-start)/(end-start)
    属于 C2(mu=after), 否则属于 C1(mu=before). 这是概念间的"概率混合", 而非线性插值.

    与 incremental (线性插值过渡) 的关键区别:
      - gradual: 每条点是 C1 或 C2 之一 (二元), 但 C2 出现频率渐增
      - incremental: 每条点的 mu 是 C1 和 C2 之间的某个插值 (连续)
    """
    rng = np.random.RandomState(seed + 1)  # 与 abrupt seed 错开
    mus = np.full((n, dim), NORMAL_MU_BEFORE)
    for i in range(n):
        if i < start:
            mus[i] = NORMAL_MU_BEFORE
        elif i >= end:
            mus[i] = NORMAL_MU_AFTER
        else:
            p_c2 = (i - start) / (end - start)
            # 以 p_c2 概率属于 C2, 否则属于 C1
            if rng.rand() < p_c2:
                mus[i] = NORMAL_MU_AFTER
            else:
                mus[i] = NORMAL_MU_BEFORE
    drift_events = [{"type": "gradual", "start_line": start, "end_line": end}]
    return mus, drift_events


def normal_means_incremental(n, dim, start=20000, end=80000):
    """
    Incremental: 整段持续从 NORMAL_MU_BEFORE 平移到 NORMAL_MU_AFTER, 每条点的 mu 是插值
    (而非 gradual 的二元采样). 比 gradual 跨度更大、更慢.
    """
    mus = np.full((n, dim), NORMAL_MU_BEFORE)
    for i in range(n):
        if i < start:
            mus[i] = NORMAL_MU_BEFORE
        elif i > end:
            mus[i] = NORMAL_MU_AFTER
        else:
            mus[i] = NORMAL_MU_BEFORE + (NORMAL_MU_AFTER - NORMAL_MU_BEFORE) * (i - start) / (end - start)
    drift_events = [{"type": "incremental", "start_line": start, "end_line": end}]
    return mus, drift_events


def normal_means_reoccurring(n, dim, switches=(30000, 60000, 90000)):
    """复现: 在 switches 时刻 mu 在 NORMAL_MU_BEFORE 和 NORMAL_MU_AFTER 之间切换"""
    mus = np.full((n, dim), NORMAL_MU_BEFORE)
    current = NORMAL_MU_BEFORE
    last = 0
    events = []
    for sw in switches:
        mus[last:sw] = current
        events.append({"type": "abrupt", "start_line": sw, "end_line": sw + 1})
        current = NORMAL_MU_AFTER if current == NORMAL_MU_BEFORE else NORMAL_MU_BEFORE
        last = sw
    mus[last:] = current
    return mus, events


GENERATORS = {
    "synth_abrupt":       normal_means_abrupt,
    "synth_gradual":      normal_means_gradual,
    "synth_incremental":  normal_means_incremental,
    "synth_reoccurring":  normal_means_reoccurring,
}


# ---------------- 采样 ----------------
def generate_stream(name, gen_fn, n, dim, anomaly_ratio, out_dir, seed):
    """生成单个数据集 (CSV + driftspec.json)"""
    rng = np.random.RandomState(seed + hash(name) % 1000)

    # 1. 决定每条记录是 normal 还是 anomaly
    is_anomaly = rng.rand(n) < anomaly_ratio

    # 2. 生成 normal 均值序列 + 漂移事件元数据
    if name == "synth_gradual":
        normal_mus, drift_events = gen_fn(n, dim, seed=seed)
    else:
        normal_mus, drift_events = gen_fn(n, dim)

    # 3. 采样特征
    features = np.zeros((n, dim))
    for i in range(n):
        if is_anomaly[i]:
            features[i] = rng.normal(ANOMALY_MU, ANOMALY_SIGMA, dim)
        else:
            features[i] = rng.normal(normal_mus[i], NORMAL_SIGMA, dim)

    # 4. 写 CSV: id,f0,...,fN,label
    csv_path = out_dir / f"{name}.csv"
    with open(csv_path, "w") as f:
        header = "id," + ",".join(f"f{i}" for i in range(dim)) + ",label"
        f.write(header + "\n")
        for i in range(n):
            row = [str(i)] + [f"{x:.6f}" for x in features[i]] + [str(int(is_anomaly[i]))]
            f.write(",".join(row) + "\n")

    # 5. 写 driftspec JSON
    spec = {
        "dataset": name,
        "n_samples": n,
        "n_features": dim,
        "anomaly_ratio_actual": float(is_anomaly.mean()),
        "anomaly_ratio_target": anomaly_ratio,
        "drift_events": drift_events,
        "csv_columns": ["id"] + [f"f{i}" for i in range(dim)] + ["label"],
        "params": {
            "normal_mu_before": NORMAL_MU_BEFORE,
            "normal_mu_after": NORMAL_MU_AFTER,
            "normal_sigma": NORMAL_SIGMA,
            "anomaly_mu": ANOMALY_MU,
            "anomaly_sigma": ANOMALY_SIGMA,
        },
        "notes": "Drift acts only on normal-class mean; anomaly class is stationary."
    }
    spec_path = out_dir / f"{name}.driftspec.json"
    with open(spec_path, "w") as f:
        json.dump(spec, f, indent=2)

    # 6. 自检打印
    print(f"  {name}: n={n}, dim={dim}, "
          f"anomalies={int(is_anomaly.sum())} ({is_anomaly.mean()*100:.2f}%), "
          f"events={len(drift_events)}")
    print(f"    -> {csv_path}")
    print(f"    -> {spec_path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default="./synth_drift",
                        help="输出目录 / output directory")
    parser.add_argument("--n", type=int, default=DEFAULT_N,
                        help=f"每个数据集点数 / samples per stream (default {DEFAULT_N})")
    parser.add_argument("--dim", type=int, default=DEFAULT_DIM,
                        help=f"特征维度 / feature dim (default {DEFAULT_DIM})")
    parser.add_argument("--ratio", type=float, default=ANOMALY_RATIO,
                        help=f"异常占比 / anomaly ratio (default {ANOMALY_RATIO})")
    parser.add_argument("--seed", type=int, default=SEED, help="随机种子")
    parser.add_argument("--only", default=None,
                        help="只生成指定名字, 逗号分隔(默认全部4个)")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    names = list(GENERATORS.keys())
    if args.only:
        names = [n.strip() for n in args.only.split(",")]

    print(f"Generating {len(names)} synthetic drift streams -> {out_dir}/")
    print(f"v2 params: normal mu {NORMAL_MU_BEFORE}->{NORMAL_MU_AFTER}, "
          f"normal sigma={NORMAL_SIGMA}, anomaly mu={ANOMALY_MU}, sigma={ANOMALY_SIGMA}")
    for name in names:
        if name not in GENERATORS:
            print(f"  WARN: unknown dataset '{name}', skip"); continue
        generate_stream(name, GENERATORS[name], args.n, args.dim, args.ratio, out_dir, args.seed)

    print("\nDONE. To send a stream to Kafka:")
    print(f"  bash deploy/scripts/5-load-data.sh {out_dir}/synth_abrupt.csv 500")


if __name__ == "__main__":
    main()
