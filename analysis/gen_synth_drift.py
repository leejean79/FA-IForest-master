#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
合成漂移流生成器 / Synthetic drift stream generator

为漂移检测延迟测量(detection latency)生成 4 个数据集:
  - synth_abrupt:        突变, 单时刻切换
  - synth_gradual:       渐变, 线性混合过渡
  - synth_incremental:   增量, 持续平移
  - synth_reoccurring:   复现, 多次回切

每个数据集:
  - CSV 文件   : id,f0,f1,f2,f3,f4,label   (与 CsvToDataPointFunction 兼容)
  - 配套 JSON  : 漂移事件时间戳列表        (后续 detection latency 计算用)

约束 / Constraints:
  - 漂移只作用于 normal 类 (label=0) 的分布参数
  - anomaly 类 (label=1) 在整条流上分布不变, 保持 iForest "few and different" 假设
  - 异常占比 5% (与 INSECTS / Online-iForest 论文设置接近)
  - normal 类 ~ N(mu, I), anomaly 类 ~ N(8, 4*I) (远离 normal, 方差更大)
"""
import argparse
import json
import os
import numpy as np
from pathlib import Path


# ---------------- 配置 ----------------
DEFAULT_N = 50000           # 每个数据集总点数
DEFAULT_DIM = 5             # 特征维度
ANOMALY_RATIO = 0.05        # 异常占比
SEED = 42                   # 可重复性

ANOMALY_MU = 8.0            # 异常类均值标量
ANOMALY_SIGMA = 2.0         # 异常类标准差


# ---------------- 各漂移类型的 normal 类均值生成器 ----------------
def normal_means_abrupt(n, dim, drift_at=25000):
    """突变: 第 drift_at 条之前 mu=[0,...], 之后 mu=[3,...]"""
    mus = np.zeros((n, dim))
    mus[drift_at:] = 3.0
    drift_events = [{"type": "abrupt", "start_line": drift_at, "end_line": drift_at + 1}]
    return mus, drift_events


def normal_means_gradual(n, dim, start=20000, end=30000):
    """渐变: [start, end] 线性从 0 → 3"""
    mus = np.zeros((n, dim))
    for i in range(n):
        if i < start:
            mus[i] = 0.0
        elif i > end:
            mus[i] = 3.0
        else:
            mus[i] = 3.0 * (i - start) / (end - start)
    drift_events = [{"type": "gradual", "start_line": start, "end_line": end}]
    return mus, drift_events


def normal_means_incremental(n, dim, start=10000, end=40000):
    """增量: 整段持续从 0 平移到 3, 比 gradual 跨度更大、更慢"""
    mus = np.zeros((n, dim))
    for i in range(n):
        if i < start:
            mus[i] = 0.0
        elif i > end:
            mus[i] = 3.0
        else:
            mus[i] = 3.0 * (i - start) / (end - start)
    drift_events = [{"type": "incremental", "start_line": start, "end_line": end}]
    return mus, drift_events


def normal_means_reoccurring(n, dim, switches=(15000, 30000, 45000)):
    """复现: 在 switches 时刻 mu 在 0 和 3 之间切换"""
    mus = np.zeros((n, dim))
    current = 0.0
    last = 0
    events = []
    for sw in switches:
        mus[last:sw] = current
        events.append({"type": "abrupt", "start_line": sw, "end_line": sw + 1})
        current = 3.0 if current == 0.0 else 0.0
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
    normal_mus, drift_events = gen_fn(n, dim)

    # 3. 采样特征
    features = np.zeros((n, dim))
    for i in range(n):
        if is_anomaly[i]:
            features[i] = rng.normal(ANOMALY_MU, ANOMALY_SIGMA, dim)
        else:
            features[i] = rng.normal(normal_mus[i], 1.0, dim)

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
    for name in names:
        if name not in GENERATORS:
            print(f"  WARN: unknown dataset '{name}', skip"); continue
        generate_stream(name, GENERATORS[name], args.n, args.dim, args.ratio, out_dir, args.seed)

    print("\nDONE. To send a stream to Kafka:")
    print(f"  bash deploy/scripts/5-load-data.sh {out_dir}/synth_abrupt.csv 500")


if __name__ == "__main__":
    main()
