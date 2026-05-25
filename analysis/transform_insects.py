#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INSECTS 异常转换 / INSECTS anomaly transformation

转换规则 (基于频率, 不依赖物种名):
  1. 统计原始 CSV 最后一列 (类标签) 的频次
  2. 频次最高的 2 个类 → normal (label=0)
  3. 频次最低的 1 个类 → anomaly (label=1)
  4. 其他类 → 丢弃

这种方法对应 Online-iForest 论文 "2 多数类 + 1 少数类" 的选法,
理论比例 ~5.5% (abrupt_imbalanced 验证: 11701/212514 = 5.506%)。

输入: CSV 文件 (无 header, 33 维特征 + 1 列 label)
输出:
  - <name>_transformed.csv    (转换后, 列: f0..f32,label)
  - <name>.driftspec.json     (漂移点元数据, 行号已映射到转换后)

漂移行号映射:
  原始 CSV 第 N 行 → 转换后 CSV 第 N' 行
  N' = (N 之前被保留的行数)

用法 / Usage:
  python3 transform_insects.py \\
      --input "INSECTS abrupt_imbalanced.csv" \\
      --output-dir transformed/ \\
      --drift-points 83859 128651 182320 242883 268380 \\
      --drift-type abrupt
"""
import argparse
import csv
import json
import os
from pathlib import Path
from collections import Counter


def transform(input_path: Path, output_dir: Path, drift_points: list[int],
              drift_type: str = "abrupt"):
    """
    转换主函数

    :param input_path: 原始 CSV
    :param output_dir: 输出目录
    :param drift_points: 原始 CSV 中的漂移行号 (1-based)
    :param drift_type: abrupt / gradual / incremental / reoccurring
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # 派生文件名 (去掉空格, 加 _transformed 后缀)
    base = input_path.stem.replace(" ", "_")
    out_csv = output_dir / f"{base}_transformed.csv"
    out_spec = output_dir / f"{base}_transformed.driftspec.json"

    # ---------- Pass 1: 统计类频次 + 总行数 ----------
    print(f"[1/3] Pass 1: counting labels in {input_path.name} ...")
    label_count = Counter()
    total_lines = 0
    with open(input_path, "r", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            label_count[row[-1].strip()] += 1
            total_lines += 1

    print(f"  Total lines: {total_lines}")
    print(f"  Label distribution:")
    for lab, cnt in label_count.most_common():
        print(f"    {lab}: {cnt}")

    if len(label_count) < 3:
        raise ValueError(f"Need at least 3 classes, got {len(label_count)}: "
                         f"{list(label_count.keys())}")

    # ---------- 选 normal/anomaly ----------
    sorted_labels = label_count.most_common()
    normal_labels = {sorted_labels[0][0], sorted_labels[1][0]}    # 最多 2 个
    anomaly_label = sorted_labels[-1][0]                          # 最少 1 个

    n_normal = sum(label_count[l] for l in normal_labels)
    n_anomaly = label_count[anomaly_label]
    n_keep = n_normal + n_anomaly
    n_drop = total_lines - n_keep

    print(f"\n  Selection:")
    print(f"    normal (label=0): {sorted(normal_labels)} → {n_normal} rows")
    print(f"    anomaly (label=1): [{anomaly_label}] → {n_anomaly} rows")
    print(f"    discard: {n_drop} rows ({n_drop/total_lines*100:.1f}%)")
    print(f"    kept total: {n_keep}")
    print(f"    anomaly ratio: {n_anomaly/n_keep*100:.3f}%")

    # ---------- Pass 2: 写转换后 CSV + 构建行号映射 ----------
    print(f"\n[2/3] Pass 2: writing transformed CSV ...")
    # original_row → transformed_row (1-based, 只对被保留的行)
    # 用 dict 太占内存 (35万行), 用列表:
    # original_to_transformed[i] = 转换后行号 (i 是原始行号 0-based; 被丢弃的为 -1)
    original_to_transformed = []
    transformed_count = 0

    # 写 header (CSV 列名, 含 id)
    header = ["id"] + [f"f{i}" for i in range(33)] + ["label"]

    with open(input_path, "r", newline="") as fin, \
         open(out_csv, "w", newline="") as fout:
        reader = csv.reader(fin)
        # lineterminator='\n' 强制 Unix 行结尾, 避免 Flink 解析时把 \r 吃进 label 字段
        writer = csv.writer(fout, lineterminator='\n')
        writer.writerow(header)

        for orig_idx, row in enumerate(reader):
            if not row:
                original_to_transformed.append(-1)
                continue
            lab = row[-1].strip()
            if lab in normal_labels:
                new_label = "0"
            elif lab == anomaly_label:
                new_label = "1"
            else:
                original_to_transformed.append(-1)
                continue

            features = [f.strip() for f in row[:-1]]
            transformed_count += 1
            # id 用转换后连续顺序号 (1-based, 与 synth 数据集格式一致)
            writer.writerow([transformed_count] + features + [new_label])
            # 转换后行号 = transformed_count (1-based)
            original_to_transformed.append(transformed_count)

    print(f"  Wrote {transformed_count} rows to {out_csv}")

    # ---------- Pass 3: 映射漂移点 ----------
    print(f"\n[3/3] Mapping drift points ...")
    mapped_drifts = []
    for orig_dp in drift_points:
        # 原始 1-based 行号 → 0-based 索引
        idx = orig_dp - 1
        if idx < 0 or idx >= len(original_to_transformed):
            print(f"  WARN: drift point {orig_dp} out of range, skip")
            continue

        # 找映射后的行号
        # 如果该行被丢弃了, 找前一个保留的行号 (向下舍入)
        new_dp = original_to_transformed[idx]
        if new_dp == -1:
            # 这个原始行被丢弃了, 找前面最近的保留行
            j = idx - 1
            while j >= 0 and original_to_transformed[j] == -1:
                j -= 1
            new_dp = original_to_transformed[j] if j >= 0 else 1
            print(f"  drift @ orig {orig_dp} (discarded) → transformed {new_dp} (nearest kept)")
        else:
            print(f"  drift @ orig {orig_dp} → transformed {new_dp}")
        mapped_drifts.append({"original_line": orig_dp, "transformed_line": new_dp})

    # 写 driftspec
    spec = {
        "dataset": base + "_transformed",
        "source": str(input_path.name),
        "transformation": {
            "method": "frequency_based",
            "normal_original_labels": sorted(normal_labels),
            "anomaly_original_label": anomaly_label,
            "discarded_labels": sorted(set(label_count.keys()) - normal_labels - {anomaly_label}),
        },
        "n_samples_original": total_lines,
        "n_samples_transformed": transformed_count,
        "n_features": 33,
        "anomaly_count": n_anomaly,
        "anomaly_ratio": round(n_anomaly / n_keep, 5),
        "drift_type": drift_type,
        "drift_events": [
            {
                "type": drift_type,
                "start_line": d["transformed_line"],    # analyze_old.py 读这个字段
                "original_line": d["original_line"],    # 保留追溯原始 CSV 行号
            }
            for d in mapped_drifts
        ],
        "notes": (
            f"Frequency-based transformation: "
            f"top-2 most populous classes ({sorted(normal_labels)}) → normal(0); "
            f"least populous class ([{anomaly_label}]) → anomaly(1); "
            f"others discarded. "
            f"start_line refers to the transformed CSV row number (1-based, excluding header); "
            f"original_line refers to the source CSV before transformation."
        ),
    }
    with open(out_spec, "w") as f:
        json.dump(spec, f, indent=2)

    print(f"\nDONE.")
    print(f"  CSV:  {out_csv}")
    print(f"  Spec: {out_spec}")
    return out_csv, out_spec


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--input", required=True, type=Path,
                        help="Original INSECTS CSV (无 header, 33 维 + label)")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="输出目录 (会自动创建)")
    parser.add_argument("--drift-points", nargs="*", type=int, default=[],
                        help="原始 CSV 中的漂移行号 (1-based), 多个用空格分隔")
    parser.add_argument("--drift-type", default="abrupt",
                        choices=["abrupt", "gradual", "incremental", "reoccurring"],
                        help="漂移类型 (默认 abrupt)")
    args = parser.parse_args()

    transform(args.input, args.output_dir, args.drift_points, args.drift_type)


if __name__ == "__main__":
    main()
