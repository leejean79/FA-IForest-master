#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INSECTS 原始数据集类频率分布检测 — feature/per-feature-hddm_w 分支。

用途:
    在重建 frequency-based 转换脚本之前,先统计原始数据集末列标签的类频率,
    用于确定转换规则的输入:top-2 最多类 → 正常(0),最少类 → 异常(1),其余丢弃。
    与现有 INSECTS(abrupt/gradual)的转换口径保持一致。

原始格式(由样本行确认):
    - 制表符(tab)分隔
    - 33 个特征列 + 末列标签,共 34 列
    - 无 id 列、无表头

本脚本只读取,不修改、不转换。分块读取,适配 ~120MB 大文件。

输出:
    - 控制台:总行数、列数校验、各类计数与占比(降序)、top-2 与最少类候选
    - 同时把类频率写出 JSON(analysis/out/classfreq_<basename>.json),供转换脚本读取

CLI:
    python3 analysis/insects_class_freq.py /path/to/INSECTS_incremental_imbalanced.csv
    python3 analysis/insects_class_freq.py <file> --sep $'\t' --label-col -1
    python3 analysis/insects_class_freq.py <file> --expect-cols 34
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"


def main():
    ap = argparse.ArgumentParser(description="INSECTS 原始数据集类频率分布检测")
    ap.add_argument("path", help="原始数据集文件路径(tab 分隔、无表头、末列标签)")
    ap.add_argument("--sep", default="\t", help="分隔符(默认 tab)")
    ap.add_argument("--label-col", type=int, default=-1,
                    help="标签列下标(默认 -1 即末列)")
    ap.add_argument("--has-header", action="store_true",
                    help="文件首行是表头(默认无表头)")
    ap.add_argument("--has-id", action="store_true",
                    help="首列是 id(默认无)。仅用于列数校验信息")
    ap.add_argument("--expect-cols", type=int, default=34,
                    help="期望列数(默认 34 = 33 特征 + 标签);不符仅警告")
    ap.add_argument("--chunk", type=int, default=200000, help="分块行数(默认 20 万)")
    args = ap.parse_args()

    path = Path(args.path)
    if not path.exists():
        print(f"[ERROR] 文件不存在:{path}")
        sys.exit(2)

    counter: Counter = Counter()
    n_rows = 0
    ncols_seen = set()
    bad_rows = 0
    first_data_row = None

    with open(path, "r") as f:
        if args.has_header:
            header = f.readline()
            print(f"[INFO] 跳过表头:{header.rstrip()[:120]}")
        buf = []
        for line in f:
            buf.append(line)
            if len(buf) >= args.chunk:
                n_rows, bad_rows, first_data_row = _consume(
                    buf, args, counter, ncols_seen, n_rows, bad_rows, first_data_row)
                buf = []
        if buf:
            n_rows, bad_rows, first_data_row = _consume(
                buf, args, counter, ncols_seen, n_rows, bad_rows, first_data_row)

    if n_rows == 0:
        print("[ERROR] 无有效数据行")
        sys.exit(2)

    # --- 列数校验 ---
    print(f"\n[INFO] 总数据行:{n_rows}")
    print(f"[INFO] 观察到的列数:{sorted(ncols_seen)}")
    if len(ncols_seen) > 1:
        print(f"[WARN] 列数不一致(可能存在脏行或分隔符问题)")
    only_ncols = sorted(ncols_seen)[0] if ncols_seen else 0
    if args.expect_cols and only_ncols != args.expect_cols:
        print(f"[WARN] 列数 {only_ncols} != 期望 {args.expect_cols};"
              f"请确认 --sep / --has-id 是否正确")
    if bad_rows:
        print(f"[WARN] 跳过 {bad_rows} 个无法解析标签的行")
    print(f"[INFO] 首数据行标签={first_data_row}")

    # --- 类频率(降序)---
    total = sum(counter.values())
    ranked = counter.most_common()
    print(f"\n=== 类频率分布(共 {len(ranked)} 类,{total} 样本)===")
    print(f"{'label':>10} {'count':>12} {'ratio':>10}")
    for lab, cnt in ranked:
        print(f"{lab:>10} {cnt:>12} {cnt/total:>9.4f}")

    # --- 转换规则候选 ---
    top2 = [lab for lab, _ in ranked[:2]]
    least = ranked[-1][0]
    discarded = [lab for lab, _ in ranked if lab not in top2 and lab != least]
    anomaly_cnt = counter[least]
    kept = sum(counter[l] for l in top2) + anomaly_cnt
    print(f"\n=== frequency-based 转换规则候选(与现有 INSECTS 口径一致)===")
    print(f"  正常类 normal (top-2 最多): {top2}")
    print(f"  异常类 anomaly (最少类):    {least}  (count={anomaly_cnt})")
    print(f"  丢弃类 discarded:           {discarded}")
    print(f"  转换后保留样本:             {kept}")
    print(f"  转换后异常占比:             {anomaly_cnt/kept:.4f}"
          f"  (现有 INSECTS abrupt 0.0551 / gradual 0.0620 作参照)")

    # 提醒:若最少类占比过高/过低,口径可能需复核
    ratio = anomaly_cnt / kept
    if not (0.02 <= ratio <= 0.12):
        print(f"  [WARN] 异常占比 {ratio:.4f} 偏离现有 INSECTS 范围(~0.05–0.06),"
              f"转换后强度可能不可比,需在离线信号检验阶段重点核对")

    # --- 写 JSON ---
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"classfreq_{path.stem}.json"
    with open(out_path, "w") as f:
        json.dump({
            "source_file": str(path),
            "sep": "tab" if args.sep == "\t" else args.sep,
            "n_rows": n_rows,
            "n_cols": only_ncols,
            "class_counts": dict(counter),
            "class_ratios": {lab: counter[lab] / total for lab in counter},
            "candidate_rule": {
                "normal_labels": top2,
                "anomaly_label": least,
                "discarded_labels": discarded,
                "kept_samples": kept,
                "anomaly_ratio_after": ratio,
            },
        }, f, ensure_ascii=False, indent=2)
    print(f"\n[OK] 写出 {out_path}")
    print("[下一步] 确认规则候选合理后,据此重建 frequency-based 转换脚本;"
          "先对现有 INSECTS 复现已知 driftspec 作正确性校验,再转换新数据集。")


def _consume(buf, args, counter, ncols_seen, n_rows, bad_rows, first_data_row):
    for line in buf:
        line = line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        parts = line.split(args.sep)
        ncols_seen.add(len(parts))
        try:
            lab = parts[args.label_col].strip()
            if lab == "":
                raise ValueError
        except (IndexError, ValueError):
            bad_rows += 1
            continue
        counter[lab] += 1
        n_rows += 1
        if first_data_row is None:
            first_data_row = lab
    return n_rows, bad_rows, first_data_row


if __name__ == "__main__":
    main()
