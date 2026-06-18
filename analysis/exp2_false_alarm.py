#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EXP2 误触发率分析 — stationary(无漂移、shuffle 打散)流上的 per-feature 检测器精度基线。

EXP2 语义:
    对 stationary 数据集(donors/http/forestcover/fraud/mulcross)做 shuffle 打散,
    消除任何时序结构 → 流中无真实概念漂移。理想检测器应几乎不触发漂移、不重训。
    本脚本统计"误触发"程度,作为 per-feature 检测器在无漂移流上的精度基线。
    主检测器 = IKS(EXP1 已定),EXP2 用 IKS 跑。

误触发口径(基于 scores.jsonl,EXP2 唯一产物):
    run 产物只有 scores.jsonl(每行 JSON:seq/id/score/label/forestVersion/phase)。
    无单独 drift/committed 事件流,故用 forestVersion 间接测误触发:
      - n_false_retrains = distinct(forestVersion) − 1
        stationary 无真实漂移,理想 = 0;每多一个版本 = 一次误触发导致的误重训。
      - 辅证:phase 字段出现 COOLDOWN/WAITING 的占比(进入过漂移响应状态的比例)。
    误触发率以"每 10 万样本的误重训次数"归一,便于跨数据集(流长不同)比较。

与现有 analyze.py --mode stationary 的关系:
    后者算 stationary AUC(稳态检测质量基线),本脚本补"误触发/误重训"基线,两者互补。
    论文 EXP2 表:每数据集 报 误重训次数(均值±std)+ 误触发率(每10万)+ 稳态 AUC。

CLI:
    python3 analysis/exp2_false_alarm.py --results-dir results-local
    python3 analysis/exp2_false_alarm.py --results-dir results-local --datasets donors http
    # 默认匹配 {ds}_USE_OLD_FOREST_default_p*_r* 的 EXP2 run(default=IKS 主检测器)

输出:
    analysis/out/exp2_false_alarm_summary.csv  每数据集一行:误重训均值/std、误触发率、run 数
    analysis/out/exp2_false_alarm_runs.csv      每 run 一行(便于查异常 run)
    控制台 打印汇总表
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Dict, List, Optional

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[1]
OUT_DIR = PROJECT_ROOT / "analysis" / "out"

DEFAULT_DATASETS = ["donors", "http", "forestcover", "fraud", "mulcross"]
# EXP2 用 USE_OLD_FOREST + default(=IKS)。兼容旧 C1 前缀。
EXP2_PATTERNS = [
    re.compile(r"^(?P<ds>.+?)_USE_OLD_FOREST_default_p\d+_r\d+"),
    re.compile(r"^(?P<ds>.+?)_C1_"),
]


def parse_scores(path: Path) -> Optional[Dict]:
    """读一个 run 的 scores.jsonl,返回误触发统计。"""
    versions = set()
    n = 0
    n_cooldown_waiting = 0
    has_phase = False
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
                n += 1
                fv = r.get("forestVersion")
                if fv is not None:
                    versions.add(fv)
                ph = r.get("phase")
                if ph is not None:
                    has_phase = True
                    if str(ph).upper() in ("COOLDOWN", "WAITING"):
                        n_cooldown_waiting += 1
    except FileNotFoundError:
        return None
    if n == 0:
        return None
    n_false_retrains = max(len(versions) - 1, 0)  # distinct version − 1
    return dict(
        n=n,
        n_false_retrains=n_false_retrains,
        false_alarm_per_100k=(n_false_retrains / n * 100_000) if n else 0.0,
        cooldown_waiting_frac=(n_cooldown_waiting / n) if (has_phase and n) else None,
    )


def match_dataset(exp_id: str, datasets: List[str]) -> Optional[str]:
    for pat in EXP2_PATTERNS:
        m = pat.match(exp_id)
        if m:
            ds = m.groupdict().get("ds")
            # C1 模式没有具名 ds 组时,从前缀粗取
            if ds is None:
                ds = exp_id.split("_")[0]
            # 仅接受目标数据集(避免把 EXP1 的 INSECTS 误纳入)
            for cand in datasets:
                if ds == cand or exp_id.startswith(cand + "_"):
                    return cand
    return None


def main():
    ap = argparse.ArgumentParser(description="EXP2 误触发率分析(stationary)")
    ap.add_argument("--results-dir", default="results-local",
                    help="结果根目录(含各 run 子目录,每个含 scores.jsonl)")
    ap.add_argument("--datasets", nargs="*", default=DEFAULT_DATASETS)
    args = ap.parse_args()

    results_root = (PROJECT_ROOT / args.results_dir
                    if not Path(args.results_dir).is_absolute()
                    else Path(args.results_dir))
    if not results_root.exists():
        print(f"[ERROR] 结果目录不存在: {results_root}")
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 收集每数据集的 run
    per_ds_runs: Dict[str, List[Dict]] = {ds: [] for ds in args.datasets}
    run_rows = []
    for d in sorted(results_root.iterdir()):
        if not d.is_dir():
            continue
        exp_id = d.name
        ds = match_dataset(exp_id, args.datasets)
        if ds is None:
            continue
        stat = parse_scores(d / "scores.jsonl")
        if stat is None:
            print(f"[WARN] {exp_id}: scores.jsonl 缺失或空,跳过")
            continue
        stat["exp_id"] = exp_id
        per_ds_runs[ds].append(stat)
        run_rows.append((ds, exp_id, stat))

    # 写 per-run CSV
    runs_csv = OUT_DIR / "exp2_false_alarm_runs.csv"
    with open(runs_csv, "w") as f:
        f.write("dataset,exp_id,n,n_false_retrains,false_alarm_per_100k,cooldown_waiting_frac\n")
        for ds, exp_id, s in run_rows:
            cw = "" if s["cooldown_waiting_frac"] is None else f"{s['cooldown_waiting_frac']:.5f}"
            f.write(f"{ds},{exp_id},{s['n']},{s['n_false_retrains']},"
                    f"{s['false_alarm_per_100k']:.4f},{cw}\n")

    # 写汇总 CSV + 控制台
    summ_csv = OUT_DIR / "exp2_false_alarm_summary.csv"
    print(f"\n{'='*72}")
    print(f"{'dataset':<14}{'runs':>5}{'retrains_mean':>15}{'retrains_std':>14}"
          f"{'falsealarm/100k_mean':>22}")
    print("-" * 72)
    with open(summ_csv, "w") as f:
        f.write("dataset,n_runs,false_retrains_mean,false_retrains_std,"
                "false_alarm_per_100k_mean,false_alarm_per_100k_std,perfect_runs_frac\n")
        for ds in args.datasets:
            runs = per_ds_runs[ds]
            if not runs:
                print(f"{ds:<14}{'0':>5}  (无 EXP2 run,未跑或未拉取)")
                f.write(f"{ds},0,,,,,\n")
                continue
            retr = [r["n_false_retrains"] for r in runs]
            fa = [r["false_alarm_per_100k"] for r in runs]
            retr_mean = statistics.mean(retr)
            retr_std = statistics.stdev(retr) if len(retr) > 1 else 0.0
            fa_mean = statistics.mean(fa)
            fa_std = statistics.stdev(fa) if len(fa) > 1 else 0.0
            perfect = sum(1 for x in retr if x == 0) / len(retr)  # 零误触发 run 占比
            print(f"{ds:<14}{len(runs):>5}{retr_mean:>15.3f}{retr_std:>14.3f}"
                  f"{fa_mean:>22.4f}")
            f.write(f"{ds},{len(runs)},{retr_mean:.4f},{retr_std:.4f},"
                    f"{fa_mean:.4f},{fa_std:.4f},{perfect:.4f}\n")
    print("=" * 72)
    print(f"[OK] {summ_csv}")
    print(f"[OK] {runs_csv}")
    print("\n判读:n_false_retrains 理想=0(stationary 无真实漂移);"
          "perfect_runs_frac=零误触发 run 占比,越高越好。")
    print("注:误触发口径 = distinct(forestVersion)−1(scores.jsonl 间接测),"
          "非 drift 事件直接计数;论文须说明此口径。")


if __name__ == "__main__":
    main()
