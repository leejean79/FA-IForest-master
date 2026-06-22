#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EXP3 扩展性分析(从 scores.jsonl,不依赖 Prometheus)。

背景:Prometheus scrape 链路未通(容器网络),改用 scores.jsonl 自带的
ingestionTime/scoreTime 算端到端吞吐与延迟。绕开所有集群监控配置。

口径(BACKLOG 模式,关键):
  - ingestionTime = Kafka record timestamp(producer 发送时刻,毫秒)。BACKLOG 下数据
    先全部灌入 Kafka,故所有记录 ingestionTime 集中在 load 期,非处理期。
  - scoreTime = 打分完成时刻(毫秒),分布在处理期。

  * 吞吐(throughput,end-to-end 打分产出速率):
      = n_records / (max(scoreTime) − min(scoreTime)) [records/sec]
    用 scoreTime span 算(打分完成的速率),不可用 ingestionTime(那是灌数据速率)。
    这是系统端到端打分吞吐;source 单分区单线,故为含 source 瓶颈的端到端口径。

  * 延迟(latency):BACKLOG 下 scoreTime−ingestionTime 含"数据在 Kafka backlog 排队"
    时间,非纯系统处理延迟,会很大且随 backlog 深度变 → 失真。本脚本两个视角都给:
      (a) raw_latency = scoreTime − ingestionTime:含排队,BACKLOG 下不代表系统处理延迟,
          仅作参考/上界。
      (b) inter_score_gap:相邻(按 scoreTime 排序)记录的打分间隔统计,反映系统稳态
          处理节奏(每条打分耗时的近似),不含 backlog 排队 → 更接近"系统处理能力"。
    论文延迟若要"系统处理延迟",用 (b) 的视角或在 USE_OLD_FOREST/实时摄入模式另测;
    BACKLOG 的 raw_latency 须标注口径,不可直接当处理延迟。

  扩展性:吞吐/延迟 vs 并行度 P(从 exp_id 的 _pN_ 解析)。

CLI:
  python3 analysis/exp3_scalability_from_scores.py --results-dir results-local
  python3 analysis/exp3_scalability_from_scores.py --results-dir results-local --datasets donors http

输出(analysis/out/):
  exp3_scalability_summary.csv   每 (数据集,并行度) 一行:吞吐、延迟两视角、run 数
  exp3_scalability_runs.csv      每 run 明细
  控制台 打印吞吐/延迟 vs P
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

DEFAULT_DATASETS = ["donors", "http"]
# exp_id 形如 donors_BACKLOG_THEN_NEW_FOREST_default_p2_r1_detectionParallelism-2_...
EXP3_PAT = re.compile(r"^(?P<ds>.+?)_BACKLOG_THEN_NEW_FOREST_default_p(?P<p>\d+)_r(?P<r>\d+)")


def parse_run(path: Path) -> Optional[Dict]:
    ingestion, score = [], []
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
                it = r.get("ingestionTime")
                st = r.get("scoreTime")
                if st is None:
                    continue
                score.append(float(st))
                ingestion.append(float(it) if it is not None else float("nan"))
    except FileNotFoundError:
        return None
    if len(score) < 2:
        return None
    score_arr = np.array(score)
    ing_arr = np.array(ingestion)
    n = len(score_arr)

    # 吞吐:用 scoreTime span(ms → s)
    score_span_s = (score_arr.max() - score_arr.min()) / 1000.0
    throughput_rps = n / score_span_s if score_span_s > 0 else float("nan")

    # 延迟视角 (a):raw = scoreTime − ingestionTime。
    # 先过滤脏时间戳:ingestionTime 必须是合理毫秒戳(> 2020-01-01 ≈ 1.577e12)、
    # ≤ scoreTime、且 raw_lat 在合理上界内(< 10 分钟 = 600000ms)。
    # 脏值来源:某些记录 ingestionTime=0/缺失,导致 raw_lat 爆成天文数字(污染中位)。
    MIN_TS = 1_577_000_000_000   # 2020-01-01 的毫秒戳;小于此视为脏
    MAX_LAT = 600_000            # 10 分钟;超过视为脏(BACKLOG 不堆积时不应这么大)
    valid = (~np.isnan(ing_arr)) & (ing_arr >= MIN_TS) & (score_arr >= ing_arr)
    raw_all = np.where(valid, score_arr - ing_arr, np.nan)
    valid = valid & (raw_all >= 0) & (raw_all <= MAX_LAT)
    n_dirty = int(n - valid.sum())
    frac_dirty = n_dirty / n if n else 0.0
    raw_lat_ms = (score_arr[valid] - ing_arr[valid]) if valid.any() else np.array([])

    # 延迟视角 (b):相邻打分间隔(按 scoreTime 排序),反映稳态处理节奏
    s_sorted = np.sort(score_arr)
    gaps_ms = np.diff(s_sorted)
    gaps_ms = gaps_ms[gaps_ms >= 0]

    # 堆积检测:若数据在堆积,raw_lat 随处理进程单调增长(越后的数据等越久)。
    # 按 ingestionTime(摄入顺序)排序 raw_lat,比较前半段 vs 后半段中位。
    # 后半段显著大于前半段 → 堆积 → 该 run 延迟失真,应作废重调低 rate。
    backlog_flag = False
    backlog_ratio = float("nan")
    lat_first_half = float("nan")
    lat_second_half = float("nan")
    if valid.sum() >= 10:
        ing_valid = ing_arr[valid]
        order = np.argsort(ing_valid)            # 按摄入时间排序
        lat_ordered = raw_lat_ms[order]
        h = len(lat_ordered) // 2
        lat_first_half = float(np.median(lat_ordered[:h]))
        lat_second_half = float(np.median(lat_ordered[h:]))
        # 比值:后半 / 前半。>2 视为明显堆积(后段延迟翻倍以上)
        if lat_first_half > 0:
            backlog_ratio = lat_second_half / lat_first_half
            backlog_flag = backlog_ratio > 2.0
        elif lat_second_half > 50:  # 前半≈0 但后半明显非零,也算堆积苗头
            backlog_flag = True

    def pct(a, q):
        return float(np.percentile(a, q)) if a.size else float("nan")

    return dict(
        n=n,
        throughput_rps=throughput_rps,
        score_span_s=score_span_s,
        raw_lat_median_ms=float(np.median(raw_lat_ms)) if raw_lat_ms.size else float("nan"),
        raw_lat_p95_ms=pct(raw_lat_ms, 95),
        gap_median_ms=float(np.median(gaps_ms)) if gaps_ms.size else float("nan"),
        gap_p95_ms=pct(gaps_ms, 95),
        backlog_flag=backlog_flag,
        backlog_ratio=backlog_ratio,
        lat_first_half_ms=lat_first_half,
        lat_second_half_ms=lat_second_half,
        n_dirty=n_dirty,
        frac_dirty=frac_dirty,
    )


def main():
    ap = argparse.ArgumentParser(description="EXP3 扩展性分析(从 scores.jsonl)")
    ap.add_argument("--results-dir", default="results-local")
    ap.add_argument("--datasets", nargs="*", default=DEFAULT_DATASETS)
    args = ap.parse_args()

    root = (PROJECT_ROOT / args.results_dir if not Path(args.results_dir).is_absolute()
            else Path(args.results_dir))
    if not root.exists():
        print(f"[ERROR] 结果目录不存在: {root}")
        return
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    groups: Dict[Tuple[str, int], List[Dict]] = {}
    run_rows = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        m = EXP3_PAT.match(d.name)
        if not m:
            continue
        ds = m.group("ds")
        if ds not in args.datasets:
            continue
        p = int(m.group("p"))
        st = parse_run(d / "scores.jsonl")
        if st is None:
            print(f"[WARN] {d.name}: scores.jsonl 无效/过短,跳过")
            continue
        st["exp_id"] = d.name
        groups.setdefault((ds, p), []).append(st)
        run_rows.append((ds, p, st))

    if not groups:
        print("[WARN] 未找到 EXP3 run(donors/http × BACKLOG × default × pN)。")
        return

    # per-run 明细
    runs_csv = OUT_DIR / "exp3_scalability_runs.csv"
    with open(runs_csv, "w") as f:
        f.write("dataset,parallelism,exp_id,n,throughput_rps,score_span_s,"
                "raw_lat_median_ms,raw_lat_p95_ms,gap_median_ms,gap_p95_ms,"
                "backlog_flag,backlog_ratio,lat_first_half_ms,lat_second_half_ms\n")
        for ds, p, s in run_rows:
            f.write(f"{ds},{p},{s['exp_id']},{s['n']},{s['throughput_rps']:.2f},"
                    f"{s['score_span_s']:.2f},{s['raw_lat_median_ms']:.2f},"
                    f"{s['raw_lat_p95_ms']:.2f},{s['gap_median_ms']:.4f},{s['gap_p95_ms']:.4f},"
                    f"{int(s['backlog_flag'])},{s['backlog_ratio']:.2f},"
                    f"{s['lat_first_half_ms']:.1f},{s['lat_second_half_ms']:.1f}\n")

    # 汇总 + 扩展性
    summ_csv = OUT_DIR / "exp3_scalability_summary.csv"
    parallelisms = sorted({p for (_, p) in groups})
    print(f"\n{'='*78}\nEXP3 扩展性(从 scores.jsonl)\n{'-'*78}")
    print(f"{'dataset':<10}{'P':>3}{'runs':>5}{'thrpt_rps':>11}{'speedup':>9}"
          f"{'gap_med_ms':>12}{'raw_lat_med_ms':>16}")
    with open(summ_csv, "w") as f:
        f.write("dataset,parallelism,n_runs,throughput_rps_mean,throughput_rps_std,"
                "speedup_vs_p1,gap_median_ms_mean,raw_lat_median_ms_mean\n")
        for ds in args.datasets:
            p1_thrpt = None
            # 先取 P=1 吞吐基线
            runs_p1 = groups.get((ds, 1), [])
            if runs_p1:
                p1_thrpt = statistics.mean([r["throughput_rps"] for r in runs_p1])
            for p in parallelisms:
                runs = groups.get((ds, p), [])
                if not runs:
                    continue
                thr = [r["throughput_rps"] for r in runs]
                thr_m = statistics.mean(thr)
                thr_std = statistics.stdev(thr) if len(thr) > 1 else 0.0
                speedup = (thr_m / p1_thrpt) if p1_thrpt else float("nan")
                gap_m = statistics.mean([r["gap_median_ms"] for r in runs])
                rawl_m = statistics.mean([r["raw_lat_median_ms"] for r in runs
                                          if not np.isnan(r["raw_lat_median_ms"])] or [float("nan")])
                sp_s = f"{speedup:.2f}x" if not np.isnan(speedup) else "—"
                print(f"{ds:<10}{p:>3}{len(runs):>5}{thr_m:>11.1f}{sp_s:>9}"
                      f"{gap_m:>12.4f}{rawl_m:>16.1f}")
                f.write(f"{ds},{p},{len(runs)},{thr_m:.2f},{thr_std:.2f},"
                        f"{speedup:.4f},{gap_m:.4f},{rawl_m:.2f}\n")
    print("=" * 78)
    print(f"[OK] {summ_csv}\n[OK] {runs_csv}")

    # 脏时间戳检测(数据质量把关)
    dirty_runs = [(ds, p, s) for ds, p, s in run_rows if s.get("frac_dirty", 0) > 0.01]
    print(f"\n{'='*78}\n脏时间戳检测(数据质量)\n{'-'*78}")
    if dirty_runs:
        print(f"[警告] {len(dirty_runs)} 个 run 的 ingestionTime 含 >1% 脏值"
              f"(0/缺失/非法,致 raw_lat 失真):")
        for ds, p, s in sorted(dirty_runs, key=lambda x: -x[2]["frac_dirty"]):
            print(f"  {ds} P={p}: 脏 {s['frac_dirty']*100:.1f}% ({s['n_dirty']}/{s['n']})  "
                  f"raw_lat(过滤后)中位={s['raw_lat_median_ms']:.0f}ms  {s['exp_id'][:40]}")
        print("  → 脏值已从 raw_lat 统计中过滤。若脏值比例高(>50%),该 run 延迟不可信,")
        print("    需查 ingestionTime 采集(为何该数据集/并行度的 record timestamp 缺失)。")
    else:
        print("[OK] 各 run 脏时间戳 ≤1%,ingestionTime 采集正常。")

    # 堆积检测汇总(延迟 run 有效性把关)
    flagged = [(ds, p, s) for ds, p, s in run_rows if s.get("backlog_flag")]
    print(f"\n{'='*78}\n堆积检测(延迟 run 把关)\n{'-'*78}")
    if flagged:
        print(f"[警告] {len(flagged)} 个 run 检测到堆积(后半段 raw_lat > 前半段 ×2),"
              f"延迟失真,应作废、调低 rate 重跑:")
        for ds, p, s in flagged:
            print(f"  {ds} P={p}: raw_lat 前半 {s['lat_first_half_ms']:.0f}ms → "
                  f"后半 {s['lat_second_half_ms']:.0f}ms (×{s['backlog_ratio']:.1f})  {s['exp_id']}")
        print("  → 这些 run 的 raw_lat 不可作为处理延迟。降低 JOB_LOAD_RATE(如减半)重跑。")
    else:
        print("[OK] 未检测到明显堆积(各 run 后/前半段 raw_lat 比值 ≤2)。")
        print("     raw_lat 反映真实处理延迟(摄入→打分,未在 backlog 排队),可用于延迟分析。")

    print("\n口径说明(写入论文):")
    print("  • 吞吐 = n / scoreTime_span(端到端打分产出速率;source 单线,含 source 瓶颈)。")
    print("  • speedup = 吞吐(P) / 吞吐(P=1);若 <P 为 sub-linear(source 单线预期内)。")
    print("  • gap_median_ms = 相邻打分间隔中位(稳态处理节奏,不含 backlog 排队)。")
    print("  • raw_lat = scoreTime−ingestionTime:未堆积时=真实处理延迟(摄入→打分完成);")
    print("    堆积时含排队失真(见上方堆积检测)。延迟 run 须用未堆积的 rate。")


if __name__ == "__main__":
    main()
