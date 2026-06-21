#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EXP3 吞吐批量分析 — 遍历结果目录,从每个 job-config.json 读 job_id + 时间窗,
逐 run 调 Prometheus 算三口径吞吐,汇总成扩展性表。

数据源(无需 tsv,直接读结果目录):
  /opt/fa-iforest/results/<exp_id>/job-config.json
    需含:flink_local_job_id(dev diff 已加)、load_end_ts、process_end_ts、parallelism、dataset
  每 run 的 job_id 与目录绑定,不会对错。

口径:三口径吞吐(source_ingress/detection/scoring)从 Prometheus 拉(见 exp3_throughput_prom 逻辑),
     按 flink_local_job_id 隔离(source/检测/打分都在 LocalProcessor job)。

用法(在 master,job 全部跑完后、Prometheus 数据未过期时):
  python3 exp3_throughput_batch.py --results-dir /opt/fa-iforest/results \
      --prometheus http://localhost:9090

  # 只分析某数据集:
  python3 exp3_throughput_batch.py --results-dir /opt/fa-iforest/results \
      --prometheus http://localhost:9090 --datasets donors

输出:
  exp3_throughput_summary.csv  每 (数据集,并行度) 三口径吞吐 + speedup
  控制台扩展性表
"""
from __future__ import annotations

import argparse
import json
import statistics
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

# 三口径(校准后的下划线 operator_name)
TIERS = {
    "source_ingress": 'operator_name=~"Kafka_Source.*"',
    "detection":      'operator_name=~"Per_Feature.*"',
    "scoring":        'operator_name=~"Local_Processor.*"',
}


def prom_query_range(base, query, start, end, step=15):
    url = base.rstrip("/") + "/api/v1/query_range?" + urllib.parse.urlencode({
        "query": query, "start": f"{start}", "end": f"{end}", "step": f"{step}s"})
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read())
    return d["data"]["result"] if d.get("status") == "success" else []


def tier_rps(base, tier_filter, start, end, job_id) -> float:
    """该 tier 在 [start,end] 的中位总吞吐(所有 subtask rate 求和)。"""
    q = (f'rate(flink_taskmanager_job_task_operator_numRecordsIn'
         f'{{{tier_filter},job_id="{job_id}"}}[1m])')
    series = prom_query_range(base, q, start, end)
    if not series:
        return float("nan")
    pts = defaultdict(float)
    for s in series:
        for t, v in s["values"]:
            try:
                pts[float(t)] += float(v)
            except ValueError:
                pass
    vals = np.array([pts[t] for t in sorted(pts)])
    vals = vals[vals > 0]
    return float(np.median(vals)) if vals.size else 0.0


def main():
    ap = argparse.ArgumentParser(description="EXP3 吞吐批量分析(遍历结果目录)")
    ap.add_argument("--results-dir", default="/opt/fa-iforest/results")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--datasets", nargs="*", default=None, help="限定数据集(默认全部)")
    args = ap.parse_args()

    root = Path(args.results_dir)
    if not root.exists():
        print(f"[ERROR] 结果目录不存在: {root}"); return

    # groups[(ds,p)] = [ {tier: rps} per run ]
    groups: Dict = defaultdict(list)
    skipped = []
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        cfg_path = d / "job-config.json"
        if not cfg_path.exists():
            continue
        try:
            cfg = json.loads(cfg_path.read_text())
        except Exception:
            skipped.append((d.name, "job-config.json 解析失败")); continue
        ds = cfg.get("dataset")
        p = cfg.get("parallelism")
        jid = cfg.get("flink_local_job_id")
        le = cfg.get("load_end_ts"); pe = cfg.get("process_end_ts")
        if args.datasets and ds not in args.datasets:
            continue
        if not jid or jid == "":
            skipped.append((d.name, "无 flink_local_job_id(需 dev diff)")); continue
        if not le or not pe or pe <= le:
            # 时间窗无效(BACKLOG 下 process_end 可能过早),回退:用 job metrics 存在区间
            # 这里简单用 [le, le+处理估计];更稳可在 exp3_throughput_prom 用 --auto-window。
            skipped.append((d.name, f"时间窗无效 load_end={le} process_end={pe};建议单跑用 --auto-window"))
            continue
        rps = {tier: tier_rps(args.prometheus, filt, le, pe, jid)
               for tier, filt in TIERS.items()}
        groups[(ds, p)].append(rps)

    if not groups:
        print("[WARN] 无可分析 run。")
        for name, why in skipped:
            print(f"  跳过 {name}: {why}")
        return

    # 汇总 + speedup
    out = Path("exp3_throughput_summary.csv")
    parallelisms = sorted({p for (_, p) in groups})
    datasets = args.datasets or sorted({ds for (ds, _) in groups})
    print(f"\n{'='*76}\nEXP3 三口径吞吐 × 并行度\n{'-'*76}")
    print(f"{'dataset':<10}{'P':>3}{'runs':>5}{'source_rps':>12}{'detect_rps':>12}"
          f"{'scoring_rps':>12}{'scoring_speedup':>16}")
    with open(out, "w") as f:
        f.write("dataset,parallelism,n_runs,source_rps,detection_rps,scoring_rps,scoring_speedup\n")
        for ds in datasets:
            p1_scoring = None
            if (ds, 1) in groups:
                p1_scoring = statistics.median([r["scoring"] for r in groups[(ds, 1)]
                                                if not np.isnan(r["scoring"])] or [float("nan")])
            for p in parallelisms:
                runs = groups.get((ds, p), [])
                if not runs:
                    continue
                def med(tier):
                    xs = [r[tier] for r in runs if not np.isnan(r[tier])]
                    return statistics.median(xs) if xs else float("nan")
                src, det, sco = med("source_ingress"), med("detection"), med("scoring")
                sp = (sco / p1_scoring) if (p1_scoring and not np.isnan(p1_scoring)) else float("nan")
                sp_s = f"{sp:.2f}x" if not np.isnan(sp) else "—"
                print(f"{ds:<10}{p:>3}{len(runs):>5}{src:>12.1f}{det:>12.1f}{sco:>12.1f}{sp_s:>16}")
                f.write(f"{ds},{p},{len(runs)},{src:.2f},{det:.2f},{sco:.2f},"
                        f"{sp if not np.isnan(sp) else ''}\n")
    print("=" * 76)
    if skipped:
        print(f"\n跳过 {len(skipped)} 个 run:")
        for name, why in skipped[:10]:
            print(f"  {name}: {why}")
    print(f"\n[OK] {out}")
    print("\n判读:source_ingress 受单分区限(~7000 端到端瓶颈);detection≈source×特征数(展开);")
    print("     scoring_speedup<P 为 sub-linear(端到端被 source 单线限制,符合架构预期)。")


if __name__ == "__main__":
    main()
