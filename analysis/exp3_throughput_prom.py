#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EXP3 吞吐分析(从 Prometheus 拉三个算子口径)。

校准来源:实测 Prometheus 的 operator_name(全部空格/箭头/括号→下划线),故 PromQL
过滤式用下划线名,不可用代码里的原算子名。

三个吞吐口径(numRecordsIn rate,records/sec):
  - 端到端入口(source):operator_name=~"Kafka_Source.*"  受 source 单分区单线限制
  - 检测面:            operator_name=~"Per_Feature.*"    detectionParallelism 并行
  - 打分面:            operator_name=~"Local_Processor.*" 全局 parallelism 并行
排除协议性 source/sink(tree/model/feature-drift/drift-round topic、Coordinator、Aggregator),
它们是控制流,非数据主链路。

延迟:用 Flink 自带算子延迟指标(若 Prometheus 采集了 latency tracking);
     否则回退 scores.jsonl 的 scoreTime−ingestionTime(但 BACKLOG 下含排队,见 exp3_scalability_from_scores.py)。
本脚本聚焦吞吐三口径;延迟分析用另一脚本或 Flink latency metric。

时间窗:每个 run 对应一个 Flink job_id。本脚本按 [start,end] 查 query_range,
       start/end 来自 job-config.json 的 load_end_ts/process_end_ts(若准),或手动传。
       注意:BACKLOG 下 process_end_ts 可能过早(数据灌完≠处理完),建议用 job 在
       Prometheus 中 metrics 存在的实际区间(见 --auto-window)。

CLI:
  # 单 run,显式时间窗:
  python3 exp3_throughput_prom.py --prometheus http://localhost:9090 \
      --start <epoch_s> --end <epoch_s> --parallelism 2

  # 自动窗口(从该 job 的 metrics 存在区间推断 start/end):
  python3 exp3_throughput_prom.py --prometheus http://localhost:9090 \
      --job-id <flink_job_id> --auto-window --parallelism 2

输出:三口径的 avg/median/p95 吞吐(rps),打印 + 可选 --out json。
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from typing import Dict, List, Optional

import numpy as np

# 校准后的算子过滤(实测 operator_name 下划线名)
TIERS = {
    "source_ingress": 'operator_name=~"Kafka_Source.*"',
    "detection":      'operator_name=~"Per_Feature.*"',
    "scoring":        'operator_name=~"Local_Processor.*"',
}


def prom_query_range(base: str, query: str, start: float, end: float, step: int = 15) -> List[Dict]:
    url = base.rstrip("/") + "/api/v1/query_range?" + urllib.parse.urlencode({
        "query": query, "start": f"{start}", "end": f"{end}", "step": f"{step}s",
    })
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read())
    if d.get("status") != "success":
        raise RuntimeError(f"Prometheus query 失败: {d}")
    return d["data"]["result"]


def prom_query_instant(base: str, query: str) -> List[Dict]:
    url = base.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=30) as r:
        d = json.loads(r.read())
    return d["data"]["result"] if d.get("status") == "success" else []


def tier_throughput(base: str, tier_filter: str, start: float, end: float,
                    job_id: Optional[str]) -> Dict:
    # rate over 1m,按 job_id 过滤(隔离不同 run)
    jid = f',job_id="{job_id}"' if job_id else ""
    q = (f'rate(flink_taskmanager_job_task_operator_numRecordsIn'
         f'{{{tier_filter}{jid}}}[1m])')
    series = prom_query_range(base, q, start, end)
    if not series:
        return dict(n_series=0, avg_rps=float("nan"), median_rps=float("nan"),
                    p95_rps=float("nan"))
    # 每个时间点:所有 subtask 的 rate 求和 = 该 tier 总吞吐
    from collections import defaultdict
    pts = defaultdict(float)
    for s in series:
        for t, v in s["values"]:
            try:
                pts[float(t)] += float(v)
            except ValueError:
                pass
    vals = np.array([pts[t] for t in sorted(pts)])
    vals = vals[vals > 0]  # 去掉 job 未真正处理的 0 点
    if vals.size == 0:
        return dict(n_series=len(series), avg_rps=0.0, median_rps=0.0, p95_rps=0.0)
    return dict(
        n_series=len(series),
        avg_rps=float(np.mean(vals)),
        median_rps=float(np.median(vals)),
        p95_rps=float(np.percentile(vals, 95)),
    )


def infer_window(base: str, job_id: str) -> tuple:
    """从该 job 的 numRecordsIn 时间序列存在区间推断 start/end。"""
    q = f'flink_taskmanager_job_task_operator_numRecordsIn{{job_id="{job_id}"}}'
    # 用一个宽 range 查最近数据(过去 2h),取实际有点的首尾
    import time
    now = time.time()
    series = prom_query_range(base, q, now - 7200, now, step=15)
    ts = []
    for s in series:
        for t, _ in s["values"]:
            ts.append(float(t))
    if not ts:
        raise RuntimeError("该 job_id 无 metrics,无法自动推断窗口(确认 job 跑过且未过 retention)")
    return min(ts), max(ts)


def main():
    ap = argparse.ArgumentParser(description="EXP3 三口径吞吐(Prometheus)")
    ap.add_argument("--prometheus", required=True)
    ap.add_argument("--start", type=float, help="epoch 秒")
    ap.add_argument("--end", type=float, help="epoch 秒")
    ap.add_argument("--job-id", help="Flink job_id(隔离 run;--auto-window 必需)")
    ap.add_argument("--auto-window", action="store_true",
                    help="从 job metrics 存在区间自动推断 start/end")
    ap.add_argument("--parallelism", type=int, default=0, help="该 run 的并行度(标注用)")
    ap.add_argument("--out", help="输出 json 路径")
    args = ap.parse_args()

    if args.auto_window:
        if not args.job_id:
            print("[ERROR] --auto-window 需要 --job-id"); sys.exit(1)
        start, end = infer_window(args.prometheus, args.job_id)
        print(f"[auto-window] start={start:.0f} end={end:.0f} (span={end-start:.0f}s)")
    else:
        if args.start is None or args.end is None:
            print("[ERROR] 需 --start/--end 或 --auto-window"); sys.exit(1)
        start, end = args.start, args.end

    print(f"\n{'='*60}\nEXP3 吞吐三口径  P={args.parallelism or '?'}\n{'-'*60}")
    print(f"{'tier':<18}{'series':>7}{'avg_rps':>11}{'median_rps':>12}{'p95_rps':>10}")
    result = {"parallelism": args.parallelism, "start": start, "end": end,
              "job_id": args.job_id, "tiers": {}}
    for name, filt in TIERS.items():
        try:
            st = tier_throughput(args.prometheus, filt, start, end, args.job_id)
        except Exception as e:
            print(f"{name:<18} [ERROR] {e}"); st = {"error": str(e)}
        result["tiers"][name] = st
        if "error" not in st:
            print(f"{name:<18}{st['n_series']:>7}{st['avg_rps']:>11.1f}"
                  f"{st['median_rps']:>12.1f}{st['p95_rps']:>10.1f}")
    print("=" * 60)
    print("口径:source_ingress=端到端入口(单线瓶颈);detection=检测面;scoring=打分面。")
    print("speedup 在合并多并行度时算(吞吐(P)/吞吐(P=1)),本脚本单 run。")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"[OK] {args.out}")


if __name__ == "__main__":
    main()
