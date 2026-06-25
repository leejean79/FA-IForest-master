#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
count_committed.py — 从各 run 的 drift-round-topic.jsonl 数投票轮结果。
产出:Table 2b 的 n_committed(across-P)+ §4.3 的 round 级别拆分(yes/no 分布)。

drift-round 消息字段(DriftRoundMessage):roundId, status∈{VOTING,COMMITTED,ABORTED},
votesYes, votesNo, votesAbstain。每轮最终决议有一条 COMMITTED 或 ABORTED(VOTING 是中间态)。

n_committed = distinct roundId 中最终 status=COMMITTED 的轮数(检测真账,非 n_retrains)。

用法(master,EXP1 across-P 跑完后):
  python3 count_committed.py --results-dir /opt/fa-iforest/results
  python3 count_committed.py --results-dir /opt/fa-iforest/results --datasets insects_abrupt_imbalanced

输出:
  exp1_committed_summary.csv  每 (数据集,并行度) 的 n_committed(median over repeats)+ round 拆分
  控制台:Table 2b 的 n_committed 列 + §4.3 的 sub-quorum 比例(如「13 轮 12 aborted」)
"""
from __future__ import annotations
import argparse, json, re, statistics
from collections import defaultdict
from pathlib import Path

# exp_id: insects_abrupt_imbalanced_BACKLOG_THEN_NEW_FOREST_default_p2_r1_iksWindowSize-2000_...
PAT = re.compile(r"^(?P<ds>insects_\w+?_imbalanced)_BACKLOG_THEN_NEW_FOREST_default_p(?P<p>\d+)_r(?P<r>\d+)")


def parse_drift_round(path: Path):
    """返回该 run 的:n_committed, n_aborted, 每轮最终(roundId→(status,yes,no,abstain))。"""
    final = {}  # roundId -> dict(status,yes,no,abstain)  取该 roundId 最后一条决议态
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
                st = r.get("status")
                rid = r.get("roundId")
                if rid is None:
                    continue
                if st in ("COMMITTED", "ABORTED"):   # 只记最终态
                    final[rid] = dict(status=st,
                                      yes=r.get("votesYes", 0),
                                      no=r.get("votesNo", 0),
                                      abstain=r.get("votesAbstain", 0))
    except FileNotFoundError:
        return None
    n_comm = sum(1 for v in final.values() if v["status"] == "COMMITTED")
    n_abort = sum(1 for v in final.values() if v["status"] == "ABORTED")
    return dict(n_committed=n_comm, n_aborted=n_abort, n_rounds=len(final), rounds=final)


def main():
    ap = argparse.ArgumentParser(description="数 n_committed + round 拆分(Table 2b / §4.3)")
    ap.add_argument("--results-dir", default="/opt/fa-iforest/results")
    ap.add_argument("--datasets", nargs="*", default=None)
    args = ap.parse_args()

    root = Path(args.results_dir)
    if not root.exists():
        print(f"[ERROR] 结果目录不存在: {root}"); return

    groups = defaultdict(list)   # (ds,p) -> [run stats]
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        m = PAT.match(d.name)
        if not m:
            continue
        ds, p = m.group("ds"), int(m.group("p"))
        if args.datasets and ds not in args.datasets:
            continue
        st = parse_drift_round(d / "drift-round-topic.jsonl")
        if st is None:
            print(f"[WARN] {d.name}: 无 drift-round-topic.jsonl,跳过"); continue
        groups[(ds, p)].append(st)

    if not groups:
        print("[WARN] 无 EXP1 across-P run(insects_*_imbalanced × BACKLOG × pN)。"); return

    out = Path("exp1_committed_summary.csv")
    parallelisms = sorted({p for _, p in groups})
    datasets = args.datasets or sorted({ds for ds, _ in groups})

    print(f"\n{'='*72}\nTable 2b — n_committed × 并行度(median over repeats)\n{'-'*72}")
    print(f"{'dataset':<28}" + "".join(f"  P={p:<8}" for p in parallelisms))
    with open(out, "w") as f:
        f.write("dataset,parallelism,n_runs,n_committed_med,n_aborted_med,n_rounds_med,sub_quorum_frac\n")
        for ds in datasets:
            cells = []
            for p in parallelisms:
                runs = groups.get((ds, p), [])
                if not runs:
                    cells.append(""); continue
                comm = [r["n_committed"] for r in runs]
                abort = [r["n_aborted"] for r in runs]
                rounds = [r["n_rounds"] for r in runs]
                cm = statistics.median(comm)
                # sub-quorum 比例 = aborted / total rounds(跨 repeat 合并)
                tot_r = sum(rounds); tot_a = sum(abort)
                sq = (tot_a / tot_r) if tot_r else 0.0
                cells.append(f"{cm:.0f}")
                f.write(f"{ds},{p},{len(runs)},{cm:.1f},{statistics.median(abort):.1f},"
                        f"{statistics.median(rounds):.1f},{sq:.3f}\n")
            print(f"{ds:<28}" + "".join(f"  {c:<10}" for c in cells))

    # §4.3 round 拆分:打印每 (ds,P) 的投票轮 yes/no 分布(坐实 sub-quorum)
    print(f"\n{'='*72}\n§4.3 round 级别拆分(sub-quorum 证据)\n{'-'*72}")
    for ds in datasets:
        for p in parallelisms:
            runs = groups.get((ds, p), [])
            if not runs:
                continue
            # 合并所有 repeat 的轮次,按 (yes,no) 分布计数
            dist = defaultdict(int)
            tot_comm = tot_rounds = 0
            for r in runs:
                for rid, v in r["rounds"].items():
                    dist[(v["yes"], v["no"], v["status"])] += 1
                    tot_rounds += 1
                    if v["status"] == "COMMITTED":
                        tot_comm += 1
            if tot_rounds == 0:
                continue
            sq = (tot_rounds - tot_comm) / tot_rounds
            print(f"\n{ds} P={p}: 共 {tot_rounds} 轮(跨 {len(runs)} repeat),"
                  f"{tot_comm} COMMITTED,{tot_rounds-tot_comm} ABORTED(sub-quorum {sq*100:.0f}%)")
            for (y, n, s), c in sorted(dist.items()):
                print(f"    yes={y} no={n} → {s}: {c} 轮")

    print(f"\n[OK] {out}")
    print("\n判读:n_committed 随 P{1,2,4} 是否稳定 → Table 2b 第二列(P-invariance);")
    print("     §4.3 拆分给出『N 轮、M aborted、sub-quorum X%』的确切证据(填 VERIFY)。")


if __name__ == "__main__":
    main()
