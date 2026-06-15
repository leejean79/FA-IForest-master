#!/usr/bin/env bash
# D1a 对照:cooldownPolicy=legacy(现状) vs d1a(进 COOLDOWN 清空+无过滤),各 3 run
# 固定 insects_abrupt × USE_OLD_FOREST × p4 × aggK=2(已有 legacy 单 run 基线:overall_auc=0.736)
#
# 前提:dev 把 D1a 做成可切换参数 cooldownPolicy(legacy/d1a)—— 见 HANDOVER_d1a 的"对照开关"。
#   若改成两个 build(无开关):设 ARMS=(d1a),每个 build 各跑一次本脚本。
#
# 用法(master):RUN_MODE=local bash exp_d1a_compare.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# ======================= 配置 =======================
DATASET=insects_abrupt_imbalanced
CONFIG=BACKLOG_THEN_NEW_FOREST            # 避开 BACKLOG 的 COOLDOWN-EOF 挂死;新森林在 USE_OLD 下仍被使用(per-version AUC 会变)
PARALLELISM=4
AGGK=2                           # 与已有基线一致,只比 D1a 开关
ARMS=(legacy d1a)
RUNS=(1 2 3)
BASE_EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=${AGGK}"
RUN_MODE="${RUN_MODE:-local}"; export RUN_MODE
DRIFT_POINTS="51255,74381,100944,142291,152207"   # 仅供参考/分析;run-experiment 不用
# ====================================================

total=$(( ${#ARMS[@]} * ${#RUNS[@]} )); i=0
for arm in "${ARMS[@]}"; do
  for r in "${RUNS[@]}"; do
    i=$((i+1))
    echo ""
    echo "=== [$i/$total] $DATASET / cooldownPolicy=$arm / p$PARALLELISM / aggK=$AGGK / r$r ==="
    bash deploy/scripts/run-experiment.sh \
      --dataset "$DATASET" --config-id "$CONFIG" --run-id "$r" --parallelism "$PARALLELISM" \
      --extra-param "${BASE_EXTRA};cooldownPolicy=${arm}"
  done
done

echo ""
echo "================================================================"
echo "6 个 run 跑完。结果在 master:results/ ,EXP_ID 含 cooldownPolicy-legacy / -d1a。"
echo "接下来(本地):"
echo "  1) 把 results/ 同步到本地 results-local/(用你平时的同步方式)"
echo "  2) bash analysis/analyze-all.sh        # 产出每个 run 的 drift_*.json"
echo "  3) bash exp_d1a_compare.sh summarize   # 汇总对照表(读 drift_*.json)"
echo "================================================================"

# ---- summarize 子命令:读 drift_*.json,按 arm 汇总 ----
if [[ "${1:-}" == "summarize" ]]; then
  RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results-local}"
  python3 - "$RESULTS_DIR" <<'PY'
import sys, glob, json, os, statistics as st
root=sys.argv[1]
files=glob.glob(os.path.join(root,"**","drift_*.json"),recursive=True) + \
      glob.glob(os.path.join(root,"drift_*.json"))
def arm_of(p):
    if "cooldownPolicy-d1a" in p: return "d1a"
    if "cooldownPolicy-legacy" in p: return "legacy"
    return None
rows={"legacy":[], "d1a":[]}
for f in files:
    a=arm_of(f)
    if not a: continue
    d=json.load(open(f))
    pv=[v["auc"] for v in d.get("per_version",{}).values()]
    rows[a].append({
        "overall_auc": d.get("overall_auc"),
        "min_pv_auc": min(pv) if pv else float("nan"),
        "n_retrains": d.get("n_retrains"),
        "n_versions": len(pv),
        "post_final": d.get("post_final_auc"),
    })
def med(a,k):
    v=[r[k] for r in rows[a] if r[k] is not None]
    return st.median(v) if v else float("nan")
print(f"\n{'metric':<22}{'legacy(med)':>14}{'d1a(med)':>14}{'变化':>10}")
for k,label in [("overall_auc","overall_auc"),("min_pv_auc","最低per-ver AUC"),
                ("n_retrains","n_retrains"),("n_versions","版本数"),("post_final","post_final")]:
    l,dd=med("legacy",k),med("d1a",k)
    try: chg=f"{dd-l:+.3f}"
    except Exception: chg="-"
    print(f"{label:<22}{l:>14.3f}{dd:>14.3f}{chg:>10}")
print(f"\nlegacy runs={len(rows['legacy'])}, d1a runs={len(rows['d1a'])}")
print("\n判据:overall_auc 止跌/回升(主) + 最低per-ver AUC 抬升、sub-0.5 版本消失(辅) + n_retrains 接近。")
print("若三者满足 → D1a 成立;否则上 D1b(原始特征域过滤),先做'异常是否原始特征离群'前提快检。")
PY
fi
