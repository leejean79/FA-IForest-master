#!/usr/bin/env bash
# A.4 隔离实验:确认 d1a 在 BACKLOG 下重训翻倍是否由 A.4(ring.isFull 提前终止)引起。
# 三臂对照(BACKLOG × insects_abrupt × p4 × aggK=2 × 3 run):
#   legacy          : cooldownPolicy=legacy
#   d1a_ringfull    : cooldownPolicy=d1a; d1aFill=ringfull      (A.4 开,复现 n_retrains=14)
#   d1a_cdsamples   : cooldownPolicy=d1a; d1aFill=cooldownsamples (A.4 关,隔离臂)
#
# 假设:d1a_cdsamples 的 n_retrains 降回 ~7、overall_auc 恢复到 ≥ legacy,
#       同时保留 d1a 的快速恢复与低假阳性优势。
#
# 前提:dev 已加 d1aFill 开关(见配套提示词)。
# 用法(master):RUN_MODE=local bash exp_d1a_a4_isolation.sh
# 已有 BACKLOG 的 legacy 与 d1a_ringfull 数据时,可把 ARMS 改为只留 d1a_cdsamples 省时间。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# ======================= 配置 =======================
DATASET=insects_abrupt_imbalanced
CONFIG=BACKLOG_THEN_NEW_FOREST
PARALLELISM=4
AGGK=2
RUNS=(1 2 3)
BASE_EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=${AGGK}"
RUN_MODE="${RUN_MODE:-local}"; export RUN_MODE

# 臂标签 → extra-param 追加项
declare -A ARM_EXTRA=(
  [legacy]="cooldownPolicy=legacy"
  [d1a_ringfull]="cooldownPolicy=d1a;d1aFill=ringfull"
  [d1a_cdsamples]="cooldownPolicy=d1a;d1aFill=cooldownsamples"
)
ARMS=(legacy d1a_ringfull d1a_cdsamples)
# ====================================================

total=$(( ${#ARMS[@]} * ${#RUNS[@]} )); i=0
for arm in "${ARMS[@]}"; do
  for r in "${RUNS[@]}"; do
    i=$((i+1))
    echo ""
    echo "=== [$i/$total] $DATASET / $arm / BACKLOG / p$PARALLELISM / aggK=$AGGK / r$r ==="
    bash deploy/scripts/run-experiment.sh \
      --dataset "$DATASET" --config-id "$CONFIG" --run-id "$r" --parallelism "$PARALLELISM" \
      --extra-param "${BASE_EXTRA};${ARM_EXTRA[$arm]}"
  done
done

echo ""
echo "================================================================"
echo "跑完。本地:同步 results/ → results-local/ → bash analysis/analyze-all.sh"
echo "         → bash exp_d1a_a4_isolation.sh summarize"
echo "================================================================"

# ---- summarize:按三臂分组 ----
if [[ "${1:-}" == "summarize" ]]; then
  RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results-local}"
  python3 - "$RESULTS_DIR" <<'PY'
import sys, glob, json, os, statistics as st
root=sys.argv[1]
files=glob.glob(os.path.join(root,"**","drift_*.json"),recursive=True)+glob.glob(os.path.join(root,"drift_*.json"))
def arm_of(p):
    if "BACKLOG" not in p: return None
    if "d1aFill-cooldownsamples" in p: return "d1a_cdsamples"
    if "d1aFill-ringfull" in p:        return "d1a_ringfull"
    if "cooldownPolicy-legacy" in p:   return "legacy"
    return None
data={"legacy":[], "d1a_ringfull":[], "d1a_cdsamples":[]}
for f in files:
    a=arm_of(f)
    if not a: continue
    j=json.load(open(f))
    data[a].append({"overall_auc":j.get("overall_auc"),
                    "n_retrains":j.get("n_retrains"),
                    "post_final":j.get("post_final_auc")})
def med(a,k):
    v=[r[k] for r in data[a] if r.get(k) is not None]
    return st.median(v) if v else float("nan")
print(f"\n{'臂':<16}{'overall_auc':>13}{'n_retrains':>12}{'post_final':>12}{'runs':>6}")
for a in ("legacy","d1a_ringfull","d1a_cdsamples"):
    if not data[a]:
        print(f"{a:<16}{'(无数据)':>13}"); continue
    print(f"{a:<16}{med(a,'overall_auc'):>13.3f}{med(a,'n_retrains'):>12.0f}{med(a,'post_final'):>12.3f}{len(data[a]):>6}")
print("\n判读:")
print("  · d1a_cdsamples 的 n_retrains 若降回 ~7 → 重训翻倍确由 A.4 引起。")
print("  · 且 overall_auc 若 ≥ legacy → d1a(不含 A.4)是干净赢家,可锁 d1a 默认 + d1aFill=cooldownsamples。")
print("  · 若 n_retrains 仍偏高 → 根因是无过滤填充本身使 COOLDOWN 过短,需另设最小持续条数。")
PY
fi
