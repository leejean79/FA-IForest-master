#!/usr/bin/env bash
# D1a 无回归确认:在 synth_abrupt 与 insects_gradual_imbalanced 上对比 legacy vs d1a。
# 目的:确认 D1a(进入 COOLDOWN 清空训练池 + 取消基于旧森林分数的过滤)不会损害这两个数据集。
#
# 与 exp_d1a_compare.sh 分开:这两个数据集的漂移点与 insects_abrupt 不同。
# 说明:run-experiment.sh 不使用漂移点;每个数据集的漂移点由 analyze-all.sh 在分析阶段自动应用。
#
# 前提:同 exp_d1a_compare.sh —— 程序需支持运行参数 cooldownPolicy(legacy/d1a)。
#   若改为两个 build:设 ARMS=(d1a),每个 build 各运行一次本脚本。
#
# 用法(master 节点):RUN_MODE=local bash exp_d1a_noregression.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# ======================= 配置 =======================
DATASETS=(synth_abrupt insects_gradual_imbalanced)
ARMS=(legacy d1a)
RUNS=(1)                          # 先单次看方向;要看方差改成 (1 2 3)
PARALLELISM=4
AGGK=2
CONFIG=USE_OLD_FOREST
BASE_EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=${AGGK}"
RUN_MODE="${RUN_MODE:-local}"; export RUN_MODE
# ====================================================

total=$(( ${#DATASETS[@]} * ${#ARMS[@]} * ${#RUNS[@]} )); i=0
for ds in "${DATASETS[@]}"; do
  for arm in "${ARMS[@]}"; do
    for r in "${RUNS[@]}"; do
      i=$((i+1))
      echo ""
      echo "=== [$i/$total] $ds / cooldownPolicy=$arm / p$PARALLELISM / aggK=$AGGK / r$r ==="
      bash deploy/scripts/run-experiment.sh \
        --dataset "$ds" --config-id "$CONFIG" --run-id "$r" --parallelism "$PARALLELISM" \
        --extra-param "${BASE_EXTRA};cooldownPolicy=${arm}"
    done
  done
done

echo ""
echo "================================================================"
echo "全部跑完。接下来(本地):"
echo "  1) 把 master:results/ 同步到本地 results-local/"
echo "  2) bash analysis/analyze-all.sh                  # 按各数据集正确的漂移点生成 drift_*.json"
echo "  3) bash exp_d1a_noregression.sh summarize        # 按 (数据集, 组) 汇总对比"
echo "================================================================"

# ---- summarize:按 (数据集, 组) 分组,不跨数据集混合 ----
if [[ "${1:-}" == "summarize" ]]; then
  RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/results-local}"
  python3 - "$RESULTS_DIR" <<'PY'
import sys, glob, json, os, statistics as st
root=sys.argv[1]
files=glob.glob(os.path.join(root,"**","drift_*.json"),recursive=True)+glob.glob(os.path.join(root,"drift_*.json"))
DATASETS=["synth_abrupt","insects_gradual_imbalanced"]
def ds_of(p):
    # 注意 insects_gradual 优先匹配,避免被 abrupt 误配
    if "insects_gradual_imbalanced" in p: return "insects_gradual_imbalanced"
    if "synth_abrupt" in p: return "synth_abrupt"
    return None
def arm_of(p):
    if "cooldownPolicy-d1a" in p: return "d1a"
    if "cooldownPolicy-legacy" in p: return "legacy"
    return None
data={d:{"legacy":[], "d1a":[]} for d in DATASETS}
for f in files:
    d, a = ds_of(f), arm_of(f)
    if not d or not a: continue
    j=json.load(open(f))
    data[d][a].append({"overall_auc":j.get("overall_auc"),
                       "post_final":j.get("post_final_auc"),
                       "n_retrains":j.get("n_retrains")})
def med(rs,k):
    v=[r[k] for r in rs if r.get(k) is not None]
    return st.median(v) if v else float("nan")
for d in DATASETS:
    L,D=data[d]["legacy"],data[d]["d1a"]
    if not L and not D:
        print(f"\n[{d}] 无结果文件"); continue
    print(f"\n[{d}]  (legacy {len(L)} 次, d1a {len(D)} 次)")
    print(f"  {'指标':<14}{'legacy':>12}{'d1a':>12}{'变化':>10}")
    for k,label in [("overall_auc","overall_auc"),("post_final","post_final"),("n_retrains","n_retrains")]:
        l,dd=med(L,k),med(D,k)
        try: chg=f"{dd-l:+.3f}"
        except Exception: chg="-"
        print(f"  {label:<14}{l:>12.3f}{dd:>12.3f}{chg:>10}")
print("\n判据:实验组(d1a)的 overall_auc / post_final 不低于对照组(legacy)即为无回归。")
PY
fi
