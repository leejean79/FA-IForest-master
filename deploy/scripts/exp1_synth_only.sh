#!/usr/bin/env bash
# EXP1 — 仅 synth 数据复跑(修好 mu_after=8 之后)
# 在 master 上跑:  RUN_MODE=local DATA_DIR=<synth目录> bash exp1_synth_only.sh
# 建议放 tmux:     tmux new -s exp1_synth  然后执行,Ctrl+B D 脱离
set -euo pipefail

# --- 自定位 repo 根(脚本不假设固定路径)---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# ======== 网格(只改这块)========
# 默认只 synth_abrupt(exp1 里的那个 synth,Q3/恢复判读最干净)。
# 要全 4 个:加 synth_gradual synth_incremental synth_reoccurring
DATASETS=(synth_abrupt)
# 你 configs 里的两个 pauseMode 配置名(上轮 exp_id 显示为下面两个;若你命名成 P_BACKLOG/P_OLD 就改这里)
CONFIGS=(BACKLOG_THEN_NEW_FOREST USE_OLD_FOREST)
PARALLELISMS=(1 2 4)
RUNS=(1 2 3)
EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=2"
RUN_MODE="${RUN_MODE:-local}"; export RUN_MODE
# ================================

# --- 防呆:确认 synth 已是修好的 mu_after=8(别再跑在病态 mu_after=4 上)---
DATA_DIR="${DATA_DIR:-data/synth}"            # ← 改成集群上 synth CSV/driftspec 所在目录
SPEC="$DATA_DIR/synth_abrupt.driftspec.json"
if [[ -f "$SPEC" ]]; then
  MU_AFTER=$(grep -o '"normal_mu_after"[[:space:]]*:[[:space:]]*[0-9.]*' "$SPEC" | grep -o '[0-9.]*$' || echo "?")
  ANOM=$(grep -o '"anomaly_mu"[[:space:]]*:[[:space:]]*[0-9.]*' "$SPEC" | grep -o '[0-9.]*$' || echo "?")
  echo "[preflight] $SPEC : normal_mu_after=$MU_AFTER , anomaly_mu=$ANOM"
  if [[ "$MU_AFTER" == "$ANOM" || "$MU_AFTER" == "4.0" || "$MU_AFTER" == "4" ]]; then
    echo "[preflight] ✗ normal_mu_after 仍撞 anomaly_mu —— 还是病态旧数据!先重生成 mu_after=8 再跑。"
    exit 1
  fi
  echo "[preflight] ✓ synth 已分离(mu_after≠anomaly_mu),继续"
else
  echo "[preflight] ⚠ 未找到 $SPEC —— 跳过校验,请自行确认集群上的 synth 是 mu_after=8(设 DATA_DIR 可启用校验)"
fi

# --- 跑网格 ---
total=$(( ${#DATASETS[@]} * ${#CONFIGS[@]} * ${#PARALLELISMS[@]} * ${#RUNS[@]} ))
i=0
echo "=== EXP1 synth-only: 共 $total 个 run (RUN_MODE=$RUN_MODE) ==="
for ds in "${DATASETS[@]}"; do
  for cfg in "${CONFIGS[@]}"; do
    for p in "${PARALLELISMS[@]}"; do
      for r in "${RUNS[@]}"; do
        i=$((i+1))
        echo ""
        echo "=== [$i/$total] $ds / $cfg / p$p / r$r ==="
        bash deploy/scripts/run-experiment.sh \
          --dataset "$ds" --config-id "$cfg" --run-id "$r" --parallelism "$p" \
          --extra-param "$EXTRA"
      done
    done
  done
done

echo ""
echo "DONE: $total 个 run 完成。"
echo "分析:  bash analysis/analyze-all.sh"
echo "重点看 synth 的 post_final_auc —— 修好后上限是 ~1.0:"
echo "  · post_final 逼近 ~0.95+  → 重训到位,stale-score COOLDOWN 池没拖后腿"
echo "  · post_final 仍卡在 ~0.79  → Q3 坐实:stale-score 池是瓶颈,接「重训质量」workstream"
