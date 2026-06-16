#!/usr/bin/env bash
# 重跑 insects_abrupt × p4 × BACKLOG_THEN_NEW_FOREST(上轮丢了 ~42% 记录:n≈124k vs 满 212514)
#
# ⚠ 警告:上轮 USE_OLD p4 是满记录、只有 BACKLOG p4 丢 —— 大概率是 BACKLOG 在 p4 下的【确定性】问题,
#         不是偶发。本脚本每跑完一轮校验记录数;若仍 <95%,会停下让你查根因(别再盲跑)。
#
# 用法(master 上):RUN_MODE=local RESULTS_DIR=<结果根目录> bash exp1_insects_p4_backlog_rerun.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || echo "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# ======== 锁定到出问题的那一格 ========
DATASET=insects_abrupt_imbalanced
CONFIG=BACKLOG_THEN_NEW_FOREST
P=4
RUNS=(1 2 3)
EXTRA="iksWindowSize=2000;iksPValue=0.001;aggK=2"
EXPECTED_N="${EXPECTED_N:-212514}"        # insects_abrupt 满记录数(上轮 USE_OLD p4 实测)
RUN_MODE="${RUN_MODE:-local}"; export RUN_MODE
RESULTS_DIR="${RESULTS_DIR:-results}"     # ← run-experiment 的输出根目录,按你的实际改
# =====================================

lost_any=0
for r in "${RUNS[@]}"; do
  echo ""
  echo "=== $DATASET / $CONFIG / p$P / r$r ==="
  bash deploy/scripts/run-experiment.sh \
    --dataset "$DATASET" --config-id "$CONFIG" --run-id "$r" --parallelism "$P" \
    --extra-param "$EXTRA"

  # --- 跑完校验记录数(防止又静默丢记录)---
  EXP_ID="${DATASET}_${CONFIG}_default_p${P}_r${r}_iksWindowSize-2000_iksPValue-0.001_aggK-2"
  SCORES=$(find "$RESULTS_DIR" -name 'scores*.jsonl' -path "*${EXP_ID}*" 2>/dev/null | head -1)
  if [[ -n "${SCORES:-}" && -f "$SCORES" ]]; then
    N=$(wc -l < "$SCORES")
    pct=$(( N * 100 / EXPECTED_N ))
    echo "[check] r$r 记录数 N=$N / 期望 $EXPECTED_N (${pct}%)"
    if (( N * 100 < EXPECTED_N * 95 )); then
      echo "[check] ✗ r$r 仍丢记录(<95%)"
      lost_any=1
    else
      echo "[check] ✓ r$r 记录完整"
    fi
  else
    echo "[check] ⚠ 没找到 r$r 的 scores 文件 —— 改 RESULTS_DIR / EXP_ID 匹配规则,或手动核对 n≈$EXPECTED_N"
  fi
done

echo ""
if (( lost_any )); then
  cat <<'EOF'
⚠ 仍有 run 丢记录 —— 确认是确定性问题,别再重跑。先分清是哪一类:

  (a) 管线真丢:Flink source 只消费了 ~124k(背压/缓冲溢出)
      → 对比 source-topic 的 consumed offset vs scores.jsonl 行数(见 runbook §4 offset 检查)。
      BACKLOG_THEN_NEW_FOREST 在 WAITING 期缓冲来流;p4 组装等 4 个 subtask、WAITING 久 +
      insects_abrupt 有 14~15 次重训 → 缓冲累积溢出丢弃。USE_OLD 不缓冲所以不丢。
      排查:backlog 缓冲容量 / 溢出策略(应背压而非丢弃)。

  (b) 只是没 dump 全:Flink 处理了满 212k,但 dumper 早停/漏了某个 output-scores partition
      → 这种 p4(分区多)更易发生,属采集 bug 非数据丢失,改 dumper 即可。

  判别:看 Flink 端 numRecordsIn / source consumed offset == 212514 吗?
        == → (b) dumper;  < → (a) 管线。
EOF
  exit 1
fi
echo "DONE: 3 个 run 记录完整(≥95%),可纳入分析。"
