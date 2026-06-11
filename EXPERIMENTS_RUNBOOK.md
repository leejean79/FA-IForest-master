# FA-iForest 实验操作手册 / Experiments Runbook

本手册说明如何用 `deploy/scripts/` 下的实验自动化脚本跑完整实验。
集群部署见 `deploy/README.md`，本手册假设集群已部署并运行。

---

## 0. 实验总览 (方向二(a))

> 已废: `exp1_hddm_w` (HDDM_W 算法对比)、`sensitivity_hddm_conf` (warnConfidence/driftConfidence 成对扫)、C1–C4 中的 `warnTimeoutBehavior` 维度 (无 WARN 状态了)。
> 详见 `HANDOVER_direction2a_phase3_arch.md`。

| 实验 | plan 名 | 规模 | 数据集 | 目的 |
|---|---|---|---|---|
| **EXP1 (核心)** | `exp1` | 54 (3×2×3×3) | synth_abrupt + 2 INSECTS | 端到端闭环 + 消稀释 (P-不变) + AUC 恢复 + 真实重训频率 |
| EXP2 | `exp2` | 150 (5×30) | 5 stationary | per-feature 在无漂移流上误触发率 = 精度基线 |
| EXP3 | `exp3` | 24 (2×4×3) | donors / http | 扩展性 (P∈{1,2,4,6});Fork 2 需手动调 `.env: SOURCE_PARTITIONS` |
| EXP4 (可选) | `exp4` | enabled: false | — | per-feature IKS vs per-feature HDDM (待 PerFeatureHDDM 落地) |
| 敏感性 | `sensitivity` | 51 | synth_abrupt | OAT 扫 `iksWindowSize / aggK / ksConfirm / ringBufferSize / cooldownSamples` |
| 冒烟 | `smoke_batch` | 2 | synth_abrupt | 验证批量调度 + topic 清理 + 断点续跑 |

**EXP1 最小网格** (不跑全组合): `{新 per-feature vs 旧 score 行并行} × P∈{1,2,4} × 数据集 × 3~5 seed`。
- 「**新**」臂 = Phase 3 检测面 build;「**旧**」臂 = 删检测前的 score 域 build (如 `feature/hddm-w` 或 Phase 3 前的 tag)。**两臂不同 build** —— Phase 3 已移除 score 检测, 同一 build 跑不了旧法。
- 不想重跑旧臂: 直接引用 finding 1 已记录的稀释数 (INSECTS abrupt: p=1→342 WARN、p=4→0), EXP1 只跑**新臂 across P** 证 P-不变即可。
- **三个观测项** (恢复曲线 / v1-v2 森林对比 / 重训频率) 定义与采集口径见 `HANDOVER_direction2a_phase3_arch.md` **附录 B**。

**配置矩阵 (收缩)**: `WARN` 已移除 → `warnTimeoutBehavior` (PROMOTE/DISCARD) 维度作废。只剩
`pauseMode ∈ {USE_OLD_FOREST, BACKLOG_THEN_NEW_FOREST}` (COOLDOWN/WAITING 期打分行为)。原 C1–C4 收为这两个 pauseMode 配置。

---

## 1. 一次性准备 (只做一次)

```bash
cd /path/to/FA-IForest

# 1.1 确认集群在线 (IP 可能因 stop-saved 变化)
ssh fa-master "docker ps --format '{{.Names}}'" || echo "集群未启动"
# 若 IP 变了:
bash deploy/scripts/refresh-ips.sh

# 1.2 上传数据集到 master (一次性, ~290MB)
bash deploy/scripts/upload-datasets.sh

# 1.3 确认 .env 有实验相关变量
grep -E 'JOB_LOAD_RATE|TOPIC_SCORE_PARTITIONS|JOB_LOCAL_PARALLELISM' deploy/.env

# 1.4 (本地) 确认本机 python3 有 pyyaml — 改 yaml 后 scp 前要先本地解析验证
python3 -c "import yaml; print(yaml.__version__)" \
    || python3 -m pip install --user --break-system-packages pyyaml
```

---

## 2. 跑实验

### 2.1 跑前先 dry-run 看清要跑什么

```bash
bash deploy/scripts/run-batch.sh --plan exp1 --dry-run
```

确认实验列表无误后再真跑。

### 2.2 两种运行模式

实验主控进程 (run-batch.sh) 可以在两个地方跑, 由环境变量 `RUN_MODE` 控制:

| 模式 | RUN_MODE | 主控在哪 | 适合 | mac 关机影响 |
|---|---|---|---|---|
| **远程** (默认) | `remote` | mac, ssh 进集群 | 调试单个实验, 人在旁边 | ⚠️ mac 睡眠/关机 → 实验中断 |
| **本地自治** (推荐跑批量) | `local` | master 上, 本地执行 | 长批量, 挂着不管 | ✅ mac 随便关, master 自治 |

**强烈推荐用 master 本地自治模式跑大批量** (exp1/2/3): 主控在 24h 在线的 master 上,
两台 mac 都能 ssh 进去看进度, mac 关机/带走/断网都不影响实验。

#### 2.2.1 master 自治模式 (推荐)

**一次性准备 master** (脚本不常改, 改了重新 scp):
```bash
# 在 mac 上, scp 脚本+配置到 master (阿里云连 GitHub 不稳, 用 scp 不用 git clone)
ssh fa-master "mkdir -p /opt/fa-iforest/repo/deploy/scripts"
scp deploy/scripts/*.sh deploy/scripts/cfg_query.py \
    fa-master:/opt/fa-iforest/repo/deploy/scripts/
scp deploy/datasets.yml deploy/experiment-configs.yml deploy/.env \
    fa-master:/opt/fa-iforest/repo/deploy/

# 确认 master 上 .env 的 NODE_MASTER_IP 是内网 IP (172.16.x.x), local 模式连 kafka 用内网
ssh fa-master "grep NODE_MASTER_IP /opt/fa-iforest/repo/deploy/.env"
# 确认 master 有 python3 + pyyaml: ssh fa-master "python3 -c 'import yaml'"
```

**用 tmux 跑批量** (tmux 让实验在 master 上脱离 ssh 持续运行):
```bash
ssh fa-master
tmux new -s exp1
cd /opt/fa-iforest/repo
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1 2>&1 | tee exp1.out
# 按 Ctrl+B 然后 D  → 脱离 tmux (实验继续)
exit                              # 退出 ssh, 实验不受影响
```

**之后任何机器看进度** (mac / Air, 配好 ssh fa-master 即可):
```bash
ssh fa-master "grep -c OK /opt/fa-iforest/repo/deploy/batch-progress-exp1.log"   # 完成数
ssh fa-master "tmux capture-pane -t exp1 -p | tail -20"                          # 当前进度
ssh fa-master -t "tmux attach -t exp1"                                           # 实时接入 (看完 Ctrl+B D 脱离, 别 Ctrl+C)
```

**tmux 操作要点**: `Ctrl+B D` 脱离(实验继续) / 别按 `Ctrl+C`(会停实验) / `exit` 退 ssh 不影响实验。

#### 2.2.2 mac 远程模式 (调试用)

在 mac 上跑, 默认 remote 模式。**必须加 `< /dev/null`** 避免 ssh 抢 stdin:

```bash
nohup bash deploy/scripts/run-batch.sh --plan exp1 > exp1.out 2>&1 < /dev/null &
echo $! > exp1.pid
jobs                              # 确认 Running 不是 Stopped
tail -f exp1.out
```

**两个坑**:
- **必须 `< /dev/null`**: 否则 ssh 抢终端 stdin 触发 SIGTTIN → 进程 `Stopped` 卡住。
- **mac 不能睡眠**: nohup 防 ssh 断开, 但防不了电脑睡眠。合盖/睡眠 → 主控进程冻结 → 集群 job 卡住。
  插电 + `caffeinate -s nohup ...` 阻止睡眠, 或直接用 2.2.1 的 master 自治模式 (推荐)。

### 2.3 单次实验 (调试用) + 新参数

```bash
bash deploy/scripts/run-experiment.sh \
    --dataset synth_abrupt --config-id <pauseMode> --run-id 1 [--parallelism N]
```

- `--dataset` 见 `datasets.yml`;`--config-id` 现取 pauseMode 配置;`--run-id` 重复序号 (EXP2 作 shuffle seed);`--parallelism N` (EXP1/EXP3 用, output-scores partition 同步);`--shuffle` (EXP2)。
- **`--algorithm` 基本废**: 检测面固定 per-feature IKS;EXP4 才用它切 per-feature HDDM。`HDDM_A/HDDM_W` 作为 score 域检测器**不再用于主实验**。

**检测面参数 (`--extra-param "k=v[;k2=v2]"` 透传), 替代旧的 `warnConfidence/driftConfidence/hddmLambda`:**

| 参数 | 默认 | 段 |
|---|---|---|
| `iksWindowSize` | 2000 | per-feature IKS 窗 W |
| `iksPValue` | 0.001 | `ca=√(−0.5·ln p)≈1.858` |
| `confirmWin` | ~W | 峰值-KS 确认窗 C |
| `ksConfirm` | 待标定 | 幅度门 (**不得伤 recall**) |
| `aggK` | 2 | 聚合器共发特征门 |
| `aggWin` | W | 聚合窗 |
| `refractory` | ~重训周期 | 去抖 |

**示例:**
```bash
bash deploy/scripts/run-experiment.sh \
    --dataset synth_abrupt --config-id USE_OLD_FOREST --run-id 1 \
    --extra-param "iksWindowSize=2000;iksPValue=0.001;aggK=2"
```
启动 banner 应打出:
- **LocalProcessor**: `IKS window size W: 2000` / `IKS pValue: 0.001` / `Confirm window C: ...` / `ksConfirm: ...`
- **CoordinatorJob**: `Aggregator k: 2` / `Aggregator window: ...` / `Refractory: ...`

(extra-param 同时透传两个 job;Flink ParameterTool 对未知 key 静默忽略,所以 `aggK` 落在 CoordinatorJob、`iks*` 落在 LocalProcessor。)
EXP_ID 形如 `synth_abrupt_USE_OLD_FOREST_default_p4_r1_iksWindowSize-2000_iksPValue-0.001_aggK-2`
(`default` 是 algo_tag,主路径不传 `--algorithm` 时的默认值)。

---

## 3. 断点续跑

### 3.1 首先确认没有完成的结果文件，并删除：
=== master 上操作 ===

ssh fa-master
cd /opt/fa-iforest/repo

1. 先看哪些未完成 (dry-run)
`bash deploy/scripts/clean-failed-results.sh --results-dir /opt/fa-iforest/results`
    应该列出 11 个 forestcover (r3,8,10,15,18,19,20,25,26,29,30)
    但 r3 你刚单跑成功了, 现在应该是 10 个

2. 确认无误后删
`bash deploy/scripts/clean-failed-results.sh --results-dir /opt/fa-iforest/results --delete`

 3. tmux 重跑 exp2 (断点续跑)
```tmux new -s exp2
cd /opt/fa-iforest/repo
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp2 2>&1 | tee exp2_retry.out
 Ctrl+B D 脱离
exit
```


`run-experiment.sh` 检查结果目录: 如果 `scores.jsonl` 已存在就 **skip**。
所以批量跑中断后, 直接重跑同一 plan, 已完成的自动跳过:

```bash
# 中断后, 直接重跑, 跳过已完成的
bash deploy/scripts/run-batch.sh --plan exp1
```

进度记录在 `deploy/batch-progress-exp1.log`, 每行一个实验的结果 (OK/TIMEOUT/JOBFAIL)。

```bash
# 看进度
cat deploy/batch-progress-exp1.log
# 统计完成数
grep -c 'OK' deploy/batch-progress-exp1.log
```

---

## 4. 实时监控

另开一个终端窗口:

```bash
bash deploy/scripts/cluster-monitor.sh        # 3 秒刷新
bash deploy/scripts/cluster-monitor.sh -i 5   # 5 秒刷新
```

显示: 3 节点 CPU/内存 / 容器状态 / Flink jobs / 6 个 topic offset / 当前实验。
6 个 topic 中 `drift-topic` 已被 **`feature-drift-topic`** 取代 (检测面 per-feature 信号);其余 `source / tree / model / drift-round / output-scores` 不变。
Ctrl+C 退出 (会清理 ssh 复用连接)。

### 4.1 从任意机器查看/控制实验

**用 master 自治模式 (2.2.1) 时**: 主控在 master 的 tmux 里, 两台 mac **对等** —
都能 ssh 进 master 查看进度、接入 tmux、停止实验。mac 关机不影响。

```bash
# 任意 mac:
ssh fa-master "grep -c OK /opt/fa-iforest/repo/deploy/batch-progress-exp1.log"   # 进度
ssh fa-master -t "tmux attach -t exp1"                                           # 接入 (Ctrl+B D 脱离)
ssh fa-master "tmux kill-session -t exp1"                                        # 停止整个批量
bash deploy/scripts/cluster-monitor.sh                                           # 集群监控
```

**用 mac 远程模式 (2.2.2) 时**: 主控在发起的那台 mac 上, 另一台只能 ssh 进集群**查看**
(看 results 数/flink list), **控制** (停/重启) 要回主控那台 mac。这是推荐用 master 自治的原因。

---

## 5. 拉结果 + 分析

### 5.1 拉结果到本地

```bash
bash deploy/scripts/pull-results.sh                  # 拉到 ./results-local/
bash deploy/scripts/pull-results.sh --clean-remote   # 拉完删 master 上的 (防磁盘满)
```

### 5.2 批量分析

```bash
bash analysis/analyze-all.sh
```

产出在 `analysis-output/`:
- `drift-summary.csv` — 实验 1/4: 各实验的 overall_auc, per_version 数
- `stationary-summary.csv` — 实验 2: 各数据集 AUC mean/std
- `scalability_*.csv` — 实验 3: 吞吐 vs parallelism

### 5.3 单个结果手动分析

```bash
python3 analysis/analyze.py --mode drift \
    --scores results-local/<exp_id>/scores.jsonl \
    --driftspec results-local/<exp_id>/driftspec.json \
    --out results-local/<exp_id>/analysis.json
```

mode: `drift` (实验1/4) / `stationary` (实验2) / `scalability` (实验3) / `throughput`

---

## 6. 结果目录结构

每个实验在 `master:/opt/fa-iforest/results/{exp_id}/`:

```
{dataset}_{config}_{algo}_p{N}_r{run}/
├── scores.jsonl              # 主结果: 每行一个 ScoreResult (seq,id,score,label,phase,forestVersion) —— 正好是 EXP1 三观测项的输入
├── driftspec.json            # 漂移真值 (有漂移的数据集才有)
├── job-config.json           # 本次实验的配置 + final_offset + status
├── feature-drift-topic.jsonl # 检测面 per-feature 确认 onset (替代旧 drift-topic.jsonl)
├── drift-round-topic.jsonl
├── model-topic.jsonl         # 全局森林版本记录
└── runtime.log               # JobManager 日志末尾 100 行
```

**exp_id 命名**: `{dataset}_{config}_{algo}_p{N}_r{run}[_extra...]`
例: `synth_abrupt_USE_OLD_FOREST_default_p4_r1_iksWindowSize-2000_aggK-2`
(`config` 现取 pauseMode 字面值 `USE_OLD_FOREST` / `BACKLOG_THEN_NEW_FOREST`,而非旧 C1–C4。)

---

## 7. 推荐执行顺序 (方向二(a))

实际规模(经 `experiment-configs.yml` 展开):
- `smoke_batch` = 2 (synth_abrupt × 2 pauseMode)
- `exp1` = 54 (3 datasets × 2 pauseMode × P∈{1,2,4} × 3 repeats),自带 plan_extras: `iksWindowSize=2000;iksPValue=0.001;aggK=2`
- `exp2` = 150 (5 stationary × 30 shuffle)
- `exp3` = 24 (2 datasets × P∈{1,2,4,6} × 3 repeats)
- `sensitivity` = 51 (OAT 扫 iksWindowSize / aggK / ksConfirm / ringBufferSize / cooldownSamples)

EXP3 升 Fork 2 要先把 `.env` 的 `SOURCE_PARTITIONS` 改成对应 `P_d`,然后重建 source-topic(`bash deploy/scripts/clean-topics.sh`)。

```bash
ssh fa-master
cd /opt/fa-iforest/repo

# (0) 冒烟 — 先跑 smoke_batch 验机制(2 个实验,~几分钟)
tmux new -s smoke
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan smoke_batch 2>&1 | tee smoke.out
# Ctrl+B D 脱离

# (1) EXP1 新臂 across P — 核心: 恢复曲线 + 重训频率 + P-不变 (54 实验)
tmux new -s exp1_new
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1 2>&1 | tee exp1_new.out
# Ctrl+B D 脱离;exit 退 ssh, 实验自治继续

# (2) (可选) 旧臂对照 — 切到 score 域 build 再跑同网格;或直接引用 finding 1 的稀释数

# (3) EXP2 (stationary 误触发基线) → EXP3 (扩展性 / Fork 2), 后做
tmux new -s exp2
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp2 2>&1 | tee exp2.out

# (4) 拉结果分析 (在 mac 上)
bash deploy/scripts/pull-results.sh
bash analysis/analyze-all.sh
```

**EXP1 三观测项** (拉回后离线重建, 见 Phase3 附录 B):
- 恢复曲线: `scores.jsonl` (seq, score, label, forestVersion) 滑窗 AUC + forestVersion 切换线。
- v1/v2 森林: 版本切换前后同一 eval 窗, 按 forestVersion 分组算 AUC (`v2≤v1` = stale-score COOLDOWN 池拖后腿, 触发「重训质量」workstream)。
- 重训频率: `model-topic.jsonl` 的 distinct version 数 / run。

> `analyze.py` 需新增 `recovery` / `v1v2` / `retrain_freq` mode (或单独脚本);旧 `drift` mode 的 WARN/voting 统计已无意义。

### 7.1 参数透传防呆 (改 yaml 后必做)

方法不变 (最易栽的坑仍是参数名拼错 / shell 拆分错 → LocalProcessor 静默兜底默认值), 只是**核对的参数换成检测面的**:

```bash
# 步骤 1: 本地 yaml 解析合法
python3 -c "import yaml; yaml.safe_load(open('deploy/experiment-configs.yml'))" && echo OK
# 步骤 1.5: scp 到 master + 远端再验
scp deploy/experiment-configs.yml fa-master:/opt/fa-iforest/repo/deploy/

# 步骤 2: dry-run 核对展开行数 + 每行 extra 列
ssh fa-master "cd /opt/fa-iforest/repo && bash deploy/scripts/run-batch.sh --plan <plan> --dry-run | head -20"

# 步骤 3: 单跑 1 次, 核对参数透传到检测面 banner (关键防呆 —— 两个 job 都要看!)
ssh fa-master "cd /opt/fa-iforest/repo && RUN_MODE=local bash deploy/scripts/run-experiment.sh \
    --dataset <ds> --config-id <cfg> --run-id 1 \
    --extra-param 'iksWindowSize=2000;iksPValue=0.001;aggK=2'"
# LocalProcessor banner 必须打: IKS window size W: 2000 / IKS pValue: 0.001
# CoordinatorJob banner 必须打: Aggregator k: 2
# 都不能是默认值。两个 banner 在不同 JobManager 日志段, 分别核对:
#   docker logs jobmanager 2>&1 | grep -E 'IKS window|IKS pValue|Aggregator k'
# (旧的 warnConfidence/driftConfidence 已废, 别再核对那两个)
```

---

## 8. 故障排查

**Q: 实验卡在 `[6/9] progress: 0`?**
A: 数据没进 source-topic。查 `ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list <内网IP>:9092 --topic source-topic"`。若为 0, producer 没灌成功 (看 producer 是否前台阻塞跑完)。

**Q: `[3/9] dumper 启动即退出 AccessDenied`?**
A: 结果目录权限。脚本已 `chmod 777 $RESULT_DIR`, 若仍失败查容器内 UID (Flink 镜像用 9999, 非 root)。

**Q: `[7/9] scores.jsonl 为空`?**
A: Dumper 没消费到数据或写盘失败。脚本会打 dumper 日志, 看是连不上 kafka 还是写权限。

**Q: 检测面 `feature-drift-topic` 一直空,没收到任何 FeatureDrift?**
A: per-feature IKS 需要先暖机 W 条 (默认 W=2000),W 条之前一律返回 STABLE。先确认数据量 ≥ W;
   再确认数据真有分布漂移 —— stationary 数据集 (EXP2) 本就预期不该 fire,这是「精度基线」目的。
   有漂移的数据集 (synth_abrupt、INSECTS_*) 若 W 条之后仍无 emit:
   `docker logs jobmanager 2>&1 | grep "featureId" | head` 看 PerFeatureIKSFunction 的 DEBUG/INFO,
   确认有没有进 confirm 态 / 是 ksConfirm 太严否决了 onset。默认 `ksConfirm = thr` 保守不该误杀。

**Q: `feature-drift-topic` 有 FeatureDrift,但 `drift-round-topic` 没 COMMITTED?**
A: 聚合器要求 ≥ `aggK` 个**不同 featureId** 落在 `aggWin` 窗内 (默认 k=2, aggWin=W=2000)。
   若漂移只影响 1 个特征,k=2 永远不触发 —— 可临时调 `aggK=1` 验证;或核对 FeatureDrift 的
   featureId 分布:`cat results-local/<exp>/feature-drift-topic.jsonl | jq -r .featureId | sort | uniq -c`。
   `aggWin` 太小也会导致不同特征的 onset 没赶到一起 —— 看 onset seq 间距。

**Q: 收到 COMMITTED 但 `model-topic` 没出新森林版本?**
A: COMMITTED 触发 COOLDOWN 收池 (z-score 过滤),需要 ≥ `cooldownSamples`(默认 2000)且
   ringBuffer (默认 1000) 被新数据覆盖才会重训。先看 LocalProcessor 日志的
   `COOLDOWN-POOL-DIAG`:`rbSize / cWrites / cN / poolAnomaly`。常见情形:
   - `cN` 卡 < `cooldownSamples` → 数据已灌完,COOLDOWN 没收够样本(降 `cooldownSamples` 或加大数据量)
   - `cWrites` 远 < `rbSize` → z-score 卡得太严过滤掉太多(`zThresholdK` 调大,默认 1.0)
   - 兜底 `cN >= cooldownSamples * 2` 触发后仍重训,但池可能很稀,质量打折(看 v1-v2 对比)。

**Q: analyze.py 报 `missing required field 'seq'`?**
A: scores.jsonl 字段名问题。ScoreResult 序列化用 originalSequence/dataPointId, 脚本 Step 7 会自动 rename 成 seq/id。若手动跑老结果, 先用脚本里那段 python 转换。

**Q: 实验 3 跑 p=2 后跑 p=4 报 partition 错?**
A: 不会。clean-topics.sh 是 delete→create (不是 alter), 任意 partition 数都行。

**Q: IP 变了连不上?**
A: stop-saved 模式重启 IP 会变。`bash deploy/scripts/refresh-ips.sh` 刷新 .env 里的公网 IP。

**Q: nohup 启动后 `jobs` 显示 Stopped, exp1.out 卡住不动?**
A: ssh 抢 stdin 导致 SIGTTIN。nohup 命令漏了 `< /dev/null`。kill 掉进程, 清残留 (cancel 集群上已提交的 job + 删 dumper + 删半截结果目录), 用 `nohup ... > exp1.out 2>&1 < /dev/null &` 重挂。注意: 进程 Stopped 期间脚本可能已在集群提交了 job, kill 本地进程不会停远端 job, 要手动 cancel。

**Q: master 自治模式 (RUN_MODE=local) 跑实验报错连不上 kafka?**
A: 检查 master 上 `.env` 的 `NODE_MASTER_IP` 是**内网 IP** (172.16.x.x) 不是公网。local 模式在 master 本地连 kafka 走内网。

**Q: 想停止 master 自治的批量实验?**
A: `ssh fa-master "tmux kill-session -t exp1"` 停 run-batch。集群上正在跑的 job 用 `docker exec jobmanager flink cancel <jid>` 单独清。已完成结果保留, 重跑断点续跑。

**Q: master 上 git clone 失败 (GnuTLS / TLS 中断)?**
A: 阿里云连 GitHub 不稳。别用 git clone, 改用 `scp` 从 mac 传脚本+配置到 master (见 2.2.1)。

**Q: 大数据集 (forestcover 等) JOBFAIL, Coordinator 日志有 `emitted forest` 但 model-topic offset 始终 0?**
A: 这是 Kafka 单消息上限 + Flink 弱可靠语义叠加导致的 ForestMessage 丢失。完整复盘见
   `COORDINATOR_FAILURE_POSTMORTEM.md`。修复要点 (`feature/hddm-w` 分支以下都已落地):

   1. **Producer 端** (CoordinatorJob `producerProps`):
      - `max.request.size=5242880` (5MB, 默认 1MB)
      - `compression.type=gzip` (减小传输/存储, 不影响校验)
   2. **Broker 端** (compose kafka-1 environment):
      - `KAFKA_MESSAGE_MAX_BYTES=5242880`
      - `KAFKA_REPLICA_FETCH_MAX_BYTES=5242880` (replication≥2 必须同步加, 否则 ISR 卡)
      改完需 `docker compose -f docker-compose.master.yml up -d --force-recreate kafka-1`。
   3. **Consumer 端** (LocalProcessor `modelKafkaProps`):
      - `max.partition.fetch.bytes=5242880`
   4. **两个 Job 都 `env.enableCheckpointing(10000)`**: 不开则 AT_LEAST_ONCE 降级为 NONE
      语义, 单条森林消息错误会被 producer 静默吞掉。

   验证: 改完跑 forestcover r8, 日志应 (a) 无 `RecordTooLargeException` (b) 无
   `Switching to NONE semantic` (c) 有 `Completed checkpoint` (d) `model-topic` offset = 1
   (e) 4 子任务全 `received global forest version 1`。

**Q: 进度脚本卡在 99% (已消费 < 目标 offset, 比如 284805 / 286048)?**
A: source consumer 漏掉了头部数据。原因是 `kafkaConsumer` 默认 startup 模式回退到 Kafka
   `auto.offset.reset=latest`, FileToKafkaProducer 在 consumer 订阅完成前已灌入若干条,
   被 latest 跳过, consumer 从非零 offset 起。修复: LocalProcessor 第 178 行加
   `kafkaConsumer.setStartFromEarliest();` (`feature/hddm-w` 分支已修)。
   验证: 日志应是 `Resetting offset for partition source-topic-0 to offset 0`, 不是非零值。

**Q: 改完 experiment-configs.yml, cfg_query 报 `yaml.parser.ParserError: while parsing a block mapping`?**
A: yaml 缩进错误, 通常是手 vim 时混入 tab、全角空格或行内空格不对齐。yaml 严格要求纯空格,
   plans/algorithms 段的 key 用 2 空格、子字段用 4 空格。诊断 + 修复:
   ```bash
   # 看出错行附近的字符 (cat -A 把 tab 显示成 ^I, 行尾 $)
   sed -n '<行号-2>,<行号+5>p' deploy/experiment-configs.yml | cat -A
   # 改完一定本地先验证, 通过再 scp
   python3 -c "import yaml; yaml.safe_load(open('deploy/experiment-configs.yml'))" && echo OK
   ```
   缩进错时不要在集群上 vim 直接改 — 容易再引入新的缩进坑。在本地仓库改, 本地 yaml 验证
   通过, 再 scp 到 master。

---

## 9. 集群成本提示

- stop-saved 模式: 不跑时在阿里云控制台停机, 省计算费 (存储费仍收)
- 重启后 IP 变 → `refresh-ips.sh`
- 数据已在 master 磁盘 (`/opt/fa-iforest/datasets/`), 停机不丢, 重启不用重传
