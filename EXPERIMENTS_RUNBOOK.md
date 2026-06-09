# FA-iForest 实验操作手册 / Experiments Runbook

本手册说明如何用 `deploy/scripts/` 下的实验自动化脚本跑完整实验。
集群部署见 `deploy/README.md`，本手册假设集群已部署并运行。

---

## 0. 实验总览

| 实验 | plan 名 | 规模 | 数据集 | 目的 |
|---|---|---|---|---|
| 实验 1 (HDDM_A) | `exp1` | 120 (6×4×5) | 4 synth + 2 insects | C1-C4 配置对漂移检测的影响 (用默认 HDDM_A_Windowed) |
| 实验 1 (HDDM_W) | `exp1_hddm_w` | 120 (6×4×5) | 同 exp1 | 同 exp1 维度, 切换为 HDDM_W (λ=0.1), 用于算法对比 |
| 实验 2 | `exp2` | 150 (5×30) | 5 stationary | stationary AUC 稳定性 (30 次 shuffle) |
| 实验 3 | `exp3` | 24 (2×4×3) | donors, http | 可扩展性 (parallelism 1/2/4/6) |
| 实验 4 | `exp4` | 禁用 | — | 算法对比 (HDDM_W 已实现; 等 IKS 实现后启用) |
| 敏感性 (buffer/window/cooldown) | `sensitivity` | 30 | synth_abrupt | OAT 参数网格扫描 (ringBufferSize / hddmWindowSize / cooldownSamples) |
| 敏感性 (HDDM 置信度) | `sensitivity_hddm_conf` | 12 | insects_abrupt_imbalanced | 成对放宽倍率: warnConfidence + driftConfidence (基线 + 10×/20×/40×) |
| 冒烟 | `smoke_batch` | 4 | synth_abrupt | 批量调度验证 (非正式实验) |

**配置矩阵 (C1-C4)**: `pauseMode` × `warnTimeoutBehavior` 的 2×2
- C1: USE_OLD_FOREST + DISCARD
- C2: USE_OLD_FOREST + PROMOTE
- C3: BACKLOG_THEN_NEW_FOREST + DISCARD
- C4: BACKLOG_THEN_NEW_FOREST + PROMOTE

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

### 2.3 单次实验 (调试用)

```bash
bash deploy/scripts/run-experiment.sh \
    --dataset synth_abrupt --config-id C1 --run-id 1
```

参数:
- `--dataset` 数据集名 (见 datasets.yml)
- `--config-id` C1/C2/C3/C4
- `--run-id` 重复序号 (实验 2 用作 shuffle seed)
- `--parallelism N` (可选, 实验 3 用; output-scores partition 同步此值)
- `--shuffle` (可选, 实验 2 用)
- `--algorithm X` (可选): 算法 id, 从 experiment-configs.yml 的 `algorithms:` 段查 detector。
  当前已注册: `HDDM_A` (→ HDDM_A_Windowed), `HDDM_A_Cumulative` (→ HDDM_A), `HDDM_W` (→ HDDM_W)。
  传错的 id (如 `IKS` 当前未实现) 会让 LocalProcessor 启动时抛 IllegalArgumentException。
- `--extra-param "k1=v1[;k2=v2...]"` (可选): 通用参数透传, 翻译成 `--k1 v1 --k2 v2` 给
  LocalProcessor。多对参数用分号分隔, 单对不需要分号。常见用法:
  - HDDM_W 的 λ:                 `--extra-param "hddmLambda=0.1"`
  - HDDM 置信度成对扫:           `--extra-param "warnConfidence=0.05;driftConfidence=0.01"`
  - sensitivity 单参数:          `--extra-param "ringBufferSize=512"`
  EXP_ID 会把 `=` 编码成 `-`、`;` 编码成 `_`, 结果目录天然不冲突。

**示例 — HDDM_W 单次冒烟测试:**
```bash
bash deploy/scripts/run-experiment.sh \
    --dataset synth_abrupt --config-id C1 --run-id 1 \
    --algorithm HDDM_W \
    --extra-param "hddmLambda=0.1"
```
LocalProcessor 启动 banner 应显示 `Detector: HDDM_W` 和 `HDDM lambda: 0.1` (不是默认值),
EXP_ID: `synth_abrupt_C1_HDDM_W_p4_r1_hddmLambda-0.1`。

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
├── scores.jsonl          # 主结果: 每行一个 ScoreResult (seq,id,score,label,phase,forestVersion)
├── driftspec.json        # 漂移真值 (有漂移的数据集才有)
├── job-config.json       # 本次实验的配置 + final_offset + status
├── drift-topic.jsonl     # 漂移事件
├── drift-round-topic.jsonl
├── model-topic.jsonl     # 全局森林版本记录
└── runtime.log           # JobManager 日志末尾 100 行
```

**exp_id 命名**: `{dataset}_{config}_{algo}_p{N}_r{run}`
例: `synth_abrupt_C1_default_p4_r1`

---

## 7. 推荐执行顺序

实际工作方式是 master 自治模式 + tmux (见 2.2.1), 不是 mac 上 nohup。各 plan 串行,
exp1 / exp1_hddm_w / exp2 / exp3 用各自的 tmux session 跑, 便于中途接入查看或停止。

```bash
ssh fa-master
cd /opt/fa-iforest/repo

# (1) 主实验链: exp1 → exp1_hddm_w → exp2 → exp3
tmux new -s exp1
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1 2>&1 | tee exp1.out
# Ctrl+B D 脱离, exit 退 ssh, 实验在 master 自治继续

# exp1 完成后, 再起 exp1_hddm_w (HDDM_W 算法对比)
tmux new -s exp1_hddm_w
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1_hddm_w 2>&1 | tee exp1_hddm_w.out

# exp2 / exp3
tmux new -s exp2
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp2 2>&1 | tee exp2.out
tmux new -s exp3
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp3 2>&1 | tee exp3.out

# (2) 敏感性分析 (改完 yaml 一定先做 7.1 的防呆步骤再批量跑)
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan sensitivity              # buffer/window/cooldown OAT
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan sensitivity_hddm_conf    # HDDM 置信度成对扫

# (3) 全部完成后拉结果分析 (在 mac 上)
bash deploy/scripts/pull-results.sh
bash deploy/scripts/analyze-all.sh
```

**算法对比的产出位置**: exp1 (HDDM_A_Windowed 默认) 与 exp1_hddm_w (HDDM_W) 的结果目录
分别带 `_default_` 和 `_HDDM_W_` 标识, 不会互相覆盖, 可直接做配对对比。

**实验 4 (IKS) 启用条件**: 等 IKS detector 实现后, 把 experiment-configs.yml 的 `algorithms:`
段取消 IKS 注释、并把 exp4 plan 的 `enabled: true` 打开即可。HDDM_W 已就绪, 不必再等。

### 7.1 sensitivity 类 plan 的防呆步骤 (改 yaml 后必做)

sensitivity 测试最容易栽的坑: **参数名拼错或 shell 拆分错, LocalProcessor 静默兜底用默认值,
跑了一堆其实 detector 行为完全没变**。所以每次改 sensitivity yaml (新增 plan / 改参数值 /
改 grids 或 configurations) 后, 按以下三步验证, 都过了再批量跑:

```bash
# 步骤 1: 本地 yaml 解析合法 (上次因缩进栽过, 必做)
python3 -c "import yaml; yaml.safe_load(open('deploy/experiment-configs.yml'))" && echo OK
# 步骤 1.5: scp 到 master
scp deploy/experiment-configs.yml fa-master:/opt/fa-iforest/repo/deploy/
ssh fa-master "python3 -c 'import yaml; yaml.safe_load(open(\"/opt/fa-iforest/repo/deploy/experiment-configs.yml\"))' && echo OK_REMOTE"

# 步骤 2: dry-run 核对展开行数 + 每行 extra 列
ssh fa-master "cd /opt/fa-iforest/repo && bash deploy/scripts/run-batch.sh --plan <plan> --dry-run | head -20"
# 预期: 展开行数 = 配置点数 × repeats; 每行末尾的 extra 列就是要扫的 k=v[;k2=v2]

# 步骤 3: 单跑 1 次, 核对参数透传到 LocalProcessor (关键防呆)
ssh fa-master "cd /opt/fa-iforest/repo && RUN_MODE=local bash deploy/scripts/run-experiment.sh \
    --dataset <ds> --config-id <cfg> --run-id 1 \
    --extra-param '<和 yaml 里某个点完全相同的字符串>'"
# 看 LocalProcessor 启动 banner 打出的参数值, 必须等于你设的值, 不能是默认值
# 比如 warnConfidence=0.05;driftConfidence=0.01 → banner 必须打 Warn confidence: 0.05 / Drift confidence: 0.01
#      不能是默认 0.005 / 0.001
```

三步全过 → 才正式 `run-batch.sh --plan <plan>`。
单跑那次的结果目录与 batch 重复, 让 batch 的断点续跑跳过它即可, 无须特别清理。

---

## 8. 故障排查

**Q: 实验卡在 `[6/9] progress: 0`?**
A: 数据没进 source-topic。查 `ssh fa-master "docker exec kafka-1 kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list <内网IP>:9092 --topic source-topic"`。若为 0, producer 没灌成功 (看 producer 是否前台阻塞跑完)。

**Q: `[3/9] dumper 启动即退出 AccessDenied`?**
A: 结果目录权限。脚本已 `chmod 777 $RESULT_DIR`, 若仍失败查容器内 UID (Flink 镜像用 9999, 非 root)。

**Q: `[7/9] scores.jsonl 为空`?**
A: Dumper 没消费到数据或写盘失败。脚本会打 dumper 日志, 看是连不上 kafka 还是写权限。

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
