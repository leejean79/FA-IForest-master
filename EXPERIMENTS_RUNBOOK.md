# FA-iForest 实验操作手册 / Experiments Runbook

本手册说明如何用 `deploy/scripts/` 下的实验自动化脚本跑完整实验。
集群部署见 `deploy/README.md`，本手册假设集群已部署并运行。

---

## 0. 实验总览

| 实验 | plan 名 | 规模 | 数据集 | 目的 |
|---|---|---|---|---|
| 实验 1 | `exp1` | 120 (6×4×5) | 4 synth + 2 insects | C1-C4 配置对漂移检测的影响 |
| 实验 2 | `exp2` | 150 (5×30) | 5 stationary | stationary AUC 稳定性 (30 次 shuffle) |
| 实验 3 | `exp3` | 24 (2×4×3) | donors, http | 可扩展性 (parallelism 1/2/4/6) |
| 实验 4 | `exp4` | 禁用 | — | 算法对比 (待 IKS/HDDM_W 实现) |
| 敏感性 | `sensitivity` | 30 | synth_abrupt | 参数网格扫描 |
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
```

---

## 2. 跑实验

### 2.1 跑前先 dry-run 看清要跑什么

```bash
bash deploy/scripts/run-batch.sh --plan exp1 --dry-run
```

确认实验列表无误后再真跑。

### 2.2 真跑 (建议挂后台 / nohup)

实验耗时长 (exp1 约 6-8 小时), 用 `nohup` 防止 ssh 断开中断。
**关键: 必须加 `< /dev/null`** 把 stdin 接到空设备:

```bash
nohup bash deploy/scripts/run-batch.sh --plan exp1 > exp1.out 2>&1 < /dev/null &
echo $! > exp1.pid          # 记下进程号

# 立刻确认是 Running 不是 Stopped
jobs

# 看实时输出
tail -f exp1.out
```

**为什么必须 `< /dev/null`**: 脚本里大量 `ssh fa-master "..."`, ssh 默认会读 stdin。
后台 (`&`) 跑且没重定向 stdin 时, ssh 抢终端 stdin 触发 SIGTTIN → 整个进程 `Stopped` (卡住不跑)。
`< /dev/null` 让 stdin 接空设备, ssh 读到 EOF 不抢终端。
**症状**: 启动后 `jobs` 显示 `[1]+ Stopped` 而不是 `Running`, `exp1.out` 卡在某步不动。
**修复**: kill 掉, 加 `< /dev/null` 重挂。

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
- `--algorithm X` (可选, 实验 4 用)

---

## 3. 断点续跑

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

### 4.1 从另一台机器查看实验 (如 Air 开发机)

实验主控进程 (run-batch.sh) 跑在**发起 nohup 的那台 mac** 上, 不在集群。
另一台机器能**查看**集群侧状态, 但**控制** (停止/重启批量) 要回主控那台。

另一台机器 (配好 ssh fa-master + .env) 可做:
```bash
# 看集群侧进度 (这些数据在集群, 任何机器 ssh 都能看)
ssh fa-master "ls /opt/fa-iforest/results/ | wc -l"          # 完成几个实验
ssh fa-master "docker exec jobmanager flink list"            # 当前跑什么
bash deploy/scripts/cluster-monitor.sh                       # 实时监控

# 不能做 (这些文件在主控 mac 上, 不在集群):
#   tail exp1.out / cat batch-progress / kill exp1.pid
```

要让两台机器对等控制, 需把主控进程改到 master 上跑 (现脚本设计为从 mac ssh 进集群,
未做此改造)。当前设计下: 一台 mac 当主控发起实验, 另一台 ssh 进集群只读查看。

---

## 5. 拉结果 + 分析

### 5.1 拉结果到本地

```bash
bash deploy/scripts/pull-results.sh                  # 拉到 ./results-local/
bash deploy/scripts/pull-results.sh --clean-remote   # 拉完删 master 上的 (防磁盘满)
```

### 5.2 批量分析

```bash
bash deploy/scripts/analyze-all.sh
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

```bash
# 先跑不依赖 IKS 的实验 (用现有 HDDM_A_Windowed)
nohup bash deploy/scripts/run-batch.sh --plan exp1 > exp1.out 2>&1 < /dev/null &
# exp1 完成后:
nohup bash deploy/scripts/run-batch.sh --plan exp2 > exp2.out 2>&1 < /dev/null &
nohup bash deploy/scripts/run-batch.sh --plan exp3 > exp3.out 2>&1 < /dev/null &
bash deploy/scripts/run-batch.sh --plan sensitivity

# 全部完成后拉结果分析
bash deploy/scripts/pull-results.sh
bash deploy/scripts/analyze-all.sh

# 实验 4 (IKS/HDDM_W): 等算法实现 + 传新 jar 后
# 把 experiment-configs.yml 的 exp4 enabled 改 true
```

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

**Q: kill 了本地 run-batch 进程, 但集群上 Flink job 还在跑?**
A: 正常。run-batch 在本地 mac, Flink job 在集群独立运行, 杀本地进程不停远端 job。手动停: `ssh fa-master "docker exec jobmanager flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | xargs -I{} docker exec jobmanager flink cancel {}"`, 再删残留 dumper。

---

## 9. 集群成本提示

- stop-saved 模式: 不跑时在阿里云控制台停机, 省计算费 (存储费仍收)
- 重启后 IP 变 → `refresh-ips.sh`
- 数据已在 master 磁盘 (`/opt/fa-iforest/datasets/`), 停机不丢, 重启不用重传
