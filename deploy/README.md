# FA-iForest 集群部署 / Cluster Deployment

3 节点最小集群: ZooKeeper + Kafka(3 broker) + Flink 1.13.6(1 JM + 2 TM) + Prometheus + Grafana。
全部通过 Docker 容器化部署, 本地一键 build + sync + up。

---

## 1. 拓扑 / Topology

```
                ┌────────────────────────────────────────┐
                │ master (2c4g)   NODE_MASTER_IP         │
                │   ZooKeeper                            │
                │   Kafka broker-1                       │
                │   Flink JobManager (Web UI :8081)      │
                │   Prometheus  (:9090)  Grafana (:3000) │
                │   node-exporter (:9100)                │
                └────────────────────────────────────────┘
                            ▲              ▲
                     internal IP    internal IP
                            │              │
        ┌───────────────────┴─┐   ┌────────┴───────────┐
        │ worker-1 (4c8g)     │   │ worker-2 (4c8g)    │
        │   Kafka broker-2    │   │   Kafka broker-3   │
        │   Flink TM (4 slots)│   │   Flink TM (4 slots)│
        │   node-exporter     │   │   node-exporter    │
        └─────────────────────┘   └────────────────────┘
```

**Topic 设计 (关键):**

| Topic | Partitions | 为什么 |
|---|---|---|
| `source-topic` | **1** | Flink source 端保序约束: 单 partition 才能保证消费顺序 = 写入顺序 |
| `tree-topic` | 1 | 各 subtask 上传 iTree |
| `model-topic` | 1 | 中央广播全局森林, 严格保版本号顺序 |
| `output-scores` | **4** | 各 subtask 落异常分数, 多 partition 让落盘不堵塞 (实验3 同步 parallelism) |
| `drift-topic` | 1 | 漂移事件审计, 量小 |
| `drift-round-topic` | 1 | 漂移轮次记录 |

`replication-factor = 2`: 3 broker 集群下容忍单节点宕机, 不用 3 是为了省 master 内存。
6 个 topic 名是代码默认 (短横线)。实验脚本 `clean-topics.sh` 会按此重建。

---

## 2. 前置 / Prerequisites

**本地 (Mac/Linux):**
- Docker Desktop (build 镜像 + save tar)
- Maven 3.x + JDK 8 (编译 fat jar)
- rsync, ssh, bash 4+

**3 个阿里云 ECS 节点:**
- Ubuntu 20.04+ / CentOS 8+
- Docker 20.10+ + docker compose v2
- SSH 免密登录已配 (本地 → 节点)
- 同 VPC, 内网互通 (重要: 用内网 IP 通信免流量费)
- 安全组放通: 22, 8081, 9090, 9092, 9094, 6123, 9249, 9100, 2181, 3000
- **注意**: stop-saved 模式重启后公网 IP 会变, 安全组里基于 IP 的规则要更新。
  Web UI (8081/9090/3000) 建议用 ssh tunnel 访问, 避免频繁改安全组:
  `ssh -N -L 8081:localhost:8081 fa-master` 然后浏览器开 localhost:8081

**节省成本提示:** 阿里云 ECS Spot Instance 比按量便宜 50-70%, 跑实验时开/跑完关。

---

## 3. 一次性部署 / One-shot deploy

```bash
cd deploy

# 0. 复制 env 模板, 填上 3 个节点的内网 IP
cp .env.example .env
vim .env                    # 必填: NODE_MASTER_IP / NODE_WORKER1_IP / NODE_WORKER2_IP

# 1. 本地构建 jar + flink 镜像
bash scripts/0-prepare-local.sh

# 2. 同步到 3 节点
bash scripts/1-sync-to-nodes.sh

# 3. 远程启动集群
bash scripts/2-up-all.sh

# 4. 创建 topics
bash scripts/3-create-topics.sh

# 5. 提交 Flink job
bash scripts/4-submit-job.sh --hasHeader true --hasId true --hasLabel true

# 6. 灌数据 (单独跑, 每个数据集一次)
bash scripts/5-load-data.sh ~/data/http.csv 500
```

---

## 4. 验证 / Verify

```bash
# 集群健康
curl http://$MASTER_IP:8081/overview              # Flink JM
curl http://$MASTER_IP:8081/taskmanagers          # 应看到 2 个 TM, 共 8 slots

# Kafka topic
ssh $MASTER docker exec kafka-1 kafka-topics.sh \
    --bootstrap-server $MASTER_IP:9092 --list

# 看 DataPoint 是否流出
ssh $WORKER1 docker logs -f taskmanager-2 | grep DataPoint

# 监控
open http://$MASTER_IP:9090       # Prometheus
open http://$MASTER_IP:3000       # Grafana (admin / admin)
```

---

## 5. 重新提交 / Resubmit job

代码改了, 想重提:
```bash
bash scripts/0-prepare-local.sh        # 重新打包 + 重新 build flink 镜像
bash scripts/1-sync-to-nodes.sh        # 重同步 jar + 镜像
# 容器需要拉新镜像
ssh $MASTER  "cd $REMOTE_HOME/compose && docker compose -f docker-compose.master.yml --env-file ../.env up -d --force-recreate jobmanager"
ssh $WORKER1 "cd $REMOTE_HOME/compose && BROKER_ID=2 NODE_SELF_IP=$NODE_WORKER1_IP docker compose -f docker-compose.worker.yml --env-file ../.env up -d --force-recreate taskmanager"
ssh $WORKER2 "cd $REMOTE_HOME/compose && BROKER_ID=3 NODE_SELF_IP=$NODE_WORKER2_IP docker compose -f docker-compose.worker.yml --env-file ../.env up -d --force-recreate taskmanager"
bash scripts/4-submit-job.sh
```

---

## 6. 拆除 / Teardown

```bash
bash scripts/9-teardown.sh             # 只停, 保留 ZK/Kafka 数据
bash scripts/9-teardown.sh --purge     # 同时清掉 volumes 和远端目录
```

---

## 7. 常见问题 / Troubleshooting

**Q: kafka-2 起来后 `INVALID_REPLICATION_FACTOR`?**
A: 等 broker 注册到 ZK 完成, 或检查 `advertised.listeners` 是否用了对的内网 IP。

**Q: Flink TM 起来但没 register 到 JM?**
A: 检查 `taskmanager.host` 是否填了 worker 节点内网 IP, 以及安全组是否放通 6123。

**Q: FileToKafkaProducer 跑得动但 Flink job 没数据?**
A: 两个常见原因: (1) `auto.offset.reset=latest` (代码 LocalProcessor.java:170 写死) ─ 必须先起 job 再灌数据。实验脚本 run-experiment.sh 已保证此顺序 (先提交 job 等 RUNNING, 再灌)。(2) producer 在 ssh 后台 `&` 跑被 SIGHUP 杀掉 ─ 实验脚本已改为前台阻塞跑完。手动跑 producer 时也别用 `&`。

**Q: 容器写挂载目录 AccessDenied?**
A: Flink 镜像进程用 UID 9999 (非 root), `--user root` 在此镜像不可靠。解决: 给挂载目录 `chmod 777` (实验脚本已对结果目录这么做)。

**Q: 想换数据集?**
A: 直接 `bash scripts/5-load-data.sh <新文件路径>`。如果列布局变了, `bash scripts/4-submit-job.sh --hasHeader X --hasLabel Y` 重提 job。

---

## 8. 扩容到 4-5 节点

`.env` 加 `NODE_WORKER3_IP`, `1-sync-to-nodes.sh` 和 `2-up-all.sh` 复制一段 worker 块, BROKER_ID=4。Topic partition 数同步加 (`kafka-topics.sh --alter`)。

---

## 9. 实验脚本 / Experiment scripts

部署脚本 (0-9 + refresh-ips) 负责搭集群。**跑实验**用另一组脚本:

| 脚本 | 作用 |
|---|---|
| `upload-datasets.sh` | 一次性上传所有数据集到 master (之后不用每次传) |
| `clean-topics.sh` | 实验间清 topic (delete→create, output-scores partition 可参数化) |
| `cfg_query.py` | 查 datasets.yml / experiment-configs.yml 的辅助工具 |
| `run-experiment.sh` | 单次实验完整 9 步流程 (核心) |
| `run-batch.sh` | 批量跑一个 plan (展开矩阵, 串行调度, 断点续跑) |
| `cluster-monitor.sh` | 实时监控 (节点资源/容器/jobs/offset) |
| `pull-results.sh` | 拉结果到本地 |
| `analyze-all.sh` | 批量分析 (分发给 analyze.py 各 mode) |

配置文件 (在 `deploy/`):
- `datasets.yml` — 11 个数据集的元数据 (路径/维度/header/漂移点)
- `experiment-configs.yml` — C1-C4 配置矩阵 + 实验 plan 定义

**完整实验流程见 `EXPERIMENTS_RUNBOOK.md`。**

数据流: `FileToKafkaProducer → source-topic → LocalProcessor(并行打分+HDDM漂移检测) →`
`output-scores → ScoreResultDumper → scores.jsonl`。每个实验重新 `flink run` 提交 job,
自动加载最新 jar (改算法后传新 jar 即可, 不用重启集群)。
