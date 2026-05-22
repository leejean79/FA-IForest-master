# 基于联邦学习的自适应异常检测系统 (FA-iForest)

> 基于 Apache Flink 和 Kafka 构建的分布式流式异常检测系统，使用 Isolation Forest 算法对实时数据流进行在线异常检测，支持冷启动训练、全局模型聚合、联邦漂移投票与自动重训。

## 1. 架构概览 (Architecture)

系统由两个独立的 Flink 作业组成，通过 Kafka topic 解耦：

```
                        ┌─────────────────────────────────┐
                        │       LocalProcessor Job        │
本地文件 (.csv/.txt)     │  (parallelism = N)              │
    │                   │                                 │
    ▼                   │  ┌───────────────────────────┐  │    tree-topic
FileToKafkaProducer     │  │ LocalProcessorFunction    │  │───────────────┐
    │                   │  │ (三阶段状态机)              │  │               │
    ▼                   │  │                           │  │  drift-topic  │
Kafka [source_topic] ──►│  │ Phase B: 冷启动训练 iTree  │  │──────────┐    ▼
                        │  │ Phase A: 积压数据消化打分   │  │          │  ┌──────────────────┐
                        │  │ Phase C: 实时打分+漂移检测  │  │          │  │ CoordinatorJob   │
                        │  └───────────────────────────┘  │          │  │ (parallelism = 1)│
                        │     ▲          ▲        │       │          │  │                  │
                        │     │          │        ▼       │          ▼  │ CoordinatorFunc  │
                        │  model-topic   │   output-scores│    ┌────────│ 聚合 iTree → 森林 │
                        │     ▲     drift-round-topic     │    │        │                  │
                        └─────┼──────────┼────────────────┘    │        │ DriftVoterFunc   │
                              │          │                     │        │ 投票决议          │
                              │          └─────────────────────┤        └────────┬─────────┘
                              └────────────────────────────────┘         model-topic
```

**数据流**：
1. `FileToKafkaProducer` 限速发送 CSV 数据到 `source_topic`
2. `LocalProcessor` 消费数据，Phase B 阶段训练 iTree 并通过 `tree-topic` 发出
3. `CoordinatorJob` 从 `tree-topic` 收集所有 iTree，组装全局森林发到 `model-topic`
4. `LocalProcessor` 通过广播流接收全局森林，进入 Phase A 消化积压 → Phase C 实时打分 + 漂移检测
5. **v3.4**：检测到漂移后上报 `drift-topic` → 协调器发起全局投票 → 多数通过才进入 COOLDOWN 重训
6. COOLDOWN 采集新概念数据 → 重训新森林 → 协调器校验同 round 后原子发布新版本
7. 评分结果输出到 `output-scores` topic

## 2. 版本迭代 (Version History)

| 版本 | 状态 | 说明 |
|------|------|------|
| v1 | ✅ 完成 | 基础 iTree 训练（LocalTrainerFunction）、树结构序列化 |
| v2.1 | ✅ 完成 | 三阶段状态机（Phase B/A/C）、广播全局森林、实时评分 |
| v2.2 | ✅ 完成 | CoordinatorJob 全局森林聚合、slot 索引、端到端流水线 |
| v3.0 | ✅ 完成 | HDDM 概念漂移检测、WARN/DRIFT 子状态机、自动触发重训 |
| v3.1 | ✅ 完成 | Phase B 环形缓冲区，warm-up 从 ~25000 条降到 ~1000 条 |
| v3.2 | ✅ 完成 | 滑动窗口 HDDM_A_Windowed、COOLDOWN 重训、概率写入环形缓冲，重训森林区分度恢复至 ≥0.15 |
| v3.3 | ✅ 完成 | 批次版本号（batchId）协调器节流，版本数从 26 降到 ≤4 |
| v3.4 | ✅ 完成 | **联邦漂移投票协议**：subtask 触发 DRIFT 不立即重训，上报协调器全局投票，多数通过才进 COOLDOWN；两种 PauseMode（USE_OLD_FOREST / BACKLOG_THEN_NEW_FOREST）；batchId 语义升级为全局 round；协调器 round 一致性校验 |

## 3. 项目结构 (Project Structure)

```
FA-iForest/
├── pom.xml
├── README.md
├── CLAUDE.md
├── docs/
│   ├── HANDOVER.md                              # 项目交接文档（架构决策 + 迭代计划）
│   ├── HANDOVER_v2.1.md                         # v2.1 交接文档
│   ├── HANDOVER_v2.2.md                         # v2.2 交接文档
│   ├── HANDOVER_v3.1.md                         # v3.1 交接文档
│   ├── HANDOVER_v3.2.md                         # v3.2 交接文档
│   ├── HANDOVER_v3.3.md                         # v3.3 交接文档
│   ├── HANDOVER_v3.4.md                         # v3.4 联邦漂移投票交接文档
│   ├── HANDOVER_v3.4.1.md                       # v3.4.1 修复交接文档
│   └── HANDOVER_v3.4.2.md                       # v3.4.2 日志依赖修复交接文档
└── src/
    ├── main/
    │   ├── java/com/leejean/
    │   │   ├── beans/
    │   │   │   ├── BroadcastEnvelope.java           # v3.4 广播流统一包装（Forest + DriftRound）
    │   │   │   ├── DataPoint.java                   # 数据点实体类
    │   │   │   ├── DriftReport.java                 # 漂移报告消息（含 DriftVote 枚举）
    │   │   │   ├── DriftRoundMessage.java           # v3.4 投票决议消息（VOTING/COMMITTED/ABORTED）
    │   │   │   ├── ForestMessage.java               # 全局森林 Kafka 消息载体
    │   │   │   ├── ITreeMessage.java                # iTree Kafka 消息载体（含 slotIndex/batchId/batchEnd）
    │   │   │   └── ScoreResult.java                 # 异常分数输出结构
    │   │   ├── common_utils/
    │   │   │   ├── CsvToDataPointFunction.java      # CSV → DataPoint 解析器 (Flink ProcessFunction)
    │   │   │   ├── KafkaHelper.java                 # Kafka 管理工具 (Topic 创建/删除/查询)
    │   │   │   ├── KafkaMainCleaner.java            # Kafka 测试数据清理工具
    │   │   │   ├── ParallelismKeys.java             # 确定性均匀 keyBy 代理 Key 生成器
    │   │   │   └── SequenceSource.java              # 序号来源策略枚举
    │   │   ├── drift/
    │   │   │   ├── DriftDetector.java               # 漂移检测器接口
    │   │   │   ├── DriftStatus.java                 # 漂移状态枚举（STABLE/WARN/DRIFT）
    │   │   │   ├── HDDM_A.java                      # HDDM_A 累积平均检测器
    │   │   │   ├── HDDM_AConfig.java                # HDDM 配置（置信度/超时参数）
    │   │   │   └── HDDM_A_Windowed.java             # 滑动窗口 HDDM_A 检测器（v3.2 默认）
    │   │   ├── flink/
    │   │   │   ├── CoordinatorFunction.java         # 协调器聚合算子（batchEnd + round 一致性触发）
    │   │   │   ├── DriftVoterFunction.java          # v3.4 投票协调算子（多数决 + 超时机制）
    │   │   │   ├── LocalProcessorFunction.java      # 三阶段状态机 + 漂移检测 + 联邦投票 + COOLDOWN 重训
    │   │   │   ├── PauseMode.java                   # v3.4 暂停模式枚举（USE_OLD_FOREST / BACKLOG_THEN_NEW_FOREST）
    │   │   │   ├── PhaseCSubState.java              # Phase C 子状态枚举（STABLE/WARN/LOCAL_DRIFT_REPORTED/COOLDOWN/WAITING）
    │   │   │   └── WarnTimeoutBehavior.java         # WARN 超时行为枚举（DISCARD/PROMOTE）
    │   │   ├── main/
    │   │   │   ├── CoordinatorJob.java              # 协调器作业入口（森林聚合 + 漂移投票双管线）
    │   │   │   └── LocalProcessor.java              # 本地处理器作业入口（source → score + drift report）
    │   │   ├── source/
    │   │   │   └── FileToKafkaProducer.java         # 文件 → Kafka 限速数据发送器
    │   │   ├── tools/
    │   │   │   ├── MockForestPublisher.java         # 测试工具：发布 mock 森林到 model-topic
    │   │   │   └── TreeTopicDumper.java             # 测试工具：导出 tree-topic 内容
    │   │   └── tree/
    │   │       ├── Forest.java                      # 森林容器（多棵 iTree + 评分逻辑）
    │   │       ├── ITree.java                       # iTree 容器（节点列表 + 元信息）
    │   │       ├── ITreeBuilder.java                # 经典 iForest 递归切分算法（含 buildFromPool）
    │   │       ├── ITreeNode.java                   # 扁平节点 POJO（Jackson 注解）
    │   │       └── RingBuffer.java                  # 固定大小环形缓冲区（FIFO 覆盖语义）
    │   └── resources/
    │       └── log4j2.properties                    # v3.4.2 日志配置（分文件输出）
    └── test/java/com/leejean/
        ├── beans/
        │   ├── BroadcastEnvelopeTest.java         # v3.4 BroadcastEnvelope 测试
        │   ├── DriftReportTest.java               # DriftReport 序列化测试（含 v3.4 投票字段）
        │   ├── DriftRoundMessageTest.java         # v3.4 DriftRoundMessage 测试
        │   ├── ForestMessageTest.java             # ForestMessage JSON 往返测试
        │   └── ITreeMessageTest.java              # ITreeMessage JSON 测试（含 batchId 兼容性）
        ├── common_utils/
        │   ├── CsvToDataPointFunctionTest.java    # CSV 解析器测试
        │   └── ParallelismKeysTest.java           # 代理 Key 均匀分布测试
        ├── drift/
        │   ├── HDDM_AConfigTest.java              # HDDM 配置测试
        │   ├── HDDM_ATest.java                    # HDDM_A 检测器测试
        │   └── HDDM_A_WindowedTest.java           # 滑动窗口 HDDM 测试（5 场景）
        ├── flink/
        │   ├── CoordinatorFunctionTest.java       # 协调器 11 场景 MiniCluster 测试（含 v3.4 round 校验）
        │   ├── DriftVoterFunctionTest.java        # v3.4 投票 5 场景 MiniCluster 测试
        │   └── LocalProcessorFunctionTest.java    # 状态机 12 场景 MiniCluster 测试（含 v3.4 投票流程）
        ├── source/
        │   └── KafkaConsumerTest.java             # Kafka Consumer 工具（需运行 broker）
        └── tree/
            ├── ForestTest.java                    # 森林评分测试
            ├── ITreeBuilderTest.java              # iTree 构建算法测试（含 buildFromPool）
            └── RingBufferTest.java                # 环形缓冲区测试（含 peekOldest）
```

## 4. 核心组件 (Key Components)

### 三阶段状态机 — LocalProcessorFunction

`KeyedBroadcastProcessFunction` 实现，每个 subtask 独立运行：

| 阶段 | 触发条件 | 行为 |
|------|---------|------|
| **Phase B** (冷启动) | 全局森林未到达 | 环形缓冲区填满后分散训 iTree → side output 发到 tree-topic |
| **Phase A** (积压消化) | 全局森林到达 | 用森林对 Phase B 积压数据逐条打分 |
| **Phase C** (实时预测+漂移检测) | 积压消化完毕 | 逐条打分 + HDDM 漂移检测子状态机 |

### Phase C 漂移检测子状态机（v3.4 联邦投票版）

```
STABLE → WARN → LOCAL_DRIFT_REPORTED → COOLDOWN → WAITING → STABLE
  ↑        ↓              ↓                                     ↑
  └─ timeout ─┘    (ABORTED → STABLE)                          │
                                                  new forest received
```

| 子状态 | 行为 |
|--------|------|
| **STABLE** | 打分 + HDDM update + 环形缓冲概率写入（p=0.3）|
| **WARN** | 打分 + HDDM update + 环形缓冲严格写入（p=0.1）|
| **LOCAL_DRIFT_REPORTED** | v3.4 已上报漂移，等协调器决议；按 PauseMode 处理数据 |
| **COOLDOWN** | 打分 + z-score 阈值筛选正常值写入环形缓冲 + 满 cooldownSamples 后重训 |
| **WAITING** | 打分 + 等待协调器广播新森林版本 |

### v3.4 联邦漂移投票协议

```
subtask DRIFT → DriftReport{INITIATE} → drift-topic
    → DriftVoterFunction 分配 roundId，广播 VOTING
    → 所有 subtask 投票 YES/NO（WARN/DRIFT=YES, STABLE=NO）
    → 多数通过 → COMMITTED → 所有 subtask 进 COOLDOWN
    → 否决 → ABORTED → 回到 STABLE，HDDM reset
```

**PauseMode**（LOCAL_DRIFT_REPORTED 期间行为）：
- `USE_OLD_FOREST`（默认）：继续用旧森林打分输出，HDDM 暂停
- `BACKLOG_THEN_NEW_FOREST`：数据进 backlog，等新森林到来后批量打分

### 协调器 — CoordinatorJob

包含两条独立管线（parallelism=1）：
- **CoordinatorFunction**：收集 iTree，检查 batchEnd + round 一致性（低 32 位）后触发新版本
- **DriftVoterFunction**：管理投票轮次，多数决（≥ parallelism/2+1），processing time 超时

### 评分公式

iForest 异常分数：`score(x) = 2^(-E[h(x)] / c(ψ))`
- `h(x)`: 数据点在每棵树中的路径长度
- `c(ψ)`: 子采样大小 ψ 对应的归一化因子

## 5. 环境依赖 (Prerequisites)

| 组件 | 版本 | 说明 |
|------|------|------|
| JDK | 1.8 | Java 编译与运行环境 |
| Maven | 3.x | 项目构建工具 |
| Apache Flink | 1.13.6 | 流处理引擎 (Scala 2.12) |
| Apache Kafka | 2.6.3 | 消息队列 |
| Log4j2 | 2.17.2 | 日志框架（SLF4J → log4j2）|
| Guava | 32.1.2-jre | RateLimiter 限流 |
| Smile | 2.6.0 | 机器学习库 |
| JSAT | 0.0.9 | Isolation Forest 参考实现 |
| Jackson | 2.13.5 | JSON 序列化 |
| JUnit 5 | 5.9.3 | 单元测试 |

## 6. 快速启动 (Quick Start)

### 6.1 编译与测试

```bash
# 编译
mvn clean compile

# 运行全部测试（92 个用例）
mvn test

# 打包（生成 shaded fat jar）
mvn clean package -DskipTests
```

### 6.2 发送数据到 Kafka

```bash
mvn exec:java -Dexec.mainClass="com.leejean.source.FileToKafkaProducer" \
    -Dexec.args="--broker localhost:9092 --topic source_topic --file /path/to/data.csv --rate 100"
```

### 6.3 启动协调器作业

```bash
mvn exec:java -Dexec.mainClass="com.leejean.main.CoordinatorJob" \
    -Dexec.args="--broker localhost:9092 --treeTopic tree-topic --modelTopic model-topic --driftTopic drift-topic --driftRoundTopic drift-round-topic --parallelism 4 --totalTrees 100 --votingTimeoutMs 5000"
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--broker` | `localhost:9092` | Kafka broker 地址 |
| `--treeTopic` | `tree-topic` | iTree 输入 topic |
| `--modelTopic` | `model-topic` | 全局森林输出 topic |
| `--driftTopic` | `drift-topic` | 漂移上报 topic（上行）|
| `--driftRoundTopic` | `drift-round-topic` | 投票决议 topic（下行）|
| `--parallelism` | `4` | LocalProcessor 的并行度 |
| `--totalTrees` | `100` | 全局森林总树数 |
| `--votingTimeoutMs` | `5000` | 投票超时毫秒数 |

### 6.4 启动本地处理器作业

```bash
mvn exec:java -Dexec.mainClass="com.leejean.main.LocalProcessor" \
    -Dexec.args="--broker localhost:9092 --topic source_topic --hasHeader true --hasId true --hasLabel true --modelTopic model-topic --scoreTopic output-scores --driftTopic drift-topic --driftRoundTopic drift-round-topic --pauseMode USE_OLD_FOREST"
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--broker` | `localhost:9092` | Kafka broker 地址 |
| `--topic` | `source_topic` | 数据源 topic |
| `--hasHeader` | `true` | CSV 是否包含标题行 |
| `--hasId` | `true` | 是否包含 id 列 |
| `--hasLabel` | `true` | 是否包含标签列 |
| `--modelTopic` | `model-topic` | 接收全局森林的 topic |
| `--scoreTopic` | `output-scores` | 评分结果输出 topic |
| `--driftTopic` | `drift-topic` | 漂移上报 topic |
| `--driftRoundTopic` | `drift-round-topic` | 投票决议 topic |
| `--pauseMode` | `USE_OLD_FOREST` | 漂移期暂停模式 |
| `--detector` | `HDDM_A_Windowed` | 漂移检测器类型 |
| `--hddmWindowSize` | `2000` | HDDM 滑动窗口大小 |
| `--cooldownSamples` | `2000` | COOLDOWN 期采集样本数 |
| `--pNormalStable` | `0.3` | STABLE 期正常数据写入概率 |
| `--pNormalWarn` | `0.1` | WARN 期正常数据写入概率 |
| `--zThresholdK` | `1.0` | COOLDOWN 期 z-score 阈值系数 |

### 6.5 Kafka Topic 说明

| Topic | 生产者 | 消费者 | 内容 |
|-------|--------|--------|------|
| `source_topic` | FileToKafkaProducer | LocalProcessor | 原始 CSV 数据 |
| `tree-topic` | LocalProcessor (side output) | CoordinatorJob | 单棵 iTree (JSON, 含 batchId) |
| `model-topic` | CoordinatorJob | LocalProcessor (broadcast) | 全局森林 (JSON) |
| `drift-topic` | LocalProcessor (side output) | CoordinatorJob | 漂移上报 (INITIATE/YES/NO) |
| `drift-round-topic` | CoordinatorJob | LocalProcessor (broadcast) | 投票决议 (VOTING/COMMITTED/ABORTED) |
| `output-scores` | LocalProcessor | 下游系统 | 异常评分结果 (JSON) |

### 6.6 日志配置

日志通过 log4j2 输出到文件（`src/main/resources/log4j2.properties`）：

| 文件 | 内容 |
|------|------|
| `logs/all.log` | 所有日志（含 Flink 框架）|
| `logs/local-processor.log` | LocalProcessorFunction 业务日志（状态切换、投票流程）|
| `logs/coordinator.log` | CoordinatorFunction + DriftVoterFunction 日志 |

```bash
# 实时查看 LocalProcessor 关键日志
tail -f logs/local-processor.log

# 实时查看协调器日志（含投票决议）
tail -f logs/coordinator.log
```

### 6.7 部署注意事项

- **版本升级**：从 v3.3 升级到 v3.4 时，需清空 `tree-topic` 和 `model-topic`（batchId 语义变更），并创建 `drift-topic` 和 `drift-round-topic`
- **topic 清空方法**：使用 `KafkaMainCleaner` 工具或 `kafka-topics.sh --delete`
- **PauseMode 切换**：通过 `--pauseMode` 参数选择，无需重新编译
