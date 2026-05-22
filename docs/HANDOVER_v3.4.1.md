# v3.4.1 修复：DriftReport sink + 日志文件

## 问题诊断

v3.4 端到端测试结果显示 sudden 数据集**没有产生任何新森林**，drift-topic / drift-round-topic 上均无消息。代码 review 发现两个 bug：

### Bug #1（致命）：LocalProcessor 没把 DriftReport 写到 drift-topic

`LocalProcessorFunction.enterLocalDriftReported()` 调用 `ctx.output(DRIFT_REPORT_TAG, report)` 写出 side output，但 **LocalProcessor.java 的 main() 中没有把这个 side output 取出来并接到 Kafka sink**。

结果：所有 DriftReport 在 side output 中被丢弃，永远到不了 drift-topic。
连锁后果：协调器收不到 INITIATE → 不分配 roundId → 不发 DriftRoundMessage → subtask 永远卡在 LOCAL_DRIFT_REPORTED → 数据进 backlog 出不来（BACKLOG 模式）或一直用旧森林（USE_OLD_FOREST 模式）。

### Bug #2（小）：drift-topic consumer 用了 setStartFromLatest

`CoordinatorJob.java:130` 用 `setStartFromLatest()`，与 tree-topic 的 `setStartFromEarliest()` 不一致。协调器重启后可能错过 drift-topic 上未处理的消息。

### Bug #3（影响诊断）：LocalProcessorFunction 全部用 System.out.printf 而非 SLF4J

LocalProcessorFunction 里 22 处日志全部用 `System.out.printf`，**而 CoordinatorFunction / DriftVoterFunction 用 SLF4J LOG.info**——混搭。后果：

- log4j2.properties 无法控制 LocalProcessorFunction 的日志（它们绕过 logger 框架）
- 日志全部进 console，无法分流到 `logs/local-processor.log`
- 无法按级别过滤（v3.4 没有 DEBUG 级别的预留）

修复必须做：所有 `System.out.printf` 改成 `LOG.info`（或对应级别）。

---

## v3.4.1 范围

**做什么**：
1. LocalProcessor.java 加 DriftReport 的 Kafka sink
2. 新增 DriftReportSerializationSchema 内部类
3. CoordinatorJob.java drift-topic consumer 改用 setStartFromEarliest
4. **LocalProcessorFunction.java 所有 System.out.printf 改 SLF4J LOG**（按下表分级）
5. 新增 `src/main/resources/log4j2.properties`，把关键日志写到本地文件

**不做什么**：
- 不改任何算法逻辑
- 不改投票协议、PauseMode 等设计
- 不改测试

**预期**：v3.4 sudden 数据集应能产生新森林，BACKLOG / USE_OLD_FOREST 两种模式都有完整流程数据。

---

## 修复 1：LocalProcessor.java 加 DriftReport sink

在 LocalProcessor.java 的 main() 中，**找到 `processed.addSink(scoreProducer)` 这一段**（约第 285 行），在它之后追加：

```java
        // ===== v3.4.1 修复：DriftReport side output → drift-topic =====
        DataStream<DriftReport> driftReportStream = processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG);

        FlinkKafkaProducer<DriftReport> driftReportProducer = new FlinkKafkaProducer<>(
                driftTopic,
                new DriftReportSerializationSchema(driftTopic),
                kafkaProducerProps,  // 复用已有 producer props
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        driftReportStream.addSink(driftReportProducer)
                .name("Drift Topic Sink [" + driftTopic + "]");

        // 可选：把 DriftReport 也打印到控制台便于调试
        driftReportStream.print("DriftReport emitted");
```

注意：
- `driftTopic` 变量已经在 main 里读过（`String driftTopic = params.get("driftTopic", "drift-topic");`）
- `kafkaProducerProps` 是现有的 producer 配置（看上下文确定准确变量名）

在 LocalProcessor.java 类内部加 DriftReport 序列化器（类似已有的 ITreeMessageSerializationSchema）：

```java
    /**
     * DriftReport JSON 序列化器
     * v3.4.1: 把 DriftReport side output 写到 Kafka drift-topic
     */
    private static class DriftReportSerializationSchema implements KafkaSerializationSchema<DriftReport> {
        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        public DriftReportSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DriftReport msg, Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DriftReport", e);
            }
        }
    }
```

import 补充：
```java
import com.leejean.beans.DriftReport;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
```

（这些 import 可能已经有了，看现有代码）

---

## 修复 2：CoordinatorJob.java drift consumer

CoordinatorJob.java 第 130 行：

```java
// 改前
driftConsumer.setStartFromLatest();

// 改后
driftConsumer.setStartFromEarliest();
```

理由：协调器无 checkpoint，靠 Kafka 重放恢复状态。drift-topic 应该和 tree-topic 一样从 earliest 开始读，确保重启后能拿到所有未处理的 INITIATE/vote 消息。

---

## 修复 3：LocalProcessorFunction 日志改 SLF4J + 分级

### 类顶部加 LOG 字段

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalProcessorFunction extends KeyedBroadcastProcessFunction<...> {
    private static final Logger LOG = LoggerFactory.getLogger(LocalProcessorFunction.class);
    // ... 现有字段
}
```

### 把所有 `System.out.printf` 改成 LOG，按下表分级

| 行号 | 当前 | 改为 | 理由 |
|---|---|---|---|
| 205 | startup config | `LOG.info(...)` | 启动级，重要 |
| 233 | received forest | `LOG.info(...)` | 事件级，每次新版本一次 |
| 245 | received DriftRound | `LOG.info(...)` | 事件级 |
| 276 | Phase A drained | `LOG.info(...)` | 一次性事件 |
| 302 | initiator updated pendingRoundId | `LOG.info(...)` | 状态机关键 |
| 372 | STABLE → WARN | `LOG.info(...)` | 状态切换 |
| 377 | STABLE → LOCAL_DRIFT_REPORTED | `LOG.info(...)` | 状态切换 |
| 394 | WARN → LOCAL_DRIFT_REPORTED | `LOG.info(...)` | 状态切换 |
| 397 | WARN → STABLE (recovery) | `LOG.info(...)` | 状态切换 |
| 403 | WARN → LOCAL_DRIFT_REPORTED (PROMOTE) | `LOG.info(...)` | 状态切换 |
| 408 | WARN → STABLE (DISCARD) | `LOG.info(...)` | 状态切换 |
| 472 | WAITING → STABLE, drained backlog | `LOG.info(...)` | 状态切换 |
| 478 | WAITING → STABLE (new forest) | `LOG.info(...)` | 状态切换 |
| 494 | detected DRIFT, reporting INITIATE | `LOG.info(...)` | 关键诊断点 |
| 529 | voted | `LOG.info(...)` | 关键诊断点 |
| 548 | COMMITTED → COOLDOWN | `LOG.info(...)` | 关键诊断点 |
| 579 | ABORTED drained backlog | `LOG.info(...)` | 状态切换 |
| 584 | ABORTED → STABLE | `LOG.info(...)` | 状态切换 |
| 593 | entered COOLDOWN | `LOG.info(...)` | 状态切换 |
| 612 | COOLDOWN done but ring buffer empty | `LOG.warn(...)` | 异常情况 |
| 635 | COOLDOWN done, emitted N trees | `LOG.info(...)` | 关键诊断点 |
| 650 | entering WAITING | `LOG.info(...)` | 状态切换 |
| **724** | **tree #N/M produced** | **`LOG.debug(...)`** | **训完每棵打印=高频，降到 DEBUG** |

### 重要：第 724 行降为 DEBUG

```java
// 改前
System.out.printf("[LocalProcessor] subtask=%d, tree #%d/%d produced (slot=%d)%n",
        subtaskIndex, produced, localTreeCount, produced - 1);

// 改后
LOG.debug("subtask={}, tree #{}/{} produced (slot={})",
        subtaskIndex, produced, localTreeCount, produced - 1);
```

理由：v1 Phase B 训 100 棵，COOLDOWN 每次再训 100 棵——这种"每棵都打印"在调试时有用，但默认 INFO 不需要。

### 其他保持 INFO 的事件

上表每一项前缀 `[LocalProcessor] subtask=N:` 在 LOG 调用里不需要重复——log4j2 pattern 已经包含 logger name 和线程信息。**改 LOG 时去掉 `[LocalProcessor]` 前缀**。

例子：
```java
// 改前
System.out.printf("[LocalProcessor] subtask=%d: STABLE → WARN (sampleCount=%d)%n",
        subtaskIndex, det.sampleCount());

// 改后
LOG.info("subtask={}: STABLE → WARN (sampleCount={})", subtaskIndex, det.sampleCount());
```

注意 `printf` 用 `%d`/`%s`，SLF4J 用 `{}` 占位符。

---

## 修复 4：日志写文件

新建 `src/main/resources/log4j2.properties`，内容用我提供的文件（log4j2.properties，已附在 outputs）。

启动 Flink 作业后：
- 所有日志写到 **`logs/all.log`**（含 Flink 框架）
- LocalProcessorFunction 业务日志（含 v3.4 投票流程）单独到 **`logs/local-processor.log`**
- CoordinatorFunction + DriftVoterFunction 日志到 **`logs/coordinator.log`**

`logs/` 目录在 Flink 作业的工作目录下（一般是项目根目录或 Flink 启动目录）。

实施步骤：
1. 创建 `src/main/resources/` 目录
2. 复制 log4j2.properties 进去
3. mvn package 重新打包（让 properties 进 jar）

---

## 验证

修复后跑端到端 sudden 数据集，应看到：

### drift-topic 应该有消息

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic drift-topic --from-beginning --max-messages 20
```

至少看到 `{..."vote":"INITIATE"...}` 和 `{..."vote":"YES"...}` 等消息。

### drift-round-topic 应该有消息

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic drift-round-topic --from-beginning --max-messages 20
```

至少看到 `{..."status":"VOTING"...}` 和 `{..."status":"COMMITTED"或"ABORTED"...}`。

### 本地日志文件应该有关键消息

`logs/local-processor.log` 应有：
```
subtask=N: detected DRIFT, reporting to coordinator
subtask=N: voted YES for round M
subtask=N: vote COMMITTED for round M, entering COOLDOWN
```

`logs/coordinator.log` 应有：
```
Coordinator: initiated drift round M by subtask N
Coordinator: round M resolved as COMMITTED (yes=3, no=1, abstain=0)
```

### 端到端预期

- sudden 数据集应产生 v2（甚至更多版本）
- 漂移后 FPR 应该恢复到 < 20%
- compute_auc.py 输出应该看到 v1 + v2 两个版本

---

## 实施顺序

1. 修复 LocalProcessor.java 加 DriftReport sink → 编译通过 → 提交
2. 修复 CoordinatorJob.java setStartFromEarliest（1 行改动）→ 提交
3. **LocalProcessorFunction 全部 System.out.printf 改 LOG**（按上表分级）→ 编译通过 → 提交
4. 新建 src/main/resources/log4j2.properties → 提交
5. mvn package → 端到端跑 sudden 数据集（两种 PauseMode）→ 文档化结果

## 验证日志生效

启动 Flink 作业后，应该看到：

```bash
# logs 目录自动创建
ls logs/
# all.log  local-processor.log  coordinator.log

# 实时查看 LocalProcessor 关键日志
tail -f logs/local-processor.log

# 实时查看协调器日志（含 DriftVoterFunction 决议）
tail -f logs/coordinator.log
```

如果 `logs/local-processor.log` 是**空文件**——说明 LOG 改造没生效（可能漏改了某些 System.out.printf 或导入有问题）。

如果 `logs/local-processor.log` 里**全是 INFO 但没有 DEBUG 的"tree produced"**——这是预期的（DEBUG 默认不输出）。临时启用：把 log4j2.properties 里 `logger.local.level = INFO` 改为 `DEBUG`。

---

## 工作风格提醒

- 这次修复**手术式**，**只动 5 个文件**：
  - LocalProcessor.java（加 sink + 序列化器内部类）
  - CoordinatorJob.java（1 行改 setStartFrom）
  - LocalProcessorFunction.java（22 处 System.out.printf 改 LOG，加 LOG 字段和 SLF4J imports）
  - 新建 log4j2.properties
  - 可能修改 pom.xml 加 log4j2 实现依赖（如果当前缺）
- **不要**顺手"清理"或"优化"其他代码
- **不要**改 LOG.info 的消息内容——只换 API（printf → SLF4J）
- 修完直接跑端到端，不要新增测试用例
- 验证步骤里的 kafka-console-consumer 命令必须先跑——如果 drift-topic 仍无消息，说明 sink 没接上，回头查代码
