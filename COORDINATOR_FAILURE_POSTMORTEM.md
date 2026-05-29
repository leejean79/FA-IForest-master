# FA-iForest 分布式协调器故障复盘
# Coordinator Forest-Assembly Failure: A Post-mortem

记录 exp2 批量实验中"协调器无全局森林产出"问题的完整排查过程：从现象、
一连串被推翻的误诊，到最终坐实的真根因。**本文档此前的根因结论（batchEnd
触发时机竞态）已被证伪**，本版为更正后的最终复盘。重点在排故方法论与
"误诊是如何产生、又如何被实测数据逐一打掉"的教训。

> 真根因一句话：**100 棵树组装成的全局 ForestMessage 序列化后约 1.09 MB，
> 超过 Kafka 单消息默认上限 1 MB（1,048,576 字节），被 broker 拒收
> （RecordTooLargeException）。** 与并发顺序、seed、数据排列、协调器逻辑均无关。

---

## 一、现象 / Symptoms

批量实验（exp2，stationary 数据集）运行中出现间歇性失败：

- donors / http（各 30 次）零失败；**forestcover 反复失败**，且看似"随机"
- 失败实验内部表现高度一致：
  - 源数据完整写入 source-topic
  - 各并行子任务正常产出局部 iTree，写入 tree-topic（恰好 100 棵，
    4 子任务各 25 棵，`(subtask, slot)` 100 个唯一组合，分布均匀，无重复）
  - tree-topic 的 Coordinator consumer LAG = 0（树全部被消费）
  - Flink UI 显示 Coordinator Function 算子 Records Received = 100、
    Records Sent = 0，状态 RUNNING、无 Exception
  - **model-topic 始终为 0** —— 看似"收齐 100 棵树却没组装出全局森林"
  - 下游打分算子等不到全局森林 → 不打分 → output-scores = 0 → 超时 JOBFAIL

**核心矛盾（贯穿整个排查）**：所有输入齐备、算子收满 100 条、却零输出、且无任何
warn/error。这个矛盾在表层无解——直到拿到完整失败现场日志才揭晓。

---

## 二、真根因 / Root Cause

### 机制

全局森林 `ForestMessage` 内含 `List<ITreeMessage> trees`（100 棵完整 iTree）。
在 forestcover（54 维、高维特征）下，该消息 JSON 序列化后约 **1,090,547 字节**，
超过 Kafka producer 的 `max.request.size` 与 broker 的 `message.max.bytes`
默认值 **1,048,576 字节（1 MB）**。Kafka 在序列化后、压缩前校验单条 record 大小，
直接抛 `org.apache.kafka.common.errors.RecordTooLargeException`，消息从未落盘。

```
Caused by: org.apache.kafka.common.errors.RecordTooLargeException:
  The message is 1090547 bytes when serialized which is larger than 1048576,
  which is the value of the max.request.size configuration.
```

### 为何"forestcover 失败、donors/http 成功"

不是 seed、不是数据排列、不是时序竞态，而是**森林体积**：
forestcover 高维 → 树结构大 → 森林 > 1 MB → 被拒；
donors/http 低维 → 森林 < 1 MB → 正常落盘。
这一条解释了从第一天起就观察到的、被误读为"数据集特殊/随机失败"的全部现象。

### 为何长期被掩盖、且表现为"随机 + 无报错"

协调器的 model-topic producer 设为 `AT_LEAST_ONCE`，但两个 Job **从未启用
checkpoint**。Flink 因此把它**强制降级为 NONE 语义**
（日志 `Switching to NONE semantic`）。NONE 语义下 producer 不等 broker ack、
不检查发送结果——于是 `RecordTooLargeException` 被**静默吞掉**：

- 协调器每次都成功执行 `out.collect(forest)`，打印 `emitted forest`
  （组装逻辑完全正确，从不出错）；
- 但这条消息被 Kafka 拒收，model-topic 保持 0；
- 算子 Records Sent = 0、日志无 warn/error —— 因为错误在 producer 回调里被吞，
  没有抛到算子层。

这正是"收满 100、零输出、无报错"这个核心矛盾的来源：**不是没 fire，是 fire 了
但消息进不去 Kafka，且错误被 NONE 语义隐藏。**

"随机性"的真相：NONE 语义下消息发出后不重试，能否侥幸落盘取决于体积是否越界——
forestcover 必越界（必失败），donors/http 不越界（必成功）。看似随机，实为确定。

---

## 三、误诊清单 / What We Got Wrong（本复盘的核心价值）

这个问题历经十余轮误诊，每个假设都曾看似合理，又都被实测数据打掉。
如实记录，供日后避坑：

| # | 误诊假设 | 被什么数据推翻 |
|---|---|---|
| 1 | seed 绑定的确定性 bug（r8/r10 反复失败） | 清理后 r8/r10 又能成功，r18 反而失败——小样本巧合 |
| 2 | batchEnd 触发时机的并发竞态（**前一版复盘的结论**） | 删除该 return 后仍失败；日志证明协调器每次都成功 emit。注：删 return 这个改动**本身是正确的健壮性改进**（fire 应基于"槽位填满"而非"收到 batchEnd 标记"，乱序到达下后者不可靠），予以保留；只是它被误当成根因修复，真正的失败另有其因（消息超限） |
| 3 | MapState 键碰撞 / 槽位缺失，filledCount < 100 | 探针实测 tree-topic 100 条、100 个唯一 (subtask,slot)、无重复 |
| 4 | 数据分发不均、某 subtask 少产树 | 实测 {0:25,1:25,2:25,3:25} 均匀 |
| 5 | 反序列化静默丢弃消息 | 源码 parse 失败会抛异常使 job FAIL，不会静默；LAG=0 |
| 6 | TaskManager 挂 / slot 不足 | 实测 2 TM 共 8 slot、有余、算子 RUNNING |
| 7 | 残留多实例并发污染 tree-topic | 清干净（No running jobs）后单跑仍失败 |
| 8 | from-latest 时序竞态漏掉森林广播 | model-topic 实测为 0——森林根本没写进去，不是没读到 |
| 9 | 参数不一致致 expectedSlots ≠ 100 | 脚本两边同源 `$PARALLELISM`，expectedSlots=100 正确 |
| 10 | broker 高负载超时导致写入失败 | broker 日志干净，无 timeout/ISR/选主 |
| 11 | gzip 一行即可治本（不必改 max.request.size） | 改 gzip 后仍报 RecordTooLarge——`max.request.size` 校验压缩前大小 |

**共同病根**：在不完整或错配的失败现场上推因果（跨 run、跨时刻、跨机器拼凑数据），
反复"凭直觉猜根因"。每一个假设单独看都合理，但都不是用"同一次失败的完整现场"
验证出来的。

**终结它的转折点**：销毁重建容器后，卡在失败点当场抓**同一次 run、两台 TM 的
同步日志 + topic offset**，读到 Kafka 抛出的精确字节数
（1090547 > 1048576）。真凶到此现形——不是任何一次聪明的推理，是取证纪律。

---

## 四、修复 / Fix

真根因是"单条消息超 Kafka 上限"，需 **producer / broker / consumer 三端 + 副本同步**
四处上限对齐，外加暴露真错误的 checkpoint 与一个同源的次要问题。

### 1. 三端 + 副本消息上限调至 5 MB（核心修复）

`max.request.size` 校验的是**压缩前**的序列化大小，gzip 无法绕过，必须显式调大。
四处缺一不可，任一处未改则消息在该端被拦：

- **Producer**（CoordinatorJob，`producerProps`）：
  `max.request.size=5242880` + `compression.type=gzip`（gzip 减小实际传输/存储，无害）
- **Broker 接收**（compose kafka-1 env）：`KAFKA_MESSAGE_MAX_BYTES=5242880`
- **Broker 副本同步**（compose kafka-1 env）：`KAFKA_REPLICA_FETCH_MAX_BYTES=5242880`
  （replication≥2 时，follower 需能 fetch 大消息，否则 ISR 收缩）
- **Consumer**（LocalProcessor，`modelKafkaProps`）：`max.partition.fetch.bytes=5242880`

### 2. 启用 checkpoint（暴露并杜绝静默丢失）

两个 Job 加 `env.enableCheckpointing(10000)`。使 `AT_LEAST_ONCE` 真正生效：
checkpoint 时 flush 并等待 broker ack，保证单条、低频、关键的森林消息可靠送达，
不再被 NONE 语义静默吞错。下游对同 version 森林重复送达幂等
（broadcast state 固定 key 覆盖），AT_LEAST_ONCE 恢复重发无副作用。

> 注：正是 checkpoint 把潜伏的 `RecordTooLargeException` 从"被吞"变为"checkpoint
> flush 失败 → task FAILED 重启循环"，真凶才暴露。checkpoint 不是制造问题，是揭露问题。

### 3. source consumer 改 setStartFromEarliest（次要、同源问题）

source `kafkaConsumer` 未设 startup 模式，默认回退 latest；producer 抢在 consumer
订阅完成前灌数据，开头若干条（实测 1243 条）被跳过 → consumer 从非零 offset 起，
进度永远差一截、卡在 99%。改 earliest 从 offset 0 读全（每 run 销毁重建容器，
source-topic 每次为空，安全）。与 model-topic 同属"latest 语义 + producer 抢跑"。

### 4. 保留：协调器 fire 判据基于"槽位填满"而非 batchEnd 标记

早先提交 `50a8e3f` 删除了 `if (!msg.isBatchEnd()) return;`，改为每棵树到达都检查
`filledCount >= expectedSlots`。该改动当时被误当成根因修复（实际未解决问题），
但其**本身是正确的健壮性改进**——乱序到达的流中，补齐最后一个槽位的树未必带
batchEnd 标记，依赖该标记触发会漏 fire。当前（删 return 的）版本已被两次成功实验
验证可用，**予以保留，不应改回**原始的 batchEnd-return 设计。

### 验证

四处修复后连续两次 forestcover r8：无 RecordTooLargeException、无协调器 FAILED 循环、
有 Completed checkpoint、`emitted forest` 仅一次、**model-topic offset = 1**、
4 子任务全部 received global forest、source 进度 100%、output-scores 有产出、
job 正常 FINISH。

---

## 五、教训 / Lessons

1. **"组件收到了输入却零输出且无报错" → 怀疑下游 I/O 边界，而非组件内部逻辑。**
   本案协调器逻辑自始至终正确，问题在它向 Kafka 写出的那一跳。我们却花了十余轮
   在组装逻辑里找不存在的 bug。Records Received=100 / Sent=0 / 无 Exception 这组
   信号，早就该指向"sink 写出失败"而非"算子没触发"。

2. **弱可靠性语义会把硬错误伪装成随机故障。** NONE 语义吞掉 `RecordTooLargeException`，
   使一个**确定性**的体积越界问题表现为"间歇随机失败"。看到"随机 + 无报错"，
   要警惕是不是错误被某层静默吞了，而非真的非确定。

3. **"间歇 + 越来越频繁"不必然是并发时序。** 前一版复盘据此断言并发竞态——错了。
   本案的"随机"实为"按数据集体积确定"，"后期加剧"实为"后期 forestcover 占比高"。
   时序假设是最诱人的误诊方向之一，必须用实测证伪而非凭特征下结论。

4. **诊断分布式问题，必须抓"同一次失败、同步、完整"的现场。** 跨 run / 跨时刻 /
   跨机器拼凑数据是本案十余轮弯路的总病根。决定性证据来自一次"销毁重建 → 卡在
   失败点 → 同时抓两台 TM 日志 + topic offset"的同步取证，一次读到真相。

5. **读到 Kafka/框架抛出的精确报错，胜过任何推理。** 真根因不是推出来的，是
   `RecordTooLargeException: 1090547 > 1048576` 这行日志直接给的。外部观测到极限时，
   答案往往就在那条一直没被完整读到的异常堆栈里。

6. **关于 Kafka 消息上限的具体事实**（排查中曾误判，记录备查）：
   `max.request.size` 校验**压缩前**大小，gzip 绕不过；需 producer、broker
   `message.max.bytes`、broker `replica.fetch.max.bytes`、consumer
   `max.partition.fetch.bytes` 四处一致调大。大对象（如整片模型）不宜整条塞进
   单个 Kafka 消息，长远应考虑分片或外部存储 + 指针广播。
