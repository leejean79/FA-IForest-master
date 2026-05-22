# v2.2 交接文档：CoordinatorJob 真协调器作业

> 本文是 v2.2 的工作范围。
> v1 已完成（Phase B 训练 + 发树）；v2.1 已完成（mock 全局森林 + Phase A/C）；
> v2.2 要做的事：把 v2.1 中"手动 mock"的环节替换为"独立 Flink 协调器作业"，
> 完成端到端自循环。

---

## v2.2 范围

**总目标**：v2.2 完成后，跑两个 Flink 作业（LocalProcessor + CoordinatorJob）+ 1 个 Kafka，
不再需要任何手动 mock 工具，整条链路自动跑通：

```
Kafka source-topic
    ↓
LocalProcessor: Phase B 训练 → tree-topic
                 ↑
                 ↓
                CoordinatorJob: 收齐 100 棵 → 拼森林 → model-topic
                 ↑
                 ↓
LocalProcessor: 收到广播森林 → Phase A/C → output-scores
```

---

## v2.2 拆分为两个子阶段

### v2.2.1：前置改造（影响 v1 代码，必须保持向后兼容）

1. ITreeMessage 加 `slotIndex` 字段
2. LocalProcessorFunction 发树时填 `slotIndex`
3. 更新所有相关测试（确保 v1+v2.1 测试仍全绿）

### v2.2.2：协调器作业（全新代码）

4. 新建 CoordinatorJob 主类
5. 新建 CoordinatorFunction 算子（按 slot 索引存树，所有 slot 填齐时触发）
6. 单元测试 + 端到端联调

**v2.2.1 必须先完成并合并、跑通 v1+v2.1 全部测试**，再开始 v2.2.2。

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| 协调器部署 | **独立 Flink 作业**（CoordinatorJob.java，独立 main） |
| 协调器并行度 | **强制 parallelism=1**（全局聚合需要单点状态） |
| keyBy 写法 | `keyBy(t -> "global")` 后 `.setParallelism(1)` |
| 状态结构 | `MapState<Tuple2<Integer,Integer>, ITreeMessage>`（key=(subtask, slot)） |
| 触发条件 | **所有位置填齐 + dirty 标志位**（详见细节 B） |
| 版本号策略 | 内部 ValueState 自增，从 1 开始；重启重置（v2.2 限制，文档化） |
| ITreeMessage 是否加 driftFlag | **不加**——"消息存在"语义代替（v3 沉默=未漂移，发=漂移） |
| 协调器存对象还是字符串 | **存对象**（v2.2 简单优先，v3 上量后再优化为字符串） |
| 协调器配置来源 | **命令行参数**（`--parallelism 4 --totalTrees 100`，与 LocalProcessor 保持一致） |

---

## v2.2.1 详细要求

### 修改 ITreeMessage（com.leejean.beans.ITreeMessage）

新增字段：

```java
public class ITreeMessage {
    // 已有字段保留：treeId, producerSubtask, createdAt, tree
    
    /**
     * 该树在该 subtask 内的位置索引，范围 [0, localTreeCount-1]。
     * 协调器用 (producerSubtask, slotIndex) 作为唯一 key 索引森林。
     * v3 漂移触发重训时，新树带上相同 slotIndex 即可覆盖旧 slot。
     *
     * Slot index of this tree within the producing subtask, range [0, localTreeCount-1].
     * The coordinator uses (producerSubtask, slotIndex) as the unique key for forest indexing.
     * In v3, after drift triggers retraining, new trees with the same slotIndex overwrite old ones.
     */
    private int slotIndex;
}
```

构造器、getter/setter、@JsonProperty 都要补全。**注意 Jackson 反序列化 v1 时代生成的旧 JSON 时，没有 slotIndex 字段会反序列化为 0**——这正确吗？

**正确**。v2.1 的 mock forest 拿出来重投也只是 slotIndex=0 全部一致——这是一个边界场景但不会让 v2.2.2 的协调器崩溃（它只是会"100 棵树都占 slot=0"，state.size()=1，永远凑不齐 100 个 slot 不触发）。**v2.2.1 完成后旧的 mock 数据不能直接复用**——需要重新跑一次 v1 让 LocalProcessor 写入带 slotIndex 的新树到 tree-topic。

### 修改 LocalProcessorFunction（com.leejean.flink.LocalProcessorFunction）

定位 `trainIfReady()` 中创建 ITreeMessage 的地方。**关键时序**：

```java
// ❌ 错误：这样 slotIndex 会是 1..localTreeCount
produced++;
treesProduced.update(produced);
ITreeMessage msg = new ITreeMessage(..., slotIndex = produced);

// ✅ 正确：先用 produced 当前值（0..localTreeCount-1），再自增
ITreeMessage msg = new ITreeMessage(
    UUID.randomUUID().toString(),
    subtaskIndex,
    System.currentTimeMillis(),
    produced,            // slotIndex = 当前的 produced 值（0..localTreeCount-1）
    tree
);
ctx.output(TREE_TAG, msg);

buffer.clear();
bufferSize.update(0);
produced++;
treesProduced.update(produced);
```

### 测试更新

- `ITreeMessageTest`：JSON 往返要包含 slotIndex
- `LocalProcessorFunctionTest`：场景 A（Phase B 训练）的断言加上"每个 subtask 产出的 25 棵树 slotIndex 应为 0..24 各一次"
- 跑全部 `mvn test`，所有测试必须绿

### 验证 v2.2.1 完成的判据

1. `mvn test` 全绿
2. **重新跑 v1 + v2.1 端到端**：用 `TreeTopicDumper` 拉新数据，每条 ITreeMessage JSON 包含 `slotIndex` 字段且值在 [0, 24]
3. 对每个 subtask 数据分组，应看到 `{slotIndex: 0..24}` 各 1 次

---

## v2.2.2 详细要求

### 新增类：CoordinatorFunction（com.leejean.coordinator.CoordinatorFunction）

类签名：

```java
public class CoordinatorFunction
    extends KeyedProcessFunction<String, ITreeMessage, ForestMessage> {
    
    // 配置参数
    private final int parallelism;       // 期望的 LocalProcessor 并行度
    private final int totalTrees;        // 期望的全局森林总棵数
    private final int localTreeCount;    // = ceil(totalTrees / parallelism)
    private final int expectedSlots;     // = parallelism * localTreeCount
    
    public CoordinatorFunction(int parallelism, int totalTrees) {
        this.parallelism = parallelism;
        this.totalTrees = totalTrees;
        this.localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        this.expectedSlots = parallelism * localTreeCount;
    }
    
    // 状态：所有已收到的树
    // key = (subtask, slot)，value = 整棵 ITreeMessage
    private transient MapState<Tuple2<Integer, Integer>, ITreeMessage> trees;
    
    // 当前森林版本号（自增）
    private transient ValueState<Long> currentVersion;
    
    // 自上次发森林后是否有树更新过
    private transient ValueState<Boolean> dirty;
}
```

### open()

```java
@Override
public void open(Configuration parameters) {
    trees = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("forest-trees",
            TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>(){}),
            TypeInformation.of(ITreeMessage.class)));
    
    currentVersion = getRuntimeContext().getState(
        new ValueStateDescriptor<>("forest-version", Types.LONG));
    
    dirty = getRuntimeContext().getState(
        new ValueStateDescriptor<>("dirty-flag", Types.BOOLEAN));
}
```

### processElement()

```java
@Override
public void processElement(ITreeMessage msg, Context ctx, Collector<ForestMessage> out)
    throws Exception {
    
    int subtask = msg.getProducerSubtask();
    int slot = msg.getSlotIndex();
    
    // 边界检查：subtask 和 slot 必须在配置范围内
    if (subtask < 0 || subtask >= parallelism) {
        LOG.warn("Received tree from subtask {} but coordinator configured for parallelism={}. Skipping.",
            subtask, parallelism);
        return;
    }
    if (slot < 0 || slot >= localTreeCount) {
        LOG.warn("Received tree with slotIndex {} but localTreeCount={}. Skipping.",
            slot, localTreeCount);
        return;
    }
    
    // 存树（覆盖语义：同 (subtask, slot) 的新树覆盖旧的）
    Tuple2<Integer, Integer> key = Tuple2.of(subtask, slot);
    trees.put(key, msg);
    dirty.update(true);
    
    // 检查是否所有位置都填齐
    int filledCount = 0;
    for (Tuple2<Integer, Integer> k : trees.keys()) filledCount++;
    
    if (filledCount >= expectedSlots && Boolean.TRUE.equals(dirty.value())) {
        fireForest(out);
        dirty.update(false);
    }
}

private void fireForest(Collector<ForestMessage> out) throws Exception {
    // 收集所有树，按 (subtask, slot) 顺序排序后输出（保证确定性）
    List<ITreeMessage> ordered = new ArrayList<>(expectedSlots);
    for (int subtask = 0; subtask < parallelism; subtask++) {
        for (int slot = 0; slot < localTreeCount; slot++) {
            ITreeMessage t = trees.get(Tuple2.of(subtask, slot));
            if (t != null) ordered.add(t);
        }
    }
    
    if (ordered.size() != expectedSlots) {
        LOG.warn("fireForest: expected {} trees but got {}, skipping.",
            expectedSlots, ordered.size());
        return;
    }
    
    // 校验所有树的 subsampleSize 一致（避免拼出无效森林）
    int subsampleSize = ordered.get(0).getTree().getSubsampleSize();
    for (ITreeMessage t : ordered) {
        if (t.getTree().getSubsampleSize() != subsampleSize) {
            LOG.error("Trees have inconsistent subsampleSize: {} vs {}, skipping.",
                subsampleSize, t.getTree().getSubsampleSize());
            return;
        }
    }
    
    // 版本号自增
    Long v = currentVersion.value();
    long version = (v == null ? 1L : v + 1);
    currentVersion.update(version);
    
    ForestMessage forest = new ForestMessage(
        UUID.randomUUID().toString(),
        version,
        System.currentTimeMillis(),
        subsampleSize,
        ordered
    );
    out.collect(forest);
    
    LOG.info("Coordinator: emitted forest version {} with {} trees", version, ordered.size());
}
```

### 新增类：CoordinatorJob（com.leejean.coordinator.CoordinatorJob）

```java
public class CoordinatorJob {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("broker", "localhost:9092");
        String treeTopic = params.get("treeTopic", "tree-topic");
        String modelTopic = params.get("modelTopic", "model-topic");
        int parallelism = params.getInt("parallelism", 4);
        int totalTrees = params.getInt("totalTrees", 100);
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 协调器始终 parallelism=1
        env.setParallelism(1);
        
        // ===== Source: 订阅 tree-topic =====
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id", "coordinator-" + UUID.randomUUID().toString().substring(0, 8));
        
        FlinkKafkaConsumer<String> treeConsumer = new FlinkKafkaConsumer<>(
            treeTopic, new SimpleStringSchema(), consumerProps);
        // v2.2 必须从 earliest 读：协调器重启后能恢复完整森林状态
        treeConsumer.setStartFromEarliest();
        
        DataStream<ITreeMessage> treeStream = env.addSource(treeConsumer)
            .name("Tree Topic Source")
            .map(new ITreeMessageDeserializer())
            .name("Parse ITreeMessage");
        
        // ===== Process: 协调聚合 =====
        DataStream<ForestMessage> forestStream = treeStream
            .keyBy(t -> "global")  // 强制全局单 key
            .process(new CoordinatorFunction(parallelism, totalTrees))
            .name("Coordinator Function");
        
        // ===== Sink: 发到 model-topic =====
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);
        
        FlinkKafkaProducer<ForestMessage> modelProducer = new FlinkKafkaProducer<>(
            modelTopic,
            new ForestMessageSerializationSchema(modelTopic),
            producerProps,
            FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        
        forestStream.addSink(modelProducer)
            .name("Model Topic Sink");
        
        forestStream.print("ForestMessage emitted");
        
        env.execute("Coordinator - Forest Assembly (v2.2)");
    }
    
    // 可以复用 LocalProcessor 里的 ITreeMessageDeserializer / ForestMessageSerializationSchema，
    // 或者各自独立。建议独立：避免协调器作业反向依赖 LocalProcessor 的内部类。
}
```

### v2.2 限制（必须文档化）

```java
/*
 * v2.2 限制：协调器作业重启会丢失版本号状态（currentVersion 重新从 1 开始）。
 * 这可能在 model-topic 上产生重复 version=1 消息。LocalProcessor 看到时会用最新一条覆盖旧的。
 * 不影响功能正确性，但版本号不再单调递增。生产环境部署需要：
 *   - 启用 Flink checkpoint 持久化协调器状态
 *   - 或启动时从 model-topic 读最新版本号 +1 作为起点
 *
 * v2.2 limitation: coordinator restart loses version state (currentVersion resets to 1).
 * Acceptable for v2.2 because:
 *   - tree-topic from-earliest replay restores tree state correctly
 *   - LocalProcessor uses the latest forest seen via broadcast (last-write-wins)
 *   - Functionality is correct; only the monotonic version number is broken
 * Production deployment will need either Flink checkpointing or version recovery from model-topic.
 */
```

### 测试

#### CoordinatorFunctionTest（MiniCluster 集成测试）

**场景 1：基本拼装**
- 构造 100 条 ITreeMessage（subtask 0..3 各 25 条，slotIndex 0..24）
- 喂给 CoordinatorFunction
- 期望产出 1 条 ForestMessage version=1，trees.size()=100，按 (subtask, slot) 排序

**场景 2：触发条件——99 棵不触发**
- 喂 99 条（缺 (3, 24)）
- 期望产出 0 条 ForestMessage

**场景 3：覆盖更新触发新版本**
- 先喂齐 100 条 → 产出 v1
- 再喂 1 条 (subtask=0, slotIndex=0) 的新树（覆盖语义）
- 期望再产出 1 条 ForestMessage version=2，且 v2 中 (0, 0) 是新树

**场景 4：重复消息不触发**
- 先喂齐 100 条 → 产出 v1
- 再喂同一条已存在的树（内容完全相同）
- 期望仍只产出 1 条 ForestMessage（dirty 不应误标为 true）

> 注意场景 4 实现：仅当 trees.put() 实际改变内容时才设 dirty=true。
> 实现要点：put 前先 get 比较（或直接判断 .equals）。
> 但简化版可以：put 总是设 dirty=true，场景 4 会触发"v2 = v1"的重复 ForestMessage。
> v2.2 接受这个简化（等同于 LocalProcessor 看到 v2 == v1，没有副作用）。

**场景 5：subsampleSize 不一致**
- 构造 99 条 subsampleSize=256 + 1 条 subsampleSize=128
- 期望产出 0 条 ForestMessage（fireForest 校验失败）+ ERROR 日志

**场景 6：超出范围的 subtask/slot**
- 构造 1 条 subtask=99 的非法消息
- 期望产出 0 条（被边界检查跳过）+ WARN 日志

#### 端到端联调

启动顺序：
1. Kafka（含 tree-topic, model-topic, source-topic, output-scores）
2. **CoordinatorJob**（先启动；初始时 tree-topic 还没数据，它在等）
3. **LocalProcessor**（启动后开始训练）
4. 数据生产者往 source-topic 灌 shuttle 数据

观察：
- LocalProcessor 日志：训完 25 棵 × 4 subtask = 100 棵后停止训练
- CoordinatorJob 日志：`emitted forest version 1 with 100 trees`
- LocalProcessor 日志：`subtask=N: received global forest version 1 with 100 trees`
- output-scores topic：开始出现 ScoreResult，phase 字段先是 "A"（积压消化），然后 "C"（实时打分）

---

## 实施顺序（每一步都要 commit + 跑测试再下一步）

### v2.2.1
1. ITreeMessage 加 slotIndex + getter/setter + @JsonProperty + 测试 → 提交
2. LocalProcessorFunction 发树时填 slotIndex（注意时序）+ 测试 → 提交
3. 跑 `mvn test`，确保 v1 + v2.1 全部测试仍绿

### v2.2.2
4. 新建 CoordinatorFunction + 单元测试场景 1-6 → 提交
5. 新建 CoordinatorJob 主类 → 提交
6. 端到端手动联调（启动两个 Flink 作业 + Kafka）→ 文档化结果

每一步报告测试结果再继续。

---

## 工作风格提醒（CLAUDE.md）

- ITreeMessage 改动会影响 v1 的多处代码（构造调用、JSON 反序列化），必须逐一更新
- 不要顺手改 LocalProcessorFunction 的其他逻辑——只改"创建 ITreeMessage 的那段时序"
- 协调器是新代码，但要复用现有的序列化器，不要重新造（如果发现现有的不能用，先讨论再改）
- v2.2 不实现：driftFlag 字段、字符串状态优化、checkpoint 持久化、漂移触发重训。这些都是 v3 的事。

---

## 当前接手时第一件事

1. 跑 `mvn test` 确认 v1 + v2.1 全绿（baseline）
2. 开始 v2.2.1 步骤 1：修改 ITreeMessage
3. 报告每一步测试结果
