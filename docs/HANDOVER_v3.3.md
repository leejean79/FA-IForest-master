# v3.3 交接文档：批次版本号（batchId）协调器节流

> 本文是 v3.3 的工作范围。
> v3.2 已完成 HDDM 滑动窗口 + COOLDOWN + 概率写入环形缓冲，重训森林质量恢复（v101 区分度 0.175，对齐 v1 的 0.190）。
> v3.3 解决最后一个核心问题：**版本爆炸**——v3.2 跑 scores15 时产生了 26 个版本，主要因为协调器对每棵新树都触发新版本。
> v3.3 引入"批次"概念：每个 subtask 一次 DRIFT 周期发出的 N 棵新树合并为一个批次，协调器在批次结束时才触发一个新版本。
>
> ⚠️ **接手前确认**：v3.2 代码已经 push 到 GitHub。如果远程仓库最新提交还是 v3.1，需要先推送 v3.2。

---

## v3.3 范围

**做什么**：
1. ITreeMessage 新增 `batchId`（long）和 `batchEnd`（boolean）两个字段
2. LocalProcessorFunction 在发树时正确填这两个字段（v1 Phase B + v3.2 COOLDOWN 重训两条路径）
3. CoordinatorFunction 仅在收到 `batchEnd=true` 的消息时才考虑触发新版本
4. 新增 keyed state `driftRoundCount` 用于生成 batchId

**不做什么**（明确划线）：
- ❌ 不做联邦投票（v3.4 才做"全局批次"）
- ❌ 不改 LocalProcessorFunction 的状态机（COOLDOWN 等子状态保留 v3.2 行为）
- ❌ 不改 HDDM 算法
- ❌ 不改环形缓冲区
- ❌ 不修改 cooldownSamples（这次单独提一次：默认改为 2000，但这只是配置默认值变更，1 行改动）

**预期效果**：
- scores15 场景下版本数从 26 个降到 **4 个**（每个 subtask 各 DRIFT 一次）
- scores35 场景下版本数从 1 个升到 **4 个**（修了 cooldownSamples=2000 后能跑完整 COOLDOWN）
- 每个 batchEnd 触发的森林版本是"原子的"——一个 subtask 一次 DRIFT 周期对应一个版本

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| batchId 类型 | `long`，**不**用 String |
| batchId 编码 | `((long) subtaskIndex << 32) \| driftRoundCount` |
| v1 Phase B 训练的 batchId | round 计数 = 0，即 `((long) subtaskIndex << 32) \| 0L` |
| v3.2 COOLDOWN 重训的 batchId | round 计数 = 1, 2, 3, ...，每次 COOLDOWN 结束 +1 |
| batchEnd 标志 | 一个批次的 25 棵中，slotIndex = localTreeCount - 1 的最后一棵 batchEnd=true |
| 协调器触发条件 | `msg.batchEnd && allSlotsFilled() && dirty` 三者都满足 |
| cooldownSamples 默认值 | 从 5000 改为 2000（让 scores35 也能跑完 COOLDOWN）|

---

## 关键实现细节

### 1. 修改 ITreeMessage

`com.leejean.beans.ITreeMessage`：

```java
public class ITreeMessage {
    // 已有字段保留：treeId, producerSubtask, slotIndex, createdAt, tree
    
    /**
     * 批次 ID，标识本树属于哪次训练批次。
     * 编码：(subtaskIndex << 32) | driftRoundCount
     * - v1 Phase B 训练：driftRoundCount = 0
     * - 每次 COOLDOWN 触发的重训：driftRoundCount += 1
     *
     * Batch ID identifying which training round this tree belongs to.
     * Encoded as (subtaskIndex << 32) | driftRoundCount.
     */
    private long batchId;
    
    /**
     * 是否是本批次的最后一棵。协调器仅在收到 batchEnd=true 时才考虑触发新版本。
     * Whether this is the last tree of the batch. Coordinator triggers a new
     * forest version only when batchEnd=true (i.e. atomic batch application).
     */
    private boolean batchEnd;
    
    // 构造器、getter/setter、@JsonProperty 补全
    // Jackson 反序列化老版本 JSON（没有这两个字段）时，batchId=0, batchEnd=false
    // ←这是合理 default，但 batchEnd=false 意味着协调器永不触发——不能直接用历史数据
}
```

#### ⚠️ 关于历史 JSON 数据的兼容性

- **tree-topic 上 v3.2 时代的旧消息**：反序列化后 batchId=0, batchEnd=false → 协调器看到永不触发新版本
- 这是 **预期行为**：v3.3 启动后，**tree-topic 上历史的"无 batchEnd"消息会被忽略**，直到新的 v3.3 LocalProcessor 发出带 batchEnd 的消息才会触发森林

实际部署时，启动 v3.3 前**清空 tree-topic 和 model-topic** 是最干净的做法。HANDOVER 文档要写明这一点。

#### ITreeMessage 测试更新

`ITreeMessageTest`：
- JSON 往返时包含 batchId 和 batchEnd
- 缺字段的旧 JSON 反序列化：batchId=0L, batchEnd=false（默认值）

### 2. LocalProcessorFunction 改造

#### 新增字段

```java
/**
 * 本 subtask 已触发的漂移轮次计数。
 * - v1 Phase B 训练：维持初始 0
 * - v3.2 每次 COOLDOWN 结束触发重训前：+1
 *
 * Counter for drift rounds triggered by this subtask.
 */
private transient ValueState<Long> driftRoundCount;
```

#### open() 中注册

```java
driftRoundCount = getRuntimeContext().getState(
    new ValueStateDescriptor<>("drift-round-count", Types.LONG));
```

#### v1 Phase B 训练路径改动（trainIfReady 或对应方法）

**当前 v3.1 实现**（每条 element 在 countdown 倒计时内训 1 棵）：

```java
if (countdown > 0) {
    ITree tree = builder.buildFromPool(pool, subsampleSize);
    int slot = (Integer) produced;  // 当前 produced 值 0..localTreeCount-1
    
    // === v3.3 改动 ===
    long batchId = ((long) subtaskIndex) << 32;  // round = 0
    boolean isLast = (slot == localTreeCount - 1);
    
    ITreeMessage msg = new ITreeMessage(
        UUID.randomUUID().toString(),
        subtaskIndex,
        System.currentTimeMillis(),
        slot,
        batchId,        // 新字段
        isLast,         // 新字段
        tree
    );
    ctx.output(TREE_TAG, msg);
    
    // ... 原有的 produced++ 和 countdown-- 逻辑
}
```

#### v3.2 COOLDOWN 重训路径改动（retrainAndEnterWaiting）

```java
private void retrainAndEnterWaiting(ReadOnlyContext ctx) throws Exception {
    // === v3.3 改动：递增漂移轮次 ===
    Long round = driftRoundCount.value();
    round = (round == null ? 1L : round + 1L);  // round 1, 2, 3, ...
    driftRoundCount.update(round);
    
    long batchId = ((long) subtaskIndex << 32) | round;
    
    RingBuffer<DataPoint> rb = ringBuffer.value();
    List<DataPoint> snapshot = rb.snapshot();
    List<double[]> pool = new ArrayList<>(snapshot.size());
    for (DataPoint dp : snapshot) pool.add(dp.getFeatures());
    
    for (int slot = 0; slot < localTreeCount; slot++) {
        ITree tree = builder.buildFromPool(pool, subsampleSize);
        boolean isLast = (slot == localTreeCount - 1);
        
        ITreeMessage msg = new ITreeMessage(
            UUID.randomUUID().toString(),
            subtaskIndex,
            System.currentTimeMillis(),
            slot,
            batchId,        // 新
            isLast,         // 新
            tree
        );
        ctx.output(TREE_TAG, msg);
    }
    
    // ... 原有的状态切换到 WAITING 逻辑
}
```

#### ⚠️ 实施陷阱：long 位运算

**坑 1**：`(long) subtaskIndex << 32` 必须显式 `(long)` 转换。Java int 左移 32 位会被截断为 0：

```java
// ❌ 错误（subtaskIndex 是 int，移 32 位变 0）
long batchId = subtaskIndex << 32 | round;

// ✅ 正确
long batchId = ((long) subtaskIndex << 32) | round;
```

**坑 2**：`|` 优先级低于 `<<`，但加括号更清晰：

```java
long batchId = ((long) subtaskIndex << 32) | round;
```

### 3. CoordinatorFunction 改造

#### processElement 修改

```java
public void processElement(ITreeMessage msg, Context ctx, Collector<ForestMessage> out) {
    int subtask = msg.getProducerSubtask();
    int slot = msg.getSlotIndex();
    
    // 边界检查保留
    if (subtask < 0 || subtask >= parallelism) { ... return; }
    if (slot < 0 || slot >= localTreeCount) { ... return; }
    
    // 存入 state（覆盖语义保留）
    Tuple2<Integer, Integer> key = Tuple2.of(subtask, slot);
    trees.put(key, msg);
    dirty.update(true);
    
    // === v3.3 改动：只在 batchEnd=true 时考虑触发 ===
    if (!msg.isBatchEnd()) {
        return;  // 等本批次最后一棵
    }
    
    // 收到 batchEnd → 检查是否满足触发条件
    int filledCount = 0;
    for (Tuple2<Integer, Integer> k : trees.keys()) filledCount++;
    
    if (filledCount >= expectedSlots && Boolean.TRUE.equals(dirty.value())) {
        fireForest(out);
        dirty.update(false);
    }
}
```

**注意**：`fireForest` 内部逻辑不变（按 slot 排序、subsampleSize 校验、版本号自增、emit）。

### 4. cooldownSamples 默认值

在 LocalProcessor.java：

```java
// 旧
int cooldownSamples = params.getInt("cooldownSamples", 5000);

// 新
int cooldownSamples = params.getInt("cooldownSamples", 2000);
LOG.info("Configuration: cooldownSamples={} (default 2000 since v3.3, was 5000 in v3.2)", cooldownSamples);
```

### 5. 测试

#### CoordinatorFunctionTest 新增场景

**场景 V33-1：单棵 batchEnd=false 不触发**
- 喂 100 棵 ITreeMessage（slots 0..24 × subtasks 0..3 全部 batchEnd=false）
- 期望：产出 0 条 ForestMessage

**场景 V33-2：单批次完成才触发**
- 喂 4 个 subtask 各 25 棵，每个 subtask 的最后一棵 batchEnd=true
- 期望：产出 ForestMessage 数量 ≤ 4（最多 4 个 batchEnd 各触发一次，但前 3 次因 100 个 slot 未填齐不会触发）
- **具体期望**：第 4 个 batchEnd 到达且 100 个 slot 满 → 触发**第 1 个** ForestMessage v1

**场景 V33-3：v3.2 重训触发新版本**
- 模拟已有完整的 v1 森林（4 subtask × 25 棵，slot 全填）
- 然后喂 subtask=0 的新批次：25 棵新树（batchId=round 1），最后一棵 batchEnd=true
- 期望：在最后一棵到达时触发 ForestMessage v2
- 验证：v2 中 (subtask=0, slot=0..24) 是新树，其他 (subtask=1/2/3, slot=*) 是旧树

**场景 V33-4：未完成批次的中间消息不触发**
- 同 V33-3 的 v1 已有完整森林
- 喂 subtask=0 的 24 棵新树（slots 0..23，全部 batchEnd=false）
- 期望：产出 0 条 ForestMessage
- 然后喂 subtask=0 的第 25 棵新树（slot=24，batchEnd=true）
- 期望：触发 v2

#### LocalProcessorFunctionTest 更新

老场景的 ITreeMessage 断言加上 batchId / batchEnd 字段验证。

具体：
- v1 Phase B 训练场景：所有 25 棵 batchId = (subtask << 32) | 0，最后一棵 batchEnd=true
- COOLDOWN 重训场景：所有新树 batchId = (subtask << 32) | round（round 从 1 开始），最后一棵 batchEnd=true

---

## 验证 v3.3 完成的判据

1. `mvn test` 全绿（v1+v2.x+v3.0+v3.1+v3.2 老测试 + v3.3 新测试 4 个场景）
2. **端到端验证 1（用 scores15 数据集）**：
   - 重新清空 tree-topic + model-topic
   - 跑端到端
   - 期望：版本数从 26 个降到 **≤ 4 个**
   - 期望：最终版本的 label 区分度仍然 ≥ 0.15
3. **端到端验证 2（用 scores35 数据集）**：
   - 期望：版本数从 1 个升到 **4 个**（每个 subtask 都触发了重训）
   - 期望：最终版本的 label 区分度 ≥ 0.15
   - 关键：scores35 数据集长 45000 条，漂移点 35000，COOLDOWN 2000 条 → 最后一个 subtask 完成 COOLDOWN 应该在 seq ~43000 之前，能跑完

---

## 实施顺序

1. ITreeMessage 加 batchId + batchEnd 字段 + getter/setter + JSON 测试 → 提交
2. LocalProcessorFunction v1 Phase B 训练填字段 + 测试 → 提交
3. LocalProcessorFunction COOLDOWN 重训填字段 + 新增 driftRoundCount state + 测试 → 提交
4. CoordinatorFunction 改 processElement 检查 batchEnd + 4 个新场景测试 → 提交
5. cooldownSamples 默认值改为 2000（独立提交，便于回滚）→ 提交
6. 端到端跑 scores15 + scores35 验证两个目标版本数 → 文档化结果

---

## 工作风格提醒

- **位运算坑**：`(long) subtaskIndex << 32` 不要漏掉强制转换
- **协调器只改一处**：只在 processElement 加 `if (!msg.isBatchEnd()) return;`，fireForest 内部不动
- **历史数据兼容**：v3.3 启动前清空 Kafka topic。这一点 README 要写明
- **不要修改其他配置默认值**：cooldownSamples 是唯一允许在 v3.3 改的默认值
- **不要顺手优化协调器**：fireForest 的内部 string concat / JSON 序列化优化是 v3.x 之后的事

---

## 当前接手时第一件事

1. 确认 GitHub 最新提交是 v3.2（如果还是 v3.1，先 push v3.2）
2. 跑 `mvn test` 确认 baseline 全绿
3. 实施步骤 1：ITreeMessage 加字段

---

## v3.3 完成后的展望

| 阶段 | 内容 | 状态 |
|---|---|---|
| v3.4 | 联邦漂移投票：协调器主导全局 batchId 分配，4 个 subtask 都 DRIFT 才触发新版本 | 未开始 |
| v4 | 性能基准 + AUC 实验 + 论文实验数据 | v3.3 完成后即可启动 |

特别注意 v3.4 的设计点：
- 协调器需要新算子 DriftVoter 接收 DriftReport 消息
- 协调器分配全局 driftRoundId 后广播给所有 subtask
- subtask 收到全局 driftRoundId 后才进入 COOLDOWN
- 这时 batchId 改为 `(subtaskIndex << 32) | globalDriftRoundId`，多个 subtask 共享低 32 位
- **v3.3 的 batchId 编码自然兼容 v3.4**——只是低 32 位的语义从"per-subtask round"变为"global round"
