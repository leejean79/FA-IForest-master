# v3.1 交接文档：环形缓冲区 + 渐进发树（Phase B 优化）

> 本文是 v3.1 的工作范围。
> v3.0 已完成完整的 STABLE/WARN/DRIFT/WAITING 状态机和 HDDM 集成。
> v3.1 的目标：用环形缓冲区改造 Phase B 训练逻辑，把首次森林产出的数据需求从 25600 条降到约 4000 条，
> 显著减少系统 warm-up 时间。

---

## v3.1 范围

**做什么**：
1. 新增 `RingBuffer<DataPoint>` 类（Java 类，可序列化，固定大小，FIFO 覆盖）
2. 改造 LocalProcessorFunction 的 Phase B 训练逻辑：
   - 用环形缓冲区替代 v1 的 `buffer + bufferSize` 状态
   - 缓冲填满 1000 条后**一次性训 25 棵 iTree**全部发出
   - 之后停止 Phase B 训练（与 v1 行为保持一致）
3. 不动 Phase C 的 STABLE/WARN/DRIFT/WAITING 状态机
4. 不动重训逻辑（v3.2 才改）

**不做什么**（明确划线）：
- ❌ 不在 Phase C 主动写入环形缓冲区（v3.2 才做）
- ❌ 不修改 candidateTrees 训练逻辑（v3.0 行为保留）
- ❌ 不修协调器的版本爆炸问题（v3.3 才做）
- ❌ 不去掉 WARN 状态（v3.2 给 WARN 重新赋予"环形缓冲过渡期"语义时再处理）

**预期效果**：
- **Phase A 长度从 25671 条降到约 4000~5000 条**（每个 subtask 1000 条 × 4 subtask = 4000 全局）
- v1 森林质量**不应下降**（采样率从 100% 降到 25.6% 在 iForest 论文范围内）

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| 环形缓冲区默认大小 | 1000（可调参数 `ringBufferSize`）|
| 渐进发树节奏 | **方案 a**：缓冲填满后**分散**到接下来的 25 个 element 各训 1 棵 |
| 子样本大小 | 256（保持 iForest 论文 ψ）|
| 缓冲区状态实现 | 自定义 RingBuffer 类作为 ValueState 内容 |
| Phase C 期间环形缓冲行为 | **不更新**（保留 v3.1 完成时的快照，等 v3.2 启用）|
| 重训逻辑（candidateTrees）| **保留 v3.0 行为不变**（v3.2 才改造）|
| 训完 25 棵后 | 停止 Phase B 训练（与 v1 一致，等 DRIFT 触发再训） |

---

## 关键实现细节

### 1. 新增类：`com.leejean.tree.RingBuffer<T>`

简单的固定大小循环缓冲，Java 实现，可序列化。

```java
package com.leejean.tree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 固定大小的环形缓冲区，FIFO 覆盖语义 / Fixed-size ring buffer with FIFO overwrite.
 *
 * <p>填充阶段（size < capacity）：每次 add 直接追加。
 * 满后：每次 add 覆盖最老的元素，size 不再增长。
 *
 * <p>用途 / Use case: 维护一个"最近 N 条"的滑动窗口，用于 iForest 子样本采集。
 *
 * @param <T> 元素类型
 */
public class RingBuffer<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final int capacity;
    private final Object[] buffer;
    private int head;        // 下一个写入位置
    private int size;        // 当前已存元素数（≤ capacity）
    
    public RingBuffer(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.buffer = new Object[capacity];
        this.head = 0;
        this.size = 0;
    }
    
    /** 添加元素（必要时覆盖最老的）/ Add element, overwriting oldest if full. */
    public void add(T element) {
        buffer[head] = element;
        head = (head + 1) % capacity;
        if (size < capacity) {
            size++;
        }
    }
    
    public int size() {
        return size;
    }
    
    public int capacity() {
        return capacity;
    }
    
    public boolean isFull() {
        return size == capacity;
    }
    
    /** 当前所有元素的快照（按未指定顺序）/ Snapshot of all current elements. */
    @SuppressWarnings("unchecked")
    public List<T> snapshot() {
        List<T> out = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            out.add((T) buffer[i]);
        }
        return out;
    }
}
```

#### RingBuffer 单元测试

`src/test/java/com/leejean/tree/RingBufferTest.java`：

1. 容量 5，添加 3 个 → size=3, !isFull, snapshot 含 3 个
2. 容量 5，添加 5 个 → size=5, isFull, snapshot 含 5 个
3. 容量 5，添加 7 个 → size=5（不增长），snapshot 含最近 5 个（最早 2 个被覆盖）
4. 容量 0 → 抛 IllegalArgumentException
5. Java 序列化往返：填充后序列化再反序列化，size 和元素一致

### 2. LocalProcessorFunction 改造

#### 状态字段调整

```java
// === v3.1 改动：Phase B 状态 ===
// v1/v2.x 用的：
//   ListState<DataPoint> buffer  ← 删除
//   ValueState<Integer> bufferSize ← 删除
// v3.1 改用：
private transient ValueState<RingBuffer<DataPoint>> ringBuffer;
private transient ValueState<Integer> trainStartCountdown;  // 训完后的"训 25 棵分散倒计时"

// === Phase C 状态 v3.0 保留不变 ===
ListState<DataPoint> backlog;
ValueState<DriftDetector> detector;
ValueState<PhaseCSubState> subState;
ListState<DataPoint> warnBuffer;
ListState<ITree> candidateTrees;
ValueState<Long> waitingForVersion;
ValueState<Boolean> pendingDriftSwitch;

// === v1 状态 treesProduced 保留 ===
ValueState<Integer> treesProduced;
```

#### 配置参数新增

```java
// LocalProcessor.main() 里：
HDDM_AConfig hddmDefaults = HDDM_AConfig.defaults();
int ringBufferSize = params.getInt("ringBufferSize", 1000);  // 新增
// ... 其他参数
```

#### Phase B 逻辑改造

**重点**：原 v1 的"满 256 训 1 棵清空"逻辑彻底替换为"环形缓冲 + 分散训 25 棵"。

```java
private void handlePhaseB(DataPoint point, ReadOnlyContext ctx) throws Exception {
    backlog.add(point);  // 原 v2.1 行为保留：所有 Phase B 数据进 backlog
    
    Integer produced = treesProduced.value();
    if (produced != null && produced >= localTreeCount) {
        return;  // 已训完 25 棵，不再训
    }
    
    // 1. 写入环形缓冲
    RingBuffer<DataPoint> rb = ringBuffer.value();
    if (rb == null) {
        rb = new RingBuffer<>(ringBufferSize);
    }
    rb.add(point);
    
    // 2. 检查是否触发"训 25 棵"
    Integer countdown = trainStartCountdown.value();
    if (countdown == null) {
        // 还没启动训练倒计时
        if (rb.isFull()) {
            // 环形缓冲首次填满 → 启动倒计时（接下来 25 个 element 每个训 1 棵）
            countdown = localTreeCount;
            trainStartCountdown.update(countdown);
        }
        ringBuffer.update(rb);
        // 注意：填满那一条 element 自身不训树，下一条 element 才开始
        return;
    }
    
    // 3. 倒计时 > 0，训 1 棵
    if (countdown > 0) {
        // 从环形缓冲随机采样 256 条训 1 棵
        ITree tree = builder.build(sampleFromBuffer(rb, subsampleSize), subsampleSize);
        ITreeMessage msg = new ITreeMessage(
            UUID.randomUUID().toString(),
            getRuntimeContext().getIndexOfThisSubtask(),
            System.currentTimeMillis(),
            produced,  // slotIndex
            tree
        );
        ctx.output(TREE_TAG, msg);
        
        produced = (produced == null) ? 1 : produced + 1;
        treesProduced.update(produced);
        
        countdown--;
        if (countdown == 0) {
            // 训完 25 棵，清理倒计时（环形缓冲保留为 v3.2 用）
            trainStartCountdown.clear();
        } else {
            trainStartCountdown.update(countdown);
        }
    }
    
    ringBuffer.update(rb);
}

/** 从环形缓冲随机采样 N 条作为子样本。 */
private double[][] sampleFromBuffer(RingBuffer<DataPoint> rb, int n) {
    List<DataPoint> snapshot = rb.snapshot();
    // 用 builder 内部的 random，保证每个 subtask seed 隔离
    // builder 已经在 open() 里用 seed + 1009*subtaskIndex 初始化
    // 这里需要直接用 builder 的 random 做采样
    int actualN = Math.min(n, snapshot.size());
    Collections.shuffle(snapshot, builderRandom);  // 注意：需要在 LocalProcessorFunction 暴露 random
    double[][] out = new double[actualN][];
    for (int i = 0; i < actualN; i++) {
        out[i] = snapshot.get(i).getFeatures();
    }
    return out;
}
```

**⚠️ 实现陷阱：关于 random 来源**

ITreeBuilder 有自己的 random（用 seed + 1009*subtaskIndex 初始化）。环形缓冲采样的 random 应该也用同一个 seed 体系，避免子样本采样和树构建用不同 seed 引入额外不确定性。

**简化做法**：直接复用 ITreeBuilder 的 random。需要在 ITreeBuilder 暴露一个 `getRandom()` 方法，或者把 sampleFromBuffer 移到 ITreeBuilder 类内部。

**最简单做法**：ITreeBuilder 加一个新方法 `buildFromPool(List<DataPoint> pool, int sampleSize)`，内部完成"随机采样 + 构建树"两步，对外只暴露这个接口。这样 random 用法封装在 ITreeBuilder 里。

我推荐这个方案。

### 3. ITreeBuilder 新增方法

```java
public class ITreeBuilder {
    // ... 已有字段（包括 random）
    
    /**
     * v3.1 新增：从一个数据池子随机采样 sampleSize 条，构建一棵 iTree。
     * 用于环形缓冲场景——pool 通常大于 sampleSize，做无放回采样。
     */
    public ITree buildFromPool(List<double[]> pool, int sampleSize) {
        if (pool.isEmpty()) {
            throw new IllegalArgumentException("pool is empty");
        }
        int actualN = Math.min(sampleSize, pool.size());
        // 复用同一个 random（seed 已初始化）
        List<double[]> shuffled = new ArrayList<>(pool);
        Collections.shuffle(shuffled, random);
        double[][] sample = new double[actualN][];
        for (int i = 0; i < actualN; i++) {
            sample[i] = shuffled.get(i);
        }
        return build(sample, actualN);  // 复用现有 build 方法
    }
}
```

#### ITreeBuilder 测试更新

`src/test/java/com/leejean/tree/ITreeBuilderTest.java` 新增 case：

1. **buildFromPool 基本功能**：池子 1000 条，sampleSize 256 → 树的 subsampleSize=256
2. **buildFromPool 池子小于 sampleSize**：池子 100 条，sampleSize 256 → 树的 subsampleSize=100（不抛错）
3. **buildFromPool 重现性**：相同 seed 和池子 → 相同的树结构

### 4. LocalProcessor.java 改造

#### 新增命令行参数

```java
int ringBufferSize = params.getInt("ringBufferSize", 1000);
LOG.info("Configuration: ringBufferSize={}", ringBufferSize);
```

实例化 LocalProcessorFunction 时把它传进去。

### 5. 测试更新

#### 已有测试可能需要修改

v1 时代的 `LocalProcessorFunctionTest`（如果有）里"喂 N 条数据期望产出 25 棵树"的逻辑要更新：

- 旧期望：喂 25*256=6400 条数据后产出 25 棵
- 新期望：喂 1000 条**填满缓冲** + 接下来 25 条 = 1025 条数据后产出 25 棵

#### 新增 RingBufferTest（前面已列）

#### 集成测试新增场景

`LocalProcessorFunctionTest` 新增：

**场景 R1：环形缓冲 Phase B**
- 喂 1100 条数据（无广播）
- 期望：side output 收到 25 棵 ITreeMessage（slotIndex 0..24，subtask 一致）
- 期望：第 1000 条之前 0 棵；第 1000 条**之后** 25 个 element 各产 1 棵

**场景 R2：环形缓冲在 Phase C 不更新**
- 喂 1100 条进 Phase B 训完 25 棵
- 喂 forest 广播
- 喂 100 条进 Phase C
- 验证：环形缓冲的 size 仍为 1000（未增长），head 位置仍是训完时的位置（v3.1 不在 C 写入）

**场景 R3：环形缓冲容量边界**
- ringBufferSize=10（极小），subsampleSize=5
- 喂 30 条数据
- 期望：第 10 条填满，第 11~35 条各训 1 棵
- 验证树的 subsampleSize=5

---

## 验证 v3.1 完成的判据

1. `mvn test` 全绿（含 v1+v2.x+v3.0 老测试 + v3.1 新测试）
2. **关键端到端验证**：用 stable 数据集跑（45000 条），观察：
   - LocalProcessor 启动后**不到 5000 条数据**（不是之前的 25600）应看到第一版森林广播下来
   - Phase A 数据条数应大幅减少
   - v1 森林的 label 区分度（异常分数 - 正常分数）应保持 ≥ 0.15（与 v3.0 baseline 相当）
3. 用 sudden 数据集跑：HDDM 仍能在漂移点之后触发，端到端流程不破

---

## 实施顺序（每步 commit + 测试再继续）

1. 新增 `RingBuffer` 类 + 完整单测（5 个场景）→ 提交
2. ITreeBuilder 新增 `buildFromPool` 方法 + 测试 → 提交
3. LocalProcessorFunction 改造 Phase B 逻辑（替换 buffer/bufferSize 为 ringBuffer）→ 单元测试场景 R1-R3 → 提交
4. LocalProcessor.java 加 ringBufferSize 参数 → 提交
5. 端到端跑 stable 数据集对比 v3.0：Phase A 长度应大幅缩短 → 文档化结果

---

## 工作风格提醒

- **v3.1 严格只动 Phase B 训练逻辑**——不要顺手改 Phase C 状态机、不要改 HDDM、不要改协调器
- **保留 backlog state 不动**——Phase A 消化的逻辑不变
- **v1 时代的测试可能需要更新**：从"6400 条产出 25 棵"改成"1025 条产出 25 棵"。但**断言"产出 25 棵"本身不能变**
- 实施陷阱：处理"环形缓冲首次填满那一条 element"——它本身**不**训树（要先 update state，下一条 element 才开始训）。看 handlePhaseB 伪代码细节
- ITreeBuilder 的 random 在多 subtask 间已用 seed+1009*subtaskIndex 隔离——buildFromPool 复用同一 random，不要新建一个

---

## 当前接手时第一件事

1. 跑 `mvn test` 确认 v1+v2.x+v3.0 全绿
2. 实施步骤 1：创建 RingBuffer 类
3. 报告每一步测试结果

---

## 下一阶段预告

v3.1 只解决 Phase B warm-up 时长。完成后：

- **v3.2**：COOLDOWN 期 + 概率写入环形缓冲 + WARN 新语义。**修复 v3.0 暴露的"重训森林质量塌陷"**问题
- **v3.3**：协调器版本节流（batchId）。**修复 7 个版本爆炸**

每一阶段都有独立的可验证目标。不要混合做。
