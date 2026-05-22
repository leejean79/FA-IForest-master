# v3.2 交接文档：滑动窗口 HDDM + COOLDOWN 重训 + 概率写入环形缓冲

> 本文是 v3.2 的工作范围。
> v3.1 已完成 Phase B 环形缓冲改造，warm-up 从 25600 降到 ~4200 条数据。
> v3.2 解决两个 v3.0 暴露的关键问题：
> (1) HDDM_A 累积平均对漂移信号迟钝（v3.1 后变得更迟钝）
> (2) 重训新森林质量塌陷（区分度从 0.18 跌到 0.07）

---

## v3.2 范围

**做什么**：
1. 新增 `HDDM_A_Windowed` 检测器实现：用滑动窗口替代累积平均
2. LocalProcessorFunction 增加 COOLDOWN 子状态（DRIFT 之后、WAITING 之前）
3. Phase C 期间环形缓冲改为"概率写入"：根据分数判断"正常"才以一定概率写入
4. COOLDOWN 期用 z-score 自适应阈值识别新概念里的"正常值"，写入环形缓冲
5. COOLDOWN 期结束后**从环形缓冲采样训 25 棵**新 iTree（替代 v3.0 的 warnBuffer + candidateTrees 机制）
6. WARN 状态新语义化（环形缓冲过渡期）

**不做什么**（明确划线）：
- ❌ 不改协调器（v3.3 才修版本爆炸/batchId）
- ❌ 不改 ITreeMessage 结构
- ❌ 不实现联邦投票（v3.x 后续）
- ❌ 不实现 EWMA 风格的 HDDM_W（用固定窗口替代，命名为 HDDM_A_Windowed 避免和论文混淆）

**预期效果**：
- HDDM_A_Windowed 在漂移信号 > 0.10 时能稳定触发，不受漂移前样本数量影响
- v2~v7 重训森林的 label 区分度恢复到 ≥ 0.15（与 v1 相当）
- 数据集脚本 `inject_drift_v2.py` 可以把 sudden 漂移点改回 35000（更真实的漂移测试场景）

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| HDDM 滑动窗口实现 | 新建 `HDDM_A_Windowed` 类，复用 `RingBuffer<Double>` |
| 默认窗口大小 | 2000（可配置 `hddmWindowSize`）|
| 默认检测器选择 | `HDDM_A_Windowed`（v3.0 的 HDDM_A 保留但默认不用）|
| COOLDOWN 期长度 | 默认 5000 条数据（可配置 `cooldownSamples`）|
| COOLDOWN 期阈值方法 | z-score 自适应：用 COOLDOWN 期内的滑动 mean + k*std |
| Phase C 概率写入：正常分数阈值 | 用最近 N 条 STABLE 期分数的 P50 作为 normal threshold |
| Phase C 概率写入：正常采样概率 | p_normal=0.3（每 10 条正常数据写入 3 条）|
| Phase C 概率写入：异常采样概率 | p_anomaly=0（异常完全不写入环形缓冲）|
| WARN 期写入策略 | 比 STABLE 更严：p_normal=0.1（更倾向不写入，等漂移确认后用 COOLDOWN 数据）|
| 状态机 | STABLE → WARN → DRIFT → **COOLDOWN** → WAITING → STABLE |
| WARN timeout | 保留 DISCARD/PROMOTE 配置（语义不变）|

---

## 状态机更新（v3.2 完整版）

```
STABLE  → WARN  → DRIFT  → COOLDOWN → WAITING → STABLE
   ↑       ↓        ↑         ↓         ↓        ↑
   └─ timeout ──────┘         │  new forest received
                              │
                              └─ COOLDOWN 期累积数据训新 25 棵
```

| 子状态 | 行为概要 |
|--------|----------|
| **STABLE** | 打分输出 + HDDM update + 环形缓冲概率写入（p_normal=0.3）|
| **WARN** | 打分输出 + HDDM update + 环形缓冲严格写入（p_normal=0.1）|
| **DRIFT** | 瞬时状态，立即切到 COOLDOWN |
| **COOLDOWN** | 打分输出 + HDDM 暂停 + z-score 阈值识别新概念正常值，写入环形缓冲<br>满 cooldownSamples 条后 → 从环形缓冲训 25 棵新 iTree → 发出 → 切 WAITING |
| **WAITING** | 打分输出 + HDDM 暂停 + 环形缓冲不更新 + 等新森林广播 |

### 状态转换表

| 转换 | 触发条件 | 动作 |
|------|----------|------|
| STABLE → WARN | HDDM update 返回 WARN | 切换概率写入策略到 p_normal=0.1 |
| WARN → STABLE | HDDM update 返回 STABLE（自然恢复）| 恢复 p_normal=0.3 |
| WARN → STABLE | warnTimeout (DISCARD 模式) | 恢复 p_normal=0.3，HDDM.reset() |
| WARN → COOLDOWN | warnTimeout (PROMOTE 模式) 或 HDDM 返回 DRIFT | cooldownStartCount=0；HDDM 暂停 |
| COOLDOWN → WAITING | cooldownStartCount ≥ cooldownSamples | 从环形缓冲采样训 25 棵 → 发出 → 等新森林 |
| WAITING → STABLE | 收到 forestVersion > waitingForVersion | HDDM.reset()，恢复 p_normal=0.3 |

---

## 关键实现细节

### 1. 新增类：`HDDM_A_Windowed`

`com.leejean.drift.HDDM_A_Windowed`

```java
package com.leejean.drift;

import com.leejean.tree.RingBuffer;
import java.io.Serializable;

/**
 * 滑动窗口版 HDDM_A / Sliding-window HDDM_A.
 *
 * <p>与论文 HDDM_A 的关键区别：用固定大小窗口 W 替代累积统计。
 * mean 和 epsilon 都基于最近 W 条数据计算，避免漂移信号被历史稀释。
 *
 * <p>注意：这不是论文里的 HDDM_W（HDDM_W 用 EWMA 加权平均）。
 * 命名为 HDDM_A_Windowed 表明它是 HDDM_A 算法 + 滑动窗口的工程改造。
 *
 * <p>Window-based variant of HDDM_A. Replaces cumulative statistics with a fixed-size
 * sliding window, so drift signals are not diluted by long stable history.
 * NOT the same as the paper's HDDM_W (which uses EWMA). Named HDDM_A_Windowed
 * to indicate it's an engineering adaptation of HDDM_A.
 */
public class HDDM_A_Windowed implements DriftDetector {
    private static final long serialVersionUID = 1L;
    
    private final HDDM_AConfig config;
    private final int windowSize;
    
    // 滑动窗口（最近 windowSize 条观察值）
    private RingBuffer<Double> window;
    private double sum;  // 窗口内总和（增量维护，避免每次重算）
    
    // 最优值（基于窗口内当前 mean）
    private double bestMean;
    private double bestBound;
    
    // WARN 进入时的样本计数
    private long n;             // 总观察数（不限于窗口）
    private long warnEnteredAt; // 0 表示未进入 WARN
    
    public HDDM_A_Windowed(HDDM_AConfig config, int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be > 0");
        }
        this.config = config;
        this.windowSize = windowSize;
        reset();
    }
    
    @Override
    public DriftStatus update(double value) {
        n++;
        
        // 注意 RingBuffer 没有 pop API；用 window.snapshot 重算 sum
        // 但每条数据都重算 sum 是 O(W) 的开销，不可接受
        // 改用：维护增量 sum，window.add 之前判断 isFull 决定是否减去最老值
        // 但 RingBuffer 不暴露最老值——需要扩展 RingBuffer 或者改逻辑
        
        // ⚠️ 实施注意：见下方"RingBuffer 扩展"章节
        
        if (window.isFull()) {
            // 取出即将被覆盖的最老值（需要 RingBuffer 扩展支持 peekOldest）
            double oldest = window.peekOldest();
            sum -= oldest;
        }
        window.add(value);
        sum += value;
        
        int W = window.size();
        double mean = sum / W;
        // Hoeffding 边界用窗口大小 W 而不是累积 n
        double epsilon = Math.sqrt(Math.log(1.0 / config.getDriftConfidence()) / (2.0 * W));
        
        // 更新 best
        if (n == 1 || mean + epsilon < bestMean + bestBound) {
            bestMean = mean;
            bestBound = epsilon;
        }
        
        double driftBound = bestBound * Math.sqrt(Math.log(1.0/config.getDriftConfidence()) /
                                                   Math.log(1.0/config.getWarnConfidence()));
        double warnBound  = bestBound * Math.sqrt(Math.log(1.0/config.getWarnConfidence()) /
                                                   Math.log(1.0/config.getDriftConfidence()));
        
        if ((mean - epsilon) > bestMean + driftBound) {
            warnEnteredAt = 0;
            return DriftStatus.DRIFT;
        }
        if ((mean - epsilon) > bestMean + warnBound) {
            if (warnEnteredAt == 0) warnEnteredAt = n;
            return DriftStatus.WARN;
        }
        warnEnteredAt = 0;
        return DriftStatus.STABLE;
    }
    
    public long warnDuration() {
        return warnEnteredAt == 0 ? 0 : (n - warnEnteredAt);
    }
    
    public boolean warnTimedOut() {
        return warnDuration() >= config.getWarnTimeoutSamples();
    }
    
    @Override
    public void reset() {
        window = new RingBuffer<>(windowSize);
        sum = 0.0;
        bestMean = 0.0;
        bestBound = 0.0;
        n = 0;
        warnEnteredAt = 0;
    }
    
    @Override
    public long sampleCount() {
        return n;
    }
}
```

#### RingBuffer 扩展（必须先做）

`HDDM_A_Windowed` 需要 RingBuffer 暴露"最老元素"的能力，用于增量维护 sum。

在 `RingBuffer` 类加一个方法：

```java
/**
 * 返回当前最老的元素（即下一次 add 满时会被覆盖的那个）。
 * Returns the oldest element currently in the buffer (the one to be overwritten next).
 * @throws IllegalStateException if buffer is empty
 */
@SuppressWarnings("unchecked")
public T peekOldest() {
    if (size == 0) {
        throw new IllegalStateException("buffer is empty");
    }
    if (size < capacity) {
        // 缓冲未满：最老的是 index=0 的元素
        return (T) buffer[0];
    }
    // 缓冲已满：head 指向下一个写入位置，也就是最老元素的位置
    return (T) buffer[head];
}
```

补一个 RingBufferTest 单测：

- 空缓冲 peekOldest 抛 IllegalStateException
- 部分填充时 peekOldest 返回最早 add 的元素
- 满后 peekOldest 返回即将被覆盖的元素（注意 head 位置）

#### HDDM_A_Windowed 单元测试

`src/test/java/com/leejean/drift/HDDM_A_WindowedTest.java`：

1. **静态分布**：喂 10000 个 mean=0.5 std=0.05 的高斯 → 全程 STABLE
2. **窗口外漂移信号被忽略**：先喂 5000 个 mean=0.5，再喂 1500 个 mean=0.7 → 第二批中后期触发 DRIFT；继续喂 5000 个 mean=0.5（第二次稳定期）→ 应该回到 STABLE（因为窗口已经被刷新）
3. **不平衡场景**：先喂 7000 个 mean=0.5 + 2000 个 mean=0.65 → DRIFT 应触发（v3.0 的 HDDM_A 在这种比例下不会触发）
4. **窗口大小 sensitivity**：window=500 vs window=2000 → 都能在 mean 跳变 0.5→0.65 时触发，但 window=500 触发更快
5. **reset 清空**：触发 DRIFT → reset → 喂稳定数据 → STABLE

### 2. LocalProcessorFunction 状态变更

#### 删除（被环形缓冲替代）

```java
// v3.0 字段删除（不再使用）
// ListState<DataPoint> warnBuffer;        ← 删除
// ValueState<Integer> warnBufferSize;     ← 删除
// ListState<ITree> candidateTrees;        ← 删除
// ValueState<Integer> candidateTreeCount; ← 删除
```

#### 新增 / 改造

```java
// 检测器类型从 HDDM_A 改为接口 DriftDetector
// 这样可以在配置层面切换 HDDM_A_Windowed
private transient ValueState<DriftDetector> detector;

// 新增 COOLDOWN 状态
public enum PhaseCSubState {
    STABLE, WARN, COOLDOWN, WAITING  // DRIFT 是瞬时状态，不存储
}

// 新增字段
private transient ValueState<Long> cooldownStartCount;     // COOLDOWN 进入时的样本数
private transient ValueState<Double> cooldownMean;         // COOLDOWN 期内分数滑动 mean
private transient ValueState<Double> cooldownM2;           // Welford 算法的 M2，用于增量算 std
private transient ValueState<Long> cooldownN;              // COOLDOWN 期内样本数

// 已有但语义改变的字段
private transient ValueState<RingBuffer<DataPoint>> ringBuffer;  // v3.1 已有，v3.2 在 Phase C 也写入
```

#### 配置参数新增

```java
String detectorType = params.get("detector", "HDDM_A_Windowed");  // or "HDDM_A"
int hddmWindowSize = params.getInt("hddmWindowSize", 2000);
int cooldownSamples = params.getInt("cooldownSamples", 5000);
double pNormalStable = params.getDouble("pNormalStable", 0.3);
double pNormalWarn = params.getDouble("pNormalWarn", 0.1);
double zThresholdK = params.getDouble("zThresholdK", 1.0);  // COOLDOWN 期 z-score 阈值的 k
```

#### 检测器工厂方法

```java
private DriftDetector createDetector() {
    switch (detectorType) {
        case "HDDM_A":
            return new HDDM_A(hddmConfig);
        case "HDDM_A_Windowed":
            return new HDDM_A_Windowed(hddmConfig, hddmWindowSize);
        default:
            throw new IllegalArgumentException("Unknown detector: " + detectorType);
    }
}
```

### 3. handleStable 改造

```java
private void handleStable(DataPoint point, double score, DriftDetector det,
                          ReadOnlyContext ctx) throws Exception {
    DriftStatus status = det.update(score);
    
    // ===== 概率写入环形缓冲 =====
    // 判断"正常"用 score 阈值（这里用一个简单常量，后续可改成 P50）
    // STABLE 期：p_normal=0.3，p_anomaly=0
    if (score < normalScoreThreshold && random.nextDouble() < pNormalStable) {
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb != null) {
            rb.add(point);
            ringBuffer.update(rb);
        }
    }
    
    // ===== 状态转换 =====
    if (status == DriftStatus.WARN) {
        subState.update(PhaseCSubState.WARN);
        // 切换到 WARN 期写入策略由 handleWarn 处理
        LOG.info("subtask={}: STABLE → WARN", subtaskIndex);
    } else if (status == DriftStatus.DRIFT) {
        // 罕见但合法：直接进入 COOLDOWN
        enterCooldown(ctx);
        LOG.info("subtask={}: STABLE → COOLDOWN (rare path, direct)", subtaskIndex);
    }
}
```

**`normalScoreThreshold` 的来源**：第一版可以直接用一个常量（比如 0.5）；更精细的版本是用最近 N 条 STABLE 期分数的 P50。第一版用 0.5 即可，HANDOVER 文档里标记为 v3.2.x 优化点。

### 4. handleWarn 改造

```java
private void handleWarn(DataPoint point, double score, DriftDetector det,
                        ReadOnlyContext ctx) throws Exception {
    DriftStatus status = det.update(score);
    
    // ===== 概率写入：WARN 期更严格 =====
    if (score < normalScoreThreshold && random.nextDouble() < pNormalWarn) {
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb != null) {
            rb.add(point);
            ringBuffer.update(rb);
        }
    }
    
    // ===== 状态转换 =====
    if (status == DriftStatus.DRIFT) {
        enterCooldown(ctx);
        LOG.info("subtask={}: WARN → COOLDOWN", subtaskIndex);
    } else if (status == DriftStatus.STABLE) {
        subState.update(PhaseCSubState.STABLE);
        LOG.info("subtask={}: WARN → STABLE (natural recovery)", subtaskIndex);
    } else if (det.warnTimedOut()) {
        if (warnTimeoutBehavior == WarnTimeoutBehavior.PROMOTE) {
            enterCooldown(ctx);
            LOG.info("subtask={}: WARN → COOLDOWN (PROMOTE timeout)", subtaskIndex);
        } else {
            subState.update(PhaseCSubState.STABLE);
            det.reset();
            LOG.info("subtask={}: WARN → STABLE (DISCARD timeout)", subtaskIndex);
        }
    }
}
```

### 5. 新增 handleCooldown

```java
private void handleCooldown(DataPoint point, double score, ReadOnlyContext ctx,
                             Collector<ScoreResult> out) throws Exception {
    // HDDM 在 COOLDOWN 期暂停，不调用 update
    
    // ===== 增量更新 cooldown 期统计（Welford's online algorithm）=====
    Long cN = cooldownN.value(); cN = (cN == null ? 0 : cN) + 1;
    Double cMean = cooldownMean.value(); if (cMean == null) cMean = 0.0;
    Double cM2 = cooldownM2.value(); if (cM2 == null) cM2 = 0.0;
    
    double delta = score - cMean;
    cMean += delta / cN;
    double delta2 = score - cMean;
    cM2 += delta * delta2;
    
    cooldownN.update(cN);
    cooldownMean.update(cMean);
    cooldownM2.update(cM2);
    
    // ===== z-score 阈值写入 =====
    // 至少积累 50 条才开始判断（避免初期 std 不稳定）
    if (cN >= 50) {
        double std = Math.sqrt(cM2 / (cN - 1));
        double threshold = cMean + zThresholdK * std;
        if (score < threshold) {  // 在新概念里属于"相对正常"
            RingBuffer<DataPoint> rb = ringBuffer.value();
            if (rb != null) {
                rb.add(point);
                ringBuffer.update(rb);
            }
        }
    } else {
        // 前 50 条全部写入（初始化）
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb != null) {
            rb.add(point);
            ringBuffer.update(rb);
        }
    }
    
    // ===== 检查 COOLDOWN 是否结束 =====
    Long startCount = cooldownStartCount.value();
    long elapsed = cN;  // cooldownN 即 COOLDOWN 期的样本数
    if (elapsed >= cooldownSamples) {
        retrainAndEnterWaiting(ctx);
    }
}

private void enterCooldown(ReadOnlyContext ctx) throws Exception {
    subState.update(PhaseCSubState.COOLDOWN);
    cooldownN.update(0L);
    cooldownMean.update(0.0);
    cooldownM2.update(0.0);
    LOG.info("subtask={}: entered COOLDOWN", subtaskIndex);
}

private void retrainAndEnterWaiting(ReadOnlyContext ctx) throws Exception {
    // 从环形缓冲采样训 localTreeCount 棵
    RingBuffer<DataPoint> rb = ringBuffer.value();
    List<DataPoint> snapshot = rb.snapshot();
    List<double[]> pool = new ArrayList<>(snapshot.size());
    for (DataPoint dp : snapshot) pool.add(dp.getFeatures());
    
    for (int slot = 0; slot < localTreeCount; slot++) {
        ITree tree = builder.buildFromPool(pool, subsampleSize);
        ITreeMessage msg = new ITreeMessage(
            UUID.randomUUID().toString(),
            subtaskIndex,
            System.currentTimeMillis(),
            slot,
            tree
        );
        ctx.output(TREE_TAG, msg);
    }
    
    long currentForestVersion = readCurrentForestVersion(ctx);
    waitingForVersion.update(currentForestVersion);
    subState.update(PhaseCSubState.WAITING);
    
    // 清理 cooldown 临时状态
    cooldownN.clear();
    cooldownMean.clear();
    cooldownM2.clear();
    
    LOG.info("subtask={}: COOLDOWN done, emitted {} new trees, entering WAITING (waiting for version > {})",
        subtaskIndex, localTreeCount, currentForestVersion);
}
```

### 6. processElement 主分发改造

```java
public void processElement(DataPoint point, ReadOnlyContext ctx, Collector<ScoreResult> out) {
    Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);
    
    if (forest == null) {
        backlog.add(point);
        trainIfReady(point, ctx, out);  // v3.1 已实现，不变
        return;
    }
    
    drainBacklogIfNeeded(forest, out);  // v2.1 Phase A，不变
    
    // ===== Phase C =====
    long currentForestVersion = forest.getVersion();
    DriftDetector det = detector.value();
    if (det == null) {
        det = createDetector();
    }
    PhaseCSubState st = subState.value();
    if (st == null) st = PhaseCSubState.STABLE;
    
    double score = forest.score(point.getFeatures());
    out.collect(buildScoreResult(point, score, currentForestVersion, "C"));
    
    switch (st) {
        case STABLE:   handleStable(point, score, det, ctx); break;
        case WARN:     handleWarn(point, score, det, ctx); break;
        case COOLDOWN: handleCooldown(point, score, ctx, out); break;
        case WAITING:  handleWaiting(currentForestVersion, det); break;
    }
    
    detector.update(det);
}
```

### 7. 测试

#### 单元测试

- `HDDM_A_WindowedTest`：5 个场景（前面已列）
- `RingBufferTest` 增加 `peekOldest` case
- `LocalProcessorFunctionTest` 增加 v3.2 场景：

**场景 V32-1：完整 STABLE → WARN → COOLDOWN → WAITING → STABLE 路径**
- 喂稳定数据 → STABLE
- 切到漂移分布 → 触发 WARN，然后 DRIFT，进入 COOLDOWN
- 喂 cooldownSamples 条数据 → 触发重训，发出 25 棵树
- 喂新 forest → 退出 WAITING

**场景 V32-2：环形缓冲在 STABLE 期的概率写入**
- 喂 10000 条混合数据（70% normal score < 0.5, 30% anomaly score > 0.5）
- 验证：环形缓冲里的数据 100% 来自 score < 0.5 的样本
- 验证：环形缓冲大小未超过 ringBufferSize

**场景 V32-3：COOLDOWN 期 z-score 阈值正确性**
- 进入 COOLDOWN，喂 N 条均值 0.6 std 0.05 的分数
- 验证：超过 50 条后，score > 0.65 (mean + 1*std) 的不写入

#### 端到端测试

跑 `shuttle_sudden.csv`（v3.2 后可以恢复 SUDDEN_DRIFT_AT=35000，强信号 +0.13）。期望：
- HDDM_A_Windowed 在漂移点之后 ~2000 条触发 DRIFT
- 进入 COOLDOWN 5000 条后，重训发出 25 棵新树
- 协调器拼出 v2 森林
- v2 森林的 label 区分度 ≥ 0.15（通过 `analyze_scores.py` 验证）

---

## 验证 v3.2 完成的判据

1. `mvn test` 全绿（含 v1+v2.x+v3.0+v3.1 全部老测试 + v3.2 新测试）
2. **关键端到端验证**（用 sudden 数据集 + drift_at=35000 重新生成）：
   - HDDM_A_Windowed 触发 DRIFT
   - 进入 COOLDOWN，5000 条后重训
   - 协调器产出 v2 森林
   - v2 森林的 label 区分度（异常分数 - 正常分数）≥ 0.15
   - 端到端没有 NPE 或状态机死锁

---

## 实施顺序（每步 commit + 测试再继续）

1. RingBuffer 扩展 peekOldest 方法 + 测试 → 提交
2. 实现 HDDM_A_Windowed + 完整单测 → 提交
3. LocalProcessorFunction 状态字段调整（删除 warnBuffer/candidateTrees，新增 COOLDOWN 相关）→ 提交（但保持原行为不变，只是字段重命名）
4. 改造 handleStable / handleWarn 加概率写入 → 测试 → 提交
5. 新增 handleCooldown + enterCooldown + retrainAndEnterWaiting → 测试场景 V32-1/2/3 → 提交
6. LocalProcessor.java 加配置参数 → 提交
7. 端到端跑 sudden 数据集（先用 drift_at=15000 验证基本路径，再用 drift_at=35000 验证强场景）→ 文档化结果

---

## 工作风格提醒

- v3.2 是个大改动，**严格按 7 步分提交**——每步独立可回滚
- HDDM_A 类**保留**不删除，只是默认配置切到 HDDM_A_Windowed
- v3.0 的 warnBuffer / candidateTrees 字段及相关逻辑是**死代码**——v3.2 步骤 3 中明确删除
- 概率写入用 `ThreadLocalRandom.current()` 而非全局 Random（避免 keyed state 序列化 Random 对象的复杂度）
- normalScoreThreshold 第一版直接用常量 0.5；不要花时间实现 P50 跟踪
- 数据集脚本的 SUDDEN_DRIFT_AT 不要在 v3.2 期间改回 35000——等 v3.2 完成端到端验证后再改

---

## 当前接手时第一件事

1. 跑 `mvn test` 确认 v1+v2.x+v3.0+v3.1 全绿
2. 实施步骤 1：RingBuffer 加 peekOldest
3. 报告每一步测试结果

---

## v3.x 路线预告

- **v3.3**：协调器版本节流（batchId）—— 修复一次 DRIFT 产生多个版本的爆炸问题
- **v3.4**：联邦漂移投票 —— DriftReport 上报，协调器做 majority vote
- **v4**：性能基准测试 + AUC 评估 + 论文实验

每一阶段独立可验证，按需推进。
