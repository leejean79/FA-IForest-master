# v3.0 交接文档：HDDM 漂移检测 + 漂移触发重训

> 本文是 v3.0 的工作范围。
> v1/v2.1/v2.2 已完成完整训练-协调-广播-打分自循环。
> v3.0 给 LocalProcessor 加上"动态适应"能力——检测漂移、重新训练、更新全局森林。

---

## v3.0 范围

**做什么**：
1. 新增 `DriftDetector` 接口 + `HDDM_A` 实现（com.leejean.drift 包）
2. 预留 `DriftReport` 消息类（v3.0 定义但不发 Kafka，v3.x 联邦投票时使用）
3. LocalProcessorFunction 集成 HDDM_A，扩展为 5 个状态的状态机
4. 漂移触发时增量发送新 iTree（按 slotIndex 渐进替换）
5. WARN timeout 处理（默认 DISCARD，可配置 PROMOTE）

**不做什么**（明确划线）：
- ❌ 不发 DriftReport 到 Kafka（v3.x 联邦投票才做）
- ❌ 不增加新的 Kafka topic（复用 v2.2 的 tree-topic / model-topic）
- ❌ 不修改 CoordinatorJob（v2.2 的协调器已经支持渐进 slot 替换）
- ❌ 不实现 HDDM_W（只做 HDDM_A）
- ❌ 不实现双向漂移检测（只检测分数均值上升，单边）

**关键架构原则**：v3.0 不引入新组件，只增强 LocalProcessorFunction 内部状态机。
对协调器和 Kafka 拓扑透明。

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| 漂移检测算法 | 自实现 HDDM_A（Frias-Blanco 2015 Algorithm 1） |
| 检测对象 | 异常分数序列（forest.score 输出） |
| 检测方向 | 单边——检测均值上升 |
| 漂移粒度 | per-subtask 独立检测（v3.0 无联邦） |
| WARN timeout | 默认 DISCARD，可配置 PROMOTE |
| 漂移响应模式 | 渐进 slot 替换（每攒 256 条训 1 棵，DRIFT 时一次性发出） |
| HDDM 状态存储 | `ValueState<HDDM_A>`（整对象序列化，HDDM_A 字段都是基本类型） |
| HDDM 重置时机 | 收到新森林后切回 STABLE 时 |
| Phase A 期间 HDDM 行为 | 完全不喂分数（避免假漂移）|
| WAITING 期间 HDDM 行为 | 暂停接收分数（等新森林） |

---

## 状态机设计（核心）

LocalProcessorFunction 的状态由两层组成：

### 外层：原 Phase（依赖 globalForest 是否存在）

```
Phase B: globalForest == null（冷启动训练，v1 逻辑）
Phase A: 第一次收到 globalForest 时消化 backlog（v2.1 逻辑）
Phase C: 之后所有打分（v2.1 逻辑 + v3.0 新增子状态机）
```

### 内层：Phase C 内的子状态（v3.0 新增）

```
STABLE  → WARN  → DRIFT      → WAITING → STABLE
   ↑       ↓       ↑              ↓        ↑
   └─ timeout ─────┘    new forest received
```

| 子状态 | 含义 | 行为 |
|--------|------|------|
| STABLE | 正常打分 | forest.score → 输出 → HDDM.update |
| WARN | HDDM 告警，预备阶段 | 继续 score+输出+HDDM.update；同时 warnBuffer 累积新数据；满 256 条训一棵存 candidateTrees |
| DRIFT | HDDM 确认漂移（瞬时状态） | 立即把 candidateTrees 发到 tree-topic（slotIndex=0,1,...），切到 WAITING |
| WAITING | 等待新森林 | 继续用旧森林 score+输出；HDDM 暂停（不更新）；warnBuffer 不累积 |

### 状态转换

| 转换 | 触发条件 | 动作 |
|------|----------|------|
| STABLE → WARN | HDDM.update 返回 WARN | warnBuffer.clear() |
| WARN → STABLE | HDDM.update 返回 STABLE（自然恢复，罕见） | 丢弃 warnBuffer 和 candidateTrees |
| WARN → STABLE | warnSampleCount ≥ warnTimeoutSamples (DISCARD 模式) | 丢弃；HDDM.reset() |
| WARN → DRIFT(→WAITING) | warnSampleCount ≥ warnTimeoutSamples (PROMOTE 模式) | 发出 candidateTrees；HDDM 暂停 |
| WARN → DRIFT(→WAITING) | HDDM.update 返回 DRIFT | 发出 candidateTrees；HDDM 暂停 |
| WAITING → STABLE | 收到 forestVersion > waitingForVersion 的新森林 | HDDM.reset() |

### ⚠️ 极端边界：WARN 期太短，candidateTrees 为空

WARN 触发 → 还没攒到 256 条 → DRIFT 触发 → candidateTrees 为空怎么办？

**v3.0 处理**：DRIFT 触发时如果 candidateTrees 为空，**不立即切到 WAITING**——继续累积 warnBuffer 直到训出至少 1 棵树，再切 WAITING。

具体实现：用 `pendingDriftSwitch` 标志。

```
WARN 期间：
  if HDDM.update 返回 DRIFT:
    if candidateTrees 不空: 发出，切 WAITING
    else: pendingDriftSwitch = true（仍留在 WARN，但 HDDM 不再 update）

WARN 期间 warnBuffer 满 256:
  训出 candidateTree[i]
  if pendingDriftSwitch:
    发出 candidateTrees，切 WAITING
```

---

## 关键实现细节

### 1. 新增包：`com.leejean.drift`

```
com.leejean.drift/
├── DriftDetector.java       # 接口
├── DriftStatus.java         # 枚举：STABLE / WARN / DRIFT
├── HDDM_A.java              # 实现
└── HDDM_AConfig.java        # 配置参数 POJO
```

### 2. DriftDetector 接口

```java
package com.leejean.drift;

import java.io.Serializable;

public interface DriftDetector extends Serializable {
    /**
     * 接收一个新观察值，返回当前漂移状态。
     * @param value 观察值（v3.0 即异常分数 ∈ [0, 1]）
     * @return 当前状态：STABLE / WARN / DRIFT
     */
    DriftStatus update(double value);
    
    /** 重置内部状态（漂移确认后必调用）。*/
    void reset();
    
    /** 累计已观察的样本数（调试和 DriftReport 用）。*/
    long sampleCount();
}
```

### 3. DriftStatus 枚举

```java
package com.leejean.drift;

public enum DriftStatus {
    STABLE, WARN, DRIFT
}
```

### 4. HDDM_AConfig

```java
package com.leejean.drift;

import java.io.Serializable;

public class HDDM_AConfig implements Serializable {
    public final double warnConfidence;       // P(false warn)，默认 0.005
    public final double driftConfidence;      // P(false drift)，默认 0.001
    public final long warnTimeoutSamples;     // WARN 持续多少样本未升级则 timeout，默认 10000
    
    public HDDM_AConfig(double warnConfidence, double driftConfidence, long warnTimeoutSamples) {
        if (warnConfidence <= 0 || warnConfidence >= 1)
            throw new IllegalArgumentException("warnConfidence must be in (0,1)");
        if (driftConfidence <= 0 || driftConfidence >= warnConfidence)
            throw new IllegalArgumentException("driftConfidence must be in (0, warnConfidence)");
        if (warnTimeoutSamples <= 0)
            throw new IllegalArgumentException("warnTimeoutSamples must be > 0");
        this.warnConfidence = warnConfidence;
        this.driftConfidence = driftConfidence;
        this.warnTimeoutSamples = warnTimeoutSamples;
    }
    
    public static HDDM_AConfig defaults() {
        return new HDDM_AConfig(0.005, 0.001, 10000L);
    }
}
```

### 5. HDDM_A 实现

参考 Frias-Blanco et al. 2015 Algorithm 1。

```java
package com.leejean.drift;

import java.io.Serializable;

/**
 * HDDM_A: Hoeffding Drift Detection Method, Average variant (single-sided upward).
 *
 * <p>检测均值上升的漂移。监控 sum 和 n，用 Hoeffding 不等式给出置信区间。
 * 如果当前均值 + 置信半径 显著低于历史最低均值 + 半径，更新最优。
 * 如果当前均值 - 置信半径 显著高于最优，告警/确认漂移。
 *
 * <p>Detects upward drift of the mean. Tracks running mean μ_t with Hoeffding bound ε_t.
 * Maintains the lowest observed (μ + ε) as best. When (μ_t - ε_t) significantly exceeds best,
 * signals warning or drift based on confidence levels.
 *
 * <p>Reference: Frias-Blanco et al. 2015, "Online and Non-parametric Drift Detection
 * Methods Based on Hoeffding's Bounds", IEEE TKDE.
 */
public class HDDM_A implements DriftDetector {
    private static final long serialVersionUID = 1L;
    
    private final HDDM_AConfig config;
    
    // 累计统计 / cumulative statistics
    private long n;           // 当前样本数
    private double sum;       // 累计和
    
    // 最优值（最低 μ+ε 的位置） / best so far
    private double bestMean;  // μ_min
    private double bestBound; // ε at the time of bestMean
    private long bestN;       // n at the time of bestMean
    
    // WARN 进入时的样本计数（用于 timeout）
    private long warnEnteredAt;  // 0 表示未进入 WARN
    
    public HDDM_A(HDDM_AConfig config) {
        this.config = config;
        reset();
    }
    
    @Override
    public DriftStatus update(double value) {
        n++;
        sum += value;
        
        double mean = sum / n;
        double epsilon = Math.sqrt(Math.log(1.0 / config.driftConfidence) / (2.0 * n));
        
        // 更新最优 / update best
        if (n == 1 || mean + epsilon < bestMean + bestBound) {
            bestMean = mean;
            bestBound = epsilon;
            bestN = n;
        }
        
        // 检测 / detection (single-sided upward)
        // drift 阈值
        double driftBound = bestBound * Math.sqrt(Math.log(1.0 / config.driftConfidence) /
                                                   Math.log(1.0 / config.warnConfidence));
        // warn 阈值
        double warnBound = bestBound * Math.sqrt(Math.log(1.0 / config.warnConfidence) /
                                                  Math.log(1.0 / config.driftConfidence));
        
        boolean isDrift = (mean - epsilon) > (bestMean + driftBound);
        boolean isWarn = (mean - epsilon) > (bestMean + warnBound);
        
        if (isDrift) {
            warnEnteredAt = 0;  // 退出 warn 计时
            return DriftStatus.DRIFT;
        }
        if (isWarn) {
            if (warnEnteredAt == 0) warnEnteredAt = n;
            // 检查 timeout：调用方根据 sampleCount 判断
            return DriftStatus.WARN;
        }
        // STABLE
        warnEnteredAt = 0;
        return DriftStatus.STABLE;
    }
    
    /** WARN 已持续多少样本（0 表示当前不在 WARN）。*/
    public long warnDuration() {
        return warnEnteredAt == 0 ? 0 : (n - warnEnteredAt);
    }
    
    public boolean warnTimedOut() {
        return warnDuration() >= config.warnTimeoutSamples;
    }
    
    @Override
    public void reset() {
        n = 0;
        sum = 0;
        bestMean = 0;
        bestBound = 0;
        bestN = 0;
        warnEnteredAt = 0;
    }
    
    @Override
    public long sampleCount() {
        return n;
    }
    
    public HDDM_AConfig getConfig() {
        return config;
    }
}
```

#### HDDM_A 的单元测试（必须）

`src/test/java/com/leejean/drift/HDDM_ATest.java`：

1. **静态分布不漂移**：喂 10000 个均值=0.5、std=0.05 的高斯分数 → 全程 STABLE
2. **突然漂移**：前 5000 条 mean=0.5，后 5000 条 mean=0.7 → 后期某点报 DRIFT
3. **渐进漂移**：mean 从 0.5 线性涨到 0.7 → 至少触发一次 WARN（不要求 DRIFT，因为渐进可能 timeout）
4. **reset 后重新计数**：触发 DRIFT → reset → 再喂稳定数据 → STABLE
5. **warn timeout**：构造长期 WARN 不升级的序列 → warnTimedOut() 返回 true

### 6. DriftReport 消息（预留）

`src/main/java/com/leejean/beans/DriftReport.java`：

```java
package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.leejean.drift.DriftStatus;
import java.io.Serializable;

/**
 * v3.0 定义但未使用，v3.x 联邦投票时由 LocalProcessor 上报到协调器。
 * Defined in v3.0, used in v3.x federated voting.
 */
public class DriftReport implements Serializable {
    private int subtask;
    private long timestamp;
    private DriftStatus status;
    private long sampleCount;
    
    // v3.x 扩展字段（v3.0 暂不填）
    private Double signal;        // 漂移信号强度
    
    // 标准 POJO，含 @JsonProperty / getters / setters / 无参构造器
}
```

简单 POJO 测试（JSON 往返）即可。

### 7. LocalProcessorFunction 改造

#### 新增/修改字段

```java
// v3.0 新增配置
private final HDDM_AConfig hddmConfig;
private final WarnTimeoutBehavior warnTimeoutBehavior;  // DISCARD or PROMOTE
private final int subsampleSize;       // 已有，复用
private final int localTreeCount;      // 已有，复用

// v3.0 新增 keyed state
private transient ValueState<DriftDetector> detector;
private transient ValueState<PhaseCSubState> subState;  // STABLE/WARN/DRIFT/WAITING
private transient ListState<DataPoint> warnBuffer;       // WARN 期累积新概念数据
private transient ListState<ITree> candidateTrees;       // WARN 期训出的待发树
private transient ValueState<Long> waitingForVersion;    // WAITING 时记录"等的森林版本"
private transient ValueState<Boolean> pendingDriftSwitch; // 极端边界用
```

#### Phase B/A 完全不变

v1 + v2.1 的逻辑保留。HDDM 在这两个 Phase 完全不参与。

#### Phase C 改造（核心）

```java
processElement(point, ctx, out):
    forest = ctx.getBroadcastState().get("current");
    
    if forest == null:
        // Phase B: 完全保留 v1 逻辑
        return;
    
    if 有 backlog 未消化:
        // Phase A: 完全保留 v2.1 逻辑（消化时 phase=A，不喂 HDDM）
        消化 backlog
        // 不在这里初始化 detector，等 STABLE 才用
    
    // ===== Phase C =====
    long currentForestVersion = readForestVersion(ctx);
    
    DriftDetector det = detector.value();
    if (det == null) {
        det = new HDDM_A(hddmConfig);
        // detector.update(...) 会在 STABLE/WARN 时调用
    }
    PhaseCSubState st = subState.value() != null ? subState.value() : PhaseCSubState.STABLE;
    
    double s = forest.score(point.getFeatures());
    out.collect(buildScoreResult(point, s, currentForestVersion, "C"));  // 始终输出分数
    
    switch (st) {
        case STABLE:
            handleStable(point, s, det, ctx);
            break;
        case WARN:
            handleWarn(point, s, det, ctx, out);
            break;
        case WAITING:
            handleWaiting(currentForestVersion, det, ctx);
            // HDDM 不更新；等新森林到来后由 processBroadcastElement 触发回 STABLE
            break;
        // DRIFT 是瞬时状态，不会在这里看到
    }
    
    detector.update(det);  // 持久化更新
```

#### handleStable

```java
DriftStatus s = det.update(score);
if (s == DriftStatus.WARN) {
    subState.update(WARN);
    warnBuffer.clear();      // 重置 buffer
    candidateTrees.clear();
}
// DRIFT 在 STABLE 直接来不太可能（必须先经过 WARN），但保险起见也处理
```

#### handleWarn

```java
DriftStatus s = det.update(score);
warnBuffer.add(point);

// 训树：每攒满 256 条训一棵
int currentBufferSize = countList(warnBuffer);  // 用 ValueState<Integer> 优化（O(1)）
if (currentBufferSize >= subsampleSize) {
    ITree newTree = builder.build(toArray(warnBuffer.get()), subsampleSize);
    candidateTrees.add(newTree);
    warnBuffer.clear();
}

// 检查状态转换
if (s == DriftStatus.DRIFT) {
    if (countList(candidateTrees) > 0) {
        emitCandidatesAndEnterWaiting(ctx, out, currentForestVersion);
    } else {
        pendingDriftSwitch.update(true);
        // 留在 WARN 等下一棵树训出
    }
} else if (s == DriftStatus.STABLE) {
    // 自然恢复（罕见）
    subState.update(STABLE);
    discardCandidates();
} else if (det.warnTimedOut()) {
    if (warnTimeoutBehavior == DISCARD) {
        subState.update(STABLE);
        discardCandidates();
        det.reset();  // timeout 后重置，不污染下一轮
    } else { // PROMOTE
        if (countList(candidateTrees) > 0) {
            emitCandidatesAndEnterWaiting(ctx, out, currentForestVersion);
        } else {
            // PROMOTE 但没训出树：和 DISCARD 等价
            subState.update(STABLE);
            det.reset();
        }
    }
} else if (Boolean.TRUE.equals(pendingDriftSwitch.value()) && countList(candidateTrees) > 0) {
    emitCandidatesAndEnterWaiting(ctx, out, currentForestVersion);
}
```

#### emitCandidatesAndEnterWaiting

```java
private void emitCandidatesAndEnterWaiting(Context ctx, Collector<ScoreResult> out,
                                            long currentForestVersion) {
    int slot = 0;
    for (ITree tree : candidateTrees.get()) {
        ITreeMessage msg = new ITreeMessage(
            UUID.randomUUID().toString(),
            getRuntimeContext().getIndexOfThisSubtask(),
            System.currentTimeMillis(),
            slot++,         // slotIndex 从 0 开始递增
            tree
        );
        ctx.output(TREE_TAG, msg);
    }
    
    candidateTrees.clear();
    warnBuffer.clear();
    pendingDriftSwitch.update(false);
    waitingForVersion.update(currentForestVersion);
    subState.update(WAITING);
    LOG.info("Subtask {}: emitted {} candidate trees, entering WAITING (current forest version={})",
             subtaskIndex, slot, currentForestVersion);
}
```

#### handleWaiting

```java
private void handleWaiting(long currentForestVersion, DriftDetector det, ReadOnlyContext ctx) {
    // HDDM 不更新（不接收分数）
    // 检查是否收到了等待的新森林
    Long waiting = waitingForVersion.value();
    if (waiting != null && currentForestVersion > waiting) {
        det.reset();
        subState.update(STABLE);
        LOG.info("Subtask {}: new forest version {} received, returning to STABLE",
                 subtaskIndex, currentForestVersion);
    }
}
```

#### processBroadcastElement 改造

v2.1 的逻辑保留（存森林到 broadcast state）。**不需要主动驱动 WAITING 状态切换**——`handleWaiting` 在每条 element 来时自动检查。

### 8. LocalProcessor.java 改造

新增参数：

```
--warnConfidence 0.005
--driftConfidence 0.001
--warnTimeoutSamples 10000
--warnTimeoutBehavior DISCARD  # 或 PROMOTE
```

实例化 LocalProcessorFunction 时把这些传进去。

---

## 测试策略

### 单元测试

1. **HDDM_A**（5 个场景，前面已列）
2. **HDDM_AConfig**：参数校验
3. **DriftReport**：JSON 往返

### 集成测试（LocalProcessorFunctionTest 扩展）

新增场景：

**场景 D：稳定 → WARN → STABLE（自然恢复）**
- 喂 forest，然后稳定数据 + 短暂分数飙高（不足以 DRIFT）+ 恢复稳定
- 期望：subState 经历 STABLE → WARN → STABLE，无树发出

**场景 E：稳定 → WARN → DRIFT → WAITING → STABLE**
- 喂 forest（v=1），稳定 5000 条
- 切到漂移分布（mean+0.2），喂直到 DRIFT 触发
- 验证：tree side output 收到至少 1 棵新树（slotIndex=0）
- 喂新 forest（v=2）
- 验证：subState 切回 STABLE，HDDM reset

**场景 F：WARN timeout DISCARD**
- 触发 WARN 后保持长时间不升级（warnTimeoutSamples 步内不到 DRIFT 阈值）
- 期望：无树发出，subState 切回 STABLE

**场景 G：WARN timeout PROMOTE**
- 同场景 F 但配置 PROMOTE
- 期望：tree side output 收到至少 1 棵新树

**场景 H：DRIFT 触发但 candidateTrees 为空**
- 短 WARN 期立刻 DRIFT（不到 256 条）
- 验证：subState 留在 WARN，pendingDriftSwitch=true
- 继续喂数据直到攒够 256 条
- 验证：candidate 训出后立即发出，切 WAITING

每个场景都要断言：phase 字段正确（"C" 永远，无论子状态如何）、score 持续输出。

### 手动端到端测试

参考 v2.1/v2.2 的端到端流程，但加一步：
- 在 source-topic 上注入"漂移数据"（比如把 shuttle 后半段的 features 整体加 5）
- 观察 LocalProcessor 日志：应看到 WARN/DRIFT 切换
- 观察 model-topic：应看到新版本森林被产生

---

## 实施顺序（每步 commit + 测试再继续）

1. 新增 com.leejean.drift 包：DriftStatus + DriftDetector + HDDM_AConfig → 测试 → 提交
2. 实现 HDDM_A + 完整单元测试（5 个场景）→ 提交
3. 新增 DriftReport 消息类 + 测试 → 提交
4. 改造 LocalProcessorFunction：加状态字段 + handleStable + handleWarn + handleWaiting + emit 函数 → 集成测试场景 D-H → 提交
5. 改造 LocalProcessor.java：加命令行参数传递 → 提交
6. 端到端联调（手动注入漂移）→ 文档化结果

---

## 工作风格提醒

- v3.0 的 LocalProcessorFunction 改动较大，但**v1/v2.1/v2.2 的所有原有行为必须保持不变**
  - Phase B（冷启动训练）：完全不变
  - Phase A（消化积压）：完全不变
  - Phase C 在 STABLE 子状态下：等价于 v2.1 行为（仅多了 HDDM 更新）
- 所有现有测试必须继续全绿
- HDDM 算法实现要严格按 Frias-Blanco 2015 Algorithm 1，不要"优化"或"改进"
- 不要把 candidateTrees 用 ValueState 存——必须用 ListState（树是不可变对象，逐个累积）
- 极端边界（场景 H）的 pendingDriftSwitch 标志容易被忽略，必须有专门测试

---

## v3.0 完成的判据

1. `mvn test` 全绿（含 v1/v2.1/v2.2 老测试 + v3.0 新测试 H 个场景）
2. 端到端：注入漂移数据 → 看到 LocalProcessor 日志 WARN/DRIFT 切换 → 看到 model-topic 产出新版本森林
3. output-scores 上 phase 字段始终为 "C"（漂移期间不应中断打分输出）
4. tree-topic 上能看到带 v3.0 时间戳的新树（slotIndex 0..k）

---

## 当前接手时第一件事

1. 跑 `mvn test` 确认 v1+v2.1+v2.2 全绿（baseline）
2. 开始步骤 1：创建 com.leejean.drift 包
3. 报告每一步测试结果
