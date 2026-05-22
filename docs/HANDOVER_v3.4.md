# v3.4 交接文档：联邦漂移投票协议

> 本文是 v3.4 的工作范围。
> v3.3 已完成 batchId 节流，sudden 数据集版本数从 26 个降到 5 个，最终森林质量达标。
> v3.4 引入"联邦漂移投票"——单个 subtask 触发 DRIFT 不再立即重训，而是上报给协调器
> 召集全局投票，多数同意才进入 COOLDOWN。这把"4 subtask 独立各 DRIFT 一次产生 4 版本"
> 进化为"全局一次漂移产生 1 版本"，并对应论文的核心学术叙事。

---

## v3.4 范围

**做什么**：
1. 新增 2 个 Kafka topic：`drift-topic`（上行）+ `drift-round-topic`（下行）
2. 启用 `DriftReport` 消息（v3.0 预留）+ 新增 `DriftRoundMessage`
3. 协调器新增 `DriftVoterFunction` 算子：分配全局 roundId、收集投票、决议
4. LocalProcessor 新增 `LOCAL_DRIFT_REPORTED` 子状态 + 2 种 PauseMode 配置
5. batchId 语义升级：从"per-subtask 计数"改为"全局 round"
6. 协调器触发新版本的条件升级为"同一全局 round 的所有 batchEnd 都到齐"
7. Flink 广播流合并：用 `BroadcastEnvelope` 包装 ForestMessage 和 DriftRoundMessage

**不做什么**（明确划线）：
- ❌ 不实现 BACKLOG_THEN_OLD_FOREST 模式（模式 2，等同 USE_OLD_FOREST 的 FPR 表现）
- ❌ 不实现 WARN 同步 COOLDOWN 优化（v3.5 候选）
- ❌ 不修改 HDDM 算法
- ❌ 不修改环形缓冲/COOLDOWN 训练逻辑

**预期效果**：
- sudden 数据集版本数从 5 个降到 **1 个**（全局唯一）
- 两种 PauseMode 实测对比，产出论文 ablation 数据
- HDDM 未触发的 subtask（少数情况）通过投票决议可参与全局漂移响应

---

## 已敲定的设计决策（不要再讨论）

| 项 | 决策 |
|----|------|
| 上行 topic | 新建 `drift-topic` |
| 下行 topic | 新建 `drift-round-topic` |
| 投票"赞成"定义 | subtask 状态 ∈ {WARN, DRIFT} = 赞成；STABLE = 反对 |
| 投票超时 | Flink processing time timer，`votingTimeoutMs` 默认 5000ms |
| 多数 | 过半数（4 subtask 中 ≥ 3 票赞成）|
| 超时未投票 | 弃权 = 反对 |
| 弃权后清理 | 协调器广播 ABORTED，所有 subtask HDDM 完全 reset |
| batchId 编码 | `(subtaskIndex << 32) \| globalRoundId`（语义升级，编码不变）|
| 协调器触发条件 | 同 globalRoundId 的所有 4 subtask 都送达 batchEnd=true |
| v1 Phase B 兼容 | 用 globalRoundId=0；协调器只检查低 32 位相等 |
| Flink 拓扑 | 方案 A：用 `BroadcastEnvelope` 包装两种广播消息合并成一条广播流 |
| PauseMode | `USE_OLD_FOREST`（默认）+ `BACKLOG_THEN_NEW_FOREST` |
| 协调器新算子 | 在现有 CoordinatorJob 中加 DriftVoterFunction（不开新 Flink 作业）|

---

## 状态机更新（v3.4 完整版）

### Phase C 内子状态扩展

```
STABLE → WARN → LOCAL_DRIFT_REPORTED → COOLDOWN → WAITING → STABLE
   ↑       ↓             ↓                                     ↑
   └─ timeout ───────────┘                                     │
                  │
                  ↓ (ABORTED 决议)
              (跳回 STABLE)
```

| 子状态 | 行为 |
|--------|------|
| STABLE | 打分 + HDDM update + 概率写环缓 (p_normal=0.3) |
| WARN | 打分 + HDDM update + 概率写环缓 (p_normal=0.1) |
| **LOCAL_DRIFT_REPORTED**（新增）| 已上报 DriftReport，HDDM 暂停，等协调器决议 |
| COOLDOWN | 等待 cooldownSamples 条数据后训新树（v3.2 行为）|
| WAITING | 已发新树，等新森林广播下来 |

### LOCAL_DRIFT_REPORTED 状态下的两种 PauseMode

**USE_OLD_FOREST**（默认）：
- 数据来了用旧森林打分输出
- HDDM 暂停
- 等决议
- 下游持续看到输出，FPR 在漂移期会飙升

**BACKLOG_THEN_NEW_FOREST**：
- 数据进 backlog（复用 v2.1 的 backlog 字段）
- 不输出
- 等决议 + 新森林产出后才批量处理 backlog
- 下游有数分钟中断，但漂移期数据用新森林打分

### 状态转换表

| 当前状态 | 触发条件 | 新状态 | 动作 |
|---|---|---|---|
| STABLE | HDDM update → WARN | WARN | 切换概率写入策略 |
| STABLE | HDDM update → DRIFT | LOCAL_DRIFT_REPORTED | 上报 DriftReport，进暂停模式 |
| WARN | HDDM update → STABLE（自然恢复）| STABLE | 恢复概率，丢 warn 计数 |
| WARN | HDDM update → DRIFT | LOCAL_DRIFT_REPORTED | 上报 DriftReport，进暂停模式 |
| WARN | warnTimeout (DISCARD) | STABLE | HDDM reset，恢复 STABLE |
| WARN | warnTimeout (PROMOTE) | LOCAL_DRIFT_REPORTED | 同 DRIFT 路径 |
| LOCAL_DRIFT_REPORTED | 收到投票请求广播（**v3.4 新**） | LOCAL_DRIFT_REPORTED | 根据当前 HDDM 状态投赞成/反对 |
| LOCAL_DRIFT_REPORTED | 收到 COMMITTED 决议 | COOLDOWN | 消化 backlog（如果 BACKLOG 模式），进 COOLDOWN |
| LOCAL_DRIFT_REPORTED | 收到 ABORTED 决议 | STABLE | 消化 backlog（用旧森林），HDDM reset |
| COOLDOWN | cooldownSamples 条满 | WAITING | 训 25 棵 + 发出，等新森林 |
| WAITING | 收到 forestVersion 增大的森林广播 | STABLE | 处理 backlog（如果有），HDDM reset |

### ⚠️ 关键流程：subtask 被动投票

不只是触发 DRIFT 的那个 subtask 参与投票——**所有 subtask 都要对每个 globalRoundId 投票**。

```
任何 subtask 收到 DriftRoundMessage{roundId, status=VOTING}：
  if 该 subtask 当前是发起投票的那个（自己上报触发）:
    已经投过赞成（不重复）
  else:
    根据当前 HDDM 状态投票：
      if 状态 ∈ {WARN, LOCAL_DRIFT_REPORTED} → 投赞成
      if 状态 ∈ {STABLE, COOLDOWN, WAITING} → 投反对
    通过 drift-topic 上报 DriftReport{subtask=N, roundId, vote=YES/NO}
```

注意：COOLDOWN/WAITING 状态的 subtask 投反对——因为它们已经在响应**之前**的某次漂移，**不参与**新的投票。

---

## 消息协议详细规范

### 1. DriftReport（subtask → 协调器，via drift-topic）

```java
public class DriftReport {
    private int subtask;              // 上报的 subtask
    private long timestamp;
    private long roundId;             // 0 = 首次触发（请求新 round），>0 = 响应已有 round 的投票
    private DriftVote vote;           // YES / NO / INITIATE
    private DriftStatus localStatus;  // 当前本地 HDDM 状态（投票决策依据）
    // v3.0 预留的 signal 字段保留但 v3.4 不填
    
    public enum DriftVote { INITIATE, YES, NO }
}
```

**两种使用场景**：

- **场景 A：subtask 本地 HDDM 触发 DRIFT**
  - vote = `INITIATE`
  - roundId = `0`（请求协调器分配）
- **场景 B：subtask 响应投票请求**
  - vote = `YES` 或 `NO`
  - roundId = 协调器分配的 round

### 2. DriftRoundMessage（协调器 → subtask, via drift-round-topic）

```java
public class DriftRoundMessage {
    private long roundId;
    private long timestamp;
    private RoundStatus status;  // VOTING / COMMITTED / ABORTED
    private int votesYes;
    private int votesNo;
    private int votesAbstain;
    
    public enum RoundStatus { VOTING, COMMITTED, ABORTED }
}
```

### 3. BroadcastEnvelope（合并广播流的 wrapper）

```java
public class BroadcastEnvelope {
    public enum Type { FOREST, DRIFT_ROUND }
    
    private Type type;
    private ForestMessage forestMessage;        // 仅 type=FOREST 时填
    private DriftRoundMessage driftRoundMessage; // 仅 type=DRIFT_ROUND 时填
    
    // Jackson 用 @JsonTypeInfo 或者直接保留两个可空字段
    // 推荐后者：简单，调试友好
}
```

订阅端用 instanceof / switch 判断。

---

## 协调器（CoordinatorJob）改造

### 当前结构

v3.3 的 CoordinatorJob：
```
tree-topic source → keyBy("global") → CoordinatorFunction → model-topic sink
```

### v3.4 结构

```
tree-topic source       → keyBy("global") → CoordinatorFunction → ┐
                                                                  ├→ BroadcastEnvelope wrapper → model-topic sink
drift-topic source      → keyBy("global") → DriftVoterFunction  → ┘
                                                                  └→ drift-round-topic sink

（注：上图是 logical view。实际两个 sink 都到同一个 Kafka cluster，
但 broadcast envelope 是订阅端组合的逻辑——实际上两个 topic 各自承载一种消息，
LocalProcessor 端把两个 topic 合并成 BroadcastEnvelope。
所以协调器侧 NOT 需要包装 envelope；envelope 是订阅端的事。）
```

修订：协调器**不**需要 BroadcastEnvelope。它分别向 model-topic 发 ForestMessage、向 drift-round-topic 发 DriftRoundMessage。

LocalProcessor 端订阅两个 topic，**source 阶段**把它们合并成一个 `DataStream<BroadcastEnvelope>`，再 broadcast。

### DriftVoterFunction 算子设计

```java
public class DriftVoterFunction extends KeyedProcessFunction<String, DriftReport, DriftRoundMessage> {
    
    // 参数
    private final int parallelism;       // 4
    private final long votingTimeoutMs;  // 5000ms 默认
    private final int majorityThreshold; // 3（过半数）
    
    // 状态
    private transient ValueState<Long> nextRoundId;
    private transient ValueState<ActiveVote> activeVote; // 当前进行中的投票
    
    public static class ActiveVote implements Serializable {
        long roundId;
        long startTimeMs;
        Set<Integer> yesVoters = new HashSet<>();
        Set<Integer> noVoters = new HashSet<>();
        // missing 用 (parallelism - yes - no) 推算
    }
}
```

#### processElement

```java
@Override
public void processElement(DriftReport report, Context ctx, Collector<DriftRoundMessage> out) {
    ActiveVote av = activeVote.value();
    
    if (report.getVote() == DriftVote.INITIATE) {
        // 场景 A：subtask 触发新一轮投票
        if (av != null) {
            // 已经有进行中的投票——忽略这个 INITIATE
            // (该 subtask 应该等当前轮次结束)
            LOG.warn("Received INITIATE while round {} is active, ignoring", av.roundId);
            return;
        }
        // 分配新 roundId
        long newId = nextRoundIdValue();
        nextRoundId.update(newId + 1);
        
        ActiveVote newAv = new ActiveVote();
        newAv.roundId = newId;
        newAv.startTimeMs = ctx.timerService().currentProcessingTime();
        newAv.yesVoters.add(report.getSubtask());  // 触发者自动算赞成
        activeVote.update(newAv);
        
        // 广播 VOTING 状态
        out.collect(new DriftRoundMessage(newId, System.currentTimeMillis(),
            RoundStatus.VOTING, 1, 0, 0));
        
        // 注册超时 timer
        ctx.timerService().registerProcessingTimeTimer(newAv.startTimeMs + votingTimeoutMs);
        
        LOG.info("Coordinator: initiated drift round {} by subtask {}", newId, report.getSubtask());
        return;
    }
    
    // 场景 B：投票响应
    if (av == null || av.roundId != report.getRoundId()) {
        // 已经决议过的轮次或不存在的 roundId
        LOG.warn("Received vote for non-active round {}, ignoring", report.getRoundId());
        return;
    }
    
    if (report.getVote() == DriftVote.YES) {
        av.yesVoters.add(report.getSubtask());
    } else if (report.getVote() == DriftVote.NO) {
        av.noVoters.add(report.getSubtask());
    }
    activeVote.update(av);
    
    // 检查是否可以提前决议
    if (av.yesVoters.size() + av.noVoters.size() == parallelism) {
        // 全部投票收齐，立即决议
        resolveVote(av, out);
    }
}

@Override
public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriftRoundMessage> out) {
    ActiveVote av = activeVote.value();
    if (av == null) {
        return; // 已经被全票决议
    }
    LOG.info("Coordinator: round {} timeout, forcing decision", av.roundId);
    resolveVote(av, out);
}

private void resolveVote(ActiveVote av, Collector<DriftRoundMessage> out) {
    int yes = av.yesVoters.size();
    int no = av.noVoters.size();
    int abstain = parallelism - yes - no;
    
    RoundStatus status = (yes >= majorityThreshold) ? RoundStatus.COMMITTED : RoundStatus.ABORTED;
    
    out.collect(new DriftRoundMessage(av.roundId, System.currentTimeMillis(),
        status, yes, no, abstain));
    
    activeVote.clear();  // 清理，准备下次投票
    
    LOG.info("Coordinator: round {} resolved as {} (yes={}, no={}, abstain={})",
        av.roundId, status, yes, no, abstain);
}
```

### CoordinatorFunction（已有）的小改动

v3.3 时协调器看 `batchId == batchId of same key`，但其实只检查整数相等。v3.4 时**只检查 batchId 低 32 位**（globalRoundId）—— 因为同 round 的不同 subtask batchId 高 32 位不同。

```java
// fireForest 内部
// 检查所有 100 棵树是否同 round
long expectedRound = -1;
for (Tuple2<Integer, Integer> key : trees.keys()) {
    ITreeMessage msg = trees.get(key);
    long round = msg.getBatchId() & 0xFFFFFFFFL;  // 低 32 位
    if (expectedRound == -1) expectedRound = round;
    else if (round != expectedRound) {
        LOG.warn("Round mismatch: expected {}, got {}", expectedRound, round);
        return; // 不触发——等所有 subtask 完成本轮
    }
}
```

这意味着 v3.4 的 dirty 触发条件升级：

```
v3.3: msg.batchEnd && allSlotsFilled() && dirty
v3.4: msg.batchEnd && allSlotsFilled() && dirty && allSameRound
```

---

## LocalProcessor 改造

### 配置新增

```java
String driftTopic = params.get("driftTopic", "drift-topic");
String driftRoundTopic = params.get("driftRoundTopic", "drift-round-topic");
long votingTimeoutMs = params.getLong("votingTimeoutMs", 5000L);
String pauseModeStr = params.get("pauseMode", "USE_OLD_FOREST");
PauseMode pauseMode = PauseMode.valueOf(pauseModeStr);
```

### 新增状态字段

```java
// LOCAL_DRIFT_REPORTED 关联
private transient ValueState<Long> pendingRoundId;  // 上报后等待协调器分配的 roundId
                                                     // 或自己收到 VOTING 时的 roundId
```

### Source 端：合并两个广播流

```java
// LocalProcessor.java main()
DataStream<ForestMessage> forestStream = env.addSource(modelTopicConsumer)
    .map(jsonString -> deserialize ForestMessage)
    .map(forest -> new BroadcastEnvelope(BroadcastEnvelope.Type.FOREST, forest, null));

DataStream<DriftRoundMessage> driftRoundStream = env.addSource(driftRoundConsumer)
    .map(jsonString -> deserialize DriftRoundMessage)
    .map(drm -> new BroadcastEnvelope(BroadcastEnvelope.Type.DRIFT_ROUND, null, drm));

DataStream<BroadcastEnvelope> mergedBroadcast = forestStream.union(driftRoundStream);

BroadcastStream<BroadcastEnvelope> broadcastStream =
    mergedBroadcast.broadcast(BROADCAST_DESC);

DataStream<DataPoint> dataStream = ...;
SingleOutputStreamOperator<ScoreResult> mainStream = dataStream
    .keyBy(...)
    .connect(broadcastStream)
    .process(new LocalProcessorFunction(...));
```

### processBroadcastElement 改造

```java
@Override
public void processBroadcastElement(BroadcastEnvelope env, Context ctx, Collector<ScoreResult> out) {
    if (env.getType() == BroadcastEnvelope.Type.FOREST) {
        // 原有逻辑：保存到 broadcast state
        ctx.getBroadcastState(FOREST_DESC).put(FOREST_KEY, env.getForestMessage().toForest());
        LOG.info("subtask={}: received global forest version {}",
            subtaskIndex, env.getForestMessage().getVersion());
    } else if (env.getType() == BroadcastEnvelope.Type.DRIFT_ROUND) {
        DriftRoundMessage drm = env.getDriftRoundMessage();
        handleDriftRoundMessage(drm, ctx);
    }
}

private void handleDriftRoundMessage(DriftRoundMessage drm, Context ctx) {
    // 注意：processBroadcastElement 不能访问 keyed state
    // 必须用 applyToKeyedState() 来对每个 subtask 单独处理
    // 但 applyToKeyedState 在 KeyedBroadcastProcessFunction 里才有
    // 当前 LocalProcessorFunction extends KeyedBroadcastProcessFunction？检查一下
    
    if (drm.getStatus() == RoundStatus.VOTING) {
        // 广播给所有 keys（subtask）——但 broadcast 本质上是给每个 task 一份
        // 每个 task 内部触发投票决策
        ctx.applyToKeyedState(SUB_STATE_DESC, (key, state) -> {
            // 检查本 key 的子状态决定投票
            DriftDetector det = detector.value();  // ← 但这里访问不到，需要从 state 拿
            // ... 复杂
        });
        // 实际上推荐换一种实现：把 VOTING 消息存到 broadcast state，
        // 等下一条 element 进 processElement 时再处理。这样能访问 keyed state。
        ctx.getBroadcastState(DRIFT_ROUND_DESC).put("active", drm);
    } else if (drm.getStatus() == RoundStatus.COMMITTED || drm.getStatus() == RoundStatus.ABORTED) {
        ctx.getBroadcastState(DRIFT_ROUND_DESC).put("decision_" + drm.getRoundId(), drm);
        // 同样，等下一条 element 进 processElement 处理
    }
}
```

#### ⚠️ Flink keyed state 访问陷阱

`processBroadcastElement` **不能**直接访问 keyed state（`detector`、`subState` 等）。两种解决：

- **方案 a**：`ctx.applyToKeyedState(...)`——对所有 keys 遍历执行 lambda。Flink 标准做法。
- **方案 b**：把广播消息存到 broadcast state，等下一条 element 进 `processElement` 时主动消费。延迟一条 element，但访问 keyed state 自然。

**v3.4 推荐方案 b**——简单、清晰、调试友好。LocalProcessor 收到广播消息只是"记录"，每个 element 进来时检查是否有未消费的广播指令。

具体：

```java
// 新增 broadcast state
MapStateDescriptor<String, DriftRoundMessage> DRIFT_ROUND_DESC = ...

// processBroadcastElement 仅做存储
processBroadcastElement: 把 DriftRoundMessage 存到 broadcast state
                         key = "active_" + roundId 或 "decision_" + roundId

processElement 主流程改：
  // 在打分之前先处理待消费的广播指令
  DriftRoundMessage activeVoting = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("active");
  if (activeVoting != null && currentSubState == STABLE/WARN) {
      // 投票：根据本 subtask 的 HDDM 状态决定 YES/NO
      castVote(activeVoting.getRoundId(), ctx);
      // 标记已投票（避免重复投票）
      ctx.getBroadcastState(...).remove("active");
  }
  
  DriftRoundMessage decision = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("decision_" + pendingRoundId);
  if (decision != null && currentSubState == LOCAL_DRIFT_REPORTED) {
      if (decision.getStatus() == COMMITTED) {
          // 进 COOLDOWN
          handleVoteCommitted();
      } else {
          // 回 STABLE
          handleVoteAborted();
      }
  }
  
  // 然后正常分发到 handleStable / handleWarn / ...
```

#### ⚠️ 这里有个 race condition 需要注意

**问题**：subtask A 触发 INITIATE 上报，但**还没收到 VOTING 广播下来时**，subtask A 的 processElement 该做什么？

时序：
```
T0: A 的 HDDM update → DRIFT
T0+1: A 上报 INITIATE 到 drift-topic
T0+2: A 把自己 subState 设为 LOCAL_DRIFT_REPORTED
T0+3: A 收到下一条 element ← 还没收到 VOTING 广播，pendingRoundId 还是 null

T1: 协调器收到 A 的 INITIATE，分配 roundId=5，广播 VOTING
T2: A 收到 VOTING 广播
```

**T0+3 时 A 处于"已上报但未确认"的状态**。这段时间数据要不要打分？

按 PauseMode 处理：
- USE_OLD_FOREST：用旧森林打分输出
- BACKLOG_THEN_NEW_FOREST：进 backlog

OK，这正是 LOCAL_DRIFT_REPORTED 状态本身的语义。简单——A 上报 INITIATE 后立刻进入 LOCAL_DRIFT_REPORTED 状态，按对应模式行为，等 VOTING 广播下来后更新 pendingRoundId。

### handleStable / handleWarn 改造

当 HDDM update 返回 DRIFT 时（以前 → enterCooldown）：

```java
// v3.4 改动
if (status == DriftStatus.DRIFT) {
    // 不再直接 enterCooldown，而是 enterLocalDriftReported
    enterLocalDriftReported(ctx);
}

private void enterLocalDriftReported(ReadOnlyContext ctx) throws Exception {
    subState.update(PhaseCSubState.LOCAL_DRIFT_REPORTED);
    pendingRoundId.update(0L);  // 0 = 还未分配，等 VOTING 广播
    
    // 上报 INITIATE 到 drift-topic（用 side output）
    DriftReport report = new DriftReport();
    report.setSubtask(subtaskIndex);
    report.setTimestamp(System.currentTimeMillis());
    report.setRoundId(0L);  // INITIATE 不带 round
    report.setVote(DriftVote.INITIATE);
    report.setLocalStatus(DriftStatus.DRIFT);
    ctx.output(DRIFT_REPORT_TAG, report);
    
    LOG.info("subtask={}: detected DRIFT, reporting to coordinator", subtaskIndex);
}
```

### LOCAL_DRIFT_REPORTED 处理（PauseMode 分支）

```java
private void handleLocalDriftReported(DataPoint point, Forest forest,
                                       ReadOnlyContext ctx, Collector<ScoreResult> out) {
    // HDDM 暂停（不喂分数）
    
    if (pauseMode == PauseMode.USE_OLD_FOREST) {
        // 模式 1：继续用旧森林打分输出
        double score = forest.score(point.getFeatures());
        out.collect(buildScoreResult(point, score, forest.getVersion(), "C"));
    } else {
        // BACKLOG_THEN_NEW_FOREST 模式 3：暂存
        backlog.add(point);
    }
    
    // 检查广播指令（已在主流程开头处理）
}
```

### Vote 投票逻辑

```java
private void castVote(long roundId, ReadOnlyContext ctx) throws Exception {
    PhaseCSubState st = subState.value();
    DriftVote vote;
    if (st == PhaseCSubState.WARN || st == PhaseCSubState.LOCAL_DRIFT_REPORTED) {
        vote = DriftVote.YES;
    } else {
        vote = DriftVote.NO;
    }
    
    DriftReport report = new DriftReport();
    report.setSubtask(subtaskIndex);
    report.setTimestamp(System.currentTimeMillis());
    report.setRoundId(roundId);
    report.setVote(vote);
    DriftDetector det = detector.value();
    report.setLocalStatus(det != null ? mapToStatus(st) : DriftStatus.STABLE);
    ctx.output(DRIFT_REPORT_TAG, report);
    
    // 如果本 subtask 是 LOCAL_DRIFT_REPORTED 状态，记录 pendingRoundId
    if (st == PhaseCSubState.LOCAL_DRIFT_REPORTED) {
        pendingRoundId.update(roundId);
    }
    
    LOG.info("subtask={}: voted {} for round {}", subtaskIndex, vote, roundId);
}
```

### handleVoteCommitted / handleVoteAborted

```java
private void handleVoteCommitted(DriftRoundMessage drm, Forest forest,
                                   ReadOnlyContext ctx, Collector<ScoreResult> out) {
    // 消化 backlog（仅 BACKLOG_THEN_NEW_FOREST 模式有数据）
    // ⚠️ 但此模式下我们应该等新森林来再处理！见下文
    
    // 进入 COOLDOWN
    subState.update(PhaseCSubState.COOLDOWN);
    cooldownN.update(0L);
    cooldownMean.update(0.0);
    cooldownM2.update(0.0);
    
    LOG.info("subtask={}: vote COMMITTED for round {}, entering COOLDOWN",
        subtaskIndex, drm.getRoundId());
}

private void handleVoteAborted(DriftRoundMessage drm, Forest forest,
                                ReadOnlyContext ctx, Collector<ScoreResult> out) {
    // 消化 backlog（用旧森林打分）
    if (pauseMode == PauseMode.BACKLOG_THEN_NEW_FOREST) {
        for (DataPoint dp : backlog.get()) {
            double s = forest.score(dp.getFeatures());
            out.collect(buildScoreResult(dp, s, forest.getVersion(), "C"));
        }
        backlog.clear();
    }
    
    // HDDM 完全 reset
    DriftDetector det = createDetector();
    detector.update(det);
    
    subState.update(PhaseCSubState.STABLE);
    pendingRoundId.clear();
    
    LOG.info("subtask={}: vote ABORTED for round {}, returning to STABLE", subtaskIndex, drm.getRoundId());
}
```

### ⚠️ BACKLOG_THEN_NEW_FOREST 的 backlog 处理时机

COMMITTED 后立刻消化 backlog 用什么森林？

**正确做法**：**等新森林广播下来后再处理 backlog**。这是 BACKLOG_THEN_NEW_FOREST 的核心价值。

- COMMITTED 时：backlog 保留不动，进 COOLDOWN
- COOLDOWN 完成发新树，进 WAITING
- 收到新森林：用新森林批量打分 backlog 输出，回 STABLE

```java
// handleWaiting 已有逻辑里加：
if (newForestVersion > waitingForVersion) {
    if (pauseMode == BACKLOG_THEN_NEW_FOREST) {
        for (DataPoint dp : backlog.get()) {
            double s = newForest.score(dp.getFeatures());
            out.collect(buildScoreResult(dp, s, newForestVersion, "C"));
        }
        backlog.clear();
    }
    // ... 原有 HDDM reset + 回 STABLE
}
```

---

## DRIFT 触发后的重训：batchId 语义升级

v3.4 时 `retrainAndEnterWaiting`：

```java
private void retrainAndEnterWaiting(ReadOnlyContext ctx) throws Exception {
    Long round = pendingRoundId.value();  // ← v3.4：用全局 round，不再 +1 local count
    if (round == null) {
        // v1 Phase B 训练时 round = 0
        round = 0L;
    }
    
    long batchId = ((long) subtaskIndex << 32) | round;
    
    // 其余逻辑同 v3.3
}
```

v1 Phase B 训练时 round=0（pendingRoundId 也是 null），自然兼容。

---

## 测试要求

### 单元测试

#### DriftVoterFunction 测试场景

**V34-1：基础投票路径**
- 喂 INITIATE from subtask 0
- 期望：emit VOTING with roundId=0
- 喂 YES from subtask 1, 2
- 喂 NO from subtask 3
- 期望：emit COMMITTED with yes=3, no=1（含 subtask 0 自动赞成）

**V34-2：投票否决**
- INITIATE from 0
- NO from 1, 2, 3
- 期望：emit ABORTED with yes=1, no=3

**V34-3：超时强制决议**
- INITIATE from 0
- 不喂任何后续票，触发 timer
- 期望：emit ABORTED with yes=1, no=0, abstain=3

**V34-4：超时但已赞成多数**
- INITIATE from 0
- YES from 1, 2
- 触发 timer（subtask 3 未投票）
- 期望：emit COMMITTED with yes=3, no=0, abstain=1

**V34-5：重复 INITIATE 在进行中的轮次**
- INITIATE from 0
- 立刻又 INITIATE from 2（A 轮还没决议）
- 期望：第二个 INITIATE 被忽略（log warn），不分配新 roundId

#### LocalProcessorFunction 测试场景

**V34-6：LOCAL_DRIFT_REPORTED 状态切换**
- 喂数据触发 DRIFT
- 期望：side output 收到 DriftReport{INITIATE}，状态切到 LOCAL_DRIFT_REPORTED

**V34-7：USE_OLD_FOREST 模式**
- 进 LOCAL_DRIFT_REPORTED
- 喂数据 100 条
- 期望：100 条 ScoreResult 都输出（用旧森林）

**V34-8：BACKLOG_THEN_NEW_FOREST 模式**
- 进 LOCAL_DRIFT_REPORTED
- 喂数据 100 条
- 期望：0 条 ScoreResult 输出（数据进 backlog）

**V34-9：投票通过后流程**
- 进 LOCAL_DRIFT_REPORTED
- 收到 VOTING 广播
- 期望：side output 收到 DriftReport{YES}
- 收到 COMMITTED 广播
- 期望：状态切到 COOLDOWN

**V34-10：投票否决后流程**
- 进 LOCAL_DRIFT_REPORTED
- 收到 VOTING 广播
- 收到 ABORTED 广播
- 期望：状态切回 STABLE，HDDM reset

### 端到端测试

跑 sudden 数据集（drift_at=25000），期望：
- 4 subtask 几乎同时进 WARN（v3.3 已验证）
- 第 1 个 subtask 进 DRIFT 后触发 INITIATE
- 协调器分配 roundId=1，广播 VOTING
- 其余 subtask 投票（应至少 2 个赞成因为已在 WARN）
- 协调器决议 COMMITTED
- 所有 subtask 同进 COOLDOWN
- 所有 subtask 同发 25 棵新树（batchId 低 32 位都 = 1）
- 协调器收齐 100 棵后发新森林 v2
- **最终版本数：v1 + v2 = 2 个**（vs v3.3 的 5 个）

跑同一数据集两次，分别用 USE_OLD_FOREST 和 BACKLOG_THEN_NEW_FOREST，对比：
- 整体 AUC
- 漂移期 FPR 峰值
- 业务延迟（compute_auc.py 的定义 3）
- 输出数据条数（BACKLOG 模式总数应略低，因 backlog 中部分进 v2）

---

## 实施顺序

1. 新增 `BroadcastEnvelope` 类 + 测试 → 提交
2. 新增 `DriftRoundMessage` + DriftReport 启用（v3.0 已有 POJO，补 vote 字段）→ 测试 → 提交
3. 新增 `DriftVoterFunction` + 完整单测（场景 V34-1 ~ V34-5）→ 提交
4. CoordinatorJob 加 drift-topic source + drift-round-topic sink + DriftVoterFunction 算子 → 提交
5. LocalProcessor source 端合并广播流（forestStream + driftRoundStream → unionBroadcast）→ 提交
6. LocalProcessor 增加 LOCAL_DRIFT_REPORTED 状态 + 两种 PauseMode 分支 → 测试（V34-6, V34-7, V34-8）→ 提交
7. LocalProcessor 处理 VOTING 广播的投票逻辑 → 测试 V34-9 → 提交
8. LocalProcessor 处理 COMMITTED/ABORTED 决议 → 测试 V34-10 → 提交
9. CoordinatorFunction.fireForest 改为检查 batchId 低 32 位相同 → 提交
10. 端到端联调：sudden 数据集，两种 PauseMode 各跑一次 → 文档化结果

---

## 工作风格提醒

- **v3.4 是最大的一次改造**——10 步分提交，每步独立可测试
- **不能直接在 processBroadcastElement 访问 keyed state**——必须用方案 b（broadcast state 暂存 + 下一条 element 消费）
- **Race condition**：subtask 上报 INITIATE 后立刻进 LOCAL_DRIFT_REPORTED，先按对应 PauseMode 行为，VOTING 广播来才更新 pendingRoundId
- **BACKLOG_THEN_NEW_FOREST 模式的 backlog 必须等新森林到来才处理**，不要在 COMMITTED 时用旧森林处理
- **v1 Phase B 训练的 batchId**：round=0，pendingRoundId=null 时降级处理
- **DriftReport / DriftRoundMessage 的 JSON 反序列化要支持向后兼容**——老数据没有新字段时 default 值合理

---

## 当前接手时第一件事

1. 确认远程仓库最新提交是 v3.3（如果是 v3.2 或更老，需要先确认 v3.3 已 push）
2. 跑 `mvn test` 确认 baseline 全绿
3. 实施步骤 1：BroadcastEnvelope
4. 报告每一步测试结果

---

## v3.4 完成后预告

| 阶段 | 内容 |
|---|---|
| v3.5（候选）| WARN 同步 COOLDOWN 优化，进一步压缩漂移响应延迟 |
| v4 | 性能基准 + 完整 AUC/FPR 实验 + 论文实验数据 |

v3.4 实测结束后整理 4 张关键论文表：

1. **版本数对比**：v3.3 vs v3.4（5 个 → 1 个）
2. **AUC 对比**：v3.3 baseline vs v3.4 两种 PauseMode
3. **FPR 漂移响应**：USE_OLD_FOREST 看 FPR 飙升曲线；BACKLOG_THEN_NEW_FOREST 看输出中断时长
4. **业务延迟**：定义 1/2/3 三种延迟在不同 PauseMode 下的对比

这是论文最核心的实验章节。
