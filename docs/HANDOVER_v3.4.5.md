# v3.4.5 修复：投票决议后 broadcast state "active" 未清理导致无限投票

## 问题诊断

v3.4.4 race condition 修复**生效**（日志看到 "skipping INITIATE, waiting for decision"），但暴露了 v3.4 设计的另一个深层 bug：**COMMITTED/ABORTED 决议后 broadcast state 的 "active" VOTING 标记永远不清，导致每条新数据都触发一次投票**。

### 现象

实验 LocalProcessor 日志：

```
15:05:00.058  subtask 0/2/3: voted YES for round 1 (state=WARN)       ← 第一次投票（正常）
15:05:00.152  4 subtasks 收到 COMMITTED
15:05:00.155  subtask 0 entered COOLDOWN
15:05:00.167  subtask 0: voted NO for round 1 (state=COOLDOWN)        ← ❌ 又投一次
15:05:00.173  subtask 0: voted NO for round 1 (state=COOLDOWN)        ← ❌ 又投一次
15:05:00.174  subtask 0: voted NO for round 1 (state=COOLDOWN)        ← ❌ 永远循环
...（数万条 NO vote 充斥 drift-topic）
```

Kafka drift-topic：

```
DriftReport{subtask=2, status=STABLE, vote=NO, roundId=1}  × N
DriftReport{subtask=1, status=STABLE, vote=NO, roundId=1}  × N
DriftReport{subtask=3, status=STABLE, vote=NO, roundId=1}  × N
```

CoordinatorJob 日志：

```
DriftVoter: round 1 resolved as COMMITTED (yes=4)
WARN  - Received vote for non-active round 1 (active=none), ignoring  × 数万次
```

实验数据失真：整体 AUC 跌到 0.93，漂移前 AUC 0.948（之前正常 ≥ 0.99），因为 LocalProcessor 持续被无谓的投票输出干扰。

### 根因

`LocalProcessorFunction.processBroadcastElement`（第 239~248 行）的逻辑：

```java
if (drm.getStatus() == VOTING) {
    ctx.getBroadcastState(DRIFT_ROUND_DESC).put("active", drm);
} else {
    // COMMITTED / ABORTED
    ctx.getBroadcastState(DRIFT_ROUND_DESC).put("decision", drm);
}
```

`put("active", drm)` 把 VOTING 写入 broadcast state——**收到 COMMITTED 后没有 remove("active")**。所以下次 element 进 processElement（第 307 行）：

```java
DriftRoundMessage activeVoting = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("active");
if (activeVoting != null && activeVoting.getStatus() == VOTING) {
    ...
}
```

`activeVoting` 永远非 null——每条新 element 都进入投票分支。

v3.4.4 用 `votedForRound` keyed state 去重，但有两条漏洞路径：
- **投 NO 时不记录 votedForRound**（castVote 第 556 行 `if (vote == YES)` 条件）
- **handleVoteCommitted/Aborted 清掉 votedForRound**（第 580、619 行）让下次投票去重失效

任一漏洞被触发就进入无限循环。这次 race 修复后路径就走到了 COMMITTED → 清 votedForRound → 下条 element → 投 NO（COOLDOWN）→ 不记 votedForRound → 再投。

---

## v3.4.5 范围

**做什么**：1 处改动——`processBroadcastElement` 收到 COMMITTED/ABORTED 时清掉 broadcast state 的 "active" key。

**不做什么**：
- 不改 votedForRound 逻辑（让它保持当前 YES-only 记录 + handleVoteCommitted 清理的行为）
- 不改 castVote
- 不改协议
- 不改协调器

理由：核心 bug 是 active 没清。清掉之后第 308 行 `activeVoting != null` 直接 false，整条投票路径都不进入，**无论 votedForRound 怎么变都无影响**。

---

## 修复

`LocalProcessorFunction.java` 第 239~248 行（processBroadcastElement 的 DRIFT_ROUND 分支）：

```java
} else if (envelope.getType() == BroadcastEnvelope.Type.DRIFT_ROUND) {
    DriftRoundMessage drm = envelope.getDriftRoundMessage();
    if (drm.getStatus() == DriftRoundMessage.RoundStatus.VOTING) {
        ctx.getBroadcastState(DRIFT_ROUND_DESC).put("active", drm);
    } else {
        // COMMITTED / ABORTED — 固定 key，processElement 消费
        ctx.getBroadcastState(DRIFT_ROUND_DESC).put("decision", drm);
        // v3.4.5 修复：决议到达，清掉 active VOTING 避免无限投票循环
        // v3.4.5 fix: clear active VOTING upon decision to prevent infinite voting loop
        ctx.getBroadcastState(DRIFT_ROUND_DESC).remove("active");
    }
    LOG.info("subtask={}: received DriftRoundMessage {}",
            subtaskIndex, drm);
}
```

新增一行：`ctx.getBroadcastState(DRIFT_ROUND_DESC).remove("active");`

---

## 验证

修复后跑 BACKLOG sudden 端到端：

### 期望 LocalProcessor 日志

```
subtask=1: detected DRIFT, reporting INITIATE
subtask=0/2/3: voted YES for round 1 (state=WARN)        ← 每个 subtask 各一次
subtask=2/3/0: detected DRIFT but already voted YES, skipping INITIATE
4 subtasks: received DriftRoundMessage{roundId=1, status=COMMITTED, ...}
subtask=0/1/2/3: entered COOLDOWN
subtask=0/1/2/3: COMMITTED round 1 → COOLDOWN
（之后**不再有任何 voted NO 日志**）
subtask=*: COOLDOWN done, emitted 25 new trees (batchId=...)
subtask=*: received global forest version 2
subtask=*: WAITING → STABLE, drained X backlog with new forest v2
```

**关键验证**：COMMITTED 之后**不应出现** "voted NO for round 1 (state=COOLDOWN)" 这种日志。

### 期望 Kafka drift-topic 消息

```
DriftReport{subtask=1, vote=INITIATE, roundId=0}
DriftReport{subtask=0, vote=YES, roundId=1}
DriftReport{subtask=2, vote=YES, roundId=1}
DriftReport{subtask=3, vote=YES, roundId=1}
（这 4 条之后，drift-topic 不再有新消息）
```

### 期望 CoordinatorJob 日志

```
Coordinator: emitted forest version 1
DriftVoter: initiated round 1 by subtask 1
DriftVoter: round 1 resolved as COMMITTED (yes=4, no=0, abstain=0)
（无 "Received vote for non-active round" warning）
Coordinator: emitted forest version 2
```

### 期望 compute_auc 结果

| 指标 | 期望值 | 与之前对比 |
|---|---|---|
| 总条数 | ≈ 45000 | 持平 |
| v1 数据量 | ~28000 | 持平 |
| v2 数据量 | ~17000 | 持平 |
| **整体 AUC** | **≥ 0.98** | vs 之前 0.93 大幅提升 |
| **漂移前 v1 AUC** | **≥ 0.99** | vs 之前 0.948 |
| **v2 AUC** | **≥ 0.98** | vs 之前 0.965 |
| **v2 FPR** | **5~8%** | vs 之前 9.8% |
| **v2 FNR** | **5~10%** | vs 之前 16.9% |
| 定义 3 延迟 | ~5000~6500 条 | 接近上一轮 |

数据失真现象消失——这才是 v3.4 BACKLOG 模式真正的实验结果。

---

## 实施顺序

1. LocalProcessorFunction.processBroadcastElement 加一行 `remove("active")` → 编译 → 提交
2. mvn package → 跑 BACKLOG sudden 端到端
3. 验证 drift-topic 上没有"无限 NO 投票"消息
4. compute_auc 比较关键指标（v2 FPR/FNR）

---

## 工作风格提醒

- **只动一处代码——一行**
- 不要顺手"改进"其他东西
- 验证标准很清晰：drift-topic 上 INITIATE+3 个 YES 投票之后没有任何新消息

---

## v3.4 完整完成展望

v3.4.5 修完后：

| 阶段 | 状态 |
|---|---|
| v3.4 协议 | 完成 |
| v3.4 BACKLOG 模式 | 干净工作 |
| v3.4 USE_OLD_FOREST 模式 | 一直工作（之前已验证）|
| v3.4 race condition | v3.4.4 已修 |
| v3.4 无限投票 bug | v3.4.5 修 |

**v3.4.5 修完后 v3.4 系列彻底完成**。然后跑 6 组实验（sudden / stable / gradual × 2 PauseMode）整理论文实验数据。
