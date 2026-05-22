# v3.4.4 修复：投票期 race condition

## 问题诊断

v3.4.3 修复 backlog bug 后，BACKLOG 模式 race condition **暴露出来**：v2 FNR 暴涨到 17.7%。

### Race 的精确时序（10:55 BACKLOG 实验日志）

```
10:55:06.952  subtask=1: detected DRIFT, reporting INITIATE
10:55:06.953  subtask=1: WARN → LOCAL_DRIFT_REPORTED
10:55:07.195  4 个 subtask 收到 round 1 VOTING 广播
10:55:07.280  subtask=0/2/3: voted YES for round 1 (state=WARN)
10:55:07.316  subtask=2: detected DRIFT, reporting INITIATE     ← ⚠️ race: 投完票后又触发
10:55:07.321  subtask=3: detected DRIFT, reporting INITIATE     ← ⚠️ race
10:55:07.399  4 个 subtask 收到 round 1 COMMITTED 广播
10:55:07.400  4 个 subtask 收到 round 2 VOTING 广播             ← subtask 2 的 INITIATE 在协调器
                                                                  端被 process 时 round 1 已经
                                                                  COMMITTED，被分配为 round 2
10:55:07.428  subtask 2/3/4 entered COOLDOWN (round 1)
10:55:07.429  subtask 0/1: voted YES for round 2
10:55:07.430  subtask 0/1 entered COOLDOWN (round 1)
10:55:12.346  round 2 timeout → COMMITTED (yes=3, abstain=1)
10:55:12.420  所有 subtask 又进了一次 COOLDOWN (round 2)        ← ⚠️ 第二次 COOLDOWN
                                                                  cN 被重置为 0
                                                                  ringBuffer 累积的 round 1 数据被废
10:55:28.488  subtask 1: COOLDOWN done, emitted batchId=4294967298 (round=2)
```

### 影响

| 指标 | race 出现（10:55）| race 未出现（15:18 BACKLOG bug 版）|
|---|---|---|
| v2 AUC | 0.966 | 0.986 |
| v2 FPR | 8.3% | 7.0% |
| **v2 FNR** | **17.7%** ⚠️ | 5.8% |
| v2 precision | 0.842 | 0.878 |

race 让所有 subtask 经历两次 COOLDOWN：
1. round 1 COMMITTED 后 cN=0 开始累积
2. round 2 COMMITTED 让 cN 重置为 0，round 1 累积的数据被丢弃
3. 实际 COOLDOWN 累积了 ~5 秒漂移期 + 16 秒漂移后期数据
4. 训练池里 anomaly 比例过高 → 新森林对真异常的判别能力下降 → FNR 暴涨

## 根因

subtask 投完 round N 的 YES 票后，若自己的 HDDM 在协调器决议**之前**累积到 DRIFT 阈值，**会再次发起 INITIATE**——但**逻辑上 subtask 已经表态过对当前漂移的态度**，不应该再发新一轮。

协调器侧虽有 `activeVote != null` 保护，但**网络传输延迟**让 subtask 的二次 INITIATE 在 round 1 COMMITTED 之后才被协调器 process——这时 `activeVote == null`，被当作新一轮分配。

修复必须在 **LocalProcessor 源头**——投过 YES 后禁止本 subtask 再发 INITIATE，直到收到当前 round 的决议。

---

## v3.4.4 范围

**做什么**：
1. LocalProcessorFunction 新增 keyed state `votedForRoundId`（Long 类型）
2. `castVote` 投 YES 时记录该 roundId
3. `enterLocalDriftReported` 在发 INITIATE 前检查：若 votedForRoundId 非空，跳过 INITIATE
4. `handleVoteCommitted` / `handleVoteAborted` 清理 votedForRoundId

**不做什么**：
- 不改协议
- 不改协调器
- 不改算法
- 不实现"COOLDOWN 中收到新 round 不重置 cN"的防御机制（race 修了之后用不到）

**预期效果**：
- BACKLOG 模式 v2 FNR 回到 5~8% 范围
- 协调器只看到一次 INITIATE（除非真有跨越多个漂移周期的场景，但 sudden 数据集没有）
- 整体延迟从 25 秒（受 round 2 timeout 拖累）回到 16 秒

---

## 关键实现细节

### 1. 新增 keyed state

`LocalProcessorFunction.java` 字段区：

```java
/**
 * 已投赞成票但未收到决议的 roundId。
 * v3.4.4 race condition 修复：subtask 投过 YES 之后，
 * 在收到 COMMITTED 或 ABORTED 之前，不允许再发新 INITIATE。
 *
 * The round ID for which this subtask has voted YES but hasn't received a decision yet.
 * v3.4.4 race fix: after a subtask voted YES, no new INITIATE allowed until decision.
 */
private transient ValueState<Long> votedForRoundId;
```

open() 中注册：

```java
votedForRoundId = getRuntimeContext().getState(
    new ValueStateDescriptor<>("voted-for-round-id", Types.LONG));
```

### 2. castVote 改造（投 YES 时记录）

`castVote` 方法（约第 514 行），在 `ctx.output(DRIFT_REPORT_TAG, report);` 之后追加：

```java
// v3.4.4: 投 YES 时记录，防止后续 race 触发新 INITIATE
if (vote == DriftReport.DriftVote.YES) {
    votedForRoundId.update(roundId);
}
```

### 3. enterLocalDriftReported 改造（INITIATE 前检查）

`enterLocalDriftReported` 方法（约第 489 行）开头加：

```java
private void enterLocalDriftReported(ReadOnlyContext ctx) throws Exception {
    // v3.4.4 race condition 修复：若已投过 YES 但未收到决议，
    // 跳过 INITIATE，直接进 LOCAL_DRIFT_REPORTED 等当前 round 决议
    Long voted = votedForRoundId.value();
    if (voted != null && voted > 0) {
        subState.update(PhaseCSubState.LOCAL_DRIFT_REPORTED);
        // 不需要 pendingRoundId.update(0L)——保留原值（应该已经是 voted 这个值）
        LOG.info("subtask={}: detected DRIFT but already voted YES for round {}, " +
                 "skipping INITIATE, waiting for decision",
                subtaskIndex, voted);
        return;
    }
    
    // 原有逻辑：正常发 INITIATE
    subState.update(PhaseCSubState.LOCAL_DRIFT_REPORTED);
    pendingRoundId.update(0L);
    
    DriftReport report = new DriftReport(subtaskIndex, System.currentTimeMillis(),
            DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE);
    ctx.output(DRIFT_REPORT_TAG, report);
    
    LOG.info("subtask={}: detected DRIFT, reporting INITIATE to coordinator", subtaskIndex);
}
```

### 4. handleVoteCommitted 改造（清理 votedForRoundId）

`handleVoteCommitted` 方法（约第 541 行）末尾追加：

```java
// v3.4.4: 收到决议，清理 votedForRoundId
votedForRoundId.clear();
```

### 5. handleVoteAborted 改造（清理 votedForRoundId）

`handleVoteAborted` 方法末尾追加：

```java
// v3.4.4: 收到决议，清理 votedForRoundId
votedForRoundId.clear();
```

---

## 关于 WARN→STABLE 自然恢复后的 votedForRoundId

设计决策：**投过 YES 但 round 还在 active 时，即使本 subtask 的 HDDM 自然回到 STABLE，也保持 votedForRoundId**。理由：

- 协调器仍然在等本轮决议
- 决议结果可能是 COMMITTED（要求本 subtask 进 COOLDOWN）
- 此时 votedForRoundId 用来确保本 subtask 不在等待期间触发新 INITIATE

handleVoteCommitted/Aborted 才是清理点——不要在 WARN→STABLE 路径上清理。

**实施时不需要改 handleWarn 中 WARN→STABLE 的恢复逻辑**——本机制天然兼容。

---

## 验证

### 期望日志（修复后跑 BACKLOG sudden）

```
subtask=1: detected DRIFT, reporting INITIATE to coordinator
subtask=0/2/3: voted YES for round 1 (state=WARN)

# 关键：subtask 2/3 累积到 DRIFT 时不再发 INITIATE
subtask=2: detected DRIFT but already voted YES for round 1, skipping INITIATE, waiting for decision
subtask=3: detected DRIFT but already voted YES for round 1, skipping INITIATE, waiting for decision

# 协调器只看到 round 1，没有 round 2
DriftVoter: initiated round 1 by subtask 1
DriftVoter: round 1 resolved as COMMITTED (yes=4, no=0, abstain=0)

# 各 subtask 只进一次 COOLDOWN
subtask=0/1/2/3: entered COOLDOWN
subtask=0/1/2/3: COMMITTED round 1 → COOLDOWN

# COOLDOWN 完成发树，batchId 低 32 位都是 1（round=1）
subtask=0: COOLDOWN done, emitted 25 new trees (batchId=1)
subtask=1: COOLDOWN done, emitted 25 new trees (batchId=4294967297)   # 4294967297 = (1<<32)|1
subtask=2: COOLDOWN done, emitted 25 new trees (batchId=8589934593)   # 8589934593 = (2<<32)|1
subtask=3: COOLDOWN done, emitted 25 new trees (batchId=12884901889)  # 12884901889 = (3<<32)|1
```

注意 batchId 低 32 位**都是 1**——上次 race 后是 batchId 低 32 位都是 2（因为 round 2 才发树）。

### 期望 compute_auc 结果

| 指标 | 期望范围 | 理由 |
|---|---|---|
| 总条数 | 45000 | backlog 处理正常 |
| v1 数据量 | ~28000 | 大头 |
| v2 数据量 | ~17000 | 漂移后期 + backlog 排空 |
| v2 AUC | ≥ 0.97 | 单轮 COOLDOWN 训练池质量更好 |
| **v2 FPR** | **5~8%** | 关键指标 |
| **v2 FNR** | **5~10%** | 不再暴涨 |
| 定义 1 延迟 | ~5500 条 | round 2 不再延后训练 16 秒 |

### 期望 Kafka topic

drift-topic 不再有 subtask 2/3 的 INITIATE：

```
DriftReport{subtask=1, vote=INITIATE, roundId=0}
DriftReport{subtask=0, vote=YES, roundId=1}
DriftReport{subtask=2, vote=YES, roundId=1}
DriftReport{subtask=3, vote=YES, roundId=1}
```

drift-round-topic 不再有 round 2：

```
DriftRoundMessage{roundId=1, status=VOTING, yes=1, no=0, abstain=0}
DriftRoundMessage{roundId=1, status=COMMITTED, yes=4, no=0, abstain=0}
```

---

## 实施顺序

1. LocalProcessorFunction 加 votedForRoundId 字段 + open() 注册 → 编译 → 提交
2. castVote 加投 YES 时记录 → 编译 → 提交
3. enterLocalDriftReported 加 INITIATE 前检查 → 编译 → 提交
4. handleVoteCommitted/Aborted 加清理 → 编译 → 提交
5. mvn package → 跑 USE_OLD_FOREST + BACKLOG_THEN_NEW_FOREST 各一次

注：上面 4 步逻辑紧密相关，**可以合并为 1 个 commit**——但还是建议分开提交便于回滚定位。

---

## 工作风格提醒

- 只动 LocalProcessorFunction.java 一个文件
- 不需要新增测试用例（race 是时序相关的，单测难重现）
- 不要顺手修复其他"潜在 race"——v3.4.4 只针对本次确诊的 race
- 验证时**重点关注**：是否有 "skipping INITIATE, waiting for decision" 日志出现

---

## 当前接手时第一件事

1. 确认 v3.4.3 已 push（即 backlog Phase A 修复已经在 main）
2. 跑 `mvn test` 确认 baseline 全绿
3. 实施步骤 1：加 votedForRoundId 字段
4. 报告每一步测试结果

---

## v3.4 完成预告

v3.4.4 修完后，v3.4 系列就**全部完成**。之后会做：

| 阶段 | 内容 |
|---|---|
| v3.4 实验完整 | sudden / stable / gradual × USE_OLD_FOREST / BACKLOG = 6 组实验 |
| v3.5 候选 | WARN 同步 COOLDOWN 优化（延迟从 16 秒压到 12~13 秒） |
| v4 | 论文实验数据整理 + 性能基准 |

特别提醒：v3.5 候选只在 v3.4 实测延迟超过 15 秒时做。如果 v3.4.4 修完后定义 3 延迟 ≤ 5000 条数据——v3.5 没必要做。
