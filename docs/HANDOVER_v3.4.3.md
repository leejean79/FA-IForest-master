# v3.4.3 修复：BACKLOG_THEN_NEW_FOREST 模式 backlog 被 Phase A 逻辑误消化

## 问题诊断

v3.4.2 BACKLOG_THEN_NEW_FOREST 模式跑端到端，链路工作但结果异常：

| 指标 | USE_OLD_FOREST | BACKLOG | 异常点 |
|---|---|---|---|
| 总条数 | 45000 | 43824 | BACKLOG **少 1176 条**，应该相等 |
| v1 数据量 | 28209 | 29443 | BACKLOG **多 1234 条**，应少（漂移期数据进 backlog 后等 v2 处理）|
| v2 数据量 | 16791 | 14381 | BACKLOG **少 2410 条** |

**日志证据**（BACKLOG 模式 LOCAL_DRIFT_REPORTED 期间）：

```
15:31:56.987  subtask=1: WARN → LOCAL_DRIFT_REPORTED
15:31:56.994  subtask=1: Phase A drained 1 backlog records  ← ❌ 不应该有
```

按 BACKLOG 模式设计：LOCAL_DRIFT_REPORTED 期间数据进 backlog，**应该等 v2 到来后在 handleWaiting 中一次性消化**。但日志显示**每条数据进来后立刻被 "Phase A drained 1 backlog records" 取出**。

## 根因

`LocalProcessorFunction.processElement` 中"Phase A 消化 backlog"逻辑没有子状态保护：

```java
// 行 268-282：Phase A 消化 backlog
List<DataPoint> blList = new ArrayList<>();
for (DataPoint dp : backlog.get()) {
    blList.add(dp);
}
if (!blList.isEmpty()) {
    // ... 用当前森林打分输出，phase="A"
    backlog.clear();
}
```

这段代码**无条件**消化 backlog。在 BACKLOG 模式下：

1. subtask 进 LOCAL_DRIFT_REPORTED 后，新数据通过 `handleLocalDriftReported` 第 510 行进 backlog
2. 下次新数据进 processElement，**先走 Phase A 消化路径**，把刚塞进去的数据当 Phase A backlog 消化
3. 然后才走子状态 switch case LOCAL_DRIFT_REPORTED 把当前数据又塞 backlog
4. 循环往复——backlog 永远只有 1 条，每条数据"过一遍 backlog 用旧森林打分"后输出 phase=A

结果：
- LOCAL_DRIFT_REPORTED / COOLDOWN / WAITING 期间塞进 backlog 的数据**全部被用 v1 打分输出**（phase=A）
- 等 v2 到来时，handleWaiting 的 backlog 排空 LOG 不触发（backlog 已是空的）
- BACKLOG 模式效果**等价于 USE_OLD_FOREST**——失去了 BACKLOG 的核心价值

## v3.4.3 范围

**做什么**：
1. LocalProcessorFunction.java 给 Phase A 消化逻辑加子状态检查
2. 跑 BACKLOG_THEN_NEW_FOREST 端到端验证 backlog 真的等到 v2 才消化

**不做什么**：
- 不改算法
- 不改协议
- 不改测试

---

## 修复方案

`LocalProcessorFunction.java` 第 268~282 行：

```java
// 改前
// Phase A: 消化 backlog
List<DataPoint> blList = new ArrayList<>();
for (DataPoint dp : backlog.get()) {
    blList.add(dp);
}
if (!blList.isEmpty()) {
    long forestVersion = forest.getVersion();
    for (DataPoint dp : blList) {
        double s = forest.score(dp.getFeatures());
        out.collect(buildScoreResult(dp, s, forestVersion, "A"));
    }
    backlog.clear();
    LOG.info("subtask={}: Phase A drained {} backlog records",
            subtaskIndex, blList.size());
}
```

```java
// 改后
// Phase A: 消化 backlog
// v3.4.3 修复：仅在子状态 = STABLE 或 WARN 时执行 Phase A 消化。
// LOCAL_DRIFT_REPORTED / COOLDOWN / WAITING 状态下，backlog 由 BACKLOG_THEN_NEW_FOREST
// 模式管理，应保留等待 handleWaiting 用新森林打分。
// v3.4.3 fix: only drain backlog when sub-state is STABLE or WARN. In LOCAL_DRIFT_REPORTED /
// COOLDOWN / WAITING states, backlog is managed by BACKLOG_THEN_NEW_FOREST mode and must
// be preserved until handleWaiting uses the new forest to score it.
PhaseCSubState currentSubState = subState.value();
boolean isPhaseACompatible = (currentSubState == null
        || currentSubState == PhaseCSubState.STABLE
        || currentSubState == PhaseCSubState.WARN);

if (isPhaseACompatible) {
    List<DataPoint> blList = new ArrayList<>();
    for (DataPoint dp : backlog.get()) {
        blList.add(dp);
    }
    if (!blList.isEmpty()) {
        long forestVersion = forest.getVersion();
        for (DataPoint dp : blList) {
            double s = forest.score(dp.getFeatures());
            out.collect(buildScoreResult(dp, s, forestVersion, "A"));
        }
        backlog.clear();
        LOG.info("subtask={}: Phase A drained {} backlog records",
                subtaskIndex, blList.size());
    }
}
```

---

## 验证

修复后重跑 BACKLOG_THEN_NEW_FOREST 模式：

### 期望日志（按时序）

```
15:XX:XX  v1 emit
15:XX:XX  Phase A drained N backlog records       ← Phase A 期间正常消化
15:XX:XX  STABLE → WARN                            
15:XX:XX  WARN → LOCAL_DRIFT_REPORTED              ← 进入暂停态
（注意：之后不应该再出现 "Phase A drained 1 backlog records"）
15:XX:XX  voted YES for round 1
15:XX:XX  received COMMITTED
15:XX:XX  entered COOLDOWN, COMMITTED round 1 → COOLDOWN
15:XX:XX  COOLDOWN done, emitted 25 new trees
15:XX:XX  entering WAITING
15:XX:XX  received global forest version 2
15:XX:XX  WAITING → STABLE, drained 2000+ backlog with new forest v2   ← ⚠️ 这一行是关键
15:XX:XX  WAITING → STABLE (new forest version 2 received)
```

**关键验证点**：必须看到 "drained X backlog with new forest v2" 这一行，且 X 应该在 1000~3000 之间（4 subtask 各自 backlog 在 LOCAL_DRIFT_REPORTED + COOLDOWN + WAITING 总共约 25 秒内累积的数据量）。

### 期望 compute_auc 结果

修复后 BACKLOG 模式：

- 总条数：应等于 45000（不再丢数据）
- v1 数据量：约 24000~26000（漂移期数据进了 backlog 不再算作 v1）
- v2 数据量：约 19000~21000（漂移期数据用 v2 打分输出）
- 漂移后 v1 FPR：仍然是漂移期数据，但条数少很多
- 漂移后 v2 FPR：应该和 USE_OLD_FOREST 模式类似（同样的 v2 森林）

---

## 实施顺序

1. 修改 LocalProcessorFunction.java 第 268 行附近（加子状态检查）→ 提交
2. mvn package → 端到端跑 BACKLOG_THEN_NEW_FOREST 模式
3. 验证 LocalProcessor 日志出现 "drained X backlog with new forest v2" 一行（X > 0）
4. 验证 compute_auc 输出总条数 = 45000

---

## 工作风格提醒

- 这次修复**手术式**，**只改一处**（LocalProcessorFunction.java 第 268 行附近的 Phase A 消化逻辑）
- 不要顺手"优化"或重构其他代码
- BACKLOG 模式 LOCAL_DRIFT_REPORTED 期间日志**不应该再出现** "Phase A drained 1 backlog records"——这是验证修复成功的关键标志
