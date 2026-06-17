# 方法学对照 — 漂移响应与模型重建逻辑:score 域(`feature/hddm-w`)vs per-feature(`feature/per-feature-*`)

> 用途:论文方法学小节引用。澄清 per-feature 检测架构相对旧 score 域架构在"检测到漂移后如何重建模型"上的异同。核心结论:**模型重建(重训)核心逐行一致;漂移触发路径根本不同——检测与投票由打分面内迁移到独立检测面。** 全部基于真实源码核验(`LocalProcessorFunction.java` 两分支)。

---

## 0. 两种架构一句话区分

- **score 域(旧 `feature/hddm-w`)**:检测器内嵌于打分面。`LocalProcessorFunction` 对每条记录的异常分数跑 `DriftDetector.update()`,自身完成检测、投票、重训。
- **per-feature(`feature/per-feature-iks` / `feature/per-feature-hddm_w`)**:检测与投票整体迁出到独立检测面(`PerFeatureXxxFunction` + `DriftAggregatorFunction`),打分面退化为"收到 COMMITTED 即重训"的被动消费者。

下文分三层对比。**第一、三层一致,第二层不同**。

---

## 1. 第一层:模型重建(重训)核心 —— 逐行一致

两分支的重训本体相同。进入重训后:从环形缓冲训练池 `pool` 出发,循环 `localTreeCount = ceil(totalTrees / parallelism)` 次调用 `builder.buildFromPool(pool, subsampleSize)` 训练 iTree(`subsampleSize` 默认 256),经 side output 逐棵发到 Kafka tree-topic,`CoordinatorFunction` 收齐后组装新版本 `Forest` 经 model-topic 广播,本地算子收到新版本后由 `WAITING → STABLE`。

| 重训要素 | 两分支 | 源码 |
|---|---|---|
| 训练池 | 环形缓冲 `ringBuffer` | 一致 |
| 单 subtask 树数 | `localTreeCount = ceil(totalTrees/parallelism)` | 一致 |
| 采样规模 | `subsampleSize`(默认 256) | 一致 |
| 树构建 | `builder.buildFromPool(pool, subsampleSize)` | 一致 |
| 树分发 | side output → tree-topic | 一致 |
| 森林组装 | `CoordinatorFunction` 收齐 → 版本递增 → model-topic 广播 | 一致 |
| 重训完成 | `WAITING → STABLE` | 一致 |

**含义**:森林"怎么重建"在 per-feature 转向中未改动。IKS→HDDM_W 的检测器替换同样不触及这一层。

---

## 2. 第二层:漂移触发路径 —— 根本不同(架构转向核心)

| 维度 | 旧 `feature/hddm-w`(score 域) | 当前 per-feature 分支 |
|---|---|---|
| 检测器位置 | **打分面内**:`ValueState<DriftDetector> detector`,`createDetector()` 中 `new HDDM_W(hddmConfig, hddmLambda)`,`processElement` 对异常分数 `detector.update(score)` | **检测面**:`PerFeatureIKSFunction` / `PerFeatureHDDMFunction` 在原始特征上检测;打分面**不持有检测器** |
| Phase C 子状态机 | `STABLE → WARN → LOCAL_DRIFT_REPORTED → COOLDOWN → WAITING → STABLE`(含 WARN 中间态,源自 HDDM warn/drift 双阈值) | `STABLE → COOLDOWN → WAITING → STABLE`(**无 WARN、无 LOCAL_DRIFT_REPORTED**) |
| 进入 COOLDOWN 的触发 | 本地 `update()` 返回 DRIFT → `LOCAL_DRIFT_REPORTED` → 投票 → 协调器聚合 → COMMITTED | 被动接收聚合器广播的 **COMMITTED**(`handleVoteCommitted`)→ 直接 `enterCooldown` |
| 投票状态 | 有:`pendingRoundId`、`votedForRound`(防重复投票) | **无**:投票/聚合已移到检测面 `DriftAggregatorFunction` |
| WARN 中间态语义 | WARN 期不写训练池、可自然恢复回 STABLE(`WARN → STABLE`) | 打分面不可见;HDDM_W 的 warn/drift 状态机关在检测面内部 |

**含义**:旧架构里检测、投票、重训三件事都在 `LocalProcessorFunction` 内;per-feature 架构把检测与投票整体搬到检测面,打分面只是 COMMITTED 的消费者。这是"两个解耦平面"设计的落地。

**对 IKS→HDDM_W 替换的直接后果**:检测器在检测面被替换(`PerFeatureIKSFunction → PerFeatureHDDMFunction`),触发路径不动。**不能照搬旧 hddm-w 分支的 `createDetector` 接入方式**(那是检测器在打分面、与重训耦合的设计)。旧分支唯一可借鉴的是 `HDDM_W` 类构造参数与 `--hddmLambda` 透传命名,架构层不可复用。

**论文需点明的语义后果**:旧分支 HDDM_W 的 WARN 中间态能参与重训节流(WARN 期不入池、可自然恢复);per-feature 架构下 HDDM_W 的 warn/drift 双阈值状态机被检测面吸收,打分面只看最终 COMMITTED。因此同一 `HDDM_W` 类在两种架构下行为语义不完全等价——这也是"score 域 → per-feature 迁移代价"的一部分(另见 ε 下界分析:同一 λ=0.1 在 score 域可用、在 per-feature 归一化信号上失效)。

---

## 3. 第三层:进入 COOLDOWN 后的训练池构成 —— 一致,且为已定稿 D1a

进入 COOLDOWN 后训练池如何填充,两分支当前默认值不同,但这一差异**不是 per-feature 引入的**,而是 IKS 分支已定稿的 D1a 改进,per-feature-hddm_w 完全继承、不改动。

| 策略 | 行为 | 默认分支 |
|---|---|---|
| `legacy` | 进 COOLDOWN 保留池 + 基于旧森林异常分数做 z-score 过滤后入池 | 旧 hddm-w 默认 |
| **`d1a`** | 进 COOLDOWN 清空 `ringBuffer` + post-drift 数据**无条件**写入(取消 z-score 过滤),积累到 `cooldownSamples`(默认 2000)再训 | **per-feature 分支默认**(`cooldownPolicy` 默认 `d1a`,`LocalProcessorFunction.java:185`) |

D1a 的理据:旧森林对新概念误判会选出有偏训练子集,换进更差森林;清空 + 无条件写入提升重训质量(三次重复实验确认)。`d1aFill` 默认 `cooldownsamples`(需积满 2000,非 `ring.isFull()` 的 1000 即止),亦为隔离实验定稿(`LocalProcessorFunction.java:194`)。

**含义**:per-feature-hddm_w 的池构成 = IKS 分支已定稿的 d1a + cooldownsamples,IKS→HDDM_W 替换不触及。

---

## 4. 小结:IKS→HDDM_W 替换触及的层

| 层 | 是否被 HDDM_W 替换触及 | 位置 |
|---|---|---|
| 第一层 重训核心 | 否 | 打分面 `LocalProcessorFunction` |
| 第二层 触发路径 | **检测器在检测面被替换;路径不变** | 检测面 `PerFeatureXxxFunction` + 聚合器 |
| 第三层 池构成 D1a | 否 | 打分面 `LocalProcessorFunction` |

IKS→HDDM_W 只动检测面的检测器实现(决策一:新建 `PerFeatureHDDMFunction`),三层中只有"第二层的检测器实现"被替换,触发路径、重训核心、池构成一律不动——这正是 EXP1 同等条件单变量对比的结构基础。

---

*核验来源:`LocalProcessorFunction.java`,`origin/feature/per-feature-hddm_w`(当前)与 `origin/feature/hddm-w`(旧 score 域)。重训核心、子状态机、池构成策略均逐行比对。*
