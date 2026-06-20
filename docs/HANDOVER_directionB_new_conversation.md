# 交接文档 — 方向 B:source 多分区 + 算子内重排保序(新对话)

> 给新对话窗口 planning Claude 的完整交接。目标:探索让 source 多分区(提高吞吐)
> 同时保证检测时序(靠算子内按 originalSequence 重排),解除 EXP3 端到端吞吐被单分区
> 焊死(~7000 rps)的限制。新分支专做此事,不动主分支 feature/per-feature-hddm_w。
> 协作沿用 A 模式:planning 出理论/规格/单点 diff,dev 落地 Java,用户跑集群回传。

---

## 0. 必读背景(为什么有方向 B)

### 0.1 项目
FA-IForest:Flink 1.13.6 + Kafka 流式异常检测,iForest(totalTrees=100/subsample=256)+
per-feature 漂移检测(主检测器 IKS),目标 IEEE/ACM 论文。两大贡献:①联邦/分布式漂移响应
②分布式异常检测算法。3 节点 Alibaba Cloud(master .162/worker .163/.164)。
GitHub: leejean79/FA-IForest。主分支 feature/per-feature-hddm_w(commit e0da511)。

### 0.2 触发方向 B 的核心矛盾(EXP3 扩展性)
论文基石之一是"高吞吐低延迟"。但 EXP3 实测发现端到端吞吐被 **source 单分区**焊死:
- source-topic = 1 分区(SOURCE_PARTITIONS=1),为保检测时序。
- 1 分区 → 1 个 source subtask 读 → 端到端吞吐上限 ~7000 rps(提高 producer rate 后实测
  source_ingress median 7270,单分区 Kafka+单线 source 的真实上限)。
- **无论提高任何下游并行度,端到端吞吐不超过 ~7000**——单分区是结构瓶颈。

### 0.3 为什么单分区是必须的(当前架构)
检测器 PerFeatureIKSFunction **按数据到达顺序逐点 update 滑动 KS 窗口,无算子内重排**:
- 第 82 行 `det.update(fv.getValue())` 按到达顺序加入 IKS 窗口;KS 统计量依赖窗口内数据的
  累积分布,**乱序 → 窗口内容错 → KS 值错 → 漂移检测错**。
- `originalSequence` 字段**仅用于 dump 后分析重排**(LocalProcessorFunction:671),
  运行时检测器不用它重排。
- 故 source 多分区会让同一 feature 数据跨分区乱序到达,破坏 IKS 时序。**当前架构多分区
  不能保时序。**

### 0.4 方向 B 的任务
**给检测链路加算子内按 originalSequence 的重排,使 source 多分区下也能向检测器喂出有序流,
从而 source 多分区并行、端到端吞吐随分区数扩展,且检测正确性不变。**
用户前提:**先从理论 + 最小实验证明多分区能保检测时序,再接受**(不可为吞吐牺牲检测正确性)。

---

## 1. 方向 B 的核心技术问题(planning 要解决的)

### 1.1 重排放在哪一层
候选:
- (a) source 之后、keyBy(featureId) 之前:全局重排(需全局有序,等同单线,无意义)。
- (b) keyBy(featureId) 之后、检测器之前:**每个 feature 子流内按 originalSequence 重排**。
  ← 正确层级。每个 feature 的数据可能来自多个分区(乱序),在检测算子前按 seq 排序缓冲,
  排好序再喂 IKS。检测器看到的每个 feature 子流仍严格有序。
关键洞察:IKS 是 per-feature 独立的,只需**每个 feature 子流内有序**,不需要跨 feature 全局有序。
这正是多分区可行的理论基础——keyBy 已按 feature 分流,重排只需在子流内做。

### 1.2 重排机制(Flink 实现选型)
- **基于 watermark + 事件时间排序**:把 originalSequence 当事件时间,用 Flink 的
  KeyedProcessFunction + 定时器,缓冲乱序数据,watermark 推进时按 seq 顺序 emit。
  问题:originalSequence 是序号非时间,需映射为单调事件时间;watermark 策略要保证
  不丢数据(乱序界 bounded)。
- **或自定义缓冲重排**:KeyedProcessFunction 内维护按 seq 排序的缓冲(优先队列/TreeMap),
  收到 seq 后,emit 所有"连续就绪"的前缀(类似 TCP 重组)。需处理:缺口等待、超时强制 emit。

### 1.3 必答的理论问题(用户前提)
1. **正确性**:重排后检测器看到的每个 feature 子流,是否严格等于单分区时的顺序?
   (若是,检测结果与单分区**完全一致**,理论保证。)
2. **完备性**:多分区下数据会不会丢/重?Flink exactly-once + 重排缓冲是否保证每条 seq 恰好处理一次?
3. **乱序界**:多分区乱序的最大偏移有界吗?(producer 轮询写 N 分区,同一 feature 相邻 seq
   最多差 N 个分区的积压)→ 决定重排缓冲大小 / 等待超时。
4. **延迟代价**:重排需等乱序数据到齐,引入缓冲延迟。这与"低延迟"目标的权衡要量化。

### 1.4 最小验证实验(证明多分区保时序)
设计一个最小实验,证明"多分区+重排"的检测结果 == 单分区的检测结果:
- 同一数据集(如 INSECTS_abrupt),分别跑:
  (A) source 1 分区(基线,已知正确);
  (B) source N 分区 + 算子内重排。
- 比较:漂移点检测序列、overall_auc、forestVersion 演进是否一致(或差异在容差内)。
- 若 (B) ≈ (A) → 多分区保时序成立,可放心用多分区测吞吐扩展。
- 同时测吞吐:(B) 的 source_ingress 是否随分区数 N 提升(突破 ~7000)。

---

## 2. 关键代码位置(新对话 planning 需读)

- `src/main/java/com/leejean/flink/PerFeatureIKSFunction.java` — 检测器,重排要插在它之前。
  第 82 行 det.update 按到达序;无重排逻辑。
- `src/main/java/com/leejean/main/LocalProcessor.java` — 流水线装配。
  第 155 env.setParallelism(全局);第 189/209 keyBy;第 211 检测面 detectionParallelism;
  source addSource(173)继承全局并行度。
- `src/main/java/com/leejean/beans/DataPoint.java` / `ScoreResult.java` — originalSequence 字段。
- `deploy/scripts/clean-topics.sh` 第 57/64 — SOURCE_PARTITIONS(多分区在此设)。
- `deploy/.env` 第 92 — JOB_LOAD_RATE(已知需提高,1000→50000+ 解 producer 限速)。

---

## 3. 已知事实(避免新对话重复踩坑)

- **producer 限速**:.env JOB_LOAD_RATE=1000 焊死吞吐 1000;提高到 50000 后 source 解锁到 ~7270。
  方向 B 测吞吐必须先提高此值,否则 producer 是瓶颈不是分区。
- **BACKLOG 边灌边处理**(run-experiment 注释:"producer 跑完=数据全进 topic; Flink 同时消费")。
- **监控就绪**:Prometheus(master:9090)三 target up。operator_name 下划线化,三口径过滤:
  source `Kafka_Source.*`、检测 `Per_Feature.*`、打分 `Local_Processor.*`。
  延迟从 scoreTime−ingestionTime(BACKLOG 下含排队失真,需 Flink latency metric 或实时摄入)。
- **检测器 IKS 默认参数**:iksWindowSize=2000, iksPValue=0.001, aggK=2(锁定)。
- **协作 A 模式**:仓库写入(Java + 配置)全由 dev 落地;planning 出理论/规格/单点 diff,
  不产出可覆盖仓库的整份文件。涉及已有文件改动给单点 diff(改哪行→改成什么)。
- **测量先于假设**:本项目离线假设被集群反复推翻,一切结论以集群实测为准;
  PromQL/算子名/分区行为等必须实测确认,不可凭代码假设。

---

## 4. 方向 B 的推进顺序(建议)

1. **理论论证**(planning 先做):§1.3 四个问题,尤其正确性(子流内重排 == 单分区顺序)。
   产出一份理论分析文档,论证"per-feature 子流内按 seq 重排"的正确性与乱序界。
2. **重排机制设计**(planning 出规格,dev 实现):选 §1.2 方案,给 dev 字段表 + 算法伪码 +
   插入点(keyBy 后、检测器前的 KeyedProcessFunction)。
3. **最小验证实验**(§1.4):dev 实现后,用户跑 1分区 vs N分区,比较检测一致性 + 吞吐提升。
4. **若验证通过**:多分区 EXP3 测吞吐扩展(source_ingress 随分区数 N 提升)。
5. **若验证不通过**(检测不一致 / 延迟代价过大):回退方向 A(见主对话 PROJECT_SUMMARY_directionA.md)。

---

## 5. 风险与回退

- **风险**:重排引入延迟(等乱序数据),与"低延迟"冲突;重排缓冲增加状态/复杂度;
  改检测链路可能影响已验证的 EXP1/EXP2/EXP4 结论(故方向 B 在**独立分支**,不污染主分支)。
- **回退**:方向 B 走不通,转回方向 A——主分支已固化(PROJECT_SUMMARY_directionA.md),
  只需补完单分区 EXP3(吞吐权衡叙事 + 延迟扩展)即可进入论文撰写。方向 A 工作基本完成、无难点。
- **不变量**:无论 B 成败,EXP1/EXP2/EXP4 结论(主检测器 IKS、分布式保持准确性等)不受影响,
  它们在单分区下已验证,方向 B 只改 source 分区与重排,不改这些实验的结论。

---

## 6. 起步建议(新对话第一步)

新对话 planning 先做 §4.1 理论论证:**论证"per-feature 子流内按 originalSequence 重排后,
检测器看到的顺序严格等于单分区时的顺序"**——这是用户前提("先理论证明")的核心。
读 PerFeatureIKSFunction.java 确认 IKS 的顺序依赖具体形式,再论证重排的正确性与乱序界(§1.3)。
理论站得住,再进入 §4.2 机制设计。
