# FA-IForest 项目全面总结(方向 A 退路固化版)

> 用途:把已验证的工作完整固化。方向 B(source 多分区 + 算子内重排保序)若走不通,凭本文档可迅速转回方向 A——只需补完 EXP3(单分区口径)即可进入论文撰写。
> 分支:`feature/per-feature-hddm_w`(commit e0da511)。日期:2026-06。

---

## 0. 论文定位

**两大核心贡献:① 联邦/分布式概念漂移响应(投票协议);② 分布式异常检测算法。**
EXP1/EXP4 支撑检测器选型与漂移响应,EXP2 支撑贡献②(分布式准确性),EXP3 支撑系统级吞吐/延迟。

---

## 1. 系统架构

### 1.1 技术栈
Apache Flink 1.13.6(Scala 2.12)+ Kafka 2.6.3 + Docker;3 节点 Alibaba Cloud ECS
(master 172.16.0.162 / worker1 .163 / worker2 .164);master 自托管(RUN_MODE=local + tmux);
Prometheus(master,compose_fa-net)+ Grafana 监控;构建 maven-shade fat jar。

### 1.2 数据流水线(算子链)
```
Kafka Source [source-topic](单分区单线,保时序)
  → CSV to DataPoint → Feature Split (row → D)
  → keyBy(featureId) → Per-Feature 检测器(IKS/HDDM_W,detectionParallelism)→ feature-drift-topic
  → keyBy → Local Processor (Phase A/B/C) 打分 + 聚合驱动重训(全局 parallelism)
  → output-scores-topic
  并行控制流:Coordinator / Drift Aggregator / Forest Assembly(tree/model/drift-round topics)
```
Prometheus operator_name(下划线化):`Kafka_Source__source_topic_`、`Per_Feature_IKS__peak_KS_gate_`、`Local_Processor__Phase_A_B_C_`。

### 1.3 两个并行度旋钮
- 全局 `parallelism`(env.setParallelism,`--parallelism`):含 source(1 分区限制)+ 打分面 + 其他。
- `detectionParallelism`(独立,默认 1,经 --extra-param 透传):仅检测面(per-feature)。

### 1.4 关键设计约束:source 单分区保时序
source-topic = 1 分区(SOURCE_PARTITIONS=1)。**检测器(IKS)按数据到达顺序逐点 update 滑动窗口,
无算子内重排**(originalSequence 仅用于 dump 后分析重排,运行时不重排)。故多分区会乱序破坏检测时序。
单分区是时序正确性的保证,代价是 source 单线成端到端吞吐瓶颈(见 §4 EXP3)。

---

## 2. 算法实现

### 2.1 基础检测:Isolation Forest
totalTrees=100,subsampleSize=256(代码默认)。per-feature 列并行检测面 + 分布式打分/重训。

### 2.2 漂移检测器(配置级 --detector 切换,一套构建两检测器)
- **IKS(主检测器)**:增量 Kolmogorov-Smirnov(IKSSW 语义)。固定参考窗 + 等长滑动当前窗 W;
  阈值 `ca·√(2/W)`,`ca=√(−0.5·ln(pValue))`;复合键(value,rnd,group)随机 tiebreak。
  无量纲、双向敏感、无 ε 下界、无需逐数据集调参 —— 稳健性结构来源。默认 iksWindowSize=2000, iksPValue=0.001。
- **HDDM_W(对照)**:单样本 EWMA Hoeffding(Frías-Blanco 2015 Example 7)。
  ε=√(λ·ln(1/α)/(2(2−λ))),只由 λ/α 定。per-feature 归一化(p99 scale)+ warm-up 冻结参考均值。
  已 POJO 化(去 final、public 无参构造、7 字段 getter/setter)避免 checkpoint 走 Kryo GenericType。

### 2.3 漂移响应:投票协议(贡献①)
并发 INITIATE 当 YES(Set 幂等);yes+no ≥ parallelism 时早决议。聚合驱动重训(aggK=2 default)。
分布式并行度结构性衰减漂移检测灵敏度(P=4 时 per-branch 信号低于阈值,弱真实漂移上 quorum 罕见)——
记为研究发现,非 bug。

### 2.4 延迟测量(已实现)
延迟 = scoreTime − ingestionTime(摄入→打分完成,不含磁盘 dump)。ScoreResult 含字段、
LocalProcessorFunction 赋值、dump 含此二字段。注:BACKLOG 模式下含 backlog 排队、失真(见 §4)。

---

## 3. 实验结果(已完成,集群验证)

### 3.1 EXP1 — 主检测器选型(P=4, BACKLOG, overall_auc 中位)
| 数据集 | IKS | HDDM_W(λ*) | 结论 |
|---|---|---|---|
| INSECTS_abrupt | **0.758** | 0.740(λ*=0.002) | IKS 略高,差 0.018 在 repeat 波动内 |
| INSECTS_gradual | **0.591** | 0.539(λ*=0.005) | IKS 实质优 +0.052 |
**决定:主检测器 = IKS**(两形态均不低于 HDDM_W,gradual 实质优,abrupt 接近)。
- λ 扫描发现:HDDM_W 最优 λ 依赖漂移形态(abrupt λ*=0.002 单调越小越好,gradual λ*=0.005)。
- 离线假设被集群推翻:"abrupt 上 HDDM_W 失效""gradual 是 HDDM_W 主场"均不成立。

### 3.2 EXP2 — 分布式准确性(贡献②;3 数据集 × P{1,2,4} × 10 shuffle, BACKLOG, IKS)
**(A) 分布式不牺牲准确性**:AUC 随 P 变化均 ±0.017 内 < std。
| 数据集 | P=1 | P=2 | P=4 |
|---|---|---|---|
| donors | 0.808 | 0.797 | 0.807 |
| forestcover | 0.873 | 0.890 | 0.889 |
| fraud | 0.950 | 0.950 | 0.950 |
**(B) competitive with Leveni 2025**:P=1 落五算法区间(donors 0.808≈最佳 oIFOR 0.795;
fraud 0.950≈最佳;forestcover 0.873 低于 RRCF 0.917 但 competitive)。
**(C) 在线适应无害**:stationary 重训后 AUC 维持(retrain delta≥0,无一组恶化)——
重训是对数据内在异质性的无害适应,非误触发,与 oIFOR/asdIFOR 同源。
诚实边界:量级参考非 head-to-head(本 100 树 vs oIFOR 32 树);用"维持准确性"非"提升"。

### 3.3 EXP4 — 两检测器对比机制(分析口径,不单独跑)
取 EXP1 两臂交叉分析 + ε 闭式 + λ 敏感性 + 信号域对照。论点(经集群修正):
**IKS 总体更稳健,HDDM_W 强依赖 λ 且 gradual 偏弱**(非离线设想的"各有所长匹配律")。

### 3.4 EXP3 — 扩展性(部分完成,见 §4)
监控链路打通,三口径吞吐可测。已发现 producer 限速问题(已解,提高 JOB_LOAD_RATE)。
核心受限:source 单分区,端到端吞吐 ~7000 rps 不随并行度扩展(架构约束)。

---

## 4. EXP3 现状与方向 A 收尾方案

### 4.1 已查明的事实
- **producer 限速**:.env `JOB_LOAD_RATE=1000` 把端到端吞吐焊死 1000 rps。提高后(50000)解锁:
  source_ingress ~7270 rps median、detection ~72521(≈source×10,特征展开)、scoring ~7245。
- **BACKLOG 边灌边处理**(非先灌后处理):source 读取被 producer 喂数速度限制。
- **source 单分区上限 ~7000-9600 rps**:这是 1 分区 Kafka + 单线 source 的真实上限。
- 监控:Prometheus 三 target up,operator_name 已校准(下划线名)。三口径吞吐脚本就绪。

### 4.2 方向 A 的 EXP3 定位(若回退,按此收尾)
端到端吞吐受 source 单分区(保时序)结构性限制,**不随并行度扩展**——这是诚实的工程权衡,
非缺陷。EXP3 报告:
- **吞吐(权衡叙事)**:端到端 ~7000 rps(单分区保时序代价)+ 检测算子 ~72000 rps(算子高处理能力)。
  如实说明"高吞吐与时序保证不可兼得"是分布式流处理的经典权衡,本系统选择时序优先。
- **低延迟(扩展主线)**:测处理延迟随并行度下降(更多并行子任务分担)。这是 EXP3 能体现
  Flink 重构价值之处。**需绕开 BACKLOG 延迟失真**(scoreTime−ingestionTime 含 backlog 排队)——
  用 Flink 算子延迟指标(latency tracking)或低吞吐实时摄入专测。
- **多分区作 future work**:论文展望"端到端吞吐受单分区限制,未来可经算子内重排支持多分区并行"。

### 4.3 方向 A 收尾待办(回退后)
1. JOB_LOAD_RATE 提高后重跑 EXP3(donors/http × P{1,2,4,6} × 3),三口径吞吐 + 延迟。
2. 延迟测量方案:Flink latency metric 或实时摄入(绕 BACKLOG 失真)。
3. 写 EXP3 论文小节(吞吐权衡 + 延迟扩展 + 单分区约束讨论)。

---

## 5. 剩余问题与已知边界

- **EXP3 延迟口径**:BACKLOG 下 scoreTime−ingestionTime 失真,需另测(未决)。
- **dev 待应用单点 diff**:sensitivity_iks 去 iksWindowSize 重复行;analyze-all.sh arm 标签
  (new/old → iks/hddm_w)。均不影响已有结论。
- **sensitivity 附录**:IKS 参数稳健性、HDDM_W scaleMode/warmup —— 未跑。
- **gradual 定量样本量小**:仅 INSECTS_gradual 1 数据集 1 漂移点,synth 不可补(8σ 高估)。
- **arm 标签 bug**:drift-summary.csv 的 new=IKS/old=HDDM_W(analyze-all.sh:98 旧 build 语义),
  结论无误但标签反直觉,建议修。

---

## 6. 工具与产物索引(/mnt/user-data/outputs/)
- 分析脚本:exp2_distributed_accuracy.py、exp2_false_alarm.py、exp3_scalability_from_scores.py、
  exp3_throughput_prom.py、insects_feature_audit.py 等。
- 论文骨架:EXP2_distributed_accuracy_skeleton.md、EXP4_detector_morphology_skeleton.md。
- handover:HANDOVER_v1.0-exp3.md、DEV_FIXES_before_full_EXP.md、FIX_cfg_query_sensitivity_plan_extras.md。
- 诊断:diag-prometheus-flink.sh。
- 运行:run-exp3-single.sh。

---

## 7. 关键纪律(贯穿,转回 A 时遵守)
- 测量先于假设,下结论前读真实数据/源码(离线↔集群差距系统性,已多次被推翻)。
- overall_auc 主指标;pauseMode 须同模式内比较(BACKLOG 生产意图)。
- 协作 A 模式:所有仓库写入由 dev 落地,planning 出分析/规格/单点 diff。
- recovered 与 overall_auc 联读;measure 三口径吞吐用校准后下划线 operator_name。
