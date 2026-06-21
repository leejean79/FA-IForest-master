# EXP3 论证骨架 — 系统吞吐扩展性(集群数据定稿:吞吐部分)

> 用途:论文 EXP3 吞吐小节脚手架。支撑论文系统级"高吞吐"基石的诚实刻画。
> 数据:集群 24 run(donors/http × P∈{1,2,4,6} × 3),detector=IKS,BACKLOG,
>      JOB_LOAD_RATE=50000(解 producer 限速),source 单分区(SOURCE_PARTITIONS=1,保时序),
>      detectionParallelism=parallelism(检测/打分面同步并行)。三口径吞吐从 Prometheus
>      numRecordsIn rate 测(operator_name 校准:Kafka_Source / Per_Feature / Local_Processor)。
> 代码状态:方向 A 主分支(单分区,无多分区/重排),jar 已重新打包上传,集群代码已核对一致。

---

## 1. 论点(claim)

EXP3 吞吐部分给出一个核心的、诚实的工程发现:

**在 source 单分区(保检测时序)的约束下,系统端到端吞吐被结构性锁定在 ~7,000–9,600 rps,
不随并行度扩展,且在高并行度(P=6)因协调开销增加而略降。检测/打分算子本身具备显著更高的
处理能力(检测面达 ~77,000 rps),但受单线 source 喂数限制无法发挥。这量化了分布式流处理中
"高吞吐与严格时序保证不可兼得"的本质权衡。**

这是支撑性发现而非缺陷:它揭示并量化了一个被很多系统忽视的真实权衡。

## 2. 机制/背景

### 2.1 为什么 source 必须单分区(保时序)
检测器(per-feature IKS)按数据到达顺序逐点 update 滑动 KS 窗口,无算子内重排
(originalSequence 仅用于离线分析重排,运行时不重排)。source 多分区会使同一 feature 数据
跨分区乱序到达,破坏 KS 时序 → 检测失真。故 source 保持 1 分区,单线读取。

### 2.2 两个并行度旋钮与三口径
- 全局 parallelism + detectionParallelism 同步提高检测面/打分面并行度;source 因 1 分区单线。
- 三口径(numRecordsIn rate):source_ingress(端到端入口,受单线限)、detection(per-feature,
  按特征数展开)、scoring(打分面,受 source 喂数限)。

### 2.3 producer 限速的解除(实验前置)
原 JOB_LOAD_RATE=1000 将吞吐人为焊死 1000 rps;提高到 50000 后解锁,使 source 单分区的真实
上限(~7000-9600 rps)得以显现。这是测出真实瓶颈的前提。

## 3. 证据(集群 24 run)

### 3.1 三口径吞吐 × 并行度(中位 rps over 3 repeats)

| 数据集 | P | source_rps | detection_rps | scoring_rps | scoring_speedup |
|---|---|---|---|---|---|
| donors | 1 | 7720 | 77018 | 7704 | 1.00 |
| donors | 2 | 8571 | 85620 | 8534 | 1.11 |
| donors | 4 | 8643 | 86669 | 8663 | 1.12 |
| donors | 6 | 7409 | 75810 | 7151 | 0.93 |
| http | 1 | 8830 | 26534 | 8755 | 1.00 |
| http | 2 | 9580 | 28739 | 9129 | 1.04 |
| http | 4 | 8877 | 24160 | 7365 | 0.84 |
| http | 6 | 9338 | 31750 | 7820 | 0.89 |

### 3.2 三个关键观察

**(A) 端到端吞吐不随并行度扩展。** source_ingress 与 scoring 在所有 P 下均落在
~7,000–9,600 rps 窄带内,无随 P 上升趋势。source≈scoring(打分被 source 喂数限制)。
→ 端到端被 source 单分区(单线读取)结构性限制。

**(B) speedup ≈ 1,且 P=6 时略降(donors 0.93、http 0.89)。** 增加并行度不提升端到端吞吐
(被 source 锁定),反因算子增多带来调度/序列化/checkpoint 开销,P=6 净效应轻微下降。
→ 在单分区约束下,盲目增加并行度无益甚至有害——重要的工程结论。

**(C) 检测面吞吐远高于端到端,且 = source × 特征数。** donors detection ≈ source×10
(10 特征),http detection ≈ source×3(3 特征)。检测面具备 ~77k rps(donors)处理能力,
但被单线 source 喂数限制,无法发挥。detection 自身也不随 P 扩展(吃不饱)。
→ 算子处理能力 ≫ 端到端吞吐,瓶颈明确在最上游单分区 source。

### 3.3 数据集差异(佐证口径正确)
detection/source 比值:donors ≈10、http ≈3,精确对应各自特征维度(donors 10 维、http 3 维)。
印证 detection 口径是"特征展开后的 per-feature 信号流",非"检测更快"。

## 4. 结论与权衡叙事

**高吞吐 vs 时序保证的权衡(本系统的核心系统级发现):**
- 选择 source 单分区 → 保证检测时序正确 → 端到端吞吐受单线限制(~7-9.6k rps),不可扩展。
- 检测/打分算子可并行、具更高处理能力,但被上游单线瓶颈掩盖。
- 这是分布式流处理"顺序性 vs 并行吞吐"经典张力的具体体现,本系统选择时序优先。

**Future work(方向 B):** source 多分区 + 算子内按 originalSequence 重排,可在保时序前提下
突破单分区吞吐限。已做理论与初步探索,因实现复杂(重排正确性证明、乱序界、重排延迟代价)
留待后续工作。

## 5. 写作风险与防线
- 不夸大:不写"系统高吞吐可线性扩展"(数据明确否定)。诚实写"端到端受单分区限、检测算子高吞吐"。
- speedup<1 的 P=6 必须如实报(不藏),并解释为开销主导——这是有洞察的结果非失败。
- detection 高吞吐须说明是"特征展开"口径,不可误导为"检测面快 N 倍"。
- 数据完整性说明(见 §6):stable-K 早退致末段截断,但吞吐取中段稳态 rate,不受影响。

## 6. 数据完整性与方法学说明(须写入)
- 处理完成判据:stable-K 容忍(连续无 offset 进展视为管线尽力完成)。高 rate 下末段
  (重训/checkpoint 暂停)可能触发早退,scores.jsonl 缺末段约 4–14%。
- 对吞吐结论无实质影响:吞吐 = Prometheus numRecordsIn 的 rate(中段稳态),非依赖末段全量。
- 待办:放宽 stable-K(连续 120s 无进展才退)重跑可得完整 scores,用于对完整性更敏感的延迟分析。

---

*数据来源:集群 EXP3 吞吐 24 run(exp3_throughput_summary.csv;Prometheus 三口径,auto-window)。
代码:方向 A 主分支(单分区),jar 已核对。延迟部分待 stable-K 放宽后补。*
