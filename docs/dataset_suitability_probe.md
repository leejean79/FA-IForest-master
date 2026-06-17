# 数据集适配性探查记录 — INSECTS incremental / incremental-reoccurring

> 目的:记录两个新 INSECTS 数据集(incremental_imbalanced、incremental-reoccurring_imbalanced)是否纳入 per-feature 漂移检测对比实验的探查过程与结论。结论:**两者均不纳入定量对比,作为 per-feature 架构适用边界的诚实讨论写入论文。** 探查经双检测器离线验证,非单边推测。

---

## 1. 背景

为补强当前定量证据最弱的一环(真实渐进/再现漂移仅 INSECTS_gradual 一个、且实测为约 1900 样本的快速过渡),尝试纳入两个新数据集。漂移点来自权威来源 Souza 等《Challenges in Benchmarking Stream Learning Algorithms with Real-world Data》Table 2。

| 数据集(imbal.) | 原始样本 | 论文标注 change point | 转换后异常占比 |
|---|---|---|---|
| Incremental | 452,044 | **Throughout all the stream**(全程渐变,无离散点) | — |
| Incremental-reoccurring | 452,044 | 150683; 301365(→ 转换后 91134; 182273) | 0.04876(可比) |

---

## 2. Incremental(全程渐变):不纳入

论文标注为"贯穿整个流"的连续渐变,**无离散漂移点**。当前全部指标(命中率、恢复延迟 def1/2/3、漂移后假阳性率)均依赖离散漂移点定义"漂移前 vs 漂移后",全程渐变流无法定义命中窗、无法区分正常期与漂移期。`transform_insects.py --drift-points` 对其无意义。

**结论**:与当前离散漂移响应对比框架不兼容,不纳入。若将来做"持续渐变流上检测器持续触发行为"专项分析,可作材料,但属另一实验。

---

## 3. Incremental-reoccurring:经双检测器验证后不纳入

转换成功(异常占比 0.04876,落在现有 INSECTS 0.0488–0.062 可比范围),漂移点映射为转换后 91134/182273。但两个检测器的离线检验均显示该数据集在 per-feature 架构下无法产生干净检测。

### 3.1 HDDM_W 离线信号检验(`hddm_signal_sanity.py`,λ=0.005,p99)

| 特征 | 漂移点 | lift | 触发命中 |
|---|---|---|---|
| f0/f2/f8/f28/f27/f12 | 91134 | +0.004 ~ +0.012 | 否(散发触发均为早期噪声,first_fire 远离漂移点) |
| 同上 | 182273 | −0.008 ~ +0.011 | 否 |

per-feature signal 在两个漂移点处均无可见均值偏移(lift 全在 ±0.012 内,对比 abrupt 0.05–0.22、gradual 0.08–0.24)。HDDM_W 几乎不响应。
另注:f0/f2 的 p99 scale 高达 669/1306,系大量纲未归一化特征,signal 计算失真——INSECTS 特征量纲差异极大,top-lift 选特征会混入大量纲特征(独立问题,见 §5)。

### 3.2 IKS 离线检验(`derisk_proxy.py`)

| 指标 | 值 | 含义 |
|---|---|---|
| agg_onset | 2375 | 聚合触发点数(真漂移仅 2 个) |
| precision | 0.0408 | 每 100 次触发仅 4 次对应真漂移点 |
| recall | 1.0000 | **误导**:误报淹没真信号(latency=−34250,触发普遍早于标注点) |
| GT_A_onset | 153500 | 实测 AUC 退化点(degradation_onset),与标注点 182273 不对齐 |

IKS 全程高频误触发(2375 次),precision 仅 4%。`recall=1.0` 是假阳性覆盖了漂移点所致,非干净检测。

### 3.3 综合判断

reoccurring 的 per-feature 信号表现为**全程高频波动而非清晰分布跃迁**(来回切换 + 全程渐变的复合漂移,无稳定新平台)。两个检测器以相反方式失效:
- **IKS(敏感、分布距离)**:把波动全程误判为漂移(precision 4%);
- **HDDM_W(迟钝、受 ε 方差下界制约)**:几乎沉默(lift ±0.012)。

**没有任何一个检测器产生对应那两个概念切换点的干净检测**,无法支撑"检测器–漂移形态匹配"的清晰结论。强行纳入只会引入噪声。**结论:不纳入定量对比。**

---

## 4. 论文价值:per-feature 架构适用边界(诚实讨论)

两个数据集的探查共同揭示一个有价值的边界发现(定性观察,非定量结论):

**per-feature 检测架构适合"有明确前后分布差异"的漂移(abrupt 脉冲、gradual 持续平台),不适合"无稳定新平台"的漂移(incremental 全程渐变、reoccurring 来回切换)。** 后者在单特征信号上表现为持续波动而非分布跃迁,使敏感检测器全程误报、迟钝检测器沉默。这是 per-feature 列并行检测面的固有适用边界,应在论文中作为方法局限诚实讨论——比硬塞失效数据集进定量矩阵更有分量。

此发现与已记录的 ε 下界分析(devspec §7)、检测器–漂移形态匹配律(EXP4)互补:匹配律说明"在适配的漂移形态内,不同检测器各有所长";本边界说明"存在 per-feature 架构整体不适配的漂移形态"。

---

## 5. 附带探查:INSECTS 特征量纲 —— 经普查证实非问题

探查 reoccurring 时观察到 f0/f2 的 p99 scale 高达 669/1306(大量纲特征),一度怀疑决策 B 信号对大量纲特征失真。**经 `insects_feature_audit.py` 对 abrupt/gradual 全 33 特征普查,此怀疑被证伪:**

- abrupt:33/33 特征 verdict=ok;大量纲特征 f0–f6(原始值达 1.2 万、scale_p99 达 862–2672)**全部 ok**,signal 饱和率正常、漂移 lift 正常。
- gradual:23/33 ok,10 个 weak_signal(漂移点 lift<0.02)——这是"漂移的特征选择性"(并非所有特征都对某次漂移敏感),非失真,属正常现象;大量纲特征同样全部 ok。

**结论:决策 B 的 `|x−refMean|/scale` 对任意量纲都能正确归一化,INSECTS 的 per-feature 检测不需要额外标准化层。** 之前 reoccurring 里 f0/f2"看似失真",真实原因是 reoccurring 数据本身漂移信号极弱(lift ±0.012,§3.1),与量纲无关——是把"大 scale"与"失真"误关联。devspec 无需因此改动。

> 教训留痕:此处一度将"scale 大"当作"信号失真"的因果,普查数据(同样大 scale 的特征在 abrupt/gradual 上 verdict 全 ok)证伪之。再次印证:相关非因果,凭直觉的因果判断须用全量数据核验。

---

*探查日期数据:HDDM_W 信号检验 + IKS derisk_proxy,均对转换后 INSECTS_incremental-reoccurring(91134/182273)。结论经双检测器验证。*
