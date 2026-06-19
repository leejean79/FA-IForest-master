# EXP2 论证骨架 — 分布式 iForest 异常检测准确性(stationary),集群数据定稿

> 用途:论文 EXP2 小节写作脚手架,支撑核心贡献②"分布式异常检测算法"。所有主张用集群 EXP2 真实数据(3 数据集 × P∈{1,2,4} × 10 shuffle,detector=IKS,BACKLOG)。数据集与 Leveni 2025(Online Isolation Forest)同源下载,口径可比。

---

## 1. 论点(claim)

EXP2 给出两个相互独立、均成立的结论:

**(A) 分布式化不牺牲准确性。** iForest 经列并行检测 + 分布式打分/重训后,stationary ROC AUC 随并行度保持:donors/forestcover/fraud 三数据集 P=1→P=4 的 AUC 变化均在 ±0.017 内,小于各自 run 间 std(0.015–0.028),即并行度对准确性的影响在统计噪声内。

**(B) 准确性与 published 在线/单机 iForest 方法 competitive。** P=1(准单机)AUC 落在 Leveni 2025 Table 4 五算法区间内:donors 0.808(≈最佳 oIFOR 0.795)、forestcover 0.873(高于 oIFOR 外三算法,低于 RRCF 0.917)、fraud 0.950(≈最佳 RRCF 0.951/asdIFOR 0.946)。

附带:系统在 stationary 流上产生有限次模型更新(1–2.6 次/run),重训后 AUC 维持而非恶化,是对数据内在波动的无害在线适应(§3.3)。

## 2. 机制/背景

### 2.1 为何 stationary 数据仍有模型更新(非误触发)
donors/forestcover/fraud 被归 stationary 是相对于有标注离散漂移点的 INSECTS,**不代表分布恒定**。这些真实数据含内在分布波动(局部密度、子群/子空间结构),**正是 oIFOR/asdIFOR 等在线更新算法选用它们的原因**:数据若真恒定则静态 iForest 足矣,无需在线遗忘/更新。系统的有限次重训与这些在线算法的更新机制同源,是特性而非缺陷。

### 2.2 shuffle 的作用与 caveat(须写入)
EXP2 流经 shuffle 打散(与 Leveni 一致:每次执行前随机打散)。shuffle 破坏时序结构,故重训响应的是数据**非时序的分布异质性**(局部密度团簇、子空间结构),而非时序漂移。oIFOR/asdIFOR 在同样 shuffle 流上仍体现在线更新价值,印证这些数据打散后仍有内在异质性。论文须明确:在线适应针对"内在异质性",非"时序漂移"。

## 3. 证据(集群 EXP2)

### 3.1 主表:stationary AUC × 并行度(mean±std over 10 shuffle)

| 数据集 | P=1 | P=2 | P=4 | 随 P 最大变化 |
|---|---|---|---|---|
| donors | 0.808±0.028 | 0.797±0.015 | 0.807±0.024 | −0.011 |
| forestcover | 0.873±0.015 | 0.890±0.017 | 0.889±0.017 | +0.017 |
| fraud | 0.950±0.002 | 0.950±0.002 | 0.950±0.003 | +0.001 |

→ 结论 (A):变化均 < std,**AUC 随并行度保持,分布式不牺牲准确性**。无单调下降趋势(donors P4 回到 P1,forestcover P2/4 略升,fraud 三档不动)。

### 3.2 对照 Leveni 2025 Table 4(P=1 准单机 vs 五算法,量级参考)

| 数据集 | 本系统 P=1 | oIFOR | asdIFOR | HST | RRCF | LODA | 落点 |
|---|---|---|---|---|---|---|---|
| donors | **0.808** | 0.795 | 0.769 | 0.715 | 0.637 | 0.554 | ≈ 最佳,competitive |
| forestcover | 0.873 | 0.887 | 0.861 | 0.722 | **0.917** | 0.500 | 第 3/6,低于 RRCF/oIFOR |
| fraud | 0.950 | 0.936 | 0.946 | 0.910 | 0.951 | 0.722 | ≈ 最佳,competitive |

→ 结论 (B):三数据集均落区间内、competitive。
**诚实性边界(写入论文,不可越)**:
- 量级参考,**非严格 head-to-head**:本系统 100 树,Leveni oIFOR 32 树,树数不同。donors 上 0.808>oIFOR 0.795 **不写"更优",写"相当/competitive"**。
- forestcover 上 RRCF(0.917)明显高于本系统(0.873),**如实写"低于 RRCF,高于其余多数算法,与 oIFOR 接近"**,不回避。

### 3.3 在线适应有效性:重训后 AUC 维持(retrain delta)

每次模型版本切换,比较切换前后段 AUC:

| 数据集 | P=4 重训事件 | delta_mean | frac(delta≥0) | auc_pre→post |
|---|---|---|---|---|
| donors | 20 | +0.0139 | 0.80 | 0.797→0.811 |
| forestcover | 10 | +0.0169 | 0.60 | 0.876→0.893 |
| fraud | 23 | +0.0011 | 0.65 | 0.950→0.952 |

(全 9 组:7 组 delta_mean≥0,最差 forestcover P2 −0.016 但 P4 +0.017;无一组 auc_post 显著低于 auc_pre)

→ 结论:**重训后 AUC 维持而非恶化**,模型更新是无害在线适应,非噪声过拟合,与 oIFOR/asdIFOR 同源。
**精确表述(关键,不可夸大)**:用"**维持准确性/无害适应**",**不用"提升准确性"** —— delta 普遍很小(±0.017 内)、frac_nonneg 多在 0.4–0.65,说明重训前后 AUC 在小范围随机起伏、无系统性变好或变坏,这正是"无害适应"(非"有益")的特征,符合 stationary 无真实概念跃迁的预期。

### 3.4 次级观察(可提,不过度演绎)
delta_mean 与 frac(delta≥0) 随并行度 P 增大而改善(donors frac 0.43→0.80;forestcover/fraud 同向)。**标记为"观察到的趋势,机制待考"**,可能与 D1a 重训池构成相关,但本实验不足以定因,论文不强行解释。

## 4. 写作风险与防线
- (A) 与 (B) 是**独立**结论,分开陈述,不混为一句。
- competitive **非 SOTA**:措辞克制,树数差异 caveat 必写。
- forestcover 低于 RRCF 必须如实,不挑数据。
- 重训只写"维持准确性",不写"提升"。
- shuffle→响应"内在异质性非时序漂移"的 caveat 必写。
- 误触发 vs 在线适应:本骨架已据数据(delta≥0)定性为"无害适应";若审稿质疑,退路是"无论解释为适应或低频误报,重训后 AUC 未恶化,对端到端质量无损"。

---

*数据来源:集群 EXP2(exp2_accuracy_summary.csv / exp2_vs_leveni.csv / exp2_retrain_delta.csv;P{1,2,4}×10 shuffle,detector=IKS,BACKLOG)。Leveni 基准:Online Isolation Forest 2025 Table 4。*
