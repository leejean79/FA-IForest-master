# EXP4 论证骨架 — 两检测器对比(per-feature IKS vs HDDM_W),集群数据定稿版

> 用途:论文 EXP4 小节写作脚手架。**本版已用集群 EXP1 真实数据改写**:离线设想的"检测器–漂移形态匹配律(各有所长)"被集群数据推翻,改为数据支持的论点。所有主张用 EXP1 的 overall_auc(P=4, BACKLOG, 3 repeats)支撑。
>
> 重要修订记录:此前骨架(及多份离线分析)主张"HDDM_W 偏好 gradual、在 abrupt 上结构性失效",并预期"gradual 行两者接近、abrupt 行 IKS 显著优"。**集群结果与该预期基本相反**:HDDM_W 在 abrupt 上接近 IKS,在 gradual 上反而明显弱于 IKS。详见 §1.1。

---

## 1. 论点(claim)— 经集群修正

基于真实 INSECTS 数据(abrupt / gradual 两形态)的 EXP1 对比:

**IKS 在两种漂移形态上的 overall_auc 均不低于 HDDM_W;在 gradual 上有实质优势(+0.05),在 abrupt 上两者接近(差距 0.018,在 repeat 波动内)。HDDM_W 的有效性强依赖 λ,且最优 λ 随漂移形态变化。** 故本工作选 IKS 为主检测器。

这**不是**"普适最优检测器"的主张,而是本架构 + 本数据范围内的经验结论。信息量在于:一个无量纲、无方差惩罚、双向敏感的分布距离检测器(IKS),比受 Hoeffding 半径 ε 下界制约、需持续平台累积的均值检验(HDDM_W),在 per-feature 归一化信号上更稳健、且无需逐数据集调 λ。

### 1.1 离线假设被集群推翻的记录(方法学诚实性,建议写入论文)

离线信号分析(`hddm_signal_sanity.py`,λ=0.005)曾得"abrupt 上 HDDM_W 几乎不触发、gradual 是 HDDM_W 主场"。集群 EXP1 推翻两点:

1. **abrupt 上 HDDM_W 并未失效**:λ=0.002 时 overall_auc 中位 0.740(n_retrains=6,检测充分),接近 IKS 的 0.758。离线"结构性失效"源于固定用 λ=0.005、且离线信号判据(固定窗命中率)与端到端 AUC 不等价。
2. **gradual 不是 HDDM_W 主场**:HDDM_W gradual 中位 0.539,低于 IKS 0.591。根因是 INSECTS_gradual 漂移前基线森林本身差(v1 AUC≈0.45,数据集难),非检测器偏好;IKS 在此数据集恢复更主动(n_retrains=3 vs 2)。

教训:离线信号命中率不能外推为端到端检测质量;单一 λ 的离线结论不能代表检测器能力。论文应将此作为"为何必须集群验证"的方法学论据。

## 2. 机制论证(为什么 IKS 更稳健)

### 2.1 ε 对触发阈值的固定抬升(闭式,保留)
`ε = √(λ·ln(1/α)/(2(2−λ)))`,只由 λ、α 决定,与归一化 scale 无关。HDDM_W 稳态触发所需最小 lift 随 λ 增大而增大;降 λ 降门槛但拉长记忆窗。这解释 HDDM_W 必须调 λ、且 abrupt 偏好更小 λ —— 见 λ 扫描:abrupt λ*=0.002(单调越小越好),gradual λ*=0.005。IKS 无此 ε 下界、无需逐数据集调参,是其稳健性的结构来源。

### 2.2 IKS 的双向 + 无量纲优势
IKS 监控经验分布距离(KS 统计量):对任意方向分布变化敏感、尺度无关、直接吃原始特征。HDDM_W 只检测 EWMA 均值上升、需被监控值有界 [0,1](故 per-feature 需归一化)、依赖持续平台供 EWMA 越过 ε。在 per-feature 归一化信号(lift 仅 0.05–0.24)上,ε 成为对触发阈值的固定抬升,使 HDDM_W 对弱信号欠灵敏;IKS 累积分布距离,无此前提。

### 2.3 信号域对照(score 域 vs per-feature,补充论据)
同一 HDDM_W 同一 λ=0.1:score 域(旧 `feature/hddm-w`)可用,per-feature 归一化信号失效。差异源于信噪比:score 域台阶相对噪声大、ε 相对小;per-feature 台阶矮、ε 成固定抬升。→ HDDM_W 有效性绑定信号域,在 per-feature 架构下处不利信号域。

## 3. 证据(集群主表 + 离线辅助)

### 3.1 集群证据 — 论文定量主表(EXP1, P=4, BACKLOG, overall_auc 中位 / 3 repeats)

| 数据集 | 检测器 | overall_auc 中位(范围) | n_retrains | recovered | 说明 |
|---|---|---|---|---|---|
| insects_abrupt | **IKS** | **0.758**(0.753–0.776) | 7 | False | 主检测器 |
| insects_abrupt | HDDM_W (λ*=0.002) | 0.740(0.729–0.756) | 6 | False | 接近 IKS,差距 0.018 在波动内 |
| insects_gradual | **IKS** | **0.591**(0.587–0.596) | 3 | True | gradual 上明显优 |
| insects_gradual | HDDM_W (λ*=0.005) | 0.539(0.498–0.549) | 2 | True | 低于 IKS 0.052 |

判读要点(写入论文,避免误导):
- **abrupt 差距(0.018)与 repeat 波动(各臂极差≈0.025)同量级**:abrupt 上两检测器统计接近,不宜称 IKS 显著优。
- **gradual 差距(0.052)超出波动**:IKS 优势主要支撑。
- **recovered 反差**:gradual `recovered=True` 但 AUC 低(基线 v1≈0.45、恢复阈值低易达);abrupt `recovered=False` 但 AUC 高(基线 v1≈0.80、阈值高难达)。**recovered 不可单独作质量判据**,须与 overall_auc 联读。
- **n_retrains**:gradual 上 IKS(3)比 HDDM_W(2)重训更主动,与其更高 AUC 一致。

### 3.2 λ 敏感性(HDDM_W 专属,附表)
EXP1 前置 λ 扫描(P4, BACKLOG, 3 repeats, overall_auc 中位):

| 数据集 | λ=0.002 | λ=0.005 | λ=0.01 | λ* |
|---|---|---|---|---|
| abrupt | **0.730** | 0.670 | 0.615 | 0.002 |
| gradual | 0.498 | **0.540** | 0.513 | 0.005 |

**最优 λ 随漂移形态变化(abrupt 偏小、gradual 偏中)是论文发现**:HDDM_W 无单一普适 λ,IKS 无需此调参 —— 稳健性论据。

### 3.3 离线辅助(降级为机制示意,不作主张)
离线 per-feature lift(abrupt 0.05–0.17、gradual 0.08–0.24)与 ε 闭式,仅用于**解释机制**(为何 HDDM_W 对弱归一化信号欠灵敏),不再作"HDDM_W 偏好 gradual"的证据(已被集群推翻)。synth 四数据集(8σ 强度)仅定性,不进定量。

## 4. 与既有 EXP4 定位的衔接
- 原 EXP4 = "HDDM_A_Windowed vs HDDM_W vs IKS 三检测器对比"。本分支已完成 IKS vs HDDM_W。
- 若纳入 HDDM_A_Windowed 作第三数据点:它属 HDDM 家族、同受 ε 下界制约,预期与 HDDM_W 同类(per-feature 信号域下弱于 IKS);若其窗口化设计改变这一点,是有趣子发现。**可选扩展,非主线必需**(主检测器已定 IKS)。
- 避免与 EXP1 重复:EXP1 报单变量对比原始数;EXP4 报机制解释 + ε 闭式 + λ 敏感性 + 信号域对照。

## 5. 写作风险与防线
- **abrupt 差距在波动内**:措辞用"IKS 不低于/略高于 HDDM_W、gradual 上有实质优势",**不写"全面胜出/碾压"**。
- **gradual 定量样本量小**:可定量比较的 gradual 仅 INSECTS_gradual 1 数据集 1 漂移点;synth 不可扩充(8σ 高估)。须明说局限。
- **recovered 与 overall_auc 联读**,不单独用 recovered 或 per-version AUC 下结论。
- **pauseMode**:只在 BACKLOG 内比较,USE_OLD_FOREST 作附录稳健性。
- **数据集难度混杂**:gradual 低 AUC 部分源于数据集本身(v1≈0.45),非纯检测器差异;论文须区分"检测器贡献"与"数据集基线"。
- **离线↔集群**:离线只进机制小节,主张引集群 overall_auc。本版已修订,保留"离线假设被推翻"记录作方法学诚实性。

---

*数据来源:集群 EXP1(drift-summary.csv,P4/BACKLOG/3 repeats;arm: new=IKS / old=HDDM_W —— 注:arm 标签来自 analyze-all.sh 旧 build 语义,已建议改为直接检测器名)。λ 扫描见 §3.2。ε 闭式见 devspec §7。*
