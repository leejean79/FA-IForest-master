# EXP4 论证骨架 — 检测器–漂移形态匹配律(detector–drift-morphology matching)

> 用途:把 per-feature IKS vs HDDM_W 的对比从"谁更好"重定位为"检测器有效性绑定于信号域与漂移形态的组合"。本骨架是论文 EXP4 小节的写作脚手架 + 待集群补齐的数据位。**所有离线数仅为假设来源;论文主张一律用集群 EXP1 的 overall_auc 支撑。**

---

## 1. 论点(claim)

不存在普适最优的概念漂移检测器;在联邦 per-feature 检测架构下,检测器的有效性取决于**被监控信号域**与**漂移形态(abrupt 脉冲 vs gradual 持续平台)**的匹配:

- **IKS**(增量 Kolmogorov–Smirnov):监控经验分布距离,无量纲、无方差惩罚项,对 abrupt 与 gradual 均响应;尺度无关,直接吃原始特征。
- **HDDM_W**(EWMA + Hoeffding 均值检验):监控被监控值的 EWMA 均值上升,受 Hoeffding 半径 ε 的不可约下界制约,要求被监控值有界 [0,1];仅当漂移后形成持续平台供 EWMA 累积越过 ε 时才灵敏——故偏好 gradual,在 abrupt 脉冲上结构性欠灵敏。

**这比"IKS 优于 HDDM_W"信息量更高**:它给出何时该用哪类检测器的可操作判据。

## 2. 机制论证(为什么)

### 2.1 ε 对触发阈值的固定抬升(闭式)
`ε = √(λ·ln(1/α)/(2(2−λ)))`,只由 λ、α 决定,与归一化 scale 无关。稳态触发所需最小 lift ≈ `ε·(1+√(ln(1/α_d)/ln(1/α_w)))`。给出 λ–ε–所需 lift 表(见 devspec §7.1),说明:降 λ 降门槛但拉长记忆窗使对突变迟钝 → 结构性两难。

### 2.2 平台 vs 脉冲(为什么 gradual 更利于 HDDM_W)
EWMA 是指数加权移动平均,需要被监控值在新水平上**持续若干记忆窗**才能爬到新均值。abrupt 漂移在 per-feature 归一化信号上表现为脉冲式偏离后回落(异常点驱动,瞬时但不持久),EWMA 来不及爬升即衰减;gradual 漂移进入持续新分布平台,EWMA 充分爬升越过 ε。IKS 累积分布距离,不需要"持续平台"这个前提。

### 2.3 信号域对照(score 域 vs per-feature)
同一 HDDM_W 同一 λ=0.1:score 域(旧 `feature/hddm-w` 分支)有效,per-feature 归一化信号失效。差异源于信号域信噪比:score 域漂移台阶相对其噪声大,ε 相对小;per-feature 归一化台阶矮(0.1–0.24),ε 成为对触发阈值的固定抬升。→ 有效性绑定信号域,非检测器固有优劣。

## 3. 证据(what)

### 3.1 离线证据(分定量 / 定性两类)

**(a) 定量证据 — 仅 INSECTS(真实难度)**。warmup=2000,scaleMode=p99,`analysis/hddm_signal_sanity.py`:

| | abrupt | gradual |
|---|---|---|
| 典型 per-feature lift | 0.05–0.17(峰 0.22) | 0.08–0.24 |
| HDDM_W λ=0.02 | 0/30 触发命中 | 6/6 触发,2/6 命中窗 |
| HDDM_W λ=0.005 | 仍多漏 | 6/6 触发,3/6 命中窗 |

定量结论只立在这两行:abrupt(脉冲、弱信号)命中率显著低于 gradual(更接近持续平台)。两数据集均用固定窗 `[start, start+warmup]` 判据,口径统一。

**(b) 定性证据 — synth 四数据集(不可定量比较)**。`analysis/hddm_signal_batch.py` 定性区。**警示**:synth 漂移强度远超真实数据——driftspec 的 `params` 显示正常类均值 `normal_mu` 由 0 跳到 8(normal_sigma=1,约 **8σ** 跳变),per-feature signal 满格(≈1.0)、lift≈0.8,任何检测器都几乎必中,定量上会系统性高估。synth 仅用于**定性示意 HDDM_W 对不同漂移形态的响应**:synth_incremental/gradual 的图显示触发点遍布整个过渡区爬坡段(相对延迟比例 ≈0.13,即过渡区前 13% 即触发),定性印证"持续爬升/平台利于 EWMA 累积";但其绝对强度不能与 INSECTS 横向比较,**不单独支撑检测器优劣**。

**(c) 数据特性实测(从原始 CSV 测得,影响指标口径)**:
- **INSECTS_gradual 的过渡区约 [32096, 34000],长度仅约 1900 样本**(主特征 f20/f21/f8 在 32k 起跳、34k 到达新平台),接近快速过渡而非典型长 gradual。因过渡区 ≈ warmup(2000),固定窗判据已天然覆盖,故 INSECTS_gradual 维持单点固定窗口径、不引入操作性 end_line。此 end_line 系按 per-feature 信号滑动均值测得的**操作性定义**,非数据生成标注(INSECTS 为真实采集数据,无客观过渡终点),论文须如实说明测法。
- **INSECTS_gradual 在约 50000 处存在 driftspec 未标注的第二次分布变化**(主特征 signal 回落)。计算"漂移后假阳性率"等指标时须注意,否则会把这第二次真实漂移误计为假阳性。

### 3.2 集群证据(待补,论文定量主表)
EXP1 同等条件(P∈{1,2,4} × pauseMode × repeats=3,只换检测器),主指标 overall_auc(BACKLOG)。**定量主表仅含 INSECTS**:

| 数据集 | 检测器 | overall_auc | n_committed | n_retrains | def_2 恢复延迟 |
|---|---|---|---|---|---|
| insects_abrupt | IKS | 0.760(基线) | _待填_ | 7 | _待填_ |
| insects_abrupt | HDDM_W (λ*) | _待填_ | _待填_ | _待填_ | _待填_ |
| insects_gradual | IKS | _待填_ | _待填_ | _待填_ | _待填_ |
| insects_gradual | HDDM_W (λ*) | _待填_ | _待填_ | _待填_ | _待填_ |

> λ* = EXP1 阶段 λ∈{0.002,0.005,0.01} 小扫的最优值(取每数据集最优,避免"参数没调好"混淆;扫描结果单列附表)。
> **预期(供证伪)**:abrupt 行 HDDM_W overall_auc 显著低于 IKS、n_committed 远低;gradual 行两者接近。若集群结果与此预期相反,以集群为准、改写论点。


## 4. 与既有 EXP4 定位的衔接

- 原 EXP4 = "HDDM_A_Windowed vs HDDM_W vs IKS 三检测器对比"。本分支对比已承担 IKS vs HDDM_W 部分。
- 调整:EXP4 主轴改为"检测器–漂移形态匹配律",HDDM_A_Windowed 作为第三个数据点纳入同一框架(它是 HDDM 家族,预期与 HDDM_W 同类——偏好 gradual;若其窗口化设计改变这一点,本身是有趣的子发现)。
- 避免与 EXP1 重复:EXP1 报同等条件单变量对比的原始数;EXP4 报跨检测器×跨漂移形态的机制解释 + 2×2(扩展)矩阵 + ε 闭式分析。

## 5. 写作风险与防线

- **离线↔部署差距**:离线矩阵只进"机制假设"小节,主张句一律引集群 overall_auc。明确写"离线为假设来源"。
- **per-version AUC 混淆**:不用 per-version 下结论,overall_auc 为主指标。
- **pauseMode 混杂**:只在 BACKLOG 内比较,USE_OLD_FOREST 作附录稳健性。
- **λ 公平性**:必须报 λ 小扫、取每检测器每数据集最优,否则"HDDM_W 弱"会被质疑为没调参。
- **gradual 定量样本量小**:可定量比较的 gradual 仅 INSECTS_gradual 1 个数据集、1 个标注漂移点。synth_gradual/incremental **不能**用于扩充定量样本(强度 8σ、系统性高估,见 §3.1b),只作定性示意。因此 gradual 侧定量结论较弱,论文须明说此局限;若需更强 gradual 定量证据,应另寻强度可比的真实数据集,而非依赖 synth。

---

*数据来源:`analysis/hddm_signal_sanity.py` 离线运行(INSECTS abrupt/gradual);集群主表待 EXP1 产出。ε 闭式见 devspec §7。*
