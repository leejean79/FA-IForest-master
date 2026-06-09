# Findings:并行稀释与联邦漂移投票的两种失效模式

> 本文档汇总 FA-iForest 在分布式漂移检测 / 联邦投票层的核心研究发现,语言尽量贴近论文表述。
> 每条发现附「论文表述(EN)」可直接引用的英文句子。
> ⚠️ 文中数字来自实验日志与既往运行记录;投稿前请用规范实验记录(EXP1/EXP4)复核确切数值。

---

## 0. 系统设定(Setup)

FA-iForest 将异常评分流 round-robin 均匀分发到 `P` 个并行 subtask;每个 subtask 在其本地分支的评分序列上独立运行一个可插拔漂移检测器(`DriftDetector`)。检测到漂移的 subtask 不立即重训,而是向中央协调器发起一轮联邦投票;**多数通过**(法定人数 `⌊P/2⌋+1`)才触发全局森林重训与原子换版。本文所有发现均在此设定下(默认 `P=4`,法定人数 `=3`)。

---

## 1. 发现一:并行化在结构上削弱单分支漂移灵敏度

**机制。** round-robin 分发使每个 subtask 只见到全局流的 `1/P`。当漂移表现为评分分布的整体平移时,**单分支感受到的漂移幅度被稀释**;一旦该幅度小于检测器的判定半径(如 HDDM 的 Hoeffding 置信半径),单分支便无法越过阈值,漂移在该分支上「隐形」。这不是参数没调好,而是**数据分发拓扑带来的结构性灵敏度损失**——并行度越高,单分支信号越稀。

**证据。** 在 INSECTS abrupt 上,HDDM 在 `P=1` 时产生 342 个 WARN 样本,而在 `P=4` 时降为 **0** 个 WARN——同一数据、同一检测器,仅因并行度从 1 升到 4,漂移在每个分支上都测不到了。

> **论文表述(EN).** *Round-robin sharding across `P` workers structurally attenuates per-worker drift sensitivity: each worker observes only `1/P` of the stream, so a distribution shift of fixed global magnitude induces a smaller per-branch shift. When this per-branch magnitude falls below the detector's decision radius, the drift becomes locally undetectable. On INSECTS-abrupt, HDDM produces 342 WARN samples at `P=1` but 0 at `P=4`, holding data and detector fixed. The loss of sensitivity is a property of the sharding topology, not of detector tuning.*

---

## 2. 发现二:联邦投票沿「漂移强度」轴呈现两种相反的失效模式

把发现一放到投票层,会得到两个症状相同(**最终不产生新森林**)但成因相反的失效模式。两者由漂移强度区分。

### 2a. 弱漂移 → 法定人数不足(sub-quorum abort)

弱/真实漂移下,稀释使得在任一投票窗口内**通常只有 1 个 subtask** 越过判定阈值并发起投票,其余分支信号不足、投 NO,该轮因达不到多数而流产。

**证据(INSECTS abrupt,`P=4`,IKS)。** 全程发起 13 轮投票:**12 轮 ABORTED、1 轮 COMMITTED**。其中 10 轮为 `yes=1, no=3`、2 轮为 `yes=2, no=2`(仍不足法定人数 3),仅 1 轮恰好 3 个分支同窗对齐(`yes=3`)→ COMMITTED。即:**~92% 的投票轮因稀释无法凑齐多数票。**

> **论文表述(EN).** *Under weak drift, sharding-induced dilution means that within any voting round typically only a single worker crosses its detection threshold; the remaining workers vote NO and the round aborts for lack of quorum. On INSECTS-abrupt with `P=4`, the system opened 13 rounds of which 12 aborted (ten at `yes=1`, two at `yes=2`) and only one reached the `⌈P/2⌉+1=3` quorum and committed — a ~92% quorum-failure rate driven by per-branch dilution rather than by absence of drift.*

### 2b. 强漂移 → 发起竞争(synchronized-detection initiate race)

强漂移(如 abrupt)下,所有分支几乎**同时**越过阈值并同时发起投票。原协议只接受第一个发起者开轮,其余同时到达的 INITIATE 被丢弃;这些分支随后处于「已上报」状态、不再投票,于是被记为**弃权**——结果是**所有分支都检测到了漂移,投票却因弃权而流产**。

**证据(synth abrupt,`P=4`,IKS,修复前)。** 4 个 subtask 在约 100 ms 内全部 STABLE→DRIFT 并发起 INITIATE;第一个开轮成功,其余 3 个被丢弃 → 该轮 `yes=1, no=0, abstain=3` → ABORTED。即**一致检测却零通过**。

> **论文表述(EN).** *Under strong (abrupt) drift, all workers cross threshold near-simultaneously and issue INITIATE concurrently. A naive coordinator admits only the first as the round opener and discards the rest; those workers then await a decision without casting a vote and are tallied as abstentions, so a unanimously-detected drift can fail to commit. On synth-abrupt with `P=4`, all four workers reported DRIFT within ~100 ms, yet the round resolved `yes=1, no=0, abstain=3` (ABORTED) prior to the fix.*

---

## 3. 发现三:检测器是否有 WARN 阶段,决定模式 2b 是否暴露

模式 2b 是否触发,取决于检测器的状态机设计:

- **有 WARN 阶段的检测器(HDDM 族)** 在 DRIFT 前先经历 WARN,各分支到达 DRIFT 的时刻被 WARN 期**自然错峰**;非发起分支收到投票广播时多半仍在 WARN/STABLE,走正常 YES 投票路径——**WARN 错峰偶然地掩盖了发起竞争**。
- **同步检测器(IKS,纯 STABLE/DRIFT、无 WARN)** 没有错峰缓冲,强漂移下各分支几乎同刻直达 DRIFT、同刻发起——**把这一潜在协议缺口暴露出来**。

也就是说:模式 2b 是协议层一直存在的潜在缺口,只是被 HDDM 的 WARN 时序偶然遮住;引入无 WARN 的同步检测器(IKS)才使其显形。这揭示了**检测器状态机设计与联邦投票协议鲁棒性之间此前未被注意的耦合**。

> **论文表述(EN).** *Whether failure mode 2b manifests depends on the detector's state machine. Detectors with a WARN phase (the HDDM family) impose incidental temporal staggering on the per-worker STABLE→DRIFT transitions, so non-initiating workers are usually still in WARN/STABLE when the vote arrives and follow the normal YES path — the WARN phase inadvertently masks the initiate race. A synchronous detector with no WARN phase (IKS, pure STABLE/DRIFT) removes this staggering and exposes the latent protocol gap. This reveals a previously-unremarked coupling between detector state-machine design and federated-voting robustness.*

---

## 4. 修复(针对模式 2b)与设计权衡

**修复。** 协调器收到「活跃轮次内、来自尚未表态分支的 INITIATE」时,不再丢弃,而是将其计为该分支对当前轮的 **YES**(幂等)。独立发起 = 对同一漂移最强的同意信号。修复后,synth-abrupt 同一场景由 `yes=1, abstain=3`(ABORTED)变为 `yes=4`(COMMITTED)。此修复在协调器单点完成,对所有检测器通用,不改变法定人数语义。

> **论文表述(EN).** *We close mode 2b by treating an INITIATE that arrives during an active round, from a worker that has not yet voted, as that worker's YES for the round (idempotent): an independent detection is the strongest possible agreement signal. The same synth-abrupt scenario then resolves `yes=4` (COMMITTED) instead of `yes=1, abstain=3` (ABORTED). The fix is a single coordinator-side change, detector-agnostic, and leaves the quorum rule unchanged.*

**法定人数即灵敏度旋钮(对模式 2a)。** 模式 2a 无法靠协议改写消除——它是发现一的直接后果。法定人数 `⌊P/2⌋+1` 是「灵敏度 vs 误提交」的核心调节量:阈值越高,弱漂移越难提交(更多 2a 流产);越低越易提交,但越易被噪声误触发。该阈值适合作为实验扫描维度,刻画稀释下的 quorum 成功率 / 误报率前沿。

> **论文表述(EN).** *Mode 2a cannot be removed by protocol changes — it is a direct consequence of Finding 1. The quorum threshold `⌊P/2⌋+1` is the governing sensitivity knob: a higher threshold makes weak drift harder to commit (more 2a aborts), a lower one eases commitment at the cost of noise-triggered false commits. We treat this threshold as an experimental sweep dimension characterising the quorum-success / false-commit frontier under dilution.*

---

## 5. 小结与对实验的指引

- 两种失效模式沿漂移强度轴互补:**弱漂移 → 2a(凑不齐多数)**,**强漂移 → 2b(发起竞争,已修)**;两者都表现为「不产新森林」。
- 模式 2b 的暴露依赖检测器无 WARN(IKS),为「检测器设计 × 投票鲁棒性」耦合提供了实例。
- EXP1/EXP4 建议报告**每轮 yes 票分布 / quorum 成功率**,并对不同 `P` 与法定人数做扫描,直接量化发现一与发现二。
- 模式 2a 的**代价**取决于漂移对模型质量的损害程度:在 INSECTS 这类漂移仅小幅扰动可分性的数据上,漏掉的 commit 代价小;在漂移显著恶化模型的数据上则代价大——这点宜在论文中按数据集区分讨论。

---

*文档状态:草稿。数字以规范实验记录为准;发现一的 342/0 WARN 来自既往运行,发现二的 13 轮 / 100ms 来自本次诊断日志,投稿前请复核。*
