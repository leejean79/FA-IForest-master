# EXP1 投票协议与失效模式 — 早期发现数据(§4 / §4.3 写作材料)

> 用途:补 §4 联邦投票协议 + §4.3 两种失效模式的关键数据。这些结论来自项目早期
> (EXP1/EXP4 + 投票协议设计期,2026-06 初的对话),不在 EXP2/3/4/sensitivity 骨架中。
> **数据来源分级**(决定 VERIFY 标记):
>   [既往运行/诊断日志] — 早期 smoke test / 诊断得出,投稿前须用规范 EXP1/EXP4 记录终核。
>   [代码确认] — 已由源码/回归测试确认,可直接用。
> 出处对话:`iks`(2026-06-09,投票协议+失效模式)、`HDDM与Flink架构`(2026-06-02,342/0 WARN)。
> 原始草稿曾产出为 docs/findings.md(论文级英文表述),本文档据搜索记录重建并核对。

---

## 1. 投票协议语义(§4.2)—— [代码确认]

53/53 回归测试通过的版本,协议语义如下(与 dev 初稿一致,确认):

- **消息**:DriftReport{subtask, roundId, vote∈{INITIATE,YES,NO}}(subtask→coordinator,via drift-topic);
  DriftRoundMessage{roundId, status∈{VOTING,COMMITTED,ABORTED}, votes}(coordinator→subtask,via drift-round-topic)。
- **一轮生命周期**:检测到漂移的 subtask 发 INITIATE(roundId=0 请求分配)→ coordinator 开轮、
  广播 VOTING、发起者计为 YES → 其余 subtask 据本地状态投 YES/NO → 收齐或超时即决议。
- **quorum(commit 条件)**:`⌊P/2⌋+1`(P=4 时为 3)。决议时 |yes| ≥ ⌊P/2⌋+1 才 COMMIT,否则 ABORT。
- **早决议**:`yes+no ≥ parallelism`(全员已报)即早决议;否则 votingTimeout 强制决议。
- **幂等并发处理(v5.0 修复)**:活跃轮次内、来自尚未表态分支的 INITIATE 计为该分支对本轮的 YES
  (幂等,Set 成员去重);已投 YES 的 subtask 在收到决议前不再发新 INITIATE(votedForRoundId 机制)。
  此为协调器单点改动、detector-agnostic、不改 quorum 语义。
- **aggregation-driven retraining**:参数 aggK,default 2。
  ⚠️ **aggK 精确语义未在早期记录中明确定义**(聚合多少 committed round / 何种聚合不详)。
  正文建议维持"aggregation-driven retraining (parameter aggK, default 2)"一句带过,
  精确语义须读代码(DriftVoterFunction/Coordinator)确认后再展开,**不可猜**。

### dev 的 quorum 订正(确认正确)
dev 把早先误写的"⌊P/2⌋+1 多数"拆分为:round 在「全员已报(yes+no=P)或 votingTimeout」时决议,
|yes| ≥ ⌊P/2⌋+1 才 COMMIT 否则 ABORT。**此订正准确,与代码一致,可确认。** Algorithm 1 同此。
(细节:早决议条件严格为 yes+no ≥ parallelism,因并发 INITIATE 计 YES 后报数经幂等去重,用 ≥ 更稳;实际等价 =P。)

---

## 2. 发现一:并行稀释结构性削弱单分支灵敏度(§4.3 导言 / §1 贡献④)

**机制**:round-robin 分发使每 subtask 只见全局流 1/P。漂移表现为评分分布平移时,单分支感受幅度被稀释;
一旦小于检测器判定半径(如 HDDM Hoeffding 置信半径),漂移在该分支隐形。结构性灵敏度损失,非调参问题。

**证据(342/0 WARN)** —— [既往运行,标 VERIFY]:
- 数据集 INSECTS abrupt,**检测器 = HDDM**(注意:非 IKS,正文须注明),同数据同检测器:
  - P=1 → **342 个 WARN 样本**
  - P=4 → **0 个 WARN 样本**
- 仅并行度 1→4,漂移在每分支都测不到。
- ⚠️ VERIFY:此数来自既往运行,投稿前用规范 EXP 记录终核;正文须标明是 HDDM 检测器。

> EN: *On INSECTS-abrupt, HDDM produces 342 WARN samples at P=1 but 0 at P=4, holding data and
> detector fixed. The loss of sensitivity is a property of the sharding topology, not of detector tuning.*

---

## 3. 发现二:沿漂移强度轴的两种失效模式(§4.3 核心)

两种症状相同(最终不产新森林)但成因相反的失效,由漂移强度区分。

### 2a 弱漂移 → sub-quorum abort —— [诊断日志,标 VERIFY]

**机制**:稀释使任一投票窗口内通常只 1 个 subtask 越阈发起,余者投 NO,达不到多数流产。

**证据(13 轮)**:INSECTS abrupt,P=4,IKS:
- 全程 **13 轮投票:12 ABORTED / 1 COMMITTED**。
- 拆分:**10 轮 yes=1,no=3;2 轮 yes=2,no=2(仍 <quorum 3);1 轮 yes=3 → COMMITTED**。
- 约 **92%** 投票轮因稀释凑不齐多数(quorum=⌊P/2⌋+1=3)。
- ⚠️ VERIFY:来自诊断日志,投稿前用规范 EXP 记录终核。

> EN: *On INSECTS-abrupt with P=4, the system opened 13 rounds of which 12 aborted (ten at yes=1,
> two at yes=2) and only one reached the quorum of 3 and committed — a ~92% quorum-failure rate
> driven by per-branch dilution rather than absence of drift.*

### 2b 强漂移 → 发起竞争(initiate race)—— [诊断日志,标 VERIFY]

**机制**:强漂移下所有分支几乎同时越阈、同发 INITIATE。原协议只接受第一个开轮,其余被丢弃、
记为弃权 → 一致检测却零通过。

**证据(synth-abrupt 修复演示)**:synth abrupt,P=4,IKS:
- **修复前**:4 个 subtask 在 ~100ms 内全部 STABLE→DRIFT 并发 INITIATE;第一个开轮,
  其余 3 个被丢弃 → 该轮 **yes=1, no=0, abstain=3 → ABORTED**(一致检测却零通过)。
- **修复后**(并发 INITIATE 计 YES):**yes=4 → COMMITTED**。
- ⚠️ VERIFY:来自诊断日志;且 synth 数据集当时有 normal_mu_after 撞 anomaly_mu 的生成 bug
  (后修为 8.0),若重跑须用修复后的 synth。本演示仅示协议修复效果(yes 票数变化),不依赖数据质量。

> EN: *On synth-abrupt with P=4, all four workers reported DRIFT within ~100 ms, yet the round
> resolved yes=1, no=0, abstain=3 (ABORTED) prior to the fix; after the fix it resolves yes=4 (COMMITTED).*

### 发现三:检测器 WARN 阶段决定 2b 是否暴露 —— [机制分析,可用]

- 有 WARN 的检测器(HDDM 族):DRIFT 前经 WARN,各分支到达 DRIFT 时刻被 WARN 期自然错峰,
  非发起分支收到投票时多在 WARN/STABLE,走正常 YES → WARN 错峰偶然掩盖了发起竞争。
- 无 WARN 的同步检测器(IKS,纯 STABLE/DRIFT):无错峰,强漂移下各分支同刻直达 DRIFT、同刻发起
  → 暴露 2b 这一潜在协议缺口。
- 揭示"检测器状态机设计 × 联邦投票鲁棒性"此前未被注意的耦合(可作小贡献点)。

> EN: *Detectors with a WARN phase impose incidental temporal staggering that inadvertently masks
> the initiate race; a synchronous detector with no WARN phase (IKS) removes this staggering and
> exposes the latent protocol gap — a previously-unremarked coupling between detector state-machine
> design and federated-voting robustness.*

---

## 4. quorum 作为灵敏度旋钮(§4.3 / 讨论)—— [机制分析]

模式 2a 无法靠协议改写消除(是发现一的直接后果)。quorum 阈值 `⌊P/2⌋+1` 是"灵敏度 vs 误提交"
核心调节量:越高弱漂移越难提交(更多 2a abort),越低越易提交但越易噪声误触发。
适合作实验扫描维度,刻画稀释下 quorum 成功率/误报率前沿。

---

## 5. 写作注意(诚实性边界)
- **342/0 WARN 是 HDDM**,13 轮/synth 修复是 IKS——正文勿混检测器。
- 所有数标 [既往运行/诊断日志] 的,VERIFY 保留,以规范 EXP1/EXP4 记录终核确切值;若有出入以规范记录为准。
- aggK 精确语义未确认,正文一句带过,勿猜。
- synth 数据曾有生成 bug(normal_mu_after=anomaly_mu),2b 修复演示只示协议效果(yes 票变化),
  不作 synth 的准确性结论。
- 发现一/二/三是 novelty 核心(并行稀释 + 两失效模式 + 检测器耦合),§4.3 与 §1 贡献须突出。

---

*重建自早期对话(iks 2026-06-09 / HDDM架构 2026-06-02)的 findings 草稿 + 搜索记录核对。
原 docs/findings.md 若在仓库/早期 outputs,以那份为准并交叉核对本文档。*
