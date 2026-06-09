# README 更新补丁(v3.4 → v5.0)

> 外科手术式更新:只列出需要新增/替换的段落,逐处给出「定位 + 改成什么」。
> 不整篇重写(避免丢失未涉及章节)。逐条应用到 `README.md` 即可。

---

## 补丁 1 — 版本迭代表(§2 Version History):在 v3.4 行后追加

```markdown
| v3.4.1–v3.4.7 | ✅ 完成 | 投票协议健壮性修复(`votedForRoundId` 防同一 subtask 重复投票等);**v3.4.7**:COOLDOWN 期改用 z-score 自适应写池(`threshold = mean + k·std`),STABLE/WARN 期**不再写环形缓冲**(v2 训练池由 COOLDOWN 过滤样本独占,避免旧概念稀释);写入计数与 cooldownN 解耦 + 双终止条件 |
| v4.0 | ✅ 完成 | **HDDM_W 检测器**(Frías-Blanco 2015,单样本 EWMA Hoeffding 界,ε=√(λ·ln(1/α)/(2(2−λ))));EWMA 在数学上替代显式 RingBuffer |
| v5.0 | ✅ 完成 | **IKS 检测器**(Incremental KS,dos Reis 2016;IKSSW 固定 reference + 等长滑动 current,内嵌 Treap;纯 STABLE/DRIFT、无 WARN 阶段)。**投票协议同步检测修复**:活跃轮次内收到的独立 INITIATE 计为该 subtask 的 YES——修复无 WARN 检测器下多 subtask 几乎同时 INITIATE 导致「全检测到却投票流产」的缺口 |
```

---

## 补丁 2 — 新增小节「漂移检测器(Drift Detectors)」(放在 §4 核心组件内,评分公式前)

```markdown
### 漂移检测器(可插拔,`--detector` 选择)

所有检测器实现统一接口 `DriftDetector`(`update(score) → STABLE/WARN/DRIFT`、`reset()`、`sampleCount()`、`warnTimedOut()`),在异常分数序列上做在线漂移检测:

| detector | 来源 | 特点 |
|---|---|---|
| `HDDM_A` | Frías-Blanco 2015 | 累积 Hoeffding 界,A-test |
| `HDDM_A_Windowed`(默认) | 同上 + 滑窗 | 滑动窗口版,对近期变化更敏感 |
| `HDDM_W` | Frías-Blanco 2015 | 单样本 EWMA Hoeffding 界;无显式缓冲 |
| `IKS` | dos Reis 2016 | 增量 KS 检验(IKSSW):固定 reference + 等长滑动 current 两窗,内嵌 Treap O(log W) 维护 KS 统计量;**纯 STABLE/DRIFT,无 WARN 阶段** |

> **无 WARN 检测器(如 IKS)的状态流**:`STABLE → LOCAL_DRIFT_REPORTED`(直达,跳过 WARN);`warnTimeoutBehavior`(DISCARD/PROMOTE)对这类检测器为空操作。
```

---

## 补丁 3 — Phase C 子状态机表(§4):替换 STABLE / WARN 两行(v3.4.7 已变)

把原表里 STABLE / WARN 两行的「环形缓冲概率写入 / 严格写入」描述**替换**为:

```markdown
| **STABLE** | 打分 + 检测器 update(v3.4.7 起**不写环形缓冲**)|
| **WARN** | 打分 + 检测器 update(v3.4.7 起**不写环形缓冲**;无 WARN 的检测器不进入此态)|
```

COOLDOWN 行保持(已是 v3.4.7 的 z-score 写池逻辑)。子状态机箭头图加一条旁注:

```markdown
> 注:无 WARN 检测器(IKS)走 STABLE → LOCAL_DRIFT_REPORTED 直达路径。
```

---

## 补丁 4 — 联邦漂移投票协议(§4):投票规则补一条 v5.0

在「所有 subtask 投票 YES/NO」那段后追加:

```markdown
**v5.0 同步检测修复**:若某 subtask 在某轮**已活跃**时才上报 INITIATE(它独立检测到同一漂移,但没抢到发起权),协调器将其 INITIATE 计为该 subtask 对当前轮的 **YES**(幂等,已投票则不重复)。
此前这类 INITIATE 被直接丢弃 → 在无 WARN 的同步检测下(多 subtask ~同时越线),除发起者外全被记为弃权,导致「四个都检测到却 ABORTED」。
```

---

## 补丁 5 — 快速启动 LocalProcessor 参数表(§6.4):追加三行

```markdown
| `--detector` | `HDDM_A_Windowed` | 漂移检测器:`HDDM_A` / `HDDM_A_Windowed` / `HDDM_W` / `IKS` |
| `--hddmLambda` | `0.1` | v4.0:HDDM_W 的 EWMA 遗忘因子 λ |
| `--iksWindowSize` | `2000` | v5.0:IKS reference/current 窗口大小 W |
| `--iksPValue` | `0.001` | v5.0:IKS KS 检验 p-value(内部派生 ca=√(−0.5·ln p))|
```

(原表已有 `--detector`/`--hddmWindowSize`/`--cooldownSamples`/`--zThresholdK` 等,补齐缺的即可;`--pNormalStable`/`--pNormalWarn` 若代码已随 v3.4.7 移除,顺手从表里删掉。)

---

## 补丁 6(可选)— 填充 CLAUDE.md 空着的「## Architecture Notes」

```markdown
## Architecture Notes

两个 Flink 作业经 Kafka 解耦:
- **LocalProcessor**(parallelism=N):Phase B 冷启动训 iTree → Phase A 消化积压 → Phase C 实时打分 + 漂移检测三阶段状态机。
- **CoordinatorJob**(parallelism=1):`CoordinatorFunction` 聚合 iTree 成全局森林;`DriftVoterFunction` 管理联邦漂移投票(多数决 ≥ N/2+1)。

漂移响应链:subtask 检测到 DRIFT → INITIATE → 协调器发起投票轮 → 多数通过 COMMITTED → 各 subtask 进 COOLDOWN 采集新概念样本(z-score 写池)→ 重训 iTree → 协调器组装并原子发布新森林版本。

可插拔检测器:HDDM_A / HDDM_A_Windowed / HDDM_W / IKS(见 README §4)。IKS 无 WARN 阶段,走 STABLE→DRIFT 直达路径。
```

---

## 没有写进文档的(刻意排除)

- **synth_* 数据集漂移后 AUC 偏低**:那是 driftspec 的 `normal_mu_after` 撞上 `anomaly_mu` 的生成 bug(待改成 8.0 重新生成),是**待修的数据问题**、不是项目特性,不进 README。
- **「并行稀释削弱漂移灵敏度」/ 两种联邦失效模式**:这是论文级研究发现,建议放进论文或单独的 `docs/findings.md`,而非项目 README。如果你想要,我可以单独起一份。

---

## 说明

以上是针对我能看到的 README 章节做的定向补丁。若你希望我直接产出**整篇合并后的 `README.md`**(而不是补丁),把当前完整的 `README.md` 贴给我,我基于它合并、保证不丢未涉及的章节(如 §3 项目结构、§6.5 之后的 topic 表等)。
