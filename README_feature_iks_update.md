# README 更新 — `feature/iks` 分支(方向二(a))

> 应用方式:(1) 在 §2 版本迭代表末尾加下面那一行;(2) 把"新增章节"整段插入(建议放 §2 之后,作为 §3,后续章节顺延);(3) §1 数据流第 5 步在本分支被检测面取代,可加一句脚注指向新章节。
> 风格沿用 README 现有双语 + 紧凑表格。

---

## (1) 版本迭代表加一行

```markdown
| feature/iks (方向二a) | 🚧 进行中 | **检测轴 行→列**:per-feature IKS 列并行检测消除并行稀释;两平面解耦(检测/打分),聚合替代投票;de-risk proxy 成立;EXP1 synth 端到端通过、pauseMode×P 量化;COOLDOWN EOF 挂死与 dump 截断修复 spec 已出 |
```

---

## (2) 新增章节

## 3. `feature/iks` 分支:方向二(a) — 列并行 per-feature 漂移检测

### 3.1 动机:并行稀释 (Parallelism Dilution)

v3.4 的检测在 **iForest 得分域 + 行并行**:数据 round-robin 分到 N 个 subtask,每个只见 1/N 行,漂移信号被稀释。实测 INSECTS abrupt:HDDM WARN 数 **p=1 → 342、p=4 → 0**,检测随并行度坍塌。
→ **这是研究发现,不是 bug**:行并行天然稀释弱漂移信号;p=4、弱真实漂移时 quorum 罕见。

### 3.2 转向:检测轴 行 → 列 (Detection axis: row → column)

把检测从"行并行、得分域"改为"**列并行、原始特征域**":每个特征一路 IKS(Incremental KS),`keyBy(featureId)`,每路看**全部行**(与并行度无关)→ 检测 P-不变。

- **两平面解耦**:检测面(per-feature IKS,仅传感)+ 打分/重训面(行并行,保留完整特征向量,**整条不变**)。单 Kafka source fork 成两路,偏斜由 Flink 背压有界。
- **聚合替代投票**:k≥2 个特征共发 → 合成 COMMITTED → 复用既有 `feature-drift-topic →(聚合)→ drift-round-topic → handleVoteCommitted → COOLDOWN → 重训 → 组装 → 广播`(下游整条不变);COOLDOWN/WAITING 充当天然 refractory。
- 参数:IKSSW `W=2000, p=0.001, ca≈1.858`;topic `drift-topic → feature-drift-topic`;source 分区=1(Fork 1,保 per-feature 顺序)。
- 定位:**消稀释来自轴改**(每路见全部行),**分布只买扩展性**——两者作独立论点。

### 3.3 收获 / 关键发现 (Findings)

| 主题 | 发现 |
|------|------|
| 消稀释机理 | 轴改(行→列)消除稀释,每路 IKS 见全部行;分布与消稀释解耦 |
| 两种联邦失效模式 | 沿漂移强度轴:**强漂移** → 近同时 INITIATE 致投票坍塌(并发 INITIATE 计 YES 修复);**弱漂移** → 分支信号低于阈值、quorum 罕见 |
| de-risk 裁决 | per-feature IKS vs 冻结森林 GT:标注漂移召回 **100%**(INSECTS 5/5);proxy 成立,不加联合确认 |
| 离线精度=悲观下界 | INSECTS 离线 precision ~0.12;部署的 COOLDOWN/WAITING 是真实 refractory,远强于离线测量 |
| **pauseMode × P 权衡**(EXP1) | BACKLOG:overall AUC ~0.977、恢复 ~2200,**P-不变**;USE_OLD:0.954→0.893、恢复 4600→10600,**随 P 退化**(WAITING 期旧森林打分随 P 拉长) |
| 重训到位(synth) | 修好 synth(`mu_after=8`)上 deployed `post_final≈1.0`=可分上限 → stale-score COOLDOWN 池在易数据上不拖后腿 |
| 合成数据陷阱 | 原 synth `NORMAL_MU_AFTER=4.0` 撞 `ANOMALY_MU=4.0` → 漂移后不可分(上限 0.80);修为 8.0 后上限恢复 1.0 |

### 3.4 已知问题 / 待办 (Known Issues & TODO)

| 问题 | 现状 |
|------|------|
| **COOLDOWN EOF 挂死(P-依赖)** | 距流末 < ~(ringBuffer 填充 × P) 条时触发的漂移 → COOLDOWN 凑不满 cN/ringBuffer → 不重训 → job 挂。日志确诊(round 15 @ seq 209313, p4)。修复 spec 已出(ABORT + dump 容差);属有界流 artifact |
| dump 截断 | dump 用空闲超时,管线卡死段无新消息 → 早退,确定性截断 ~124k/212k。改 `--max-messages` + 完成检查容差(数据未丢,topic 完整) |
| INSECTS 低精度的运营代价 | 过触发不只浪费——末段伪 COMMITTED 直接挂死 job。治本 = 峰值-KS 精度门(`ksConfirm` 待标定) |
| 消稀释 headline 未坐实 | `n_retrains` 被重训周期节流(随 P 变),非纯检测度量;需 `n_committed`(数 drift-round-topic) + 旧臂对照 |
| 重训池 stale-score 选样 | COOLDOWN 池筛选仍用**旧森林**得分做 z-score 门(独立 "重训质量" workstream,本期未碰)。synth 已清,INSECTS 待严格"同窗 v1/v2"(离线反序列化 model-topic 森林重打分) |

### 3.5 当前状态 (Status)

- **架构规格**:Phase 3 落地规格完成(Java 架构 + EXP1 观测项 + 脚本/topic + 分析口径,四附录)。
- **de-risk**:Phase 0/1/1.5 完成,proxy 成立。
- **EXP1**:synth 端到端通过 + Q3-on-synth 清(post_final≈1.0)+ pauseMode×P 量化;`insects×p4×BACKLOG` 待 Fix B 重跑;消稀释 headline 待 `n_committed`;Q3-on-INSECTS 待严格 v1/v2。
- **修复**:COOLDOWN EOF + dump 截断修复 spec 已出,待 dev 落地。

---

## (3) §1 数据流脚注(可选)

§1 第 5–7 步描述的是 main(v3.4)的"得分域检测 + 联邦投票"。**在 `feature/iks` 分支,第 5 步被检测面取代**:per-feature IKS 列并行检测 → 聚合器 k≥2 共发 → 合成 COMMITTED;第 6–7 步(COOLDOWN/重训/打分输出)不变。详见 §3。
