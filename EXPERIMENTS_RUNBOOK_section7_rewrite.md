# 实验执行顺序(最终编排)— 替换 EXPERIMENTS_RUNBOOK.md §7

> 收口编排:两检测器(per-feature IKS / HDDM_W,分支级)、INSECTS_abrupt + INSECTS_gradual 为主。
> 总策略:**λ 扫描 → EXP1 选胜者 → EXP2/3 → EXP4 汇总 → 论文写作**。
> 脚本接口不变(`run-batch.sh --plan`、`run-experiment.sh --extra-param`、master 自治 tmux、断点续跑、`analyze-all.sh`)。

## 规模总览(经 experiment-configs.yml 展开)

| 阶段 | plan | 分支 | 规模 | 说明 |
|---|---|---|---|---|
| 0 冒烟 | `smoke_batch` | 任一 | 2 | 调度/清理/续跑验证 |
| 1 λ 扫描 | `lambda_sweep` | **hddm_w** | 18 | 2 ds × 3 λ × BACKLOG × P4 × 3 |
| 2 EXP1-HDDM | `exp1_hddm_w` | **hddm_w** | 36 | 2 ds × 2 pause × P{1,2,4} × 3 |
| 3 EXP1-IKS | `exp1_iks` | **iks** | 36 | 同网格,IKS 基线 |
| 4 EXP2 | `exp2` | 主检测器 | 150 | 5 stationary × 30 shuffle |
| 5 EXP3 | `exp3` | 主检测器 | 24 | 2 ds × P{1,2,4,6} × 3 |
| 6 EXP4 | — | — | 0 | 取 EXP1 两臂结果交叉分析,无新增运行 |

总集群运行约 266 个实验(EXP4 不增量)。

## 依赖关系(关键)

- **λ 扫描是 EXP1 的前置,不是补充**。EXP1 的 HDDM_W 臂必须用 λ 扫描选出的各数据集 λ\* 参赛,否则与 IKS 比较时混入"参数没调好"的偏差(EXP4 论证有效性要求,见 EXP4 骨架 §3.2、§5)。
- **每数据集各取最优 λ\***:abrupt 与 gradual 的 λ\* 可能不同(离线显示 gradual 偏好更小 λ)。若不同,`exp1_hddm_w` 拆成两次单数据集运行,各填对应 λ;论文需说明"HDDM_W 最优 λ 依赖漂移形态"。
- **主检测器 = EXP1 两臂 overall_auc(BACKLOG)胜者**。EXP2/3 只用主检测器所在分支跑。

## 执行步骤

```bash
ssh fa-master
cd /opt/fa-iforest/repo

# ---- 0. 冒烟(任一分支,先验调度机制)----
git checkout feature/per-feature-hddm_w   # 部署用 scp,非 git;此处示意分支语义
tmux new -s smoke
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan smoke_batch 2>&1 | tee smoke.out
# Ctrl+B D 脱离

# ================= HDDM_W 分支 =================
# 确保部署的是 per-feature-hddm_w 分支构建(含 PerFeatureHDDMFunction)

# ---- 1. λ 扫描(18 实验)----
tmux new -s lambda
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan lambda_sweep 2>&1 | tee lambda.out
# Ctrl+B D 脱离
# 跑完拉结果分析,从 overall_auc 选各数据集 λ*,回填 experiment-configs.yml 的
# exp1_hddm_w.plan_extras.hddmLambda(若两 ds 的 λ* 不同,见下方拆分运行)

# ---- 2. EXP1 HDDM_W 臂(36 实验)----
# 情形 A:两数据集 λ* 相同 → 直接跑
tmux new -s exp1_hddm
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1_hddm_w 2>&1 | tee exp1_hddm.out
# 情形 B:两数据集 λ* 不同 → 单跑(用 run-experiment.sh 逐数据集,或临时拆 plan)
#   每数据集 × 2 pauseMode × P{1,2,4} × 3,各填自己的 hddmLambda

# ================= IKS 分支 =================
# 切换部署为 per-feature-iks 分支构建

# ---- 3. EXP1 IKS 臂(36 实验)----
tmux new -s exp1_iks
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp1_iks 2>&1 | tee exp1_iks.out

# ---- 比较两臂,定主检测器 ----
# 拉两臂结果,比 INSECTS_abrupt/gradual × BACKLOG 的 overall_auc 中位。
# 胜者为主检测器,其分支构建用于 EXP2/3。

# ================= 主检测器分支 =================
# ---- 4. EXP2 stationary 误触发(150 实验)----
tmux new -s exp2
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp2 2>&1 | tee exp2.out

# ---- 5. EXP3 扩展性(24 实验,Fork 2 需先改 .env SOURCE_PARTITIONS + clean-topics)----
tmux new -s exp3
RUN_MODE=local bash deploy/scripts/run-batch.sh --plan exp3 2>&1 | tee exp3.out

# ================= 拉结果分析(mac)=================
bash deploy/scripts/pull-results.sh
bash analysis/analyze-all.sh
```

## 各阶段产出与论文映射

- **λ 扫描** → HDDM_W 最优 λ\*(每数据集),λ 敏感性曲线作论文附表;"最优 λ 依赖漂移形态"作发现。
- **EXP1 两臂** → 论文定量主表(EXP4 §3.2):2 检测器 × 2 数据集 × overall_auc / n_committed / n_retrains / 恢复延迟 def_1/2/3 / 漂移后假阳性率 def_3。主指标 overall_auc(BACKLOG)。
- **EXP4 交叉分析** → 检测器–漂移形态匹配律(无新增运行):IKS vs HDDM_W 在 abrupt(脉冲)vs gradual(平台)上的分裂 + 离线 ε 下界分析(devspec §7)+ synth 定性形态示意(离线 hddm_signal_batch.py)。
- **EXP2** → stationary 误触发率,主检测器精度基线。
- **EXP3** → 分布式扩展性 / 并行稀释(per-feature 消稀释为研究发现)。

## 防呆(改 yaml 后必做,沿用 §7.1,核对参数随分支变)

```bash
# 1. 本地 yaml 解析
python3 -c "import yaml; yaml.safe_load(open('deploy/experiment-configs.yml'))" && echo OK
# 1.5 scp 到 master
scp deploy/experiment-configs.yml fa-master:/opt/fa-iforest/repo/deploy/
# 2. dry-run 核对展开行数 + 每行 extra
ssh fa-master "cd /opt/fa-iforest/repo && bash deploy/scripts/run-batch.sh --plan <plan> --dry-run | head -20"
# 3. 单跑 1 次核对启动信息输出(banner)——按分支核对对应参数:
#   HDDM_W 分支: LocalProcessor 必须打 HDDM lambda / HDDM scaleMode(p99)/ HDDM warmup
#   IKS 分支:    LocalProcessor 必须打 IKS window size W: 2000 / IKS pValue: 0.001
#   两分支:     CoordinatorJob 必须打 Aggregator k: 2
#   docker logs jobmanager 2>&1 | grep -E 'HDDM lambda|HDDM scaleMode|IKS window|Aggregator k'
# 都不能是静默兜底默认值。
```

## EXP1 离线重建三观测项(沿用,拉回后从 jsonl 重建)

- 恢复曲线:`scores.jsonl`(seq, score, label, forestVersion)滑窗 AUC + forestVersion 切换线。
- v1/v2 森林:版本切换前后同一 eval 窗按 forestVersion 分组算 AUC。
- 重训频率:`model-topic.jsonl` 的 distinct version 数 / run。

> `analyze.py` 的 drift mode 已支持多漂移点逐点 def_1/2/3;HDDM_W 与 IKS 两臂用同一分析口径,保证对比公平。

---

*与原 §7 的差异:① 数据集主线收窄为 INSECTS_abrupt+gradual(synth 仅冒烟/定性,移出 exp1 定量);② 检测器从"IKS 主 + HDDM 可选"改为两臂并列、EXP1 选胜者;③ 新增 λ 扫描作 EXP1 前置;④ sensitivity 段并入 λ 扫描(HDDM_W 核心变量),原 IKS 参数 OAT 扫描如需保留可另跑;⑤ EXP4 不单独运行,取 EXP1 两臂交叉分析。*
