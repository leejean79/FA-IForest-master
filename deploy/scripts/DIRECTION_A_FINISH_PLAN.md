# 方向 A 收尾路线图

> 目标:补完 EXP3(吞吐 + 延迟)+ sensitivity 附录,清掉 dev 待办,进入论文撰写。
> 主分支 feature/per-feature-hddm_w。协作 A 模式(仓库写入由 dev,planning 出规格/diff)。

---

## 关键技术结论(本次查明,简化了延迟方案)

- pauseMode(USE_OLD_FOREST / BACKLOG)只控漂移期打分行为,**与"实时摄入 vs 堆积"无关**。
- "是否堆积 backlog"取决于 **producer 速率(JOB_LOAD_RATE)vs Flink 处理能力**:
  - rate < 处理能力 → 不堆积 → scoreTime−ingestionTime = 干净处理延迟。
  - 证据:限速 1000 的 donors P2 run,raw_lat_median = 49ms(未堆积,即真实处理延迟)。
- **故延迟测量无需开 Flink latency tracking、无需改 Java**:用适中 rate(不堆积)的 run +
  scoreTime−ingestionTime 即可。吞吐与延迟分两组 rate 测(目标矛盾):
  - 吞吐 run:JOB_LOAD_RATE 高(50000),压满系统,测最大吞吐(此时堆积、延迟失真但吞吐准)。
  - 延迟 run:JOB_LOAD_RATE 适中(如 2000,< 处理能力),不堆积,测干净延迟。

---

## 任务 1 — EXP3 吞吐(三口径)

### 配置
- JOB_LOAD_RATE 提高(.env:50000),解 producer 限速(已验证 source 解锁到 ~7270)。
- source 保持 1 分区(SOURCE_PARTITIONS=1,保时序);output-scores 分区 ≥ P(P=6 需 --score-partitions 6)。
- run:donors/http × P{1,2,4,6} × 3,detector=IKS(default),BACKLOG。
  全局 parallelism=P + extra-param detectionParallelism=P(检测/打分面都并行,source 单线)。

### 分析
`exp3_throughput_prom.py --prometheus http://localhost:9090 --job-id <id> --auto-window --parallelism P`
- 三口径:source_ingress(端到端,~7000 单分区上限)/ detection(~×10 特征展开)/ scoring。
- 在 master 上跑(Prometheus 在 master);operator_name 已校准(下划线名)。

### 预期叙事(诚实)
端到端吞吐 ~7000 rps 受 source 单分区(保时序)结构性限制,**不随并行度扩展**——
"高吞吐与时序保证不可兼得"的工程权衡;检测算子吞吐 ~72000 证明算子处理能力。
多分区扩展作 future work(方向 B 探索过,实现复杂,留待后续)。

---

## 任务 2 — EXP3 延迟(scoreTime−ingestionTime,不堆积)

### 配置
- JOB_LOAD_RATE 适中(.env 临时调,如 2000;须 < Flink 处理能力以不堆积)。
- run:donors/http × P{1,2,4,6} × 3(或与吞吐 run 数据集一致),detector=IKS,BACKLOG。
- 其余同任务 1。

### 分析(用 exp3_scalability_from_scores.py,已有 raw_lat / gap)
- 延迟 = raw_lat(scoreTime−ingestionTime)median/p95,随并行度 P 的变化。
- **验证不堆积**:raw_lat 须平稳、不随 seq 单调增长(堆积则递增)。建议给脚本加"延迟随时间
  漂移检测"(分前后半段 raw_lat,差异大=堆积,该 run 作废重调低 rate)。
- 预期:P 越大、并行分担越多 → 延迟越低,这是"分布式低延迟"的证据(EXP3 扩展主线)。

### planning 待做
给 exp3_scalability_from_scores.py 加"堆积检测"(前后半段 raw_lat 对比)——本对话可补。

---

## 任务 3 — sensitivity 附录

### IKS 参数稳健性(sensitivity_iks plan)
- 先应用修复:plan_extras 去掉重复的 iksWindowSize(见 FIX_cfg_query_sensitivity_plan_extras.md)。
- 扫 iksWindowSize{1000,2000,4000} / ringBufferSize{512,1000,2000} / cooldownSamples{1000,2000,5000},
  OAT,INSECTS_abrupt,BACKLOG,3 repeats。看 overall_auc 对各参数的敏感度(应稳健=波动小)。

### HDDM_W 参数稳健性(sensitivity_hddm_w plan)
- 扫 scaleMode{p99,maxdev} / warmup,看 overall_auc 敏感度。

### 分析
overall_auc vs 各参数,出稳健性表。论点:IKS 对参数不敏感(稳健性论据),HDDM_W 依赖 λ/scaleMode。
planning 待做:sensitivity 分析脚本(若现有 analyze 不够)。

---

## 任务 4 — dev 待应用单点 diff(清掉)

1. sensitivity_iks 去 iksWindowSize 重复行(FIX_cfg_query_sensitivity_plan_extras.md)。
2. analyze-all.sh:98 arm 标签 new/old → iks/hddm_w(直接检测器名)。
3. （延迟方案确定无需改 Java,故无 latency tracking diff。）
4. .env JOB_LOAD_RATE：吞吐 run 用 50000、延迟 run 用 2000（两组分别跑，或参数化）。

---

## 任务 5 — 论文写作(实验齐备后)

已有骨架:EXP2_distributed_accuracy_skeleton.md、EXP4_detector_morphology_skeleton.md。
补 EXP3 小节:
- 吞吐:三口径 + source 单分区权衡叙事 + 检测算子高吞吐。
- 延迟:scoreTime−ingestionTime 随并行度下降(分布式低延迟)。
- 权衡讨论:高吞吐(端到端受单分区限)vs 低延迟(并行降低)vs 时序保证(单分区)的三角权衡。
- 多分区扩展作 future work。
全文整合:贡献① (漂移响应/投票协议) + 贡献② (分布式 iForest) + EXP1-4 + EXP3 系统级。

---

## 执行顺序建议

1. dev 应用任务 4 的 diff(sensitivity_iks 修复 + arm 标签)。
2. 任务 1 吞吐:.env rate=50000,跑 EXP3 吞吐 24 run,exp3_throughput_prom.py 分析。
3. 任务 2 延迟:.env rate=2000,跑 EXP3 延迟 run,验不堆积 + 延迟 vs P。
4. 任务 3 sensitivity:跑两个 sensitivity plan,出稳健性表。
5. 任务 5 论文:补 EXP3 小节 + 全文整合。

planning(本对话)可立即做:exp3_scalability_from_scores.py 加堆积检测、sensitivity 分析脚本。
