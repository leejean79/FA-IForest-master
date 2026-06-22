# Sensitivity 附录执行计划 — IKS / HDDM_W 参数稳健性

> 论文附录:并列呈现两检测器对参数的稳健性。论点(来自 EXP4):IKS 对参数不敏感(稳健),
> HDDM_W 依赖 λ/scaleMode。本附录用集群数据量化两者参数敏感度。
> 数据集 insects_abrupt_imbalanced,BACKLOG,3 repeats,主指标 overall_auc。

---

## 0. 前置(必做,否则 IKS 数据废)

**dev 清单项 4 必须先应用**:sensitivity_iks 的 plan_extras 删掉 `iksWindowSize: 2000`。
现状(已实测确认):cfg_query 展开为 `iksWindowSize=1000;iksWindowSize=2000;...`,
plan_extras 的固定值覆盖 grids 扫描值 → OAT 失效(iksWindowSize 恒 2000,扫不到 1000/4000)。
**改后验证**:
  python3 deploy/scripts/cfg_query.py plan sensitivity_iks | grep -o "iksWindowSize=[0-9]*" | sort -u
  应输出 1000/2000/4000 三个不同值(无重复覆盖)。不修则 IKS sensitivity 全废。

---

## 1. IKS 参数稳健性(OAT)

### plan(已定义,修复后即可用)
- dataset: insects_abrupt_imbalanced,config: BACKLOG,repeats: 3
- grids(OAT,逐参数扫,其余取默认):
  - iksWindowSize: [1000, 2000, 4000]
  - ringBufferSize: [512, 1000, 2000]
  - cooldownSamples: [1000, 2000, 5000]
- OAT 语义:每次只变一个参数,其余固定默认(iksWindowSize=2000, ringBufferSize=1000,
  cooldownSamples=2000)。基线点(全默认)被多个 OAT 轴共享。
- 规模 ≈ 7 个独立点 × 3 repeats ≈ 21 run(三轴各 3 点,共享中心基线,去重)。

### 跑法
  # 确认前置修复后:
  python3 deploy/scripts/cfg_query.py plan sensitivity_iks   # 核对展开正确
  RUN_MODE=local bash deploy/scripts/run-batch.sh --plan sensitivity_iks
  # (此 plan 不涉及并行度扩展,run-batch 即可;不需 detectionParallelism 同步)

### 分析
每个 run 用 analyze.py mode_drift 出 overall_auc,汇总为"参数值 → overall_auc(mean±std)"。
  论点验证:各参数在扫描范围内 overall_auc 波动小(如 < 0.02)→ IKS 参数稳健。

---

## 2. HDDM_W 参数稳健性(configurations)

### plan(已定义)
- 4 个配置点(detector=HDDM_W 显式携带,避免 grids 展开不透传 detector 误跑 IKS):
  - {hddmLambda: 0.005, scaleMode: p99,    warmup: 2000}  ← 基线
  - {hddmLambda: 0.005, scaleMode: maxdev, warmup: 2000}  ← 换 scaleMode
  - {hddmLambda: 0.005, scaleMode: p99,    warmup: 1000}  ← 换 warmup
  - {hddmLambda: 0.005, scaleMode: p99,    warmup: 4000}  ← 换 warmup
- 注:hddmLambda 用 lambda_sweep 选出的 abrupt λ*(=0.002,见 EXP1)替换 0.005——
  **跑前确认 λ 值与 EXP1 一致**(当前 plan 写 0.005,EXP1 abrupt λ*=0.002,需对齐)。
- 规模 = 4 点 × 3 repeats = 12 run(λ 已在 lambda_sweep 充分扫,此处只补 scaleMode/warmup)。

### 跑法
  RUN_MODE=local bash deploy/scripts/run-batch.sh --plan sensitivity_hddm_w

### 分析
同 IKS,出 overall_auc。论点:HDDM_W 对 scaleMode(p99 vs maxdev)敏感、warmup 有影响
→ 与 IKS 的稳健形成对比。

---

## 3. 论文附录呈现

并列两表:
- 表 A(IKS):3 参数 × 各 3 值 → overall_auc,显示波动小(稳健)。
- 表 B(HDDM_W):scaleMode/warmup → overall_auc,显示 scaleMode 切换带来的变化(敏感)。
结论句:IKS 在合理参数范围内 overall_auc 稳定(无需逐数据集调参),HDDM_W 依赖 scaleMode/λ,
        印证 EXP4"IKS 总体更稳健"的论点。

---

## 4. 待确认/风险
- IKS 前置修复(§0)必做,且跑前用 cfg_query 验证展开正确。
- HDDM_W 的 hddmLambda 要对齐 EXP1 的 abrupt λ*(0.002),当前 plan 写 0.005,跑前确认/改。
- OAT 基线点共享:分析时注意中心点(全默认)是三轴公共点,不要重复计数。
- 主指标 overall_auc(less-confounded);若需要也看 recovered,但稳健性看 overall_auc 为主。
