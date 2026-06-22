# Sensitivity 附录骨架 — IKS / HDDM_W 参数稳健性(集群数据定稿)

> 论文附录 A。数据:insects_abrupt_imbalanced,BACKLOG,P=4,3 repeats,主指标 overall_auc。
> IKS:OAT 扫 iksWindowSize/ringBufferSize/cooldownSamples(9 点×3=27 run)。
> HDDM_W:configurations 扫 scaleMode/warmup(4 点×3=12 run,hddmLambda=0.002 对齐 EXP1)。

---

## 1. 论点
IKS 对检测核心参数(窗口大小、缓冲大小)稳健,仅 cooldownSamples 有下界要求;
HDDM_W 对 warmup 与 scaleMode 均敏感。整体 IKS 参数稳健性优于 HDDM_W,印证 EXP4
"IKS 总体更稳健、HDDM_W 强依赖参数"的论点。

## 2. IKS 参数稳健性(表 A1)

| 参数 | 扫描值 | overall_auc(mean±std) | 极差 | 稳健性 |
|---|---|---|---|---|
| iksWindowSize | 1000 / 2000 / 4000 | 0.775±0.015 / 0.767±0.005 / 0.780±0.038 | 0.013 | 稳健 |
| ringBufferSize | 512 / 1000 / 2000 | 0.762±0.006 / 0.769±0.010 / 0.771±0.006 | 0.010 | 稳健 |
| cooldownSamples | 1000 / 2000 / 5000 | 0.695±0.003 / 0.769±0.008 / 0.759±0.009 | 0.074 | 有下界 |

观察:
- iksWindowSize、ringBufferSize 极差 ~0.01(repeat 噪声量级)→ 这两个核心参数稳健,
  无需逐数据集调参。
- cooldownSamples=1000 时 AUC 骤降至 0.695(其余 ~0.77),极差 0.074 → cooldownSamples
  有下界要求(应 ≥2000);锁定的默认值 2000 正落在安全区(该轴最高 0.769)。
- iksWindowSize=4000 单次方差略大(std 0.038)但均值仍稳健。

## 3. HDDM_W 参数敏感性(表 A2)

| 配置 | overall_auc(mean±std) |
|---|---|
| p99, warmup=1000 | 0.666±0.003 |
| p99, warmup=2000 | 0.737±0.018 |
| p99, warmup=4000 | 0.760±0.006 |
| maxdev, warmup=2000 | 0.697±0.007 |

观察:
- warmup 敏感:1000→4000 时 AUC 0.666→0.760,极差 0.094(大于 IKS 任一参数)。
- scaleMode 敏感:同 warmup=2000,p99(0.737)vs maxdev(0.697)差 0.040。
- HDDM_W 对 warmup 与 scaleMode 均敏感,需调参才能达到较好性能。

## 4. 对比结论
- IKS 核心参数极差 ≤0.013,HDDM_W warmup 极差 0.094 —— **IKS 稳健性约高一个量级**。
- IKS 例外(cooldownSamples 下界)是单调可预测的(太小则差),给出明确下界建议即可;
  HDDM_W 的 warmup/scaleMode 敏感则需逐场景调参,实用性弱于 IKS。
- 印证 EXP4:IKS 无 ε 下界、无量纲、无需逐数据集调参 = 稳健性结构来源。

## 5. 写作诚实性边界
- **不写"IKS 对所有参数完全稳健"**——cooldownSamples 是真实例外,如实纳入(诚实反增可信度)。
- cooldownSamples 例外要正面框成"明确的下界建议(≥2000)",非缺陷;默认值在安全区。
- HDDM_W 敏感性如实报,但不贬低——它是有效检测器,只是需更多调参(与 IKS 的对比是稳健性维度)。
- 单数据集(insects_abrupt)上的 sensitivity,作 limitation 提一句(其他数据集可能不同)。

---

*数据来源:集群 sensitivity_iks(27 run)+ sensitivity_hddm_w(12 run),drift-summary.csv,
overall_auc。OAT 中心点(iksWindowSize=2000/ringBufferSize=1000/cooldownSamples=2000 = 全默认同一配置)
应得相近 AUC(0.767/0.769/0.769),一致性良好,佐证 repeat 稳定。*
