package com.leejean.flink;

import com.leejean.beans.FeatureDrift;
import com.leejean.beans.FeatureValue;
import com.leejean.drift.HDDM_AConfig;
import com.leejean.drift.HDDM_W;
import com.leejean.drift.DriftStatus;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 检测面 per-feature HDDM_W 检测器（dev handover §1）。IKS 版（{@link PerFeatureIKSFunction}）
 * 保留并存，本类是 EXP1 对比里唯一被替换的检测面变量。
 * Per-feature HDDM_W detector, keyed by featureId. Drop-in replacement for the
 * IKS variant on the detection side; the IKS class is kept alongside for rollback.
 *
 * <p><b>归一化（决策 B）</b>：FeatureSplitFlatMap 吐出的是 33 个量纲不一的原始
 * 未标准化特征值，而 HDDM_W 的 Hoeffding 界假定输入 ∈ [0,1]，因此归一化层是必需的。
 * 本类监控"特征值相对冻结参考均值的归一化偏离量"
 * {@code signal = min(|x − refMean| / scale, 1.0)}，捕双向漂移、有界 [0,1]。
 * Normalizes each raw feature value to a bounded deviation signal in [0,1]
 * before feeding HDDM_W, capturing bidirectional drift.
 *
 * <p><b>warm-up（单遍近似）</b>：单遍流无法同时算出 refMean 又算偏离统计，故 warm-up 期
 * 用运行均值 {@code runMean = warmSum/count} 作临时参考，收集 {@code |x − runMean|}；
 * warm-up 末把 {@code refMean = warmSum/warmup} 冻结，{@code scale} 由收集到的偏离值算出
 * 后清空收集列表。warm-up 段足够长（默认 2000）时运行均值与最终均值的差异可忽略。
 * refMean 一经冻结即不再自适应——自适应（如 EWMA 参考）会把漂移信号抹平。
 *
 * <p><b>归一化尺度 scale（dev handover §1.4，离线验证修正）</b>：默认 {@code p99}——取
 * warm-up 段 {@code |x − runMean|} 的 P99 分位，抗 INSECTS 异常点（5.5%/6.2%）干扰；
 * {@code maxdev} 取最大偏离，会被 warm-up 段异常点撑大致信号压扁，离线已证在 INSECTS 上
 * 失效，仅保留作鲁棒性消融开关。
 *
 * <p><b>状态机</b>：warm-up 期只累积不检测；检测期把 signal 喂给 HDDM_W，仅 DRIFT 触发
 * {@link FeatureDrift}（WARN 不上报，靠 HDDM_W 内部 warn→drift 升级滤瞬态，与 IKS 版
 * 靠 confirmWin 滤瞬态对齐）。DRIFT 后清空参考并 {@code hddm.clear()} 重新 warm-up
 * （下一条检测样本重建一个全新的 HDDM_W），对应 IKS 的 {@code rebase()}。
 */
public class PerFeatureHDDMFunction
        extends KeyedProcessFunction<Integer, FeatureValue, FeatureDrift> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PerFeatureHDDMFunction.class);

    /** scale 防除零下限 / floor on scale to avoid division by zero（dev handover §1.4）. */
    private static final double EPS = 1e-12;
    /** p99 分位 / quantile used by scaleMode=p99. */
    private static final double P99 = 0.99;

    private final int warmup;
    private final double lambda;
    private final double warnConfidence;
    private final double driftConfidence;
    private final long warnTimeoutSamples;
    // scaleMode 在构造期校验为 {maxdev, p99}，运行期决策压成基本类型 useP99，
    // 让本函数实例字段全为基本类型，避免 Flink ClosureCleaner 递归清理对象（String）字段
    // 在 JDK 17+ 上触发反射不可访问（java.base 未 open java.lang）。
    // The validated scaleMode is collapsed to a primitive boolean so all instance fields
    // stay primitive, sidestepping ClosureCleaner reflecting into a String field on JDK 17+.
    private final boolean useP99;

    // ===== keyed state（per featureId）=====
    private transient ValueState<HDDM_W> hddm;      // 每特征一个检测器实例
    private transient ValueState<Long> count;       // 已观察样本数（判断是否过 warm-up）
    private transient ValueState<Double> refMean;    // 冻结的参考均值（warm-up 末确定）
    private transient ValueState<Double> scale;      // 归一化尺度（warm-up 末确定）
    private transient ValueState<Double> warmSum;    // warm-up 期 Σx（算 refMean 用）
    private transient ListState<Double> warmDevs;    // warm-up 期 |x−runMean| 收集，末端算 scale 后清空

    public PerFeatureHDDMFunction(int warmup, double lambda,
                                  double warnConfidence, double driftConfidence,
                                  long warnTimeoutSamples, String scaleMode) {
        if (warmup <= 0) throw new IllegalArgumentException("warmup must be > 0");
        if (lambda <= 0 || lambda > 1) throw new IllegalArgumentException("lambda must be in (0,1]");
        // HDDM_AConfig 构造自带 warn/drift/timeout 的取值校验，这里提前实例化以 fail-fast
        new HDDM_AConfig(warnConfidence, driftConfidence, warnTimeoutSamples);
        if (!"maxdev".equals(scaleMode) && !"p99".equals(scaleMode)) {
            throw new IllegalArgumentException("scaleMode must be one of {p99, maxdev}, got: " + scaleMode);
        }
        this.warmup = warmup;
        this.lambda = lambda;
        this.warnConfidence = warnConfidence;
        this.driftConfidence = driftConfidence;
        this.warnTimeoutSamples = warnTimeoutSamples;
        this.useP99 = "p99".equals(scaleMode);
    }

    @Override
    public void open(Configuration parameters) {
        hddm = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-hddm", HDDM_W.class));
        count = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-count", Types.LONG));
        refMean = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-refmean", Types.DOUBLE));
        scale = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-scale", Types.DOUBLE));
        warmSum = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-warmsum", Types.DOUBLE));
        warmDevs = getRuntimeContext().getListState(
                new ListStateDescriptor<>("per-feature-warmdev", Types.DOUBLE));
    }

    @Override
    public void processElement(FeatureValue fv, Context ctx, Collector<FeatureDrift> out)
            throws Exception {
        long c = count.value() == null ? 0L : count.value();

        if (c < warmup) {
            // ── warm-up：只累积，不检测 ──
            double ws = (warmSum.value() == null ? 0.0 : warmSum.value()) + fv.getValue();
            long newCount = c + 1;
            double runMean = ws / newCount;

            warmSum.update(ws);
            warmDevs.add(Math.abs(fv.getValue() - runMean));
            count.update(newCount);

            if (newCount == warmup) {
                refMean.update(ws / warmup);
                scale.update(Math.max(scaleEstimate(warmDevs), EPS));   // EPS 防除零
                warmDevs.clear();                                       // 释放 warm-up 收集
                LOG.debug("featureId={} warm-up done: refMean={} scale={} (mode={})",
                        fv.getFeatureId(), ws / warmup, scale.value(), useP99 ? "p99" : "maxdev");
                // 此时不 init hddm，首个检测样本在下一条
            }
            return;
        }

        // ── 检测期 ──
        HDDM_W det = hddm.value();
        if (det == null) {
            det = new HDDM_W(new HDDM_AConfig(warnConfidence, driftConfidence, warnTimeoutSamples), lambda);
        }

        // signal ∈ [0,1]，双向偏离归一化
        double signal = Math.min(Math.abs(fv.getValue() - refMean.value()) / scale.value(), 1.0);
        DriftStatus status = det.update(signal);

        if (status == DriftStatus.DRIFT) {
            // seq=onset（当前样本原始序号，满足聚合器契约）；ks 字段填 signal 做审计
            out.collect(new FeatureDrift(fv.getFeatureId(), fv.getSeq(), signal));
            // 漂移后重估参考：清空 refMean/scale/warmSum，重新 warm-up（对应 IKS rebase）
            refMean.clear();
            scale.clear();
            warmSum.clear();
            count.update(0L);
            hddm.clear();   // 下一条检测样本重建一个全新（reset 等价）的 HDDM_W
            LOG.info("featureId={} DRIFT onset seq={} signal={}",
                    fv.getFeatureId(), fv.getSeq(), signal);
            return;
        }

        // WARN 不上报，仅让 HDDM_W 内部状态机演进；只有 DRIFT 合成 FeatureDrift
        hddm.update(det);
        count.update(c + 1);
    }

    /**
     * 由 warm-up 段收集的偏离值估计归一化尺度。warm-up 末调用一次，列表恰有 warmup 个元素。
     * Estimates the normalization scale from the warm-up deviation list (called once at warm-up end).
     * {@code p99} 用最近秩法取 P99 分位（抗离群）；{@code maxdev} 取最大偏离。
     */
    private double scaleEstimate(ListState<Double> devs) throws Exception {
        List<Double> vals = new ArrayList<>();
        for (Double d : devs.get()) {
            vals.add(d);
        }
        if (vals.isEmpty()) return 0.0;
        if (!useP99) {
            double mx = 0.0;
            for (double d : vals) {
                mx = Math.max(mx, d);
            }
            return mx;
        }
        Collections.sort(vals);
        int rank = (int) Math.ceil(P99 * vals.size());
        if (rank < 1) rank = 1;
        if (rank > vals.size()) rank = vals.size();
        return vals.get(rank - 1);
    }
}
