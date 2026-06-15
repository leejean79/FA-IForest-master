package com.leejean.flink;

import com.leejean.beans.FeatureDrift;
import com.leejean.beans.FeatureValue;
import com.leejean.drift.HDDM_AConfig;
import com.leejean.drift.HDDM_W;
import com.leejean.drift.DriftStatus;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 检测面 per-feature HDDM_W 检测器（devspec §1）。IKS 版（{@link PerFeatureIKSFunction}）
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
 * 用运行均值 {@code runMean = warmSum/count} 作临时参考累积偏离统计；warm-up 末把
 * {@code refMean = warmSum/warmup} 冻结，{@code scale} 由累积的偏离统计得出。
 * warm-up 段足够长（默认 2000）时运行均值与最终均值的差异可忽略。
 *
 * <p><b>状态机</b>：warm-up 期只累积不检测；检测期把 signal 喂给 HDDM_W，仅 DRIFT 触发
 * {@link FeatureDrift}（WARN 不上报，靠 HDDM_W 内部 warn→drift 升级滤瞬态，与 IKS 版
 * 靠 confirmWin 滤瞬态对齐）。DRIFT 后 {@code reset()} 并清空参考、重新 warm-up，
 * 对应 IKS 的 {@code rebase()}。
 */
public class PerFeatureHDDMFunction
        extends KeyedProcessFunction<Integer, FeatureValue, FeatureDrift> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PerFeatureHDDMFunction.class);

    /** scale 防除零下限 / floor on scale to avoid division by zero. */
    private static final double EPS = 1e-9;

    private final int warmup;
    private final double lambda;
    private final double warnConfidence;
    private final double driftConfidence;
    private final long warnTimeoutSamples;
    // 注意：scaleMode 在构造期校验后不保留为字段——首版只实装 maxdev，运行期无需它；
    // 同时让本函数的实例字段全为基本类型，避免 Flink ClosureCleaner 递归清理对象字段
    // 在 JDK 17+ 上触发 String 反射不可访问。待实装 p99 时再连同 p99 逻辑加回字段。
    // scaleMode is validated in the ctor but not kept as a field: v1 only implements
    // maxdev and has no runtime use for it. Keeping all instance fields primitive also
    // avoids Flink's ClosureCleaner recursing into an object field (JDK 17+ String access).

    // ===== keyed state（per featureId）=====
    private transient ValueState<HDDM_W> hddm;      // 每特征一个检测器实例
    private transient ValueState<Long> count;       // 已观察样本数（判断是否过 warm-up）
    private transient ValueState<Double> refMean;    // 冻结的参考均值（warm-up 末确定）
    private transient ValueState<Double> scale;      // 归一化尺度（warm-up 末确定）
    private transient ValueState<Double> warmSum;    // warm-up 期 Σx（算 refMean 用）
    private transient ValueState<Double> warmMaxDev; // warm-up 期 max|x−runMean|（maxdev scale 用）

    public PerFeatureHDDMFunction(int warmup, double lambda,
                                  double warnConfidence, double driftConfidence,
                                  long warnTimeoutSamples, String scaleMode) {
        if (warmup <= 0) throw new IllegalArgumentException("warmup must be > 0");
        if (lambda <= 0 || lambda > 1) throw new IllegalArgumentException("lambda must be in (0,1]");
        // HDDM_AConfig 构造自带 warn/drift/timeout 的取值校验，这里提前实例化以 fail-fast
        new HDDM_AConfig(warnConfidence, driftConfidence, warnTimeoutSamples);
        if ("p99".equals(scaleMode)) {
            // 首版只实现 maxdev；p99 留参数位待数值验证后再启用（devspec §3、§6）
            throw new UnsupportedOperationException(
                    "scaleMode=p99 not implemented yet; only maxdev is available in this version");
        }
        if (!"maxdev".equals(scaleMode)) {
            throw new IllegalArgumentException("scaleMode must be one of {maxdev, p99}, got: " + scaleMode);
        }
        this.warmup = warmup;
        this.lambda = lambda;
        this.warnConfidence = warnConfidence;
        this.driftConfidence = driftConfidence;
        this.warnTimeoutSamples = warnTimeoutSamples;
    }

    @Override
    public void open(Configuration parameters) {
        hddm = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-hddm", HDDM_W.class));
        count = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-count", Types.LONG));
        refMean = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-ref-mean", Types.DOUBLE));
        scale = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-scale", Types.DOUBLE));
        warmSum = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-warm-sum", Types.DOUBLE));
        warmMaxDev = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-warm-maxdev", Types.DOUBLE));
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
            double dev = Math.abs(fv.getValue() - runMean);
            double md = Math.max(warmMaxDev.value() == null ? 0.0 : warmMaxDev.value(), dev);

            warmSum.update(ws);
            warmMaxDev.update(md);
            count.update(newCount);

            if (newCount == warmup) {
                refMean.update(ws / warmup);
                scale.update(Math.max(md, EPS));   // maxdev：用 warm-up 段最大偏离作尺度
                LOG.debug("featureId={} warm-up done: refMean={} scale={}",
                        fv.getFeatureId(), ws / warmup, Math.max(md, EPS));
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
            det.reset();
            // 漂移后重估参考：清空 refMean/scale/warmSum/warmMaxDev，重新 warm-up（对应 IKS rebase）
            refMean.clear();
            scale.clear();
            warmSum.clear();
            warmMaxDev.clear();
            count.update(0L);
            hddm.update(det);
            LOG.info("featureId={} DRIFT onset seq={} signal={}",
                    fv.getFeatureId(), fv.getSeq(), signal);
            return;
        }

        // WARN 不上报，仅让 HDDM_W 内部状态机演进；只有 DRIFT 合成 FeatureDrift
        hddm.update(det);
        count.update(c + 1);
    }
}
