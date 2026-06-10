package com.leejean.flink;

import com.leejean.beans.FeatureDrift;
import com.leejean.beans.FeatureValue;
import com.leejean.drift.IKS;
import com.leejean.drift.IKSConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 检测面 per-feature IKSSW + 峰值-KS 确认门（HANDOVER §3.1）。
 * Per-feature IKSSW with peak-KS confirmation gate, keyed by featureId.
 *
 * <p>状态机：
 * <ul>
 *   <li>非确认态：ks &gt; thr → 进确认态，记 startSeq=seq, peakKs=ks</li>
 *   <li>确认态：peakKs = max(peakKs, ks)；当 seq − startSeq ≥ C：
 *     <ul>
 *       <li>peakKs ≥ ksConfirm → emit FeatureDrift(featureId, startSeq, peakKs)，
 *           调 ikssw.rebase()，退确认态</li>
 *       <li>否则瞬态 → 弃，退确认态，不 rebase</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>注意：ksConfirm 默认应≈thr（保守），靠 confirmWin C 滤瞬态，
 * 不靠激进幅度门——recall 是已证成立的硬指标。
 */
public class PerFeatureIKSFunction
        extends KeyedProcessFunction<Integer, FeatureValue, FeatureDrift> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PerFeatureIKSFunction.class);

    private final int windowSize;
    private final double pValue;
    private final int confirmWin;
    private final double ksConfirm;

    // ===== keyed state（per featureId）=====
    private transient ValueState<IKS> ikssw;
    private transient ValueState<Boolean> confirming;
    private transient ValueState<Long> startSeq;
    private transient ValueState<Double> peakKs;

    public PerFeatureIKSFunction(int windowSize, double pValue, int confirmWin, double ksConfirm) {
        if (windowSize <= 0) throw new IllegalArgumentException("windowSize must be > 0");
        if (pValue <= 0 || pValue >= 1) throw new IllegalArgumentException("pValue must be in (0,1)");
        if (confirmWin <= 0) throw new IllegalArgumentException("confirmWin must be > 0");
        if (ksConfirm < 0) throw new IllegalArgumentException("ksConfirm must be ≥ 0");
        this.windowSize = windowSize;
        this.pValue = pValue;
        this.confirmWin = confirmWin;
        this.ksConfirm = ksConfirm;
    }

    @Override
    public void open(Configuration parameters) {
        ikssw = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-ikssw", IKS.class));
        confirming = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-confirming", Types.BOOLEAN));
        startSeq = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-start-seq", Types.LONG));
        peakKs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("per-feature-peak-ks", Types.DOUBLE));
    }

    @Override
    public void processElement(FeatureValue fv, Context ctx, Collector<FeatureDrift> out)
            throws Exception {
        IKS det = ikssw.value();
        if (det == null) {
            det = new IKS(new IKSConfig(windowSize, pValue));
        }
        det.update(fv.getValue());

        // warm-up 期 nCurrent != nReference，IKS.ks() 会抛；用 initialized 代理 = sampleCount >= W
        if (det.sampleCount() < windowSize) {
            ikssw.update(det);
            return;
        }

        double ks = det.ks();
        double thr = det.threshold();
        boolean inConfirm = Boolean.TRUE.equals(confirming.value());

        if (!inConfirm) {
            if (ks > thr) {
                confirming.update(Boolean.TRUE);
                startSeq.update(fv.getSeq());
                peakKs.update(ks);
                LOG.debug("featureId={} confirm started at seq={} ks={} thr={}",
                        fv.getFeatureId(), fv.getSeq(), ks, thr);
            }
            ikssw.update(det);
            return;
        }

        // 确认态：更新峰值
        double pk = peakKs.value();
        if (ks > pk) {
            pk = ks;
            peakKs.update(pk);
        }

        long s0 = startSeq.value();
        if (fv.getSeq() - s0 >= confirmWin) {
            if (pk >= ksConfirm) {
                out.collect(new FeatureDrift(fv.getFeatureId(), s0, pk));
                det.rebase();
                LOG.info("featureId={} CONFIRMED onset seq={} peakKs={} (thr={}, ksConfirm={})",
                        fv.getFeatureId(), s0, pk, thr, ksConfirm);
            } else {
                LOG.debug("featureId={} TRANSIENT discarded startSeq={} peakKs={} < ksConfirm={}",
                        fv.getFeatureId(), s0, pk, ksConfirm);
            }
            confirming.clear();
            startSeq.clear();
            peakKs.clear();
        }

        ikssw.update(det);
    }
}
