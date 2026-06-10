package com.leejean.flink;

import com.leejean.beans.FeatureDrift;
import com.leejean.beans.FeatureValue;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PerFeatureIKSFunction MiniCluster 集成测试。
 * Verifies the per-feature peak-KS confirmation gate (HANDOVER §3.1).
 */
class PerFeatureIKSFunctionTest {

    private static final class Sink implements SinkFunction<FeatureDrift> {
        static final List<FeatureDrift> values = Collections.synchronizedList(new ArrayList<>());
        @Override public void invoke(FeatureDrift value, Context context) { values.add(value); }
    }

    @BeforeEach
    void setUp() { Sink.values.clear(); }

    /** 全段同分布稳定数据 → 无 onset 上报 / no confirmed onset on i.i.d. stable stream. */
    @Test
    void stable_stream_emits_nothing() throws Exception {
        int W = 200;
        double p = 0.001;
        int C = 200;
        // ksConfirm = thr → 保守门：稳定流靠 confirmWin 滤瞬态，无 emit
        double ksConfirm = Math.sqrt(-0.5 * Math.log(p)) * Math.sqrt(2.0 / W);

        runJob(W, p, C, ksConfirm, buildStable(0, 4 * W, 0.0, 1.0, 7L, 0));

        assertTrue(Sink.values.isEmpty(),
                "stable stream should not produce confirmed onset, got " + Sink.values.size());
    }

    /** warm-up + 稳定 + 突变 → 至少 1 个 FeatureDrift,seq 在突变点之后. */
    @Test
    void abrupt_change_emits_confirmed_onset() throws Exception {
        int W = 200;
        double p = 0.001;
        int C = 200;
        double ksConfirm = Math.sqrt(-0.5 * Math.log(p)) * Math.sqrt(2.0 / W);

        List<FeatureValue> data = new ArrayList<>();
        // 单特征 id=0：前 2W 条 N(0,1)，后 2W 条 N(10,1)
        data.addAll(buildStable(0, 2 * W, 0.0, 1.0, 7L, 0));
        data.addAll(buildStable(0, 2 * W, 10.0, 1.0, 9L, 2 * W));

        runJob(W, p, C, ksConfirm, data);

        assertFalse(Sink.values.isEmpty(),
                "abrupt change should produce ≥1 confirmed onset");
        for (FeatureDrift fd : Sink.values) {
            assertEquals(0, fd.getFeatureId(), "featureId must match key");
            assertTrue(fd.getSeq() >= W,
                    "onset seq must be past warm-up, got " + fd.getSeq());
            assertTrue(fd.getKs() > 0, "peak KS must be > 0");
        }
    }

    /** 多特征并行：feature 0 漂移,feature 1 稳定 → 只 emit feature 0. */
    @Test
    void per_feature_isolation() throws Exception {
        int W = 200;
        double p = 0.001;
        int C = 200;
        double ksConfirm = Math.sqrt(-0.5 * Math.log(p)) * Math.sqrt(2.0 / W);

        List<FeatureValue> data = new ArrayList<>();
        Random rng = new Random(11);
        long seq = 0;
        // 阶段一：两特征都稳定
        for (int i = 0; i < 2 * W; i++) {
            data.add(new FeatureValue(0, rng.nextGaussian(), seq));
            data.add(new FeatureValue(1, rng.nextGaussian(), seq));
            seq++;
        }
        // 阶段二：feature 0 漂移，feature 1 仍稳定
        for (int i = 0; i < 2 * W; i++) {
            data.add(new FeatureValue(0, rng.nextGaussian() + 10.0, seq));
            data.add(new FeatureValue(1, rng.nextGaussian(), seq));
            seq++;
        }

        runJob(W, p, C, ksConfirm, data);

        assertFalse(Sink.values.isEmpty(), "feature 0 drift should produce ≥1 emit");
        for (FeatureDrift fd : Sink.values) {
            assertEquals(0, fd.getFeatureId(),
                    "only feature 0 should emit, got featureId=" + fd.getFeatureId());
        }
    }

    // ===== helpers =====

    private static List<FeatureValue> buildStable(int featureId, int n,
                                                   double mean, double std,
                                                   long seed, long startSeq) {
        Random rng = new Random(seed);
        List<FeatureValue> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            out.add(new FeatureValue(featureId, rng.nextGaussian() * std + mean, startSeq + i));
        }
        return out;
    }

    private static void runJob(int W, double pValue, int C, double ksConfirm,
                                List<FeatureValue> data) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<FeatureValue> source = env.fromCollection(data);
        KeyedStream<FeatureValue, Integer> keyed = source.keyBy(
                (KeySelector<FeatureValue, Integer>) FeatureValue::getFeatureId);

        SingleOutputStreamOperator<FeatureDrift> out = keyed
                .process(new PerFeatureIKSFunction(W, pValue, C, ksConfirm))
                .name("per-feature-iks");

        out.addSink(new Sink());

        env.execute("PerFeatureIKS test");
    }
}
