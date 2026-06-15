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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PerFeatureHDDMFunction MiniCluster 集成测试。
 * Verifies the per-feature HDDM_W detector state machine (devspec §1):
 * warm-up freezes refMean/scale, detection feeds the normalized [0,1] deviation
 * signal to HDDM_W, and only DRIFT emits a FeatureDrift.
 *
 * <p>测试用 lambda=0.05（落在 devspec §4 的 λ∈{0.05,0.1,0.2} 扫描区间内）：
 * 由于 signal 被 clamp 到 [0,1]，HDDM_W 的 EWMA Hoeffding 漂移阈值随 lambda
 * 增大而变宽，较小的 lambda 才能让饱和的突变后 signal 稳定越过阈值。
 */
class PerFeatureHDDMFunctionTest {

    private static final class Sink implements SinkFunction<FeatureDrift> {
        static final List<FeatureDrift> values = Collections.synchronizedList(new ArrayList<>());
        @Override public void invoke(FeatureDrift value, Context context) { values.add(value); }
    }

    @BeforeEach
    void setUp() { Sink.values.clear(); }

    // ===== 构造参数校验 =====

    @Test
    void constructor_rejects_invalid_args() {
        // warmup <= 0
        assertThrows(IllegalArgumentException.class,
                () -> new PerFeatureHDDMFunction(0, 0.1, 0.005, 0.001, 2000L, "maxdev"));
        // lambda 越界
        assertThrows(IllegalArgumentException.class,
                () -> new PerFeatureHDDMFunction(200, 1.5, 0.005, 0.001, 2000L, "maxdev"));
        // HDDM_AConfig 约束：driftConf 必须 < warnConf
        assertThrows(IllegalArgumentException.class,
                () -> new PerFeatureHDDMFunction(200, 0.1, 0.001, 0.005, 2000L, "maxdev"));
        // p99 首版未实现 → UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class,
                () -> new PerFeatureHDDMFunction(200, 0.1, 0.005, 0.001, 2000L, "p99"));
        // 未知 scaleMode
        assertThrows(IllegalArgumentException.class,
                () -> new PerFeatureHDDMFunction(200, 0.1, 0.005, 0.001, 2000L, "zscore"));
    }

    /** 全段同分布稳定数据 → 无 DRIFT 上报 / no drift on i.i.d. stable stream. */
    @Test
    void stable_stream_emits_nothing() throws Exception {
        int W = 200;
        runJob(W, 0.05, buildStable(0, 6 * W, 0.0, 1.0, 7L, 0));

        assertTrue(Sink.values.isEmpty(),
                "stable stream should not produce drift, got " + Sink.values.size());
    }

    /** warm-up + 稳定 + 突变 → 至少 1 个 FeatureDrift,seq 在 warm-up 之后,signal>0. */
    @Test
    void abrupt_change_emits_drift() throws Exception {
        int W = 200;

        List<FeatureValue> data = new ArrayList<>();
        // 单特征 id=0：前 2W 条 N(0,1)，后 4W 条 N(20,1)（大幅突变使 signal 饱和到 1）
        data.addAll(buildStable(0, 2 * W, 0.0, 1.0, 7L, 0));
        data.addAll(buildStable(0, 4 * W, 20.0, 1.0, 9L, 2 * W));

        runJob(W, 0.05, data);

        assertFalse(Sink.values.isEmpty(),
                "abrupt change should produce ≥1 drift");
        for (FeatureDrift fd : Sink.values) {
            assertEquals(0, fd.getFeatureId(), "featureId must match key");
            assertTrue(fd.getSeq() >= W,
                    "onset seq must be past warm-up, got " + fd.getSeq());
            assertTrue(fd.getKs() > 0, "signal (audit field) must be > 0");
        }
    }

    /** 多特征并行：feature 0 漂移,feature 1 稳定 → 只 emit feature 0. */
    @Test
    void per_feature_isolation() throws Exception {
        int W = 200;

        List<FeatureValue> data = new ArrayList<>();
        Random rng = new Random(11);
        long seq = 0;
        // 阶段一：两特征都稳定
        for (int i = 0; i < 2 * W; i++) {
            data.add(new FeatureValue(0, rng.nextGaussian(), seq));
            data.add(new FeatureValue(1, rng.nextGaussian(), seq));
            seq++;
        }
        // 阶段二：feature 0 大幅漂移，feature 1 仍稳定
        for (int i = 0; i < 4 * W; i++) {
            data.add(new FeatureValue(0, rng.nextGaussian() + 20.0, seq));
            data.add(new FeatureValue(1, rng.nextGaussian(), seq));
            seq++;
        }

        runJob(W, 0.05, data);

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

    private static void runJob(int warmup, double lambda, List<FeatureValue> data) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<FeatureValue> source = env.fromCollection(data);
        KeyedStream<FeatureValue, Integer> keyed = source.keyBy(
                (KeySelector<FeatureValue, Integer>) FeatureValue::getFeatureId);

        SingleOutputStreamOperator<FeatureDrift> out = keyed
                .process(new PerFeatureHDDMFunction(warmup, lambda, 0.005, 0.001, 2000L, "maxdev"))
                .name("per-feature-hddm");

        out.addSink(new Sink());

        env.execute("PerFeatureHDDM test");
    }
}
