package com.leejean.flink;

import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.FeatureDrift;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * DriftAggregatorFunction MiniCluster 集成测试。
 * k-of-D + refractory + monotonic roundId (HANDOVER §4).
 */
class DriftAggregatorFunctionTest {

    private static final class Sink implements SinkFunction<DriftRoundMessage> {
        static final List<DriftRoundMessage> values = Collections.synchronizedList(new ArrayList<>());
        @Override public void invoke(DriftRoundMessage v, Context ctx) { values.add(v); }
    }

    @BeforeEach
    void setUp() { Sink.values.clear(); }

    /** 单特征 onset 不达 k → 无 COMMITTED. */
    @Test
    void single_feature_below_k_does_not_emit() throws Exception {
        runJob(2, 1000L, 0L, Arrays.asList(
                new FeatureDrift(3, 100L, 0.5),
                new FeatureDrift(3, 200L, 0.6),
                new FeatureDrift(3, 300L, 0.7)
        ));
        assertTrue(Sink.values.isEmpty(),
                "k=2 with only 1 distinct feature must not emit, got " + Sink.values.size());
    }

    /** k=2 个不同特征在 aggWin 内 → 1 个 COMMITTED, roundId=1. */
    @Test
    void two_distinct_features_within_window_emit_one_committed() throws Exception {
        runJob(2, 1000L, 0L, Arrays.asList(
                new FeatureDrift(0, 100L, 0.5),
                new FeatureDrift(1, 500L, 0.6)   // within aggWin=1000 of feature 0
        ));
        assertEquals(1, Sink.values.size(), "should emit exactly 1 COMMITTED");
        DriftRoundMessage drm = Sink.values.get(0);
        assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, drm.getStatus());
        assertEquals(1L, drm.getRoundId(), "first COMMITTED roundId should be 1");
        assertTrue(drm.getVotesYes() >= 2, "nFired should be ≥ k");
    }

    /** 同特征两次 onset → 不算两个 distinct,不触发. */
    @Test
    void duplicate_feature_does_not_count_twice() throws Exception {
        runJob(2, 1000L, 0L, Arrays.asList(
                new FeatureDrift(7, 100L, 0.5),
                new FeatureDrift(7, 500L, 0.6)
        ));
        assertTrue(Sink.values.isEmpty(),
                "same featureId twice must not satisfy k=2");
    }

    /** refractory 抑制紧随其后的触发. */
    @Test
    void refractory_suppresses_immediate_followup() throws Exception {
        runJob(2, 1000L, 10000L, Arrays.asList(
                new FeatureDrift(0, 100L, 0.5),
                new FeatureDrift(1, 200L, 0.6),    // → COMMITTED@200
                new FeatureDrift(2, 300L, 0.7),    // 在 refractory=10000 内 → suppressed
                new FeatureDrift(3, 400L, 0.8)     // still in refractory
        ));
        assertEquals(1, Sink.values.size(),
                "refractory should suppress follow-up triggers, got " + Sink.values.size());
    }

    /** refractory 之后允许再次触发,roundId 单调递增. */
    @Test
    void after_refractory_roundId_monotonic() throws Exception {
        runJob(2, 1000L, 500L, Arrays.asList(
                new FeatureDrift(0, 100L, 0.5),
                new FeatureDrift(1, 200L, 0.6),    // → COMMITTED@200 roundId=1
                new FeatureDrift(2, 1000L, 0.7),   // 距上次 emit 800 > refractory=500
                new FeatureDrift(3, 1100L, 0.8)    // 配对 → COMMITTED@1100 roundId=2
        ));
        assertEquals(2, Sink.values.size(), "should emit 2 COMMITTED rounds");
        assertEquals(1L, Sink.values.get(0).getRoundId());
        assertEquals(2L, Sink.values.get(1).getRoundId());
    }

    // ===== helpers =====

    private static void runJob(int k, long aggWin, long refractory,
                                List<FeatureDrift> data) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<FeatureDrift> source = env.fromCollection(data);
        SingleOutputStreamOperator<DriftRoundMessage> out = source
                .keyBy((KeySelector<FeatureDrift, String>) f -> "global")
                .process(new DriftAggregatorFunction(k, aggWin, refractory))
                .name("drift-aggregator");

        out.addSink(new Sink());

        env.execute("DriftAggregator test");
    }
}
