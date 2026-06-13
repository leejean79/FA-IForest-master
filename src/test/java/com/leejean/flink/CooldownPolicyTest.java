package com.leejean.flink;

import com.leejean.beans.*;
import com.leejean.common_utils.ParallelismKeys;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** 验证 cooldownPolicy=d1a 路径能正常驱动 retrain (HANDOVER §对照开关)。 */
public class CooldownPolicyTest {

    private static final List<ITreeMessage> TREES = Collections.synchronizedList(new ArrayList<>());

    @BeforeEach
    public void setUp() { TREES.clear(); }

    @Test
    public void testD1aPolicyDrivesRetrain() throws Exception {
        runWith("d1a");
        long expectedBatchId = ((long) 0 << 32) | 7L;
        int n = (int) TREES.stream().filter(m -> m.getBatchId() == expectedBatchId).count();
        assertTrue(n >= 2, "d1a path should drive retrain, got " + n);
    }

    @Test
    public void testLegacyPolicyDrivesRetrain() throws Exception {
        runWith("legacy");
        long expectedBatchId = ((long) 0 << 32) | 7L;
        int n = (int) TREES.stream().filter(m -> m.getBatchId() == expectedBatchId).count();
        assertTrue(n >= 2, "legacy path should drive retrain, got " + n);
    }

    private void runWith(String policy) throws Exception {
        int parallelism = 1;
        int maxP = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", "32");
            put("totalTrees", "2");
            put("ringBufferSize", "200");
            put("cooldownSamples", "300");
            put("seed", "42");
            put("cooldownPolicy", policy);
        }});
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxP);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        List<DataPoint> data = new ArrayList<>();
        Random rng = new Random(99);
        for (int i = 0; i < 2000; i++) {
            double[] f = new double[9];
            for (int d = 0; d < 9; d++) f[d] = rng.nextGaussian() * 10 + 50;
            DataPoint dp = new DataPoint(String.valueOf(i), System.currentTimeMillis(), f, 0);
            dp.setOriginalSequence(i);
            data.add(dp);
        }
        DataStream<DataPoint> src = env.fromCollection(data);
        String[] keys = ParallelismKeys.generate(parallelism, maxP);
        KeyedStream<DataPoint, String> ks = src.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // mock forest v1
        ForestMessage mock = buildMockForest();
        DriftRoundMessage cm = new DriftRoundMessage(7L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.COMMITTED, 2, 0, 0);
        List<BroadcastEnvelope> bd = new ArrayList<>();
        bd.add(BroadcastEnvelope.forest(mock));
        bd.add(BroadcastEnvelope.driftRound(cm));
        DataStream<BroadcastEnvelope> bs = env.fromCollection(bd);
        BroadcastStream<BroadcastEnvelope> bcast = bs.broadcast(
                LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> p = ks.connect(bcast)
                .process(new LocalProcessorFunction()).name("LP");
        p.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeCollector());
        p.addSink(new ScoreSinkNoop());
        env.execute("policy=" + policy);
    }

    private ForestMessage buildMockForest() {
        // 引用同测试目录下的 helper —— 改用简版
        Random rng = new Random(123);
        double[][] td = new double[256][9];
        for (int i = 0; i < 256; i++)
            for (int d = 0; d < 9; d++) td[i][d] = rng.nextGaussian() * 10 + 50;
        com.leejean.tree.ITreeBuilder b = new com.leejean.tree.ITreeBuilder(123L);
        List<ITreeMessage> tm = new ArrayList<>();
        for (int t = 0; t < 10; t++) {
            tm.add(new ITreeMessage(UUID.randomUUID().toString(), 0,
                    System.currentTimeMillis(), b.build(td, 256)));
        }
        return new ForestMessage(UUID.randomUUID().toString(), 1L, System.currentTimeMillis(), 256, tm);
    }

    private static class TreeCollector implements SinkFunction<ITreeMessage> {
        @Override public void invoke(ITreeMessage v, Context ctx) { TREES.add(v); }
    }

    private static class ScoreSinkNoop implements SinkFunction<ScoreResult> {
        @Override public void invoke(ScoreResult v, Context ctx) { }
    }
}
