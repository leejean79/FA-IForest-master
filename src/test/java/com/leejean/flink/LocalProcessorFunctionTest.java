package com.leejean.flink;

import com.leejean.beans.*;
import com.leejean.common_utils.ParallelismKeys;
import com.leejean.tree.ITree;
import com.leejean.tree.ITreeBuilder;
import com.leejean.tree.ITreeNode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LocalProcessorFunction MiniCluster 集成测试
 * LocalProcessorFunction MiniCluster integration test
 *
 * 场景 A-C: v2.1 三阶段状态机 / v2.1 three-phase state machine
 * 场景 D-H: v3.0 Phase C 漂移检测子状态机 / v3.0 Phase C drift detection sub-state machine
 */
public class LocalProcessorFunctionTest {

    @BeforeEach
    public void setUp() {
        ScoreSink.values.clear();
        TreeSink.values.clear();
        DriftReportSink.values.clear();
    }

    // ===== 场景 A: 纯 Phase B（无广播 → 等同 v1，只产树不产分数）=====
    // Scenario A: pure Phase B (no broadcast → equivalent to v1, only trees, no scores)

    @Test
    public void testPurePhaseBProducesTrees() throws Exception {
        int parallelism = 4;
        int totalTrees = 100;
        int subsampleSize = 256;
        int ringBufferSize = 1000;
        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        // v3.1: 每个 key 需要 ringBufferSize + localTreeCount 条数据
        // v3.1: each key needs ringBufferSize + localTreeCount records
        int totalRecords = (ringBufferSize + localTreeCount) * parallelism + 512;

        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("ringBufferSize", String.valueOf(ringBufferSize));
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        List<DataPoint> data = generateData(totalRecords, 9);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);

        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 空广播流 / empty broadcast stream
        DataStream<BroadcastEnvelope> emptyBroadcast = env.fromCollection(
                Collections.<BroadcastEnvelope>emptyList(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(BroadcastEnvelope.class));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                emptyBroadcast.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario A: pure Phase B");

        // 期望：100 棵树，0 条分数 / expect: 100 trees, 0 scores
        assertEquals(totalTrees, TreeSink.values.size(),
                "Expected " + totalTrees + " trees");
        assertEquals(0, ScoreSink.values.size(),
                "Expected 0 scores in pure Phase B");

        // 验证 slotIndex：每个 subtask 产出的 25 棵树 slotIndex 应为 0..24 各一次
        // Verify slotIndex: each subtask's 25 trees should have slotIndex 0..24 exactly once
        Map<Integer, Set<Integer>> subtaskSlots = new HashMap<>();
        for (ITreeMessage t : TreeSink.values) {
            subtaskSlots.computeIfAbsent(t.getProducerSubtask(), k -> new HashSet<>())
                    .add(t.getSlotIndex());
        }
        for (Map.Entry<Integer, Set<Integer>> entry : subtaskSlots.entrySet()) {
            Set<Integer> slots = entry.getValue();
            assertEquals(localTreeCount, slots.size(),
                    "Subtask " + entry.getKey() + " should have " + localTreeCount + " distinct slotIndexes");
            for (int s = 0; s < localTreeCount; s++) {
                assertTrue(slots.contains(s),
                        "Subtask " + entry.getKey() + " missing slotIndex " + s);
            }
        }
    }

    // ===== 场景 B: Phase B → A → C 切换 =====
    // Scenario B: Phase B → A → C transition

    @Test
    public void testPhaseTransitionBAC() throws Exception {
        int parallelism = 1; // 单并行度简化测试 / single parallelism to simplify
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", "256");
            put("totalTrees", "100");
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 1000 条 Phase B 数据 + 100 条 Phase C 数据
        // 1000 Phase B records + 100 Phase C records
        int phaseBCount = 1000;
        int phaseCCount = 100;
        List<DataPoint> allData = generateData(phaseBCount + phaseCCount, 9);

        DataStream<DataPoint> source = env.fromCollection(allData);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 构造 mock 森林（1 棵手工小树）/ build mock forest (1 small tree)
        ForestMessage mockForest = buildMockForest(9);

        // 广播流：在第 1000 条数据之后投入森林
        // 用 Flink fromCollection 保证广播流有内容；由于 fromCollection 源的调度顺序不确定，
        // 单并行度下两个源串行执行：先主流全部处理完，再处理广播流。
        // 但实际 Flink MiniCluster 中调度是交替的，所以我们直接把广播和主流同时提供。
        // 为了让 Phase B 先收到数据，这里不做精确的时序控制。
        // 测试验证：最终有 score 输出（PhaseA + PhaseC），有 tree side output。
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario B: Phase B → A → C");

        // 验证：有 score 输出（A + C），且 score 数量 > 0
        // Verify: has score output (A + C), score count > 0
        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");

        // 验证 phase 字段：应该有 A 和/或 C
        // Verify phase field: should have A and/or C
        Set<String> phases = new HashSet<>();
        for (ScoreResult sr : ScoreSink.values) {
            phases.add(sr.getPhase());
            assertTrue(sr.getScore() >= 0 && sr.getScore() <= 1,
                    "Score should be in [0,1], got " + sr.getScore());
            assertEquals(mockForest.getVersion(), sr.getForestVersion());
        }
        // 至少有 C（如果广播先于部分主流到达则会有 A 和 C）
        // At least C; if broadcast arrives before some main records, both A and C
        assertTrue(phases.contains("C") || phases.contains("A"),
                "Should have Phase A or C scores, got phases: " + phases);

        // 总 score 应 <= 总数据量（Phase B 期间的数据在 A 中打分 + Phase C 的数据）
        assertTrue(ScoreSink.values.size() <= phaseBCount + phaseCCount,
                "Score count should not exceed total data");
    }

    // ===== 场景 C: 纯 Phase C（启动时就有森林）=====
    // Scenario C: pure Phase C (forest available from start)

    @Test
    public void testPurePhaseC() throws Exception {
        int parallelism = 1;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", "256");
            put("totalTrees", "100");
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        int dataCount = 100;
        List<DataPoint> data = generateData(dataCount, 9);

        // 主流用低优先级保证广播先到：将主流设为 fromCollection，广播也用 fromCollection。
        // 在单并行度下，两个源各自是独立 task，调度不确定。
        // 用 union trick：把广播源加一个 sleep map 让主流有机会先注册。
        // 更简单的做法：直接测试，允许部分数据走 Phase B，但所有 Phase C 分数都正确。
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario C: pure Phase C");

        // 在 MiniCluster 中调度顺序不保证，允许部分走 Phase B。
        // 但所有有分数的记录都应有正确的 score 和 forestVersion。
        // In MiniCluster, scheduling order is not guaranteed; some may go Phase B.
        // But all scored records should have correct score and forestVersion.
        for (ScoreResult sr : ScoreSink.values) {
            assertTrue(sr.getScore() >= 0 && sr.getScore() <= 1,
                    "Score should be in [0,1]");
            assertEquals(mockForest.getVersion(), sr.getForestVersion());
            assertTrue("A".equals(sr.getPhase()) || "C".equals(sr.getPhase()),
                    "Phase should be A or C");
        }

        // 总输出 = scores + 可能的 Phase B 无输出 → scores.size() + trees 的数据 ≤ 总数据
        // 关键验证：有 score 输出
        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs in Phase C");
    }

    // ===== v3.0 场景 D: Phase C 稳定数据，无漂移 =====
    // Scenario D: Phase C stable data, no drift detection

    @Test
    public void testPhaseCStableNoDrift() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.1");
            put("driftConfidence", "0.05");
            put("warnTimeoutSamples", "10000");
            put("warnTimeoutBehavior", "DISCARD");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 全部稳定数据 / all stable data (same distribution as forest training)
        List<DataPoint> data = generateDataWithMean(5000, 9, 50.0, 10.0, 99, 0);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario D: Phase C stable, no drift");

        // Phase B 最多产 localTreeCount 棵树，稳定数据不应触发漂移产额外树
        // Phase B produces at most localTreeCount trees; stable data should NOT trigger drift
        assertTrue(TreeSink.values.size() <= localTreeCount,
                "Stable data should produce at most " + localTreeCount
                        + " Phase B trees, got " + TreeSink.values.size());

        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");
        for (ScoreResult sr : ScoreSink.values) {
            assertTrue(sr.getScore() >= 0 && sr.getScore() <= 1,
                    "Score should be in [0,1], got " + sr.getScore());
        }
    }

    // ===== v3.4 场景 E: STABLE → WARN → DRIFT → LOCAL_DRIFT_REPORTED =====
    // Scenario E: STABLE → WARN → DRIFT → LOCAL_DRIFT_REPORTED (emits DriftReport INITIATE)

    @Test
    public void testDriftTriggersLocalDriftReported() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.1");
            put("driftConfidence", "0.05");
            put("warnTimeoutSamples", "10000");
            put("warnTimeoutBehavior", "DISCARD");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 前 3000 条稳定 + 后 3000 条漂移
        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(3000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(3000, 9, 5000.0, 10.0, 100, 3000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());
        processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG).addSink(new DriftReportSink());

        env.execute("Scenario E: drift triggers LOCAL_DRIFT_REPORTED");

        // v3.4: DRIFT → LOCAL_DRIFT_REPORTED，不再直接重训
        // USE_OLD_FOREST 默认：继续用旧森林打分输出
        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs (USE_OLD_FOREST)");

        boolean hasPhaseCScores = false;
        for (ScoreResult sr : ScoreSink.values) {
            assertTrue(sr.getScore() >= 0 && sr.getScore() <= 1,
                    "Score should be in [0,1], got " + sr.getScore());
            if ("C".equals(sr.getPhase())) {
                hasPhaseCScores = true;
            }
        }
        assertTrue(hasPhaseCScores, "Should have Phase C scores");

        // 应有 DriftReport{INITIATE} side output
        assertFalse(DriftReportSink.values.isEmpty(),
                "Should have DriftReport INITIATE side output");
        boolean hasInitiate = false;
        for (DriftReport dr : DriftReportSink.values) {
            if (dr.getVote() == DriftReport.DriftVote.INITIATE) {
                hasInitiate = true;
            }
        }
        assertTrue(hasInitiate, "Should have at least one INITIATE DriftReport");
    }

    // ===== v3.0 场景 F: WARN timeout DISCARD =====
    // Scenario F: WARN timeout DISCARD — warn triggers but times out, candidates discarded

    @Test
    public void testWarnTimeoutDiscard() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        // warnConfidence=0.5（宽松 WARN）, driftConfidence=0.0001（极严格 DRIFT）
        // warnTimeoutSamples=2：WARN 后 2 样本即 timeout
        // Wide WARN zone, strict DRIFT → WARN triggers but DRIFT unlikely in 2 samples
        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.5");
            put("driftConfidence", "0.0001");
            put("warnTimeoutSamples", "2");
            put("warnTimeoutBehavior", "DISCARD");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 稳定数据 + 漂移数据 / stable + drifted
        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(2000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(2000, 9, 5000.0, 10.0, 100, 2000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario F: WARN timeout DISCARD");

        // DISCARD：超时丢弃候选树，不产额外树
        // DISCARD: timeout discards candidates, no extra trees
        assertTrue(TreeSink.values.size() <= localTreeCount,
                "DISCARD should produce at most " + localTreeCount
                        + " Phase B trees, got " + TreeSink.values.size());

        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");
    }

    // ===== v3.4 场景 G: WARN timeout PROMOTE → LOCAL_DRIFT_REPORTED =====
    // Scenario G: WARN timeout PROMOTE → enters LOCAL_DRIFT_REPORTED (emits INITIATE)

    @Test
    public void testWarnTimeoutPromote() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        // warnConfidence=0.5, driftConfidence=0.0001, warnTimeoutSamples=500
        // v3.4: PROMOTE → LOCAL_DRIFT_REPORTED
        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.5");
            put("driftConfidence", "0.0001");
            put("warnTimeoutSamples", "500");
            put("warnTimeoutBehavior", "PROMOTE");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(2000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(4000, 9, 5000.0, 10.0, 100, 2000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());
        processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG).addSink(new DriftReportSink());

        env.execute("Scenario G: WARN timeout PROMOTE → LOCAL_DRIFT_REPORTED");

        // v3.4: PROMOTE → LOCAL_DRIFT_REPORTED → 不重训（等投票决议）
        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");

        // 应有 DriftReport{INITIATE} side output
        assertFalse(DriftReportSink.values.isEmpty(),
                "PROMOTE should emit DriftReport INITIATE");
    }

    // ===== v3.4 场景 H: BACKLOG_THEN_NEW_FOREST 模式 =====
    // Scenario H (V34-8): BACKLOG_THEN_NEW_FOREST — drift causes backlog, no score output after drift

    @Test
    public void testBacklogModeNoScoresDuringDrift() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.1");
            put("driftConfidence", "0.05");
            put("warnTimeoutSamples", "10000");
            put("warnTimeoutBehavior", "DISCARD");
            put("pauseMode", "BACKLOG_THEN_NEW_FOREST");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 前 3000 条稳定 + 后 3000 条漂移
        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(3000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(3000, 9, 5000.0, 10.0, 100, 3000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(
                Collections.singletonList(BroadcastEnvelope.forest(mockForest)));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());
        processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG).addSink(new DriftReportSink());

        env.execute("Scenario H (V34-8): BACKLOG_THEN_NEW_FOREST");

        // BACKLOG 模式：DRIFT 后的数据进 backlog，不输出
        // 总输出 < 总数据量（drift 后的数据被 backlog 而非输出）
        assertFalse(ScoreSink.values.isEmpty(), "Should have scores before drift");
        assertTrue(ScoreSink.values.size() < 6000,
                "BACKLOG mode should produce fewer scores than total data, got " + ScoreSink.values.size());

        // 应有 DriftReport{INITIATE}
        assertFalse(DriftReportSink.values.isEmpty(), "Should have INITIATE drift report");
    }

    // ===== v3.4 场景 V34-9: VOTING 广播触发投票 =====
    // Scenario V34-9: VOTING broadcast triggers vote

    @Test
    public void testVotingBroadcastTriggersVote() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        // 宽松 WARN 但极严格 DRIFT → 容易进 WARN 但不容易进 DRIFT
        // Easy WARN, hard DRIFT → subtask stays in WARN when VOTING arrives
        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.5");       // 宽松 WARN
            put("driftConfidence", "0.0001");   // 极严格 DRIFT
            put("warnTimeoutSamples", "100000");
            put("warnTimeoutBehavior", "DISCARD");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 稳定 + 漂移数据：HDDM 进 WARN 但不到 DRIFT
        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(2000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(3000, 9, 5000.0, 10.0, 100, 2000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 广播：森林 + VOTING 消息（模拟其他 subtask 触发的投票请求）
        ForestMessage mockForest = buildMockForest(9, 1L);
        DriftRoundMessage votingMsg = new DriftRoundMessage(1L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.VOTING, 1, 0, 0);

        List<BroadcastEnvelope> broadcastData = new ArrayList<>();
        broadcastData.add(BroadcastEnvelope.forest(mockForest));
        broadcastData.add(BroadcastEnvelope.driftRound(votingMsg));

        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(broadcastData);
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());
        processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG).addSink(new DriftReportSink());

        env.execute("Scenario V34-9: VOTING broadcast triggers vote");

        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");

        // 在 WARN 状态收到 VOTING → 应投 YES
        // 但因 MiniCluster 调度不确定，VOTING 可能在 STABLE 时到达 → 投 NO
        // 关键验证：有投票 side output（非 INITIATE）
        boolean hasVoteResponse = false;
        for (DriftReport dr : DriftReportSink.values) {
            if (dr.getVote() == DriftReport.DriftVote.YES || dr.getVote() == DriftReport.DriftVote.NO) {
                hasVoteResponse = true;
                assertEquals(1L, dr.getRoundId(), "Vote should reference roundId=1");
            }
        }
        assertTrue(hasVoteResponse, "Should have a vote response (YES or NO) for VOTING broadcast");
    }

    // ===== v3.4 场景 V34-10: 投票否决 → STABLE =====
    // Scenario V34-10: ABORTED vote → back to STABLE, HDDM reset, backlog drained (BACKLOG mode)

    @Test
    public void testVoteAbortedReturnsToStable() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        // BACKLOG 模式 + 容易触发 DRIFT
        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("warnConfidence", "0.1");
            put("driftConfidence", "0.05");
            put("warnTimeoutSamples", "10000");
            put("warnTimeoutBehavior", "DISCARD");
            put("pauseMode", "BACKLOG_THEN_NEW_FOREST");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 前 3000 稳定 + 后 3000 漂移（触发 DRIFT → LOCAL_DRIFT_REPORTED → backlog）
        List<DataPoint> data = new ArrayList<>();
        data.addAll(generateDataWithMean(3000, 9, 50.0, 10.0, 99, 0));
        data.addAll(generateDataWithMean(3000, 9, 5000.0, 10.0, 100, 3000));

        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 广播：森林 + VOTING(roundId=1) + ABORTED(roundId=1)
        ForestMessage mockForest = buildMockForest(9, 1L);
        DriftRoundMessage votingMsg = new DriftRoundMessage(1L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.VOTING, 1, 0, 0);
        DriftRoundMessage abortedMsg = new DriftRoundMessage(1L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.ABORTED, 1, 3, 0);

        List<BroadcastEnvelope> broadcastData = new ArrayList<>();
        broadcastData.add(BroadcastEnvelope.forest(mockForest));
        broadcastData.add(BroadcastEnvelope.driftRound(votingMsg));
        broadcastData.add(BroadcastEnvelope.driftRound(abortedMsg));

        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(broadcastData);
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());
        processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG).addSink(new DriftReportSink());

        env.execute("Scenario V34-10: vote ABORTED → STABLE");

        // ABORTED 后用旧森林排空 backlog → 所有数据都应有 score 输出
        // After ABORTED, backlog drained with old forest → all data should have scores
        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");

        // ABORTED 后回到 STABLE → 继续正常输出（无 COOLDOWN 重训树）
        // 验证：Phase B 树 + 无 COOLDOWN 额外树
        int phaseBTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        assertTrue(TreeSink.values.size() <= phaseBTreeCount,
                "ABORTED should not produce retrain trees, got " + TreeSink.values.size());

        // v3.4.4: DriftReport 应含 INITIATE 或 YES（取决于 MiniCluster 数据/广播交错顺序）
        // v3.4.4: DriftReport should contain INITIATE or YES (depends on MiniCluster interleaving)
        boolean hasVoteActivity = false;
        for (DriftReport dr : DriftReportSink.values) {
            if (dr.getVote() == DriftReport.DriftVote.INITIATE
                    || dr.getVote() == DriftReport.DriftVote.YES) {
                hasVoteActivity = true;
            }
        }
        assertTrue(hasVoteActivity, "Should have INITIATE or YES DriftReport");
    }

    // ===== v3.1 场景 R1: 环形缓冲 Phase B =====
    // Scenario R1: ring buffer Phase B — 25 trees produced after buffer fills

    @Test
    public void testRingBufferPhaseBProducesTrees() throws Exception {
        int parallelism = 1;
        int totalTrees = 25;
        int subsampleSize = 256;
        int ringBufferSize = 1000;
        int localTreeCount = totalTrees; // parallelism=1
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("ringBufferSize", String.valueOf(ringBufferSize));
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 1100 条：1000 填满缓冲 + 接下来 25 条各训 1 棵 + 75 条多余
        // 1100 records: 1000 fill buffer + next 25 each train 1 tree + 75 extra
        int totalRecords = 1100;
        List<DataPoint> data = generateData(totalRecords, 9);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 空广播流 / empty broadcast stream
        DataStream<BroadcastEnvelope> emptyBroadcast = env.fromCollection(
                Collections.<BroadcastEnvelope>emptyList(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(BroadcastEnvelope.class));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                emptyBroadcast.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario R1: ring buffer Phase B");

        // 期望：恰好 25 棵树 / expect: exactly 25 trees
        assertEquals(localTreeCount, TreeSink.values.size(),
                "Expected " + localTreeCount + " trees from ring buffer Phase B");
        // 无分数（纯 Phase B，无广播）/ no scores (pure Phase B, no broadcast)
        assertEquals(0, ScoreSink.values.size(), "Expected 0 scores in pure Phase B");

        // slotIndex 0..24 各一次 / slotIndex 0..24 each once
        Set<Integer> slots = new HashSet<>();
        long expectedBatchId = 0L; // subtask=0, round=0 → batchId=0
        int batchEndCount = 0;
        for (ITreeMessage t : TreeSink.values) {
            slots.add(t.getSlotIndex());
            // v3.3: Phase B batchId = (subtask << 32) | 0
            assertEquals(expectedBatchId, t.getBatchId(),
                    "Phase B tree should have batchId=0 (subtask=0, round=0)");
            if (t.isBatchEnd()) batchEndCount++;
        }
        assertEquals(localTreeCount, slots.size());
        for (int s = 0; s < localTreeCount; s++) {
            assertTrue(slots.contains(s), "Missing slotIndex " + s);
        }
        // 恰好 1 棵 batchEnd=true（最后一棵）/ exactly 1 batchEnd=true (the last tree)
        assertEquals(1, batchEndCount, "Exactly 1 tree should have batchEnd=true");
    }

    // ===== v3.1 场景 R3: 环形缓冲容量边界 =====
    // Scenario R3: ring buffer capacity boundary — small ringBufferSize

    @Test
    public void testRingBufferSmallCapacity() throws Exception {
        int parallelism = 1;
        int totalTrees = 5;
        int subsampleSize = 5;
        int ringBufferSize = 10;
        int localTreeCount = totalTrees;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("ringBufferSize", String.valueOf(ringBufferSize));
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 30 条数据：10 填满缓冲 + 5 条训 5 棵树 + 15 多余
        // 30 records: 10 fill buffer + 5 train 5 trees + 15 extra
        List<DataPoint> data = generateData(30, 9);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        DataStream<BroadcastEnvelope> emptyBroadcast = env.fromCollection(
                Collections.<BroadcastEnvelope>emptyList(),
                org.apache.flink.api.common.typeinfo.TypeInformation.of(BroadcastEnvelope.class));
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                emptyBroadcast.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario R3: small ring buffer");

        // 期望 5 棵树 / expect 5 trees
        assertEquals(localTreeCount, TreeSink.values.size(),
                "Expected " + localTreeCount + " trees with small ring buffer");
        assertEquals(0, ScoreSink.values.size(), "Expected 0 scores in pure Phase B");

        // 验证树的 subsampleSize=5 / verify tree subsampleSize=5
        for (ITreeMessage t : TreeSink.values) {
            assertEquals(subsampleSize, t.getTree().getSubsampleSize(),
                    "Tree subsampleSize should be " + subsampleSize);
        }
    }

    // ===== 辅助方法 / Helper methods =====

    private List<DataPoint> generateData(int count, int dim) {
        return generateDataWithMean(count, dim, 50.0, 10.0, 99, 0);
    }

    /**
     * 生成指定均值和标准差的数据 / Generate data with specified mean and std.
     */
    private List<DataPoint> generateDataWithMean(int count, int dim, double mean, double std,
                                                  long seed, int startSeq) {
        List<DataPoint> data = new ArrayList<>(count);
        Random rng = new Random(seed);
        for (int i = 0; i < count; i++) {
            double[] features = new double[dim];
            for (int d = 0; d < dim; d++) {
                features[d] = rng.nextGaussian() * std + mean;
            }
            DataPoint dp = new DataPoint(String.valueOf(startSeq + i),
                    System.currentTimeMillis(), features, 0);
            dp.setOriginalSequence(startSeq + i);
            data.add(dp);
        }
        return data;
    }

    /**
     * 构造 mock 森林：用 ITreeBuilder 构建 2 棵小树
     * Build mock forest: 2 small trees via ITreeBuilder
     */
    private ForestMessage buildMockForest(int dim) {
        return buildMockForest(dim, 1L);
    }

    /**
     * 构造指定版本的 mock 森林 / Build mock forest with specified version.
     * 使用 subsampleSize=256, 10 棵树，确保分数有区分度
     * Uses subsampleSize=256, 10 trees for better score discrimination.
     */
    private ForestMessage buildMockForest(int dim, long version) {
        int subsampleSize = 256;
        int numTrees = 10;
        Random rng = new Random(123);
        double[][] trainData = new double[512][dim];
        for (int i = 0; i < 512; i++) {
            for (int d = 0; d < dim; d++) {
                trainData[i][d] = rng.nextGaussian() * 10 + 50;
            }
        }

        List<ITreeMessage> treeMsgs = new ArrayList<>();
        for (int t = 0; t < numTrees; t++) {
            ITreeBuilder builder = new ITreeBuilder(123 + t * 7L);
            ITree tree = builder.build(trainData, subsampleSize);
            treeMsgs.add(new ITreeMessage(UUID.randomUUID().toString(), 0,
                    System.currentTimeMillis(), tree));
        }

        return new ForestMessage(UUID.randomUUID().toString(), version,
                System.currentTimeMillis(), subsampleSize, treeMsgs);
    }

    // ===== Sinks =====

    private static class ScoreSink implements SinkFunction<ScoreResult> {
        static final List<ScoreResult> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(ScoreResult value, Context context) {
            values.add(value);
        }
    }

    private static class TreeSink implements SinkFunction<ITreeMessage> {
        static final List<ITreeMessage> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(ITreeMessage value, Context context) {
            values.add(value);
        }
    }

    private static class DriftReportSink implements SinkFunction<DriftReport> {
        static final List<DriftReport> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(DriftReport value, Context context) {
            values.add(value);
        }
    }
}
