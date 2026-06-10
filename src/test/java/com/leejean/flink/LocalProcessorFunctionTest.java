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
 * LocalProcessorFunction MiniCluster 集成测试（方向二(a) Phase 3）。
 * Scoring-side only — detection lives on a separate column-parallel pipeline
 * and is exercised by its own tests (FeatureSplit / PerFeatureIKS / Aggregator).
 *
 * <p>场景：
 * <ul>
 *   <li>A/B/C：三阶段状态机（Phase B 训树 / Phase A 排空 / Phase C 打分）</li>
 *   <li>D：Phase C 稳定数据 — 无 detector，仅打分</li>
 *   <li>E：COMMITTED 广播 → COOLDOWN → 重训 → WAITING → STABLE</li>
 *   <li>H：BACKLOG_THEN_NEW_FOREST 模式下 COMMITTED 后数据进 backlog</li>
 *   <li>R1/R3：环形缓冲容量边界</li>
 * </ul>
 */
public class LocalProcessorFunctionTest {

    @BeforeEach
    public void setUp() {
        ScoreSink.values.clear();
        TreeSink.values.clear();
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

    // ===== 场景 E: COMMITTED 广播 → COOLDOWN → 重训 → WAITING → STABLE =====
    // Scenario E: COMMITTED broadcast drives COOLDOWN → retrain → WAITING → STABLE.

    @Test
    public void testCommittedTriggersCooldownAndRetrain() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int ringBufferSize = 200;
        int cooldownSamples = 300;
        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("ringBufferSize", String.valueOf(ringBufferSize));
            put("cooldownSamples", String.valueOf(cooldownSamples));
            put("seed", "42");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        // 稳定数据，足够让 COOLDOWN 满足终止条件并触发重训
        List<DataPoint> data = generateDataWithMean(2000, 9, 50.0, 10.0, 99, 0);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        // 广播：森林 v1 + COMMITTED(roundId=7)
        ForestMessage mockForest = buildMockForest(9, 1L);
        DriftRoundMessage committedMsg = new DriftRoundMessage(7L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.COMMITTED, 2, 0, 0);

        List<BroadcastEnvelope> broadcastData = new ArrayList<>();
        broadcastData.add(BroadcastEnvelope.forest(mockForest));
        broadcastData.add(BroadcastEnvelope.driftRound(committedMsg));

        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(broadcastData);
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario E: COMMITTED triggers COOLDOWN + retrain");

        assertFalse(ScoreSink.values.isEmpty(), "Should have score outputs");

        // COMMITTED → COOLDOWN → 终止条件满足 → 重训：batchId 低 32 位 = roundId=7
        long expectedBatchId = ((long) 0 << 32) | 7L;
        int retrainTrees = 0;
        for (ITreeMessage msg : TreeSink.values) {
            if (msg.getBatchId() == expectedBatchId) retrainTrees++;
        }
        assertTrue(retrainTrees >= localTreeCount,
                "COMMITTED should drive retrain batch (batchId=" + expectedBatchId
                        + ", localTreeCount=" + localTreeCount + ") got " + retrainTrees);
    }

    // ===== 场景 H: BACKLOG_THEN_NEW_FOREST 模式 — COMMITTED 后数据进 backlog =====
    // Scenario H: BACKLOG mode — after COMMITTED, COOLDOWN/WAITING points go to backlog
    // (not emitted as scores) until a new forest drains them.

    @Test
    public void testBacklogModeDeferredOnCommitted() throws Exception {
        int parallelism = 1;
        int totalTrees = 2;
        int subsampleSize = 32;
        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);

        ParameterTool params = ParameterTool.fromMap(new HashMap<String, String>() {{
            put("subsampleSize", String.valueOf(subsampleSize));
            put("totalTrees", String.valueOf(totalTrees));
            put("seed", "42");
            put("pauseMode", "BACKLOG_THEN_NEW_FOREST");
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setGlobalJobParameters(params);

        List<DataPoint> data = generateDataWithMean(2000, 9, 50.0, 10.0, 99, 0);
        DataStream<DataPoint> source = env.fromCollection(data);

        final String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);
        KeyedStream<DataPoint, String> keyedStream = source.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        keys[Math.abs(dp.getId().hashCode() % keys.length)]);

        ForestMessage mockForest = buildMockForest(9, 1L);
        DriftRoundMessage committedMsg = new DriftRoundMessage(1L, System.currentTimeMillis(),
                DriftRoundMessage.RoundStatus.COMMITTED, 2, 0, 0);

        List<BroadcastEnvelope> broadcastData = new ArrayList<>();
        broadcastData.add(BroadcastEnvelope.forest(mockForest));
        broadcastData.add(BroadcastEnvelope.driftRound(committedMsg));

        DataStream<BroadcastEnvelope> envelopeStream = env.fromCollection(broadcastData);
        BroadcastStream<BroadcastEnvelope> broadcastStream =
                envelopeStream.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor");

        processed.addSink(new ScoreSink());
        processed.getSideOutput(LocalProcessorFunction.TREE_TAG).addSink(new TreeSink());

        env.execute("Scenario H: BACKLOG mode after COMMITTED");

        // BACKLOG 模式下 COMMITTED 之后的 COOLDOWN/WAITING 期数据进 backlog → 总打分数少于输入
        // (MiniCluster 中广播可能在主流之前就位，极端时 0 score 全 backlog —— 仍满足 < input)
        assertTrue(ScoreSink.values.size() < data.size(),
                "BACKLOG mode should produce fewer scores than input (some buffered), got "
                        + ScoreSink.values.size() + "/" + data.size());
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

}
