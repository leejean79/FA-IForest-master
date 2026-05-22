package com.leejean.flink;

import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.leejean.tree.ITree;
import com.leejean.tree.ITreeBuilder;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CoordinatorFunction MiniCluster 集成测试
 * CoordinatorFunction MiniCluster integration tests
 */
public class CoordinatorFunctionTest {

    private static final int PARALLELISM = 4;
    private static final int TOTAL_TREES = 100;
    private static final int LOCAL_TREE_COUNT = 25; // ceil(100/4)
    private static final int EXPECTED_SLOTS = 100;  // 4 * 25
    private static final int SUBSAMPLE_SIZE = 256;

    @BeforeEach
    public void setUp() {
        ForestSink.values.clear();
    }

    /**
     * 场景 1：基本拼装——100 棵树全齐（每个 subtask 最后一棵 batchEnd=true）→ 产出 1 条 ForestMessage v1
     * Scenario 1: basic assembly — all 100 trees with batchEnd → emit 1 ForestMessage v1
     */
    @Test
    public void testBasicAssembly() throws Exception {
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);
        assertEquals(EXPECTED_SLOTS, trees.size());

        runCoordinator(trees);

        assertEquals(1, ForestSink.values.size(), "Should emit exactly 1 forest");
        ForestMessage fm = ForestSink.values.get(0);
        assertEquals(1L, fm.getVersion());
        assertEquals(EXPECTED_SLOTS, fm.getTrees().size());
        assertEquals(SUBSAMPLE_SIZE, fm.getSubsampleSize());
    }

    /**
     * 场景 2：99 棵不触发（缺少最后一个 subtask 的最后一棵 batchEnd=true）
     * Scenario 2: 99 trees — should NOT trigger (missing the batchEnd tree)
     */
    @Test
    public void testIncompleteDoesNotTrigger() throws Exception {
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);
        // 移除最后一棵（它是 batchEnd=true 的）/ remove the last one (which has batchEnd=true)
        trees.remove(trees.size() - 1);
        assertEquals(EXPECTED_SLOTS - 1, trees.size());

        runCoordinator(trees);

        assertEquals(0, ForestSink.values.size(), "Should not emit forest with 99 trees");
    }

    /**
     * 场景 3：v3.4 单个 subtask 覆盖不同 round → 不触发（round 不一致）
     * Scenario 3: v3.4 single subtask overwrite with different round → no v2 (round mismatch)
     */
    @Test
    public void testSingleSubtaskOverwriteDoesNotTriggerV2() throws Exception {
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);

        // 追加一棵覆盖 (subtask=0, slot=0) 的新树（round=1），但其余 subtask 仍 round=0
        ITree newTree = new ITreeBuilder(999L).build(generateData(SUBSAMPLE_SIZE, 9, 999L), SUBSAMPLE_SIZE);
        long batchId = ((long) 0 << 32) | 1L;  // subtask=0, round=1
        ITreeMessage replacement = new ITreeMessage(
                UUID.randomUUID().toString(), 0, System.currentTimeMillis(), 0,
                batchId, true, newTree);
        trees.add(replacement);

        runCoordinator(trees);

        // v3.4: round 不一致（subtask=0 round=1, 其余 round=0）→ 只触发 v1
        assertEquals(1, ForestSink.values.size(), "v3.4: round mismatch should only emit v1");
        assertEquals(1L, ForestSink.values.get(0).getVersion());
    }

    /**
     * 场景 4：重复 batchEnd=true 消息仍触发
     * Scenario 4: duplicate batchEnd message still triggers
     */
    @Test
    public void testDuplicateMessageTriggers() throws Exception {
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);

        // 找到一个 batchEnd=true 的消息复制 / find a batchEnd=true message to duplicate
        ITreeMessage duplicate = null;
        for (ITreeMessage t : trees) {
            if (t.isBatchEnd()) { duplicate = t; break; }
        }
        assertNotNull(duplicate);
        trees.add(duplicate);

        runCoordinator(trees);

        // 重复 batchEnd=true 消息 + dirty=true → 触发第 2 次
        assertEquals(2, ForestSink.values.size(),
                "duplicate batchEnd put triggers second forest");
    }

    /**
     * 场景 5：subsampleSize 不一致 → 不触发
     * Scenario 5: inconsistent subsampleSize → no forest emitted
     */
    @Test
    public void testInconsistentSubsampleSizeSkips() throws Exception {
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);

        // 替换最后一棵为 subsampleSize=128 的树（保持 batchEnd=true）
        trees.remove(trees.size() - 1);
        ITree badTree = new ITreeBuilder(777L).build(generateData(128, 9, 777L), 128);
        long batchId = ((long) (PARALLELISM - 1)) << 32;
        ITreeMessage badMsg = new ITreeMessage(
                UUID.randomUUID().toString(), PARALLELISM - 1, System.currentTimeMillis(),
                LOCAL_TREE_COUNT - 1, batchId, true, badTree);
        trees.add(badMsg);

        runCoordinator(trees);

        assertEquals(0, ForestSink.values.size(),
                "Should not emit forest when subsampleSize is inconsistent");
    }

    /**
     * 场景 6：超出范围的 subtask/slot → 跳过，不崩溃
     * Scenario 6: out-of-range subtask/slot → skipped, no crash
     */
    @Test
    public void testOutOfRangeSkipped() throws Exception {
        List<ITreeMessage> trees = new ArrayList<>();

        ITree tree = new ITreeBuilder(42L).build(generateData(SUBSAMPLE_SIZE, 9, 42L), SUBSAMPLE_SIZE);
        ITreeMessage illegal = new ITreeMessage(
                UUID.randomUUID().toString(), 99, System.currentTimeMillis(), 0,
                0L, true, tree);
        trees.add(illegal);

        runCoordinator(trees);

        assertEquals(0, ForestSink.values.size(),
                "Should not emit forest for out-of-range subtask");
    }

    // ===== v3.3 场景 V33-1: 全部 batchEnd=false → 不触发 =====

    @Test
    public void testAllBatchEndFalseDoesNotTrigger() throws Exception {
        List<ITreeMessage> trees = buildAllTreesNoBatchEnd(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);
        assertEquals(EXPECTED_SLOTS, trees.size());

        runCoordinator(trees);

        assertEquals(0, ForestSink.values.size(),
                "All batchEnd=false should produce 0 ForestMessage");
    }

    // ===== v3.3 场景 V33-2: 单批次完成才触发 =====

    @Test
    public void testSingleBatchCompletionTriggers() throws Exception {
        // 4 个 subtask 各 25 棵，每个 subtask 最后一棵 batchEnd=true
        // 只有第 4 个 subtask 的 batchEnd 到达时 100 个 slot 全填齐 → 触发 v1
        List<ITreeMessage> trees = buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE);

        runCoordinator(trees);

        // 第 4 个 batchEnd 到达时触发 v1
        assertEquals(1, ForestSink.values.size(), "Should emit exactly 1 forest when all batches complete");
        assertEquals(1L, ForestSink.values.get(0).getVersion());
    }

    // ===== v3.4 场景 V34-C1: 全局重训（所有 subtask 同 round）触发 v2 =====

    @Test
    public void testGlobalRetrainTriggersNewVersion() throws Exception {
        // 先拼出完整 v1 森林（round=0）
        List<ITreeMessage> allTrees = new ArrayList<>(buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE));

        // v3.4: 所有 subtask 都发 round=1 的新树（全局漂移投票通过后）
        ITreeBuilder builder = new ITreeBuilder(999L);
        double[][] data = generateData(SUBSAMPLE_SIZE, 9, 999L);
        for (int subtask = 0; subtask < PARALLELISM; subtask++) {
            long batchId = ((long) subtask << 32) | 1L;  // round=1
            for (int slot = 0; slot < LOCAL_TREE_COUNT; slot++) {
                ITree tree = builder.build(data, SUBSAMPLE_SIZE);
                boolean isLast = (slot == LOCAL_TREE_COUNT - 1);
                allTrees.add(new ITreeMessage(
                        UUID.randomUUID().toString(), subtask, System.currentTimeMillis(),
                        slot, batchId, isLast, tree));
            }
        }

        runCoordinator(allTrees);

        // v1（round=0）+ v2（round=1，所有 subtask 重训完成）
        assertEquals(2, ForestSink.values.size(), "Should emit v1 + v2");
        assertEquals(1L, ForestSink.values.get(0).getVersion());
        assertEquals(2L, ForestSink.values.get(1).getVersion());

        ForestMessage v2 = ForestSink.values.get(1);
        assertEquals(EXPECTED_SLOTS, v2.getTrees().size());
    }

    // ===== v3.4 场景 V34-C2: 单 subtask 重训（round 不一致）不触发 v2 =====

    @Test
    public void testSingleSubtaskRetrainDoesNotTriggerV2() throws Exception {
        List<ITreeMessage> allTrees = new ArrayList<>(buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE));

        // 只有 subtask=0 发 round=1 的新树，其余 subtask 仍 round=0
        long batchId = ((long) 0 << 32) | 1L;
        ITreeBuilder builder = new ITreeBuilder(999L);
        double[][] data = generateData(SUBSAMPLE_SIZE, 9, 999L);
        for (int slot = 0; slot < LOCAL_TREE_COUNT; slot++) {
            ITree tree = builder.build(data, SUBSAMPLE_SIZE);
            boolean isLast = (slot == LOCAL_TREE_COUNT - 1);
            allTrees.add(new ITreeMessage(
                    UUID.randomUUID().toString(), 0, System.currentTimeMillis(),
                    slot, batchId, isLast, tree));
        }

        runCoordinator(allTrees);

        // v3.4: subtask=0 round=1, subtask 1-3 round=0 → round 不一致，只触发 v1
        assertEquals(1, ForestSink.values.size(), "v3.4: round mismatch should only emit v1");
        assertEquals(1L, ForestSink.values.get(0).getVersion());
    }

    // ===== v3.3 场景 V33-4: 未完成批次不触发 =====

    @Test
    public void testIncompleteBatchDoesNotTrigger() throws Exception {
        // 先拼出 v1
        List<ITreeMessage> allTrees = new ArrayList<>(buildAllTrees(PARALLELISM, LOCAL_TREE_COUNT, SUBSAMPLE_SIZE));

        // subtask=0 发 24 棵新树（全部 batchEnd=false）
        long batchId = ((long) 0 << 32) | 1L;
        ITreeBuilder builder = new ITreeBuilder(888L);
        double[][] data = generateData(SUBSAMPLE_SIZE, 9, 888L);
        for (int slot = 0; slot < LOCAL_TREE_COUNT - 1; slot++) {
            ITree tree = builder.build(data, SUBSAMPLE_SIZE);
            allTrees.add(new ITreeMessage(
                    UUID.randomUUID().toString(), 0, System.currentTimeMillis(),
                    slot, batchId, false, tree));
        }

        runCoordinator(allTrees);

        // 只有 v1，24 棵 batchEnd=false 不触发 v2
        assertEquals(1, ForestSink.values.size(), "Incomplete batch should not trigger v2");
        assertEquals(1L, ForestSink.values.get(0).getVersion());
    }

    // ===== 辅助方法 / Helper methods =====

    private void runCoordinator(List<ITreeMessage> trees) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<ITreeMessage> source = env.fromCollection(trees);

        source.keyBy((KeySelector<ITreeMessage, String>) t -> "global")
                .process(new CoordinatorFunction(PARALLELISM, TOTAL_TREES))
                .name("Coordinator")
                .addSink(new ForestSink());

        env.execute("CoordinatorFunction Test");
    }

    /** 构建所有树，每个 subtask 最后一棵 batchEnd=true / All trees, last per subtask has batchEnd=true. */
    private List<ITreeMessage> buildAllTrees(int parallelism, int localTreeCount, int subsampleSize) {
        List<ITreeMessage> trees = new ArrayList<>();
        ITreeBuilder builder = new ITreeBuilder(42L);
        double[][] data = generateData(subsampleSize, 9, 42L);

        for (int subtask = 0; subtask < parallelism; subtask++) {
            long batchId = ((long) subtask) << 32; // round=0
            for (int slot = 0; slot < localTreeCount; slot++) {
                ITree tree = builder.build(data, subsampleSize);
                boolean isLast = (slot == localTreeCount - 1);
                trees.add(new ITreeMessage(
                        UUID.randomUUID().toString(), subtask, System.currentTimeMillis(),
                        slot, batchId, isLast, tree));
            }
        }
        return trees;
    }

    /** 构建所有树但全部 batchEnd=false / All trees with batchEnd=false. */
    private List<ITreeMessage> buildAllTreesNoBatchEnd(int parallelism, int localTreeCount, int subsampleSize) {
        List<ITreeMessage> trees = new ArrayList<>();
        ITreeBuilder builder = new ITreeBuilder(42L);
        double[][] data = generateData(subsampleSize, 9, 42L);

        for (int subtask = 0; subtask < parallelism; subtask++) {
            for (int slot = 0; slot < localTreeCount; slot++) {
                ITree tree = builder.build(data, subsampleSize);
                trees.add(new ITreeMessage(
                        UUID.randomUUID().toString(), subtask, System.currentTimeMillis(),
                        slot, 0L, false, tree));
            }
        }
        return trees;
    }

    private double[][] generateData(int n, int dim, long seed) {
        Random rng = new Random(seed);
        double[][] data = new double[n][dim];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = rng.nextGaussian() * 10 + 50;
            }
        }
        return data;
    }

    private static class ForestSink implements SinkFunction<ForestMessage> {
        static final List<ForestMessage> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(ForestMessage value, Context context) {
            values.add(value);
        }
    }
}
