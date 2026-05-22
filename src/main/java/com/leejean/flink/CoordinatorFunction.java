package com.leejean.flink;

import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * v3.3 协调器算子：收集所有 subtask 的 iTree，拼成全局森林发出
 * v3.3 Coordinator operator: collects iTrees from all subtasks, assembles global forest
 *
 * <p>状态结构：MapState key=(subtask, slot)，value=ITreeMessage。
 * 当所有 expectedSlots 填齐且 dirty=true 时触发 fireForest()。
 *
 * <p>State: MapState keyed by (subtask, slot), value=ITreeMessage.
 * Fires forest when all expectedSlots are filled and dirty=true.
 *
 * <p>v2.2 限制：重启会丢失版本号状态（currentVersion 重新从 1 开始）。
 * v2.2 limitation: restart loses version state (currentVersion resets to 1).
 */
public class CoordinatorFunction
        extends KeyedProcessFunction<String, ITreeMessage, ForestMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorFunction.class);

    private final int parallelism;
    private final int totalTrees;
    private final int localTreeCount;
    private final int expectedSlots;

    public CoordinatorFunction(int parallelism, int totalTrees) {
        this.parallelism = parallelism;
        this.totalTrees = totalTrees;
        this.localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        this.expectedSlots = parallelism * localTreeCount;
    }

    // key = (subtask, slot), value = ITreeMessage
    private transient MapState<Tuple2<Integer, Integer>, ITreeMessage> trees;
    private transient ValueState<Long> currentVersion;
    private transient ValueState<Boolean> dirty;

    @Override
    public void open(Configuration parameters) throws Exception {
        trees = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("forest-trees",
                        TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {}),
                        TypeInformation.of(ITreeMessage.class)));

        currentVersion = getRuntimeContext().getState(
                new ValueStateDescriptor<>("forest-version", Types.LONG));

        dirty = getRuntimeContext().getState(
                new ValueStateDescriptor<>("dirty-flag", Types.BOOLEAN));
    }

    @Override
    public void processElement(ITreeMessage msg, Context ctx, Collector<ForestMessage> out)
            throws Exception {

        int subtask = msg.getProducerSubtask();
        int slot = msg.getSlotIndex();

        // 边界检查 / boundary check
        if (subtask < 0 || subtask >= parallelism) {
            LOG.warn("Received tree from subtask {} but coordinator configured for parallelism={}. Skipping.",
                    subtask, parallelism);
            return;
        }
        if (slot < 0 || slot >= localTreeCount) {
            LOG.warn("Received tree with slotIndex {} but localTreeCount={}. Skipping.",
                    slot, localTreeCount);
            return;
        }

        // 存树（覆盖语义）/ store tree (overwrite semantics)
        Tuple2<Integer, Integer> key = Tuple2.of(subtask, slot);
        trees.put(key, msg);
        dirty.update(true);

        // v3.3: 只在 batchEnd=true 时考虑触发 / only consider firing on batchEnd=true
        if (!msg.isBatchEnd()) {
            return;
        }

        // 检查是否所有位置都填齐 / check if all slots are filled
        int filledCount = 0;
        for (Tuple2<Integer, Integer> k : trees.keys()) {
            filledCount++;
        }

        if (filledCount >= expectedSlots && Boolean.TRUE.equals(dirty.value())) {
            fireForest(out);
            dirty.update(false);
        }
    }

    private void fireForest(Collector<ForestMessage> out) throws Exception {
        // 收集所有树，按 (subtask, slot) 顺序排序 / collect all trees, ordered by (subtask, slot)
        List<ITreeMessage> ordered = new ArrayList<>(expectedSlots);
        for (int subtask = 0; subtask < parallelism; subtask++) {
            for (int slot = 0; slot < localTreeCount; slot++) {
                ITreeMessage t = trees.get(Tuple2.of(subtask, slot));
                if (t != null) {
                    ordered.add(t);
                }
            }
        }

        if (ordered.size() != expectedSlots) {
            LOG.warn("fireForest: expected {} trees but got {}, skipping.",
                    expectedSlots, ordered.size());
            return;
        }

        // v3.4: 校验所有树的 batchId 低 32 位（globalRoundId）一致
        // v3.4: verify all trees share the same globalRoundId (low 32 bits of batchId)
        long expectedRound = ordered.get(0).getBatchId() & 0xFFFFFFFFL;
        for (ITreeMessage t : ordered) {
            long round = t.getBatchId() & 0xFFFFFFFFL;
            if (round != expectedRound) {
                LOG.warn("fireForest: round mismatch, expected {} but subtask {} slot {} has round {}. Skipping.",
                        expectedRound, t.getProducerSubtask(), t.getSlotIndex(), round);
                return;
            }
        }

        // 校验所有树的 subsampleSize 一致 / verify consistent subsampleSize
        int subsampleSize = ordered.get(0).getTree().getSubsampleSize();
        for (ITreeMessage t : ordered) {
            if (t.getTree().getSubsampleSize() != subsampleSize) {
                LOG.error("Trees have inconsistent subsampleSize: {} vs {}, skipping.",
                        subsampleSize, t.getTree().getSubsampleSize());
                return;
            }
        }

        // 版本号自增 / increment version
        Long v = currentVersion.value();
        long version = (v == null ? 1L : v + 1);
        currentVersion.update(version);

        ForestMessage forest = new ForestMessage(
                UUID.randomUUID().toString(),
                version,
                System.currentTimeMillis(),
                subsampleSize,
                ordered
        );
        out.collect(forest);

        LOG.info("Coordinator: emitted forest version {} with {} trees", version, ordered.size());
    }
}
