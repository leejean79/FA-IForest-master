package com.leejean.flink;

import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.FeatureDrift;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 检测面聚合器（HANDOVER §4）：keyBy("global") + parallelism=1。
 * 滑动窗 {@code aggWin} 内不同特征确认 onset 数 ≥ {@code k} 时合成 COMMITTED；
 * {@code refractory} 去抖避免单轮重复触发。
 *
 * <p>Cross-feature aggregator: synthesises COMMITTED DriftRoundMessage when
 * ≥ k distinct features confirm onset within {@code aggWin}, then enforces
 * {@code refractory} between consecutive emits. Drives downstream COOLDOWN /
 * WAITING / STABLE via the existing drift-round-topic broadcast path.
 */
public class DriftAggregatorFunction
        extends KeyedProcessFunction<String, FeatureDrift, DriftRoundMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DriftAggregatorFunction.class);

    private final int k;
    private final long aggWin;
    private final long refractory;

    /** featureId → 最近一次确认 onset 的 seq（用于窗内去重 + 滑动剔除）。 */
    private transient MapState<Integer, Long> recentByFeature;
    private transient ValueState<Long> roundCounter;
    private transient ValueState<Long> lastEmitSeq;

    public DriftAggregatorFunction(int k, long aggWin, long refractory) {
        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
        if (aggWin <= 0) throw new IllegalArgumentException("aggWin must be > 0");
        if (refractory < 0) throw new IllegalArgumentException("refractory must be ≥ 0");
        this.k = k;
        this.aggWin = aggWin;
        this.refractory = refractory;
    }

    @Override
    public void open(Configuration parameters) {
        recentByFeature = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("agg-recent-by-feature", Types.INT, Types.LONG));
        roundCounter = getRuntimeContext().getState(
                new ValueStateDescriptor<>("agg-round-counter", Types.LONG));
        lastEmitSeq = getRuntimeContext().getState(
                new ValueStateDescriptor<>("agg-last-emit-seq", Types.LONG));
    }

    @Override
    public void processElement(FeatureDrift fd, Context ctx, Collector<DriftRoundMessage> out)
            throws Exception {
        long now = fd.getSeq();

        // 1) 记录/更新该特征最近 onset seq
        recentByFeature.put(fd.getFeatureId(), now);

        // 2) 滑动剔除窗外特征
        long lo = now - aggWin;
        List<Integer> stale = new ArrayList<>();
        for (Map.Entry<Integer, Long> e : recentByFeature.entries()) {
            if (e.getValue() < lo) stale.add(e.getKey());
        }
        for (Integer fid : stale) recentByFeature.remove(fid);

        // 3) refractory 去抖：上次 emit 后 R 内不再触发
        Long last = lastEmitSeq.value();
        if (last != null && now - last < refractory) {
            LOG.debug("DriftAggregator: in refractory (now={}, lastEmit={}, R={}), suppressed",
                    now, last, refractory);
            return;
        }

        // 4) 窗内 distinct featureId 数 ≥ k → 合成 COMMITTED
        int nFired = 0;
        for (Map.Entry<Integer, Long> e : recentByFeature.entries()) {
            if (e.getValue() >= lo) nFired++;
        }
        if (nFired >= k) {
            Long rc = roundCounter.value();
            long nextId = (rc == null ? 1L : rc + 1L);
            roundCounter.update(nextId);
            lastEmitSeq.update(now);

            out.collect(new DriftRoundMessage(
                    nextId,
                    System.currentTimeMillis(),
                    DriftRoundMessage.RoundStatus.COMMITTED,
                    nFired, 0, 0));

            // 触发后清空窗内特征集合，避免下个特征到来再次触发同一轮
            recentByFeature.clear();

            LOG.info("DriftAggregator: COMMITTED round={} (nFired={}, seq={})",
                    nextId, nFired, now);
        }
    }
}
