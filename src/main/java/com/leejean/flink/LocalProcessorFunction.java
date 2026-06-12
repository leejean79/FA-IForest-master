package com.leejean.flink;

import com.leejean.beans.BroadcastEnvelope;
import com.leejean.beans.DataPoint;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.leejean.beans.ScoreResult;
import com.leejean.tree.Forest;
import com.leejean.tree.ITree;
import com.leejean.tree.ITreeBuilder;
import com.leejean.tree.RingBuffer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * 打分面三阶段状态机算子（方向二(a) Phase 3 简化版）。
 * Scoring-side three-phase state machine after detection is moved to the
 * column-parallel detection side.
 *
 * <p>Phase B：冷启动训树 → side output 到 Kafka tree-topic。
 * <p>Phase A：消化 backlog（用首版全局森林打分）。
 * <p>Phase C：用当前森林对每条到达样本打分；不再做任何漂移检测，
 * 全程等待外部检测面经聚合器广播的 COMMITTED 决议驱动重训。
 *
 * <p>Phase C 子状态：{@code STABLE → (COMMITTED) → COOLDOWN → WAITING → STABLE}。
 */
public class LocalProcessorFunction
        extends KeyedBroadcastProcessFunction<String, DataPoint, BroadcastEnvelope, ScoreResult> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalProcessorFunction.class);

    /** iTree side output tag / iTree side output 标签 */
    public static final OutputTag<ITreeMessage> TREE_TAG =
            new OutputTag<ITreeMessage>("tree-output") {};

    /** 广播 state 描述符：全局森林 / Broadcast state descriptor: global forest */
    public static final MapStateDescriptor<String, Forest> FOREST_DESC =
            new MapStateDescriptor<>("global-forest", String.class, Forest.class);

    /** 广播 state 描述符：聚合器合成的 COMMITTED 决议 / Broadcast state: COMMITTED decisions */
    public static final MapStateDescriptor<String, DriftRoundMessage> DRIFT_ROUND_DESC =
            new MapStateDescriptor<>("drift-round", String.class, DriftRoundMessage.class);

    private static final String FOREST_KEY = "current";

    // ===== Keyed State =====

    // Phase B 环形缓冲 + 分散训树倒计时
    @SuppressWarnings("rawtypes")
    private transient ValueState<RingBuffer> ringBuffer;
    private transient ValueState<Integer> trainStartCountdown;

    // 本 subtask 已产出的树数 / trees produced by this subtask
    private transient ValueState<Integer> treesProduced;

    // Phase B 期间累积的数据（等 Phase A 消化）/ all data accumulated during Phase B
    private transient ListState<DataPoint> backlog;

    // Phase C 子状态 / Phase C sub-state
    private transient ValueState<PhaseCSubState> subState;
    private transient ValueState<Long> waitingForVersion;

    // COOLDOWN 相关 state / COOLDOWN-related state
    private transient ValueState<Long> cooldownN;        // COOLDOWN 期内样本数
    private transient ValueState<Double> cooldownMean;   // Welford mean
    private transient ValueState<Double> cooldownM2;     // Welford M2
    private transient ValueState<Long> cooldownWrites;   // ringBuffer 实际写入数

    // 当前进行中的 roundId（聚合器分配，用于 retrain batchId）
    private transient ValueState<Long> currentRoundId;
    // 已处理的决议 roundId，防重复
    private transient ValueState<Long> lastProcessedDecision;
    // 当前 COOLDOWN 注册的 processing-time timer 时戳（orphan-timer 防误触发）
    private transient ValueState<Long> cooldownTimerExpiry;

    // ===== 运行时参数 / Runtime parameters =====
    private transient int subsampleSize;
    private transient int localTreeCount;
    private transient ITreeBuilder builder;
    private transient int subtaskIndex;
    private transient int ringBufferSize;
    private transient int cooldownSamples;
    private transient double zThresholdK;
    private transient long cooldownTimeoutMs;  // COOLDOWN 卡死兜底：超时则 ABORT,保旧森林,排 backlog
    private transient PauseMode pauseMode;


    @Override
    public void open(Configuration parameters) throws Exception {
        ringBuffer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ring-buffer", RingBuffer.class));
        trainStartCountdown = getRuntimeContext().getState(
                new ValueStateDescriptor<>("train-countdown", Types.INT));
        treesProduced = getRuntimeContext().getState(
                new ValueStateDescriptor<>("trees-produced", Types.INT));
        backlog = getRuntimeContext().getListState(
                new ListStateDescriptor<>("backlog", DataPoint.class));

        subState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("phase-c-substate", PhaseCSubState.class));
        waitingForVersion = getRuntimeContext().getState(
                new ValueStateDescriptor<>("waiting-for-version", Types.LONG));

        cooldownN = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-n", Types.LONG));
        cooldownMean = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-mean", Types.DOUBLE));
        cooldownM2 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-m2", Types.DOUBLE));
        cooldownWrites = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-writes", Types.LONG));

        currentRoundId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("current-round-id", Types.LONG));
        lastProcessedDecision = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-processed-decision", Types.LONG));
        cooldownTimerExpiry = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-timer-expiry", Types.LONG));

        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();

        subsampleSize = params.getInt("subsampleSize", 256);
        ringBufferSize = params.getInt("ringBufferSize", 1000);
        int totalTrees = params.getInt("totalTrees", 100);
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);

        int idx = getRuntimeContext().getIndexOfThisSubtask();
        if (params.has("seed")) {
            long seed = params.getLong("seed");
            builder = new ITreeBuilder(seed + 1009L * idx);
        } else {
            builder = new ITreeBuilder(System.nanoTime() + 1009L * idx);
        }

        subtaskIndex = idx;

        cooldownSamples = params.getInt("cooldownSamples", 2000);
        zThresholdK = params.getDouble("zThresholdK", 1.0);
        cooldownTimeoutMs = params.getLong("cooldownTimeoutMs", 60_000L);

        String pauseModeStr = params.get("pauseMode", "USE_OLD_FOREST");
        pauseMode = PauseMode.valueOf(pauseModeStr);

        LOG.info("subtask={}, subsampleSize={}, ringBufferSize={}, localTreeCount={}, totalTrees={}, cooldownSamples={}, cooldownTimeoutMs={}, pauseMode={}",
                subtaskIndex, subsampleSize, ringBufferSize, localTreeCount, totalTrees, cooldownSamples, cooldownTimeoutMs, pauseMode);

    }

    // ===== 广播流回调 / Broadcast callback =====

    @Override
    public void processBroadcastElement(BroadcastEnvelope envelope, Context ctx, Collector<ScoreResult> out)
            throws Exception {
        if (envelope.getType() == BroadcastEnvelope.Type.FOREST) {
            ForestMessage msg = envelope.getForestMessage();
            List<ITree> trees = msg.getTrees().stream()
                    .map(ITreeMessage::getTree)
                    .collect(Collectors.toList());
            Forest forest = new Forest(trees, msg.getSubsampleSize(), msg.getVersion());
            ctx.getBroadcastState(FOREST_DESC).put(FOREST_KEY, forest);

            LOG.info("subtask={}: received global forest version {} with {} trees",
                    subtaskIndex, msg.getVersion(), trees.size());
        } else if (envelope.getType() == BroadcastEnvelope.Type.DRIFT_ROUND) {
            // 聚合器现在只发 COMMITTED；老 VOTING/ABORTED 不会再来
            DriftRoundMessage drm = envelope.getDriftRoundMessage();
            ctx.getBroadcastState(DRIFT_ROUND_DESC).put("decision", drm);
            LOG.info("subtask={}: received DriftRoundMessage {}", subtaskIndex, drm);
        }
    }

    // ===== 主流回调：Phase B / A / C 状态机 =====

    @Override
    public void processElement(DataPoint point, ReadOnlyContext ctx, Collector<ScoreResult> out)
            throws Exception {
        Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);

        long ingestionTime = ctx.timestamp() != null ? ctx.timestamp() : 0L;

        if (forest == null) {
            // Phase B: 冷启动
            backlog.add(point);
            trainIfReady(point, ctx, out);
            return;
        }

        // Phase A: 消化 backlog
        // 仅当子状态为 STABLE 时排空（COOLDOWN/WAITING 期的 backlog 由 BACKLOG 模式管理，等新森林）
        PhaseCSubState currentSubState = subState.value();
        boolean isPhaseACompatible = (currentSubState == null
                || currentSubState == PhaseCSubState.STABLE);

        if (isPhaseACompatible) {
            List<DataPoint> blList = new ArrayList<>();
            for (DataPoint dp : backlog.get()) {
                blList.add(dp);
            }
            if (!blList.isEmpty()) {
                long forestVersion = forest.getVersion();
                for (DataPoint dp : blList) {
                    double s = forest.score(dp.getFeatures());
                    out.collect(buildScoreResult(dp, s, forestVersion, "A", 0L));
                }
                backlog.clear();
                LOG.info("subtask={}: Phase A drained {} backlog records",
                        subtaskIndex, blList.size());
            }
        }

        // Phase C: 打分 + COMMITTED 驱动状态机
        long currentForestVersion = forest.getVersion();
        PhaseCSubState st = subState.value();
        if (st == null) {
            st = PhaseCSubState.STABLE;
        }

        // 消费广播中的 COMMITTED 决议（聚合器输出）
        DriftRoundMessage decision = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("decision");
        if (decision != null
                && decision.getStatus() == DriftRoundMessage.RoundStatus.COMMITTED) {
            Long lastDecision = lastProcessedDecision.value();
            if (lastDecision == null || lastDecision != decision.getRoundId()) {
                handleCommitted(decision, ctx);
                lastProcessedDecision.update(decision.getRoundId());
                st = subState.value();
                if (st == null) st = PhaseCSubState.STABLE;
            }
        }

        double score = forest.score(point.getFeatures());

        switch (st) {
            case STABLE:
                out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                break;
            case COOLDOWN:
                // BACKLOG 模式：数据进 backlog 等新森林处理
                if (pauseMode == PauseMode.USE_OLD_FOREST) {
                    out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                } else {
                    backlog.add(point);
                }
                handleCooldown(point, score, ctx);
                break;
            case WAITING:
                if (pauseMode == PauseMode.USE_OLD_FOREST) {
                    out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                } else {
                    backlog.add(point);
                }
                handleWaiting(currentForestVersion, forest, out);
                break;
        }
    }

    // ===== Phase C 子状态处理 =====

    /**
     * 收到聚合器合成的 COMMITTED → 进 COOLDOWN，记录 roundId 用作 retrain batchId。
     */
    private void handleCommitted(DriftRoundMessage decision, ReadOnlyContext ctx) throws Exception {
        long roundId = decision.getRoundId();
        currentRoundId.update(roundId);
        enterCooldown(ctx);
        LOG.info("subtask={}: COMMITTED round {} → COOLDOWN", subtaskIndex, roundId);
    }

    private void handleCooldown(DataPoint point, double score, ReadOnlyContext ctx)
            throws Exception {
        // 增量更新 cooldown 期统计（Welford's online algorithm）
        Long cN = cooldownN.value();
        cN = (cN == null ? 0 : cN) + 1;
        Double cMean = cooldownMean.value();
        if (cMean == null) cMean = 0.0;
        Double cM2 = cooldownM2.value();
        if (cM2 == null) cM2 = 0.0;
        Long cWrites = cooldownWrites.value();
        if (cWrites == null) cWrites = 0L;

        double delta = score - cMean;
        cMean += delta / cN;
        double delta2 = score - cMean;
        cM2 += delta * delta2;

        cooldownN.update(cN);
        cooldownMean.update(cMean);
        cooldownM2.update(cM2);

        // z-score 阈值写入环形缓冲 / z-score threshold ring buffer write
        boolean written = false;
        if (cN >= 50) {
            double std = Math.sqrt(cM2 / (cN - 1));
            double threshold = cMean + zThresholdK * std;
            if (score < threshold) {
                writeToRingBuffer(point);
                written = true;
            }
        } else {
            // 前 50 条全部写入（初始化）/ first 50 all written (initialization)
            writeToRingBuffer(point);
            written = true;
        }

        if (written) {
            cWrites += 1;
            cooldownWrites.update(cWrites);
        }

        // 双终止条件 / dual termination condition
        // (1) 正常：ringBuffer 已被新数据完全覆盖 AND 至少经过 cooldownSamples 条
        // (2) 兜底：经过 cooldownSamples*2 条仍未写满（z-score 严格场景）
        boolean fillCondition = (cWrites >= ringBufferSize) && (cN >= cooldownSamples);
        boolean fallbackCondition = (cN >= (long) cooldownSamples * 2L);

        if (fillCondition) {
            LOG.info("subtask={}: COOLDOWN done normally, cN={}, writes={}",
                    subtaskIndex, cN, cWrites);
            retrainAndEnterWaiting(ctx);
        } else if (fallbackCondition) {
            LOG.warn("subtask={}: COOLDOWN forced training, cN={} writes={} (only {}/{} ringBuffer filled)",
                    subtaskIndex, cN, cWrites, cWrites, ringBufferSize);
            retrainAndEnterWaiting(ctx);
        }
    }

    private void handleWaiting(long currentForestVersion, Forest forest, Collector<ScoreResult> out)
            throws Exception {
        Long waiting = waitingForVersion.value();
        if (waiting != null && currentForestVersion > waiting) {
            subState.update(PhaseCSubState.STABLE);

            // BACKLOG 模式：用新森林排空 backlog
            if (pauseMode == PauseMode.BACKLOG_THEN_NEW_FOREST && forest != null) {
                List<DataPoint> blList = new ArrayList<>();
                for (DataPoint dp : backlog.get()) {
                    blList.add(dp);
                }
                if (!blList.isEmpty()) {
                    for (DataPoint dp : blList) {
                        double s = forest.score(dp.getFeatures());
                        out.collect(buildScoreResult(dp, s, currentForestVersion, "C", 0L));
                    }
                    backlog.clear();
                    LOG.info("subtask={}: WAITING → STABLE, drained {} backlog with new forest v{}",
                            subtaskIndex, blList.size(), currentForestVersion);
                }
            }

            currentRoundId.clear();
            LOG.info("subtask={}: WAITING → STABLE (new forest version {} received)",
                    subtaskIndex, currentForestVersion);
        }
    }

    private void enterCooldown(ReadOnlyContext ctx) throws Exception {
        subState.update(PhaseCSubState.COOLDOWN);
        cooldownN.update(0L);
        cooldownMean.update(0.0);
        cooldownM2.update(0.0);
        cooldownWrites.update(0L);

        // 注册 processing-time timer：cooldownTimeoutMs 后若仍在 COOLDOWN 即 ABORT，
        // 保旧森林、排 backlog、回 STABLE。无界流（在线部署）变安静时能退出；
        // 有界流（实验）EOF 不一定触发 timer，故 EXP1 还依赖 run-experiment.sh 的 Fix B 兜底。
        long expiry = ctx.timerService().currentProcessingTime() + cooldownTimeoutMs;
        ctx.timerService().registerProcessingTimeTimer(expiry);
        cooldownTimerExpiry.update(expiry);

        LOG.info("subtask={}: entered COOLDOWN (timer expiry={} → +{}ms)",
                subtaskIndex, expiry, cooldownTimeoutMs);
    }

    @SuppressWarnings("unchecked")
    private void retrainAndEnterWaiting(ReadOnlyContext ctx) throws Exception {
        // 使用 currentRoundId（聚合器分配）作为 batchId 低 32 位
        Long globalRound = currentRoundId.value();
        if (globalRound == null || globalRound == 0L) {
            // 兜底：理论上不应发生（COOLDOWN 必经 handleCommitted 触发）
            globalRound = 1L;
        }
        long batchId = ((long) subtaskIndex << 32) | globalRound;

        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb == null || rb.size() == 0) {
            LOG.warn("subtask={}: COOLDOWN done but ring buffer empty, entering WAITING",
                    subtaskIndex);
        } else {
            List<DataPoint> snapshot = rb.snapshot();

            // 诊断：训练池规模 + 异常占比
            int poolAnomaly = 0;
            for (DataPoint dp : snapshot) {
                if (dp.getLabel() == 1) poolAnomaly++;
            }
            LOG.info("subtask={}: COOLDOWN-POOL-DIAG rbSize={} cWrites={} cN={} poolAnomaly={} poolNormal={} anomalyFrac={}",
                    subtaskIndex, rb.size(), cooldownWrites.value(), cooldownN.value(),
                    poolAnomaly, snapshot.size() - poolAnomaly,
                    (double) poolAnomaly / snapshot.size());

            List<double[]> pool = new ArrayList<>(snapshot.size());
            for (DataPoint dp : snapshot) {
                pool.add(dp.getFeatures());
            }

            for (int slot = 0; slot < localTreeCount; slot++) {
                ITree tree = builder.buildFromPool(pool, subsampleSize);
                boolean isLast = (slot == localTreeCount - 1);
                ITreeMessage msg = new ITreeMessage(
                        UUID.randomUUID().toString(),
                        subtaskIndex,
                        System.currentTimeMillis(),
                        slot,
                        batchId,
                        isLast,
                        tree
                );
                ctx.output(TREE_TAG, msg);
            }
            LOG.info("subtask={}: COOLDOWN done, emitted {} new trees (batchId={})",
                    subtaskIndex, localTreeCount, batchId);
        }

        Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);
        long currentForestVersion = (forest != null) ? forest.getVersion() : 0;
        waitingForVersion.update(currentForestVersion);
        subState.update(PhaseCSubState.WAITING);

        cooldownN.clear();
        cooldownMean.clear();
        cooldownM2.clear();
        cooldownWrites.clear();
        // 清掉本轮 COOLDOWN 注册的 abort timer（正常完成走重训路径）
        Long expiry = cooldownTimerExpiry.value();
        if (expiry != null) {
            ctx.timerService().deleteProcessingTimeTimer(expiry);
            cooldownTimerExpiry.clear();
        }

        LOG.info("subtask={}: entering WAITING (waiting for version > {})",
                subtaskIndex, currentForestVersion);
    }

    /**
     * COOLDOWN 兜底:数据不足以训出有意义的新森林 → 放弃本轮重训,保留当前森林,
     * 排空 backlog,回 STABLE。语义是「正确退化」而非降级。
     *
     * 触发源:onTimer 在 cooldownTimeoutMs 到期且仍在 COOLDOWN 时调用。
     */
    private void abortCooldown(OnTimerContext ctx, Collector<ScoreResult> out) throws Exception {
        Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);
        long currentForestVersion = (forest != null) ? forest.getVersion() : 0L;

        // BACKLOG 模式:COOLDOWN 期堆积的样本必须用「当前(旧)」森林打分排空,
        // 否则这些记录从 output-scores 永远消失。USE_OLD_FOREST 模式 COOLDOWN
        // 期数据本就是即时 emit,无需排空。
        int drained = 0;
        if (pauseMode == PauseMode.BACKLOG_THEN_NEW_FOREST && forest != null) {
            List<DataPoint> blList = new ArrayList<>();
            for (DataPoint dp : backlog.get()) {
                blList.add(dp);
            }
            for (DataPoint dp : blList) {
                double s = forest.score(dp.getFeatures());
                out.collect(buildScoreResult(dp, s, currentForestVersion, "C", 0L));
            }
            backlog.clear();
            drained = blList.size();
        }

        subState.update(PhaseCSubState.STABLE);
        cooldownN.clear();
        cooldownMean.clear();
        cooldownM2.clear();
        cooldownWrites.clear();
        cooldownTimerExpiry.clear();
        currentRoundId.clear();

        LOG.warn("subtask={}: COOLDOWN ABORTED (insufficient data before timeout), " +
                "kept forest v{}, flushed {} backlog records",
                subtaskIndex, currentForestVersion, drained);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<ScoreResult> out) throws Exception {
        // 只有「当前在 COOLDOWN 且 timestamp 是本轮注册的 expiry」才触发 abort,
        // 防 orphan timer(上一轮 COOLDOWN 已正常完成但 timer 还在队列)误触发。
        PhaseCSubState st = subState.value();
        if (st != PhaseCSubState.COOLDOWN) {
            return;
        }
        Long expectedExpiry = cooldownTimerExpiry.value();
        if (expectedExpiry == null || expectedExpiry != timestamp) {
            return;
        }
        abortCooldown(ctx, out);
    }

    @SuppressWarnings("unchecked")
    private void writeToRingBuffer(DataPoint point) throws Exception {
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb != null) {
            rb.add(point);
            ringBuffer.update(rb);
        }
    }

    /** Phase B 训练逻辑：环形缓冲区 + 分散训树。 */
    @SuppressWarnings("unchecked")
    private void trainIfReady(DataPoint point, ReadOnlyContext ctx, Collector<ScoreResult> out)
            throws Exception {
        Integer produced = treesProduced.value();
        if (produced == null) {
            produced = 0;
        }
        if (produced >= localTreeCount) {
            return;
        }

        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb == null) {
            rb = new RingBuffer<>(ringBufferSize);
        }
        rb.add(point);

        Integer countdown = trainStartCountdown.value();
        if (countdown == null) {
            if (rb.isFull()) {
                countdown = localTreeCount;
                trainStartCountdown.update(countdown);
            }
            ringBuffer.update(rb);
            return;
        }

        if (countdown > 0) {
            List<DataPoint> snapshot = rb.snapshot();
            List<double[]> pool = new ArrayList<>(snapshot.size());
            for (DataPoint dp : snapshot) {
                pool.add(dp.getFeatures());
            }
            ITree tree = builder.buildFromPool(pool, subsampleSize);
            long batchId = ((long) subtaskIndex) << 32;
            boolean isLast = (produced == localTreeCount - 1);
            ITreeMessage msg = new ITreeMessage(
                    UUID.randomUUID().toString(),
                    subtaskIndex,
                    System.currentTimeMillis(),
                    produced,
                    batchId,
                    isLast,
                    tree
            );
            ctx.output(TREE_TAG, msg);

            produced++;
            treesProduced.update(produced);

            countdown--;
            if (countdown == 0) {
                trainStartCountdown.clear();
            } else {
                trainStartCountdown.update(countdown);
            }

            LOG.debug("subtask={}, tree #{}/{} produced (slot={})",
                    subtaskIndex, produced, localTreeCount, produced - 1);
        }

        ringBuffer.update(rb);
    }

    private ScoreResult buildScoreResult(DataPoint dp, double score,
                                         long forestVersion, String phase,
                                         long ingestionTime) {
        long scoreTime = System.currentTimeMillis();
        return new ScoreResult(
                dp.getOriginalSequence(),
                dp.getId(),
                dp.getTimestamp(),
                dp.getFeatures(),
                dp.getLabel(),
                score,
                forestVersion,
                phase,
                ingestionTime,
                scoreTime
        );
    }
}
