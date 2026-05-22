package com.leejean.flink;

import com.leejean.beans.BroadcastEnvelope;
import com.leejean.beans.DataPoint;
import com.leejean.beans.DriftReport;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.leejean.beans.ScoreResult;
import com.leejean.drift.DriftDetector;
import com.leejean.drift.DriftStatus;
import com.leejean.drift.HDDM_A;
import com.leejean.drift.HDDM_AConfig;
import com.leejean.drift.HDDM_A_Windowed;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * v3.2 三阶段状态机算子 + Phase C 内 HDDM 漂移检测子状态机 + COOLDOWN 重训
 * v3.2 three-phase state machine operator + HDDM drift detection + COOLDOWN retrain.
 *
 * <p>Phase B（冷启动）：环形缓冲区填满后分散训 localTreeCount 棵 iTree → side output 发到 Kafka tree-topic
 * <p>Phase A（积压消化）：全局模型到达后，给 backlog 中的积压数据打分
 * <p>Phase C（正常预测 + 漂移检测）：给每个新到达的点打分，同时喂 HDDM 检测漂移
 *
 * <p>Phase C 子状态：STABLE → WARN → COOLDOWN → WAITING → STABLE
 */
public class LocalProcessorFunction
        extends KeyedBroadcastProcessFunction<String, DataPoint, BroadcastEnvelope, ScoreResult> {

    private static final Logger LOG = LoggerFactory.getLogger(LocalProcessorFunction.class);

    /** iTree side output tag / iTree side output 标签 */
    public static final OutputTag<ITreeMessage> TREE_TAG =
            new OutputTag<ITreeMessage>("tree-output") {};

    /** v3.4 DriftReport side output tag / v3.4 漂移上报 side output 标签 */
    public static final OutputTag<DriftReport> DRIFT_REPORT_TAG =
            new OutputTag<DriftReport>("drift-report-output") {};

    /** 广播 state 描述符：全局森林 / Broadcast state descriptor: global forest */
    public static final MapStateDescriptor<String, Forest> FOREST_DESC =
            new MapStateDescriptor<>("global-forest", String.class, Forest.class);

    /** v3.4 广播 state 描述符：漂移投票决议 / Broadcast state: drift round messages */
    public static final MapStateDescriptor<String, DriftRoundMessage> DRIFT_ROUND_DESC =
            new MapStateDescriptor<>("drift-round", String.class, DriftRoundMessage.class);

    private static final String FOREST_KEY = "current";

    // ===== Keyed State =====

    // v3.1 Phase B 环形缓冲区 + 分散训树倒计时
    @SuppressWarnings("rawtypes")
    private transient ValueState<RingBuffer> ringBuffer;
    private transient ValueState<Integer> trainStartCountdown;

    // 本 subtask 已产出的树数 / trees produced by this subtask
    private transient ValueState<Integer> treesProduced;

    // Phase B 期间累积的所有数据（等 Phase A 消化）/ all data accumulated during Phase B
    private transient ListState<DataPoint> backlog;

    // ===== v3.2 Phase C keyed state =====

    private transient ValueState<DriftDetector> detector;
    private transient ValueState<PhaseCSubState> subState;
    private transient ValueState<Long> waitingForVersion;

    // v3.2 COOLDOWN 相关 state / COOLDOWN-related state
    private transient ValueState<Long> cooldownN;        // COOLDOWN 期内样本数
    private transient ValueState<Double> cooldownMean;   // Welford mean
    private transient ValueState<Double> cooldownM2;     // Welford M2

    // v3.3 批次版本号 / v3.3 batch version counter
    private transient ValueState<Long> driftRoundCount;

    // v3.4 投票相关 / v3.4 voting related
    private transient ValueState<Long> pendingRoundId;
    private transient ValueState<Long> votedForRound;           // 已投票的 roundId，防重复
    private transient ValueState<Long> lastProcessedDecision;  // 已处理的决议 roundId，防重复

    // ===== 运行时参数 / Runtime parameters =====
    private transient int subsampleSize;
    private transient int localTreeCount;
    private transient ITreeBuilder builder;
    private transient int subtaskIndex;

    // v3.1 配置 / v3.1 configuration
    private transient int ringBufferSize;

    // v3.2 配置 / v3.2 configuration
    private transient HDDM_AConfig hddmConfig;
    private transient WarnTimeoutBehavior warnTimeoutBehavior;
    private transient String detectorType;
    private transient int hddmWindowSize;
    private transient int cooldownSamples;
    private transient double pNormalStable;
    private transient double pNormalWarn;
    private transient double zThresholdK;
    private static final double NORMAL_SCORE_THRESHOLD = 0.5;

    // v3.4 配置 / v3.4 configuration
    private transient PauseMode pauseMode;

    @Override
    public void open(Configuration parameters) throws Exception {
        // v3.1 Phase B state
        ringBuffer = getRuntimeContext().getState(
                new ValueStateDescriptor<>("ring-buffer", RingBuffer.class));
        trainStartCountdown = getRuntimeContext().getState(
                new ValueStateDescriptor<>("train-countdown", Types.INT));
        treesProduced = getRuntimeContext().getState(
                new ValueStateDescriptor<>("trees-produced", Types.INT));
        backlog = getRuntimeContext().getListState(
                new ListStateDescriptor<>("backlog", DataPoint.class));

        // v3.2 Phase C state
        detector = getRuntimeContext().getState(
                new ValueStateDescriptor<>("drift-detector", DriftDetector.class));
        subState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("phase-c-substate", PhaseCSubState.class));
        waitingForVersion = getRuntimeContext().getState(
                new ValueStateDescriptor<>("waiting-for-version", Types.LONG));

        // v3.2 COOLDOWN state
        cooldownN = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-n", Types.LONG));
        cooldownMean = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-mean", Types.DOUBLE));
        cooldownM2 = getRuntimeContext().getState(
                new ValueStateDescriptor<>("cooldown-m2", Types.DOUBLE));

        // v3.3 batch version
        driftRoundCount = getRuntimeContext().getState(
                new ValueStateDescriptor<>("drift-round-count", Types.LONG));

        // v3.4 voting state
        pendingRoundId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-round-id", Types.LONG));
        votedForRound = getRuntimeContext().getState(
                new ValueStateDescriptor<>("voted-for-round", Types.LONG));
        lastProcessedDecision = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-processed-decision", Types.LONG));

        ParameterTool params = (ParameterTool) getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();

        subsampleSize = params.getInt("subsampleSize", 256);
        ringBufferSize = params.getInt("ringBufferSize", 1000);
        int totalTrees = params.getInt("totalTrees", 100);
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
        localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);

        // 构建 ITreeBuilder，混入 subtaskIndex 区分各 subtask 的随机序列
        int idx = getRuntimeContext().getIndexOfThisSubtask();
        if (params.has("seed")) {
            long seed = params.getLong("seed");
            builder = new ITreeBuilder(seed + 1009L * idx);
        } else {
            builder = new ITreeBuilder(System.nanoTime() + 1009L * idx);
        }

        subtaskIndex = idx;

        // v3.2 HDDM 配置 / v3.2 HDDM configuration
        HDDM_AConfig hddmDefaults = HDDM_AConfig.defaults();
        double warnConf = params.getDouble("warnConfidence", hddmDefaults.getWarnConfidence());
        double driftConf = params.getDouble("driftConfidence", hddmDefaults.getDriftConfidence());
        long warnTimeout = params.getLong("warnTimeoutSamples", hddmDefaults.getWarnTimeoutSamples());
        hddmConfig = new HDDM_AConfig(warnConf, driftConf, warnTimeout);

        String behavior = params.get("warnTimeoutBehavior", "DISCARD");
        warnTimeoutBehavior = WarnTimeoutBehavior.valueOf(behavior);

        // v3.2 新增配置 / v3.2 new configuration
        detectorType = params.get("detector", "HDDM_A_Windowed");
        hddmWindowSize = params.getInt("hddmWindowSize", 2000);
        cooldownSamples = params.getInt("cooldownSamples", 2000);
        pNormalStable = params.getDouble("pNormalStable", 0.3);
        pNormalWarn = params.getDouble("pNormalWarn", 0.1);
        zThresholdK = params.getDouble("zThresholdK", 1.0);

        // v3.4 pause mode
        String pauseModeStr = params.get("pauseMode", "USE_OLD_FOREST");
        pauseMode = PauseMode.valueOf(pauseModeStr);

        LOG.info("subtask={}, subsampleSize={}, ringBufferSize={}, localTreeCount={}, totalTrees={}, detector={}, hddmWindowSize={}, cooldownSamples={}, warnTimeout={}, pauseMode={}",
                subtaskIndex, subsampleSize, ringBufferSize, localTreeCount, totalTrees, detectorType, hddmWindowSize, cooldownSamples, warnTimeoutBehavior, pauseMode);
    }

    private DriftDetector createDetector() {
        switch (detectorType) {
            case "HDDM_A":
                return new HDDM_A(hddmConfig);
            case "HDDM_A_Windowed":
                return new HDDM_A_Windowed(hddmConfig, hddmWindowSize);
            default:
                throw new IllegalArgumentException("Unknown detector: " + detectorType);
        }
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
            // v3.4: 存到 broadcast state，等 processElement 消费（方案 b）
            // Store to broadcast state, consumed by processElement (pattern b)
            DriftRoundMessage drm = envelope.getDriftRoundMessage();
            if (drm.getStatus() == DriftRoundMessage.RoundStatus.VOTING) {
                ctx.getBroadcastState(DRIFT_ROUND_DESC).put("active", drm);
            } else {
                // COMMITTED / ABORTED — 固定 key，processElement 消费
                ctx.getBroadcastState(DRIFT_ROUND_DESC).put("decision", drm);
                // v3.4.5 fix: clear active VOTING upon decision to prevent infinite voting loop
                ctx.getBroadcastState(DRIFT_ROUND_DESC).remove("active");
            }
            LOG.info("subtask={}: received DriftRoundMessage {}",
                    subtaskIndex, drm);
        }
    }

    // ===== 主流回调：Phase B / A / C 状态机 =====

    @Override
    public void processElement(DataPoint point, ReadOnlyContext ctx, Collector<ScoreResult> out)
            throws Exception {
        Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);

        // v3.4.6: extract ingestion time for latency measurement
        long ingestionTime = ctx.timestamp() != null ? ctx.timestamp() : 0L;

        if (forest == null) {
            // Phase B: 冷启动
            backlog.add(point);
            trainIfReady(point, ctx, out);
            return;
        }

        // Phase A: 消化 backlog
        // v3.4.3 fix: only drain backlog when sub-state is STABLE or WARN. In LOCAL_DRIFT_REPORTED /
        // COOLDOWN / WAITING states, backlog is managed by BACKLOG_THEN_NEW_FOREST mode and must
        // be preserved until handleWaiting uses the new forest to score it.
        PhaseCSubState currentSubState = subState.value();
        boolean isPhaseACompatible = (currentSubState == null
                || currentSubState == PhaseCSubState.STABLE
                || currentSubState == PhaseCSubState.WARN);

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

        // Phase C: 打分 + 漂移检测子状态机
        long currentForestVersion = forest.getVersion();

        DriftDetector det = detector.value();
        if (det == null) {
            det = createDetector();
        }
        PhaseCSubState st = subState.value();
        if (st == null) {
            st = PhaseCSubState.STABLE;
        }

        // v3.4: 消费广播中的 VOTING 指令（方案 b：broadcast state 暂存 + processElement 消费）
        DriftRoundMessage activeVoting = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("active");
        if (activeVoting != null && activeVoting.getStatus() == DriftRoundMessage.RoundStatus.VOTING) {
            Long voted = votedForRound.value();
            if (voted == null || voted != activeVoting.getRoundId()) {
                Long pending = pendingRoundId.value();
                if (pending != null && pending == 0L && st == PhaseCSubState.LOCAL_DRIFT_REPORTED) {
                    // 本 subtask 是发起者——已被协调器自动计为赞成，只更新 pendingRoundId
                    // This subtask is the initiator — already counted as YES by coordinator
                    pendingRoundId.update(activeVoting.getRoundId());
                    // v3.4.4: initiator is implicitly YES — record to prevent race INITIATE
                    votedForRound.update(activeVoting.getRoundId());
                    LOG.info("subtask={}: initiator updated pendingRoundId={}",
                            subtaskIndex, activeVoting.getRoundId());
                } else {
                    // 非发起者——根据当前状态投票 / non-initiator: vote based on current state
                    castVote(activeVoting.getRoundId(), st, ctx);
                }
            }
        }

        // v3.4: 消费广播中的 COMMITTED/ABORTED 决议
        DriftRoundMessage decision = ctx.getBroadcastState(DRIFT_ROUND_DESC).get("decision");
        if (decision != null) {
            Long lastDecision = lastProcessedDecision.value();
            if (lastDecision == null || lastDecision != decision.getRoundId()) {
                if (decision.getStatus() == DriftRoundMessage.RoundStatus.COMMITTED) {
                    handleVoteCommitted(decision, forest, det, ctx, out);
                } else if (decision.getStatus() == DriftRoundMessage.RoundStatus.ABORTED) {
                    handleVoteAborted(decision, forest, det, ctx, out);
                }
                lastProcessedDecision.update(decision.getRoundId());
                // 重新读取 st（可能已变更）/ re-read st (may have changed)
                st = subState.value();
                if (st == null) st = PhaseCSubState.STABLE;
                det = detector.value();
                if (det == null) det = createDetector();
            }
        }

        // v3.4: LOCAL_DRIFT_REPORTED 模式下可能不输出分数（BACKLOG 模式）
        double score = forest.score(point.getFeatures());

        switch (st) {
            case STABLE:
                out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                handleStable(point, score, det, ctx);
                break;
            case WARN:
                out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                handleWarn(point, score, det, ctx);
                break;
            case LOCAL_DRIFT_REPORTED:
                handleLocalDriftReported(point, score, currentForestVersion, ingestionTime, ctx, out);
                break;
            case COOLDOWN:
                out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                handleCooldown(point, score, ctx, out);
                break;
            case WAITING:
                out.collect(buildScoreResult(point, score, currentForestVersion, "C", ingestionTime));
                handleWaiting(currentForestVersion, det, forest, out);
                break;
        }

        detector.update(det);
    }

    // ===== Phase C 子状态处理 / Phase C sub-state handlers =====

    private void handleStable(DataPoint point, double score, DriftDetector det,
                              ReadOnlyContext ctx) throws Exception {
        DriftStatus status = det.update(score);

        // 概率写入环形缓冲 / probabilistic ring buffer write
        if (score < NORMAL_SCORE_THRESHOLD && ThreadLocalRandom.current().nextDouble() < pNormalStable) {
            writeToRingBuffer(point);
        }

        if (status == DriftStatus.WARN) {
            subState.update(PhaseCSubState.WARN);
            LOG.info("subtask={}: STABLE → WARN (sampleCount={})",
                    subtaskIndex, det.sampleCount());
        } else if (status == DriftStatus.DRIFT) {
            // v3.4: DRIFT → LOCAL_DRIFT_REPORTED（上报协调器）
            enterLocalDriftReported(ctx);
            LOG.info("subtask={}: STABLE → LOCAL_DRIFT_REPORTED (rare direct path)",
                    subtaskIndex);
        }
    }

    private void handleWarn(DataPoint point, double score, DriftDetector det,
                            ReadOnlyContext ctx) throws Exception {
        DriftStatus status = det.update(score);

        // WARN 期更严格的概率写入 / stricter probabilistic write during WARN
        if (score < NORMAL_SCORE_THRESHOLD && ThreadLocalRandom.current().nextDouble() < pNormalWarn) {
            writeToRingBuffer(point);
        }

        if (status == DriftStatus.DRIFT) {
            // v3.4: DRIFT → LOCAL_DRIFT_REPORTED
            enterLocalDriftReported(ctx);
            LOG.info("subtask={}: WARN → LOCAL_DRIFT_REPORTED", subtaskIndex);
        } else if (status == DriftStatus.STABLE) {
            subState.update(PhaseCSubState.STABLE);
            LOG.info("subtask={}: WARN → STABLE (natural recovery)",
                    subtaskIndex);
        } else if (det.warnTimedOut()) {
            if (warnTimeoutBehavior == WarnTimeoutBehavior.PROMOTE) {
                // v3.4: PROMOTE → LOCAL_DRIFT_REPORTED
                enterLocalDriftReported(ctx);
                LOG.info("subtask={}: WARN → LOCAL_DRIFT_REPORTED (PROMOTE timeout)",
                        subtaskIndex);
            } else {
                subState.update(PhaseCSubState.STABLE);
                det.reset();
                LOG.info("subtask={}: WARN → STABLE (DISCARD timeout)",
                        subtaskIndex);
            }
        }
    }

    private void handleCooldown(DataPoint point, double score, ReadOnlyContext ctx,
                                 Collector<ScoreResult> out) throws Exception {
        // HDDM 在 COOLDOWN 期暂停 / HDDM paused during COOLDOWN

        // 增量更新 cooldown 期统计（Welford's online algorithm）
        Long cN = cooldownN.value();
        cN = (cN == null ? 0 : cN) + 1;
        Double cMean = cooldownMean.value();
        if (cMean == null) cMean = 0.0;
        Double cM2 = cooldownM2.value();
        if (cM2 == null) cM2 = 0.0;

        double delta = score - cMean;
        cMean += delta / cN;
        double delta2 = score - cMean;
        cM2 += delta * delta2;

        cooldownN.update(cN);
        cooldownMean.update(cMean);
        cooldownM2.update(cM2);

        // z-score 阈值写入环形缓冲 / z-score threshold ring buffer write
        if (cN >= 50) {
            double std = Math.sqrt(cM2 / (cN - 1));
            double threshold = cMean + zThresholdK * std;
            if (score < threshold) {
                writeToRingBuffer(point);
            }
        } else {
            // 前 50 条全部写入（初始化）/ first 50 all written (initialization)
            writeToRingBuffer(point);
        }

        // 检查 COOLDOWN 是否结束 / check if COOLDOWN is done
        if (cN >= cooldownSamples) {
            retrainAndEnterWaiting(ctx);
        }
    }

    private void handleWaiting(long currentForestVersion, DriftDetector det,
                               Forest forest, Collector<ScoreResult> out) throws Exception {
        Long waiting = waitingForVersion.value();
        if (waiting != null && currentForestVersion > waiting) {
            det.reset();
            subState.update(PhaseCSubState.STABLE);

            // v3.4 BACKLOG 模式：用新森林排空 backlog / drain backlog with new forest
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

            pendingRoundId.clear();
            LOG.info("subtask={}: WAITING → STABLE (new forest version {} received)",
                    subtaskIndex, currentForestVersion);
        }
    }

    // ===== v3.4 LOCAL_DRIFT_REPORTED 状态 / v3.4 LOCAL_DRIFT_REPORTED state =====

    private void enterLocalDriftReported(ReadOnlyContext ctx) throws Exception {
        // v3.4.4 race condition fix: if already voted YES for an active round but no decision yet,
        // skip INITIATE to prevent a spurious round 2
        Long voted = votedForRound.value();
        if (voted != null && voted > 0) {
            subState.update(PhaseCSubState.LOCAL_DRIFT_REPORTED);
            LOG.info("subtask={}: detected DRIFT but already voted YES for round {}, " +
                     "skipping INITIATE, waiting for decision",
                    subtaskIndex, voted);
            return;
        }

        subState.update(PhaseCSubState.LOCAL_DRIFT_REPORTED);
        pendingRoundId.update(0L);  // 0 = 还未分配，等 VOTING 广播 / 0 = not yet assigned

        // 上报 INITIATE 到 drift-topic（用 side output）/ report INITIATE via side output
        DriftReport report = new DriftReport(subtaskIndex, System.currentTimeMillis(),
                DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE);
        ctx.output(DRIFT_REPORT_TAG, report);

        LOG.info("subtask={}: detected DRIFT, reporting INITIATE to coordinator",
                subtaskIndex);
    }

    private void handleLocalDriftReported(DataPoint point, double score, long forestVersion,
                                           long ingestionTime, ReadOnlyContext ctx,
                                           Collector<ScoreResult> out) throws Exception {
        // HDDM 暂停（不喂分数）/ HDDM paused (no score feeding)
        if (pauseMode == PauseMode.USE_OLD_FOREST) {
            // 模式 1：继续用旧森林打分输出 / mode 1: keep scoring with old forest
            out.collect(buildScoreResult(point, score, forestVersion, "C", ingestionTime));
        } else {
            // BACKLOG_THEN_NEW_FOREST：暂存，不输出 / backlog, no output
            backlog.add(point);
        }
    }

    private void castVote(long roundId, PhaseCSubState st, ReadOnlyContext ctx) throws Exception {
        DriftReport.DriftVote vote;
        if (st == PhaseCSubState.WARN || st == PhaseCSubState.LOCAL_DRIFT_REPORTED) {
            vote = DriftReport.DriftVote.YES;
        } else {
            vote = DriftReport.DriftVote.NO;
        }

        DriftStatus localStatus;
        switch (st) {
            case WARN: localStatus = DriftStatus.WARN; break;
            case LOCAL_DRIFT_REPORTED: localStatus = DriftStatus.DRIFT; break;
            default: localStatus = DriftStatus.STABLE; break;
        }

        DriftReport report = new DriftReport(subtaskIndex, System.currentTimeMillis(),
                localStatus, roundId, vote);
        ctx.output(DRIFT_REPORT_TAG, report);

        // v3.4.4: record YES vote to prevent race condition INITIATE
        if (vote == DriftReport.DriftVote.YES) {
            votedForRound.update(roundId);
        }

        LOG.info("subtask={}: voted {} for round {} (state={})",
                subtaskIndex, vote, roundId, st);
    }

    /**
     * v3.4 投票通过：所有 subtask 进 COOLDOWN / vote committed: all subtasks enter COOLDOWN.
     * BACKLOG 模式下保留 backlog（等新森林到来后在 handleWaiting 中排空）。
     */
    private void handleVoteCommitted(DriftRoundMessage decision, Forest forest,
                                      DriftDetector det, ReadOnlyContext ctx,
                                      Collector<ScoreResult> out) throws Exception {
        long roundId = decision.getRoundId();
        pendingRoundId.update(roundId);

        // 所有 subtask 统一进 COOLDOWN / all subtasks enter COOLDOWN uniformly
        enterCooldown();
        det.reset();
        detector.update(det);

        // v3.4.4: decision received, clear votedForRound
        votedForRound.clear();

        LOG.info("subtask={}: COMMITTED round {} → COOLDOWN",
                subtaskIndex, roundId);
    }

    /**
     * v3.4 投票否决：回到 STABLE，重置 HDDM / vote aborted: back to STABLE, HDDM reset.
     * BACKLOG 模式下用旧森林排空 backlog。
     */
    private void handleVoteAborted(DriftRoundMessage decision, Forest forest,
                                    DriftDetector det, ReadOnlyContext ctx,
                                    Collector<ScoreResult> out) throws Exception {
        long roundId = decision.getRoundId();

        det.reset();
        detector.update(det);
        subState.update(PhaseCSubState.STABLE);
        pendingRoundId.clear();

        // BACKLOG 模式：用旧森林排空 backlog / BACKLOG mode: drain with old forest
        if (pauseMode == PauseMode.BACKLOG_THEN_NEW_FOREST && forest != null) {
            List<DataPoint> blList = new ArrayList<>();
            for (DataPoint dp : backlog.get()) {
                blList.add(dp);
            }
            if (!blList.isEmpty()) {
                long forestVersion = forest.getVersion();
                for (DataPoint dp : blList) {
                    double s = forest.score(dp.getFeatures());
                    out.collect(buildScoreResult(dp, s, forestVersion, "C", 0L));
                }
                backlog.clear();
                LOG.info("subtask={}: ABORTED round {}, drained {} backlog with old forest",
                        subtaskIndex, roundId, blList.size());
            }
        }

        // v3.4.4: decision received, clear votedForRound
        votedForRound.clear();

        LOG.info("subtask={}: ABORTED round {} → STABLE",
                subtaskIndex, roundId);
    }

    private void enterCooldown() throws Exception {
        subState.update(PhaseCSubState.COOLDOWN);
        cooldownN.update(0L);
        cooldownMean.update(0.0);
        cooldownM2.update(0.0);
        LOG.info("subtask={}: entered COOLDOWN", subtaskIndex);
    }

    @SuppressWarnings("unchecked")
    private void retrainAndEnterWaiting(ReadOnlyContext ctx) throws Exception {
        // v3.4: 使用 pendingRoundId（全局轮次）作为 batchId 低 32 位
        // v3.4: use pendingRoundId (global round) as batchId low 32 bits
        Long globalRound = pendingRoundId.value();
        if (globalRound == null || globalRound == 0L) {
            // 兜底：如果没有全局轮次（不应发生），使用本地计数
            Long round = driftRoundCount.value();
            globalRound = (round == null ? 1L : round + 1L);
            driftRoundCount.update(globalRound);
        }
        long batchId = ((long) subtaskIndex << 32) | globalRound;

        // 从环形缓冲采样训 localTreeCount 棵 / train localTreeCount trees from ring buffer
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb == null || rb.size() == 0) {
            LOG.warn("subtask={}: COOLDOWN done but ring buffer empty, entering WAITING",
                    subtaskIndex);
        } else {
            List<DataPoint> snapshot = rb.snapshot();
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

        // 读取当前森林版本 / read current forest version
        Forest forest = ctx.getBroadcastState(FOREST_DESC).get(FOREST_KEY);
        long currentForestVersion = (forest != null) ? forest.getVersion() : 0;
        waitingForVersion.update(currentForestVersion);
        subState.update(PhaseCSubState.WAITING);

        // 清理 cooldown 临时状态 / clear cooldown temp state
        cooldownN.clear();
        cooldownMean.clear();
        cooldownM2.clear();

        LOG.info("subtask={}: entering WAITING (waiting for version > {})",
                subtaskIndex, currentForestVersion);
    }

    @SuppressWarnings("unchecked")
    private void writeToRingBuffer(DataPoint point) throws Exception {
        RingBuffer<DataPoint> rb = ringBuffer.value();
        if (rb != null) {
            rb.add(point);
            ringBuffer.update(rb);
        }
    }

    /**
     * v3.1 Phase B 训练逻辑：环形缓冲区 + 分散训树
     */
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
            // v3.3: Phase B batchId = (subtask << 32) | 0, batchEnd on last tree
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
        // v3.4.6: 时间戳支持业务延迟分析
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
