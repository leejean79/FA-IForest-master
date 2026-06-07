package com.leejean.flink;

import com.leejean.beans.DriftReport;
import com.leejean.beans.DriftRoundMessage;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * v3.4 漂移投票协调算子：收集 subtask 上报的 DriftReport，投票决议后广播 DriftRoundMessage
 * v3.4 drift voting coordinator: collects DriftReports, resolves votes, broadcasts DriftRoundMessage.
 *
 * <p>parallelism=1, keyBy("global")。
 *
 * <p>流程：
 * <ol>
 *   <li>收到 INITIATE → 分配 roundId，广播 VOTING，注册超时 timer</li>
 *   <li>收到 YES/NO → 记录投票，全票到齐时立即决议</li>
 *   <li>超时 → 未投票视为弃权（=反对），强制决议</li>
 * </ol>
 */
public class DriftVoterFunction
        extends KeyedProcessFunction<String, DriftReport, DriftRoundMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(DriftVoterFunction.class);

    private final int parallelism;
    private final long votingTimeoutMs;
    private final int majorityThreshold;

    public DriftVoterFunction(int parallelism, long votingTimeoutMs) {
        this.parallelism = parallelism;
        this.votingTimeoutMs = votingTimeoutMs;
        this.majorityThreshold = parallelism / 2 + 1; // 过半数 / majority
    }

    /** 当前进行中的投票 / Currently active vote round. */
    public static class ActiveVote implements Serializable {
        private static final long serialVersionUID = 1L;
        long roundId;
        long timerTimestamp;
        Set<Integer> yesVoters = new HashSet<>();
        Set<Integer> noVoters = new HashSet<>();
    }

    private transient ValueState<Long> nextRoundId;
    private transient ValueState<ActiveVote> activeVote;

    @Override
    public void open(Configuration parameters) throws Exception {
        nextRoundId = getRuntimeContext().getState(
                new ValueStateDescriptor<>("next-round-id", Types.LONG));
        activeVote = getRuntimeContext().getState(
                new ValueStateDescriptor<>("active-vote", ActiveVote.class));
    }

    @Override
    public void processElement(DriftReport report, Context ctx, Collector<DriftRoundMessage> out)
            throws Exception {

        ActiveVote av = activeVote.value();

        if (report.getVote() == DriftReport.DriftVote.INITIATE) {
            if (av != null) {
                // v5.0: 活跃轮次内的独立 INITIATE = 该 subtask 对本轮的同意 → 计为 YES(幂等)。
                // 旧逻辑直接丢弃,导致同步检测(IKS 无 WARN,多 subtask 同时 INITIATE)下
                // 本可通过的投票被全丢成弃权而流产。
                // v5.0: an INITIATE arriving during an active round means this subtask
                // independently detected the same drift → count it as a YES (idempotent).
                if (!av.yesVoters.contains(report.getSubtask())
                        && !av.noVoters.contains(report.getSubtask())) {
                    av.yesVoters.add(report.getSubtask());
                    activeVote.update(av);
                    LOG.info("DriftVoter: INITIATE from subtask {} during active round {} counted as YES",
                            report.getSubtask(), av.roundId);
                    if (av.yesVoters.size() + av.noVoters.size() >= parallelism) {
                        resolveVote(av, ctx, out);
                    }
                }
                return;
            }

            // 分配新 roundId / allocate new roundId
            Long nextId = nextRoundId.value();
            long newId = (nextId == null ? 1L : nextId);
            nextRoundId.update(newId + 1);

            ActiveVote newAv = new ActiveVote();
            newAv.roundId = newId;
            newAv.yesVoters.add(report.getSubtask()); // 触发者自动赞成 / initiator auto-yes
            newAv.timerTimestamp = ctx.timerService().currentProcessingTime() + votingTimeoutMs;
            activeVote.update(newAv);

            // 注册超时 timer / register timeout timer
            ctx.timerService().registerProcessingTimeTimer(newAv.timerTimestamp);

            // 广播 VOTING / broadcast VOTING
            out.collect(new DriftRoundMessage(newId, System.currentTimeMillis(),
                    DriftRoundMessage.RoundStatus.VOTING, 1, 0, 0));

            LOG.info("DriftVoter: initiated round {} by subtask {}", newId, report.getSubtask());
            return;
        }

        // 投票响应 / vote response
        if (av == null || av.roundId != report.getRoundId()) {
            LOG.warn("Received vote for non-active round {} (active={}), ignoring",
                    report.getRoundId(), av != null ? av.roundId : "none");
            return;
        }

        if (report.getVote() == DriftReport.DriftVote.YES) {
            av.yesVoters.add(report.getSubtask());
        } else if (report.getVote() == DriftReport.DriftVote.NO) {
            av.noVoters.add(report.getSubtask());
        }
        activeVote.update(av);

        // 全票到齐立即决议 / all votes in → immediate resolution
        if (av.yesVoters.size() + av.noVoters.size() >= parallelism) {
            resolveVote(av, ctx, out);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DriftRoundMessage> out)
            throws Exception {
        ActiveVote av = activeVote.value();
        if (av == null) {
            return; // 已被全票决议清理 / already resolved
        }
        LOG.info("DriftVoter: round {} timeout, forcing decision", av.roundId);
        resolveVote(av, ctx, out);
    }

    private void resolveVote(ActiveVote av, Context ctx, Collector<DriftRoundMessage> out)
            throws Exception {
        int yes = av.yesVoters.size();
        int no = av.noVoters.size();
        int abstain = parallelism - yes - no;

        DriftRoundMessage.RoundStatus status = (yes >= majorityThreshold)
                ? DriftRoundMessage.RoundStatus.COMMITTED
                : DriftRoundMessage.RoundStatus.ABORTED;

        out.collect(new DriftRoundMessage(av.roundId, System.currentTimeMillis(),
                status, yes, no, abstain));

        // 删除超时 timer（如果还没触发） / delete timer if not yet fired
        ctx.timerService().deleteProcessingTimeTimer(av.timerTimestamp);
        activeVote.clear();

        LOG.info("DriftVoter: round {} resolved as {} (yes={}, no={}, abstain={})",
                av.roundId, status, yes, no, abstain);
    }
}
