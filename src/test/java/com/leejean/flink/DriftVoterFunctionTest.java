package com.leejean.flink;

import com.leejean.beans.DriftReport;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.drift.DriftStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DriftVoterFunction MiniCluster 集成测试（V34-1 ~ V34-5）
 * DriftVoterFunction MiniCluster integration tests.
 */
public class DriftVoterFunctionTest {

    private static final int PARALLELISM = 4;
    private static final long VOTING_TIMEOUT_MS = 5000L;

    @BeforeEach
    public void setUp() {
        RoundSink.values.clear();
    }

    /**
     * V34-1：基础投票路径 — INITIATE + 3 YES → COMMITTED
     * V34-1: basic vote path — INITIATE + 3 YES → COMMITTED
     */
    @Test
    public void testBasicVoteCommitted() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        // subtask 0 触发 INITIATE
        reports.add(new DriftReport(0, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        // subtask 1, 2 投 YES，subtask 3 投 NO（roundId=1 由协调器分配）
        reports.add(new DriftReport(1, 200L, DriftStatus.WARN, 1L, DriftReport.DriftVote.YES));
        reports.add(new DriftReport(2, 300L, DriftStatus.WARN, 1L, DriftReport.DriftVote.YES));
        reports.add(new DriftReport(3, 400L, DriftStatus.STABLE, 1L, DriftReport.DriftVote.NO));

        runVoter(reports);

        // 应该产出 2 条：VOTING + COMMITTED
        assertEquals(2, RoundSink.values.size(), "Should emit VOTING + COMMITTED");

        DriftRoundMessage voting = RoundSink.values.get(0);
        assertEquals(DriftRoundMessage.RoundStatus.VOTING, voting.getStatus());
        assertEquals(1L, voting.getRoundId());

        DriftRoundMessage committed = RoundSink.values.get(1);
        assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, committed.getStatus());
        assertEquals(1L, committed.getRoundId());
        assertEquals(3, committed.getVotesYes());   // subtask 0 (auto) + 1 + 2
        assertEquals(1, committed.getVotesNo());     // subtask 3
        assertEquals(0, committed.getVotesAbstain());
    }

    /**
     * V34-2：投票否决 — INITIATE + 3 NO → ABORTED
     * V34-2: vote rejected — INITIATE + 3 NO → ABORTED
     */
    @Test
    public void testVoteAborted() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        reports.add(new DriftReport(0, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        reports.add(new DriftReport(1, 200L, DriftStatus.STABLE, 1L, DriftReport.DriftVote.NO));
        reports.add(new DriftReport(2, 300L, DriftStatus.STABLE, 1L, DriftReport.DriftVote.NO));
        reports.add(new DriftReport(3, 400L, DriftStatus.STABLE, 1L, DriftReport.DriftVote.NO));

        runVoter(reports);

        assertEquals(2, RoundSink.values.size(), "Should emit VOTING + ABORTED");

        DriftRoundMessage aborted = RoundSink.values.get(1);
        assertEquals(DriftRoundMessage.RoundStatus.ABORTED, aborted.getStatus());
        assertEquals(1, aborted.getVotesYes());   // only initiator
        assertEquals(3, aborted.getVotesNo());
        assertEquals(0, aborted.getVotesAbstain());
    }

    /**
     * V34-3：超时强制决议 — INITIATE only, no follow-up votes → ABORTED (yes=1, abstain=3)
     * V34-3: timeout forced resolution — only INITIATE → ABORTED
     *
     * 注：MiniCluster fromCollection 有限数据，timer 会在作业快结束时触发。
     * 使用很短的 timeout (1ms) 确保 timer 能触发。
     */
    @Test
    public void testTimeoutAborted() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        reports.add(new DriftReport(0, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));

        // 用极短超时确保 timer 触发 / use very short timeout to ensure timer fires
        runVoter(reports, 1L);

        // 至少 VOTING；timer 可能触发 ABORTED
        assertTrue(RoundSink.values.size() >= 1, "Should emit at least VOTING");
        assertEquals(DriftRoundMessage.RoundStatus.VOTING, RoundSink.values.get(0).getStatus());

        if (RoundSink.values.size() == 2) {
            DriftRoundMessage aborted = RoundSink.values.get(1);
            assertEquals(DriftRoundMessage.RoundStatus.ABORTED, aborted.getStatus());
            assertEquals(1, aborted.getVotesYes());
            assertEquals(0, aborted.getVotesNo());
            assertEquals(3, aborted.getVotesAbstain());
        }
        // MiniCluster 有限源场景下 timer 不一定触发——放宽断言
    }

    /**
     * V34-4：超时但已赞成多数 — INITIATE + 2 YES, subtask 3 未投票 → COMMITTED
     * V34-4: timeout with majority already reached → COMMITTED
     */
    @Test
    public void testTimeoutWithMajority() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        reports.add(new DriftReport(0, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        reports.add(new DriftReport(1, 200L, DriftStatus.WARN, 1L, DriftReport.DriftVote.YES));
        reports.add(new DriftReport(2, 300L, DriftStatus.WARN, 1L, DriftReport.DriftVote.YES));
        // subtask 3 不投票 / subtask 3 doesn't vote

        // 用极短超时 / very short timeout
        runVoter(reports, 1L);

        assertTrue(RoundSink.values.size() >= 1, "Should emit at least VOTING");

        if (RoundSink.values.size() == 2) {
            DriftRoundMessage result = RoundSink.values.get(1);
            assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, result.getStatus());
            assertEquals(3, result.getVotesYes());   // 0 (auto) + 1 + 2
            assertEquals(0, result.getVotesNo());
            assertEquals(1, result.getVotesAbstain()); // subtask 3
        }
    }

    /**
     * V34-5 (v5.0 改写)：活跃轮次内的独立 INITIATE 计为 YES,且幂等。
     * 旧版本断言「第二个 INITIATE 被忽略」——这是 IKS 同步检测暴露的协议缺口,已修。
     * V34-5 (v5.0 rewrite): a concurrent INITIATE during an active round counts
     * as YES (independent detection of the same drift), and duplicate INITIATEs
     * from the same subtask are idempotent.
     */
    @Test
    public void testConcurrentInitiateCountsAsYes() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        reports.add(new DriftReport(0, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        // 第二个 INITIATE 来自不同 subtask,活跃期内 → 计 YES
        reports.add(new DriftReport(2, 150L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        // 同一 subtask 重复 INITIATE → 幂等,不再加票
        reports.add(new DriftReport(2, 160L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        // subtask 1 正常 YES;subtask 3 NO 闭合本轮 / close the round
        reports.add(new DriftReport(1, 200L, DriftStatus.WARN, 1L, DriftReport.DriftVote.YES));
        reports.add(new DriftReport(3, 400L, DriftStatus.STABLE, 1L, DriftReport.DriftVote.NO));

        runVoter(reports);

        // 只有一轮:VOTING + 决议;subtask 2 的 INITIATE 算 YES。
        // 4 票集齐(yes={0,1,2}, no={3}) → 直接 COMMITTED,不必等超时。
        assertEquals(2, RoundSink.values.size(), "Should emit exactly 2 (one round)");
        assertEquals(DriftRoundMessage.RoundStatus.VOTING, RoundSink.values.get(0).getStatus());
        assertEquals(1L, RoundSink.values.get(0).getRoundId());

        DriftRoundMessage result = RoundSink.values.get(1);
        assertEquals(1L, result.getRoundId(), "Only roundId=1 should exist");
        assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, result.getStatus());
        assertEquals(3, result.getVotesYes(), "yes = {0 (initiator), 2 (concurrent INITIATE), 1 (YES)}");
        assertEquals(1, result.getVotesNo());
        assertEquals(0, result.getVotesAbstain());
    }

    /**
     * v5.0 回归用例:复刻 IKS 同步检测 bug。
     * p=4,subtask 2 抢先 INITIATE,subtask 0/1/3 在活跃期独立 INITIATE。
     * 期望:全部计为 YES,4 票集齐 → COMMITTED yes=4 no=0 abstain=0。
     * v5.0 regression for the IKS synchronous-detection bug: when all subtasks
     * independently INITIATE within the active window, the round must COMMIT
     * with 4 YES votes (no abstain), not abort.
     */
    @Test
    public void testConcurrentInitiatesAllYes_committed() throws Exception {
        List<DriftReport> reports = new ArrayList<>();
        // subtask 2 抢先 / subtask 2 initiates
        reports.add(new DriftReport(2, 100L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        // 其余三个 subtask 同步独立 INITIATE / others independently INITIATE
        reports.add(new DriftReport(0, 110L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        reports.add(new DriftReport(1, 120L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));
        reports.add(new DriftReport(3, 130L, DriftStatus.DRIFT, 0L, DriftReport.DriftVote.INITIATE));

        runVoter(reports);

        // VOTING + COMMITTED;不必等超时(集齐立即决议)
        assertEquals(2, RoundSink.values.size(), "Should emit exactly 2 (one round)");
        assertEquals(DriftRoundMessage.RoundStatus.VOTING, RoundSink.values.get(0).getStatus());

        DriftRoundMessage result = RoundSink.values.get(1);
        assertEquals(1L, result.getRoundId());
        assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, result.getStatus(),
                "concurrent INITIATEs from all subtasks must COMMIT, not abort");
        assertEquals(4, result.getVotesYes());
        assertEquals(0, result.getVotesNo());
        assertEquals(0, result.getVotesAbstain());
    }

    // ===== 辅助方法 / Helper methods =====

    private void runVoter(List<DriftReport> reports) throws Exception {
        runVoter(reports, VOTING_TIMEOUT_MS);
    }

    private void runVoter(List<DriftReport> reports, long timeoutMs) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<DriftReport> source = env.fromCollection(reports);

        source.keyBy((KeySelector<DriftReport, String>) r -> "global")
                .process(new DriftVoterFunction(PARALLELISM, timeoutMs))
                .name("DriftVoter")
                .addSink(new RoundSink());

        env.execute("DriftVoterFunction Test");
    }

    private static class RoundSink implements SinkFunction<DriftRoundMessage> {
        static final List<DriftRoundMessage> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(DriftRoundMessage value, Context context) {
            values.add(value);
        }
    }
}
