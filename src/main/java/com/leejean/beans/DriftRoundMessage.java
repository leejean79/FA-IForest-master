package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * v3.4 协调器 → subtask 的漂移投票决议消息（via drift-round-topic）
 * v3.4 coordinator → subtask drift voting decision message (via drift-round-topic).
 */
public class DriftRoundMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum RoundStatus { VOTING, COMMITTED, ABORTED }

    private long roundId;
    private long timestamp;
    private RoundStatus status;
    private int votesYes;
    private int votesNo;
    private int votesAbstain;

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public DriftRoundMessage() {
    }

    public DriftRoundMessage(long roundId, long timestamp, RoundStatus status,
                             int votesYes, int votesNo, int votesAbstain) {
        this.roundId = roundId;
        this.timestamp = timestamp;
        this.status = status;
        this.votesYes = votesYes;
        this.votesNo = votesNo;
        this.votesAbstain = votesAbstain;
    }

    @JsonProperty
    public long getRoundId() { return roundId; }
    public void setRoundId(long roundId) { this.roundId = roundId; }

    @JsonProperty
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @JsonProperty
    public RoundStatus getStatus() { return status; }
    public void setStatus(RoundStatus status) { this.status = status; }

    @JsonProperty
    public int getVotesYes() { return votesYes; }
    public void setVotesYes(int votesYes) { this.votesYes = votesYes; }

    @JsonProperty
    public int getVotesNo() { return votesNo; }
    public void setVotesNo(int votesNo) { this.votesNo = votesNo; }

    @JsonProperty
    public int getVotesAbstain() { return votesAbstain; }
    public void setVotesAbstain(int votesAbstain) { this.votesAbstain = votesAbstain; }

    @Override
    public String toString() {
        return String.format("DriftRoundMessage{roundId=%d, status=%s, yes=%d, no=%d, abstain=%d}",
                roundId, status, votesYes, votesNo, votesAbstain);
    }
}
