package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.leejean.drift.DriftStatus;

import java.io.Serializable;

/**
 * v3.4 漂移上报消息（subtask → 协调器，via drift-topic）
 * v3.4 drift report message (subtask → coordinator, via drift-topic).
 *
 * <p>两种使用场景 / Two usage scenarios:
 * <ul>
 *   <li>场景 A：subtask 本地 HDDM 触发 DRIFT → vote=INITIATE, roundId=0</li>
 *   <li>场景 B：subtask 响应协调器投票请求 → vote=YES/NO, roundId=协调器分配</li>
 * </ul>
 */
public class DriftReport implements Serializable {
    private static final long serialVersionUID = 2L;

    public enum DriftVote { INITIATE, YES, NO }

    private int subtask;
    private long timestamp;
    private DriftStatus status;       // v3.0: 本地 HDDM 状态 / local HDDM status
    private long sampleCount;

    // v3.0 扩展字段 / v3.0 extension field
    private Double signal;

    // v3.4 新增字段 / v3.4 new fields
    private long roundId;             // 0 = INITIATE（请求分配），>0 = 响应已有 round
    private DriftVote vote;           // INITIATE / YES / NO

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public DriftReport() {
    }

    /** v3.0 兼容构造 / v3.0 compatible constructor. */
    public DriftReport(int subtask, long timestamp, DriftStatus status, long sampleCount) {
        this.subtask = subtask;
        this.timestamp = timestamp;
        this.status = status;
        this.sampleCount = sampleCount;
    }

    /** v3.4 完整构造 / v3.4 full constructor. */
    public DriftReport(int subtask, long timestamp, DriftStatus status,
                       long roundId, DriftVote vote) {
        this.subtask = subtask;
        this.timestamp = timestamp;
        this.status = status;
        this.roundId = roundId;
        this.vote = vote;
    }

    @JsonProperty
    public int getSubtask() { return subtask; }
    public void setSubtask(int subtask) { this.subtask = subtask; }

    @JsonProperty
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @JsonProperty
    public DriftStatus getStatus() { return status; }
    public void setStatus(DriftStatus status) { this.status = status; }

    @JsonProperty
    public long getSampleCount() { return sampleCount; }
    public void setSampleCount(long sampleCount) { this.sampleCount = sampleCount; }

    @JsonProperty
    public Double getSignal() { return signal; }
    public void setSignal(Double signal) { this.signal = signal; }

    @JsonProperty
    public long getRoundId() { return roundId; }
    public void setRoundId(long roundId) { this.roundId = roundId; }

    @JsonProperty
    public DriftVote getVote() { return vote; }
    public void setVote(DriftVote vote) { this.vote = vote; }

    @Override
    public String toString() {
        return String.format("DriftReport{subtask=%d, status=%s, vote=%s, roundId=%d, sampleCount=%d}",
                subtask, status, vote, roundId, sampleCount);
    }
}
