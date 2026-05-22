package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 异常分数输出结构，发送到 Kafka output-scores topic
 * Anomaly score output, sent to Kafka output-scores topic.
 */
public class ScoreResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private long originalSequence;  // 来自 DataPoint.originalSequence，下游排序用
    private String dataPointId;
    private long timestamp;
    private double[] features;
    private int label;              // 透传 ground truth / pass-through ground truth
    private double score;           // 异常分数 [0,1] / anomaly score [0,1]
    private long forestVersion;     // 用哪个版本森林打的分 / which forest version scored this
    private String phase;           // "A" 或 "C" / "A" or "C", for experiment analysis

    // v3.4.6: 业务延迟测量 / latency measurement
    private long ingestionTime;     // Kafka record timestamp (producer 发送时刻)
    private long scoreTime;         // LocalProcessor 打分完成时刻

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public ScoreResult() {
    }

    public ScoreResult(long originalSequence, String dataPointId, long timestamp,
                       double[] features, int label, double score,
                       long forestVersion, String phase) {
        this(originalSequence, dataPointId, timestamp, features, label, score,
                forestVersion, phase, 0L, 0L);
    }

    /** v3.4.6: 含业务延迟时间戳的构造器 / Constructor with latency timestamps. */
    public ScoreResult(long originalSequence, String dataPointId, long timestamp,
                       double[] features, int label, double score,
                       long forestVersion, String phase,
                       long ingestionTime, long scoreTime) {
        this.originalSequence = originalSequence;
        this.dataPointId = dataPointId;
        this.timestamp = timestamp;
        this.features = features;
        this.label = label;
        this.score = score;
        this.forestVersion = forestVersion;
        this.phase = phase;
        this.ingestionTime = ingestionTime;
        this.scoreTime = scoreTime;
    }

    @JsonProperty
    public long getOriginalSequence() { return originalSequence; }
    public void setOriginalSequence(long originalSequence) { this.originalSequence = originalSequence; }

    @JsonProperty
    public String getDataPointId() { return dataPointId; }
    public void setDataPointId(String dataPointId) { this.dataPointId = dataPointId; }

    @JsonProperty
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    @JsonProperty
    public double[] getFeatures() { return features; }
    public void setFeatures(double[] features) { this.features = features; }

    @JsonProperty
    public int getLabel() { return label; }
    public void setLabel(int label) { this.label = label; }

    @JsonProperty
    public double getScore() { return score; }
    public void setScore(double score) { this.score = score; }

    @JsonProperty
    public long getForestVersion() { return forestVersion; }
    public void setForestVersion(long forestVersion) { this.forestVersion = forestVersion; }

    @JsonProperty
    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }

    @JsonProperty
    public long getIngestionTime() { return ingestionTime; }
    public void setIngestionTime(long ingestionTime) { this.ingestionTime = ingestionTime; }

    @JsonProperty
    public long getScoreTime() { return scoreTime; }
    public void setScoreTime(long scoreTime) { this.scoreTime = scoreTime; }

    @Override
    public String toString() {
        return String.format("ScoreResult{seq=%d, id='%s', score=%.4f, label=%d, phase=%s, forestV=%d, features=%s}",
                originalSequence, dataPointId, score, label, phase, forestVersion, Arrays.toString(features));
    }
}
