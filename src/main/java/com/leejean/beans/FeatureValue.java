package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 检测面：单条 (特征 id, 特征值, 原始序号) 元组。
 * Detection-side per-feature value tuple emitted by {@code FeatureSplitFlatMap}
 * (one row of D features → D FeatureValue records keyed by featureId).
 *
 * <p>{@code seq} = {@code DataPoint.originalSequence}，让下游 onset 定位与
 * 重训管线对齐。
 */
public class FeatureValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private int featureId;
    private double value;
    private long seq;

    /** Jackson 反序列化 / no-arg ctor required by Jackson + Flink POJO serializer. */
    public FeatureValue() {}

    public FeatureValue(int featureId, double value, long seq) {
        this.featureId = featureId;
        this.value = value;
        this.seq = seq;
    }

    @JsonProperty
    public int getFeatureId() { return featureId; }
    public void setFeatureId(int featureId) { this.featureId = featureId; }

    @JsonProperty
    public double getValue() { return value; }
    public void setValue(double value) { this.value = value; }

    @JsonProperty
    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }

    @Override
    public String toString() {
        return "FeatureValue{f=" + featureId + ", v=" + value + ", seq=" + seq + "}";
    }
}
