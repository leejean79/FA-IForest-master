package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 检测面：单特征确认 onset 上报，由 {@code PerFeatureIKSFunction}
 * 在峰值-KS 确认窗结束、幅度过门后发出。
 * Per-feature confirmed-onset report emitted after the peak-KS confirmation
 * window closes (see HANDOVER §3.1).
 *
 * <p>{@code seq} 是确认 onset 的起点（首次越过 thr 的样本序号），
 * {@code ks} 是确认窗内观测到的峰值 KS 值（用于上游审计/排查）。
 */
public class FeatureDrift implements Serializable {
    private static final long serialVersionUID = 1L;

    private int featureId;
    private long seq;     // 确认 onset 的起点（首次越过 thr 的样本序号）
    private double ks;    // 峰值 KS

    /** Jackson 反序列化 / no-arg ctor required by Jackson. */
    public FeatureDrift() {}

    public FeatureDrift(int featureId, long seq, double ks) {
        this.featureId = featureId;
        this.seq = seq;
        this.ks = ks;
    }

    @JsonProperty
    public int getFeatureId() { return featureId; }
    public void setFeatureId(int featureId) { this.featureId = featureId; }

    @JsonProperty
    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }

    @JsonProperty
    public double getKs() { return ks; }
    public void setKs(double ks) { this.ks = ks; }

    @Override
    public String toString() {
        return "FeatureDrift{f=" + featureId + ", seq=" + seq + ", ks=" + ks + "}";
    }
}
