package com.leejean.drift;

import java.io.Serializable;

public class HDDM_AConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private double warnConfidence;       // P(false warn)，默认 0.005
    private double driftConfidence;      // P(false drift)，默认 0.001
    private long warnTimeoutSamples;     // WARN 持续多少样本未升级则 timeout，默认 2000

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private HDDM_AConfig() {}

    public HDDM_AConfig(double warnConfidence, double driftConfidence, long warnTimeoutSamples) {
        if (warnConfidence <= 0 || warnConfidence >= 1)
            throw new IllegalArgumentException("warnConfidence must be in (0,1)");
        if (driftConfidence <= 0 || driftConfidence >= warnConfidence)
            throw new IllegalArgumentException("driftConfidence must be in (0, warnConfidence)");
        if (warnTimeoutSamples <= 0)
            throw new IllegalArgumentException("warnTimeoutSamples must be > 0");
        this.warnConfidence = warnConfidence;
        this.driftConfidence = driftConfidence;
        this.warnTimeoutSamples = warnTimeoutSamples;
    }

    public double getWarnConfidence() { return warnConfidence; }
    public double getDriftConfidence() { return driftConfidence; }
    public long getWarnTimeoutSamples() { return warnTimeoutSamples; }

    public static HDDM_AConfig defaults() {
        return new HDDM_AConfig(0.005, 0.001, 2000L);
    }
}
