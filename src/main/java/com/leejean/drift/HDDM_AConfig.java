package com.leejean.drift;

import java.io.Serializable;

public class HDDM_AConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private double warnConfidence;       // P(false warn)，默认 0.005
    private double driftConfidence;      // P(false drift)，默认 0.001
    private long warnTimeoutSamples;     // WARN 持续多少样本未升级则 timeout，默认 2000

    /** Flink PojoSerializer 需要 public 无参构造 / public no-arg ctor for Flink POJO serialization. */
    public HDDM_AConfig() {}

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

    // getter/setter 同时供业务读取与 Flink PojoSerializer 反射访问字段。
    // setter 不做校验：Flink 逐字段反射赋值，校验留给带参构造（重建已知合法状态无需再校验）。
    public double getWarnConfidence() { return warnConfidence; }
    public void setWarnConfidence(double warnConfidence) { this.warnConfidence = warnConfidence; }

    public double getDriftConfidence() { return driftConfidence; }
    public void setDriftConfidence(double driftConfidence) { this.driftConfidence = driftConfidence; }

    public long getWarnTimeoutSamples() { return warnTimeoutSamples; }
    public void setWarnTimeoutSamples(long warnTimeoutSamples) { this.warnTimeoutSamples = warnTimeoutSamples; }

    public static HDDM_AConfig defaults() {
        return new HDDM_AConfig(0.005, 0.001, 2000L);
    }
}
