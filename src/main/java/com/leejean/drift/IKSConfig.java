package com.leejean.drift;

import java.io.Serializable;

/**
 * IKS 漂移检测器配置 / Configuration for the IKS (IKSSW) drift detector.
 *
 * <p>{@code windowSize} 是 reference/current 两窗共同长度 W(r=1)。
 * {@code pValue} 是 KS 检验的显著性水平,派生 {@code ca = √(−0.5·ln(pValue))}。
 * 阈值固定为 {@code D > ca·√(2/W)}(对应 {@code IKS.py.Test})。
 *
 * <p>Companion config for {@link IKS}. Holds window size W and significance level
 * pValue, plus the derived constant {@code ca} used in the KS threshold
 * {@code ca·√(2/W)}.
 */
public class IKSConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private int windowSize;   // W
    private double pValue;    // KS 显著性水平 / KS significance level
    private double ca;        // 派生:√(−0.5·ln(pValue)) / derived constant for KS threshold

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private IKSConfig() {}

    public IKSConfig(int windowSize, double pValue) {
        if (windowSize <= 0)
            throw new IllegalArgumentException("windowSize must be > 0");
        if (pValue <= 0 || pValue >= 1)
            throw new IllegalArgumentException("pValue must be in (0,1)");
        this.windowSize = windowSize;
        this.pValue = pValue;
        this.ca = Math.sqrt(-0.5 * Math.log(pValue));
    }

    public int getWindowSize() { return windowSize; }
    public double getPValue() { return pValue; }
    public double getCa() { return ca; }

    public static IKSConfig defaults() {
        return new IKSConfig(2000, 0.001);
    }
}
