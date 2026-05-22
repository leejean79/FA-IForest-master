package com.leejean.drift;

import java.io.Serializable;

public interface DriftDetector extends Serializable {
    /**
     * 接收一个新观察值，返回当前漂移状态。
     * @param value 观察值（v3.0 即异常分数 ∈ [0, 1]）
     * @return 当前状态：STABLE / WARN / DRIFT
     */
    DriftStatus update(double value);

    /** 重置内部状态（漂移确认后必调用）。*/
    void reset();

    /** 累计已观察的样本数（调试和 DriftReport 用）。*/
    long sampleCount();

    /** WARN 是否超时 / Whether WARN has timed out. */
    boolean warnTimedOut();
}
