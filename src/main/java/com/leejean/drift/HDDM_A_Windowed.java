package com.leejean.drift;

import com.leejean.tree.RingBuffer;

/**
 * 滑动窗口版 HDDM_A / Sliding-window HDDM_A.
 *
 * <p>与论文 HDDM_A 的关键区别：用固定大小窗口 W 替代累积统计。
 * mean 和 epsilon 都基于最近 W 条数据计算，避免漂移信号被历史稀释。
 *
 * <p>注意：这不是论文里的 HDDM_W（HDDM_W 用 EWMA 加权平均）。
 * 命名为 HDDM_A_Windowed 表明它是 HDDM_A 算法 + 滑动窗口的工程改造。
 *
 * <p>Window-based variant of HDDM_A. Replaces cumulative statistics with a fixed-size
 * sliding window, so drift signals are not diluted by long stable history.
 * NOT the same as the paper's HDDM_W (which uses EWMA). Named HDDM_A_Windowed
 * to indicate it's an engineering adaptation of HDDM_A.
 */
public class HDDM_A_Windowed implements DriftDetector {
    private static final long serialVersionUID = 1L;

    private final HDDM_AConfig config;
    private final int windowSize;

    // 滑动窗口（最近 windowSize 条观察值）/ sliding window of recent observations
    private RingBuffer<Double> window;
    private double sum;  // 窗口内总和（增量维护）/ incremental sum over window

    // 最优值（最低 mean+bound）/ best so far (lowest mean+bound)
    private double bestMean;
    private double bestBound;

    // 总观察数（不限于窗口）/ total observations (not limited to window)
    private long n;
    // WARN 进入时的样本计数（0 表示未进入 WARN）/ sample count when WARN was entered
    private long warnEnteredAt;

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private HDDM_A_Windowed() {
        this.config = null;
        this.windowSize = 0;
    }

    public HDDM_A_Windowed(HDDM_AConfig config, int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be > 0");
        }
        this.config = config;
        this.windowSize = windowSize;
        reset();
    }

    @Override
    public DriftStatus update(double value) {
        n++;

        // 增量维护 sum：满时减去即将被覆盖的最老值 / maintain sum incrementally
        if (window.isFull()) {
            double oldest = window.peekOldest();
            sum -= oldest;
        }
        window.add(value);
        sum += value;

        int W = window.size();
        double mean = sum / W;
        // Hoeffding 边界用窗口大小 W 而不是累积 n / Hoeffding bound uses window size W
        double epsilon = Math.sqrt(Math.log(1.0 / config.getDriftConfidence()) / (2.0 * W));

        // 更新 best / update best
        if (n == 1 || mean + epsilon < bestMean + bestBound) {
            bestMean = mean;
            bestBound = epsilon;
        }

        // 检测阈值 / detection thresholds
        double driftBound = bestBound * Math.sqrt(Math.log(1.0 / config.getDriftConfidence()) /
                                                   Math.log(1.0 / config.getWarnConfidence()));
        double warnBound  = bestBound * Math.sqrt(Math.log(1.0 / config.getWarnConfidence()) /
                                                   Math.log(1.0 / config.getDriftConfidence()));

        if ((mean - epsilon) > bestMean + driftBound) {
            warnEnteredAt = 0;
            return DriftStatus.DRIFT;
        }
        if ((mean - epsilon) > bestMean + warnBound) {
            if (warnEnteredAt == 0) warnEnteredAt = n;
            return DriftStatus.WARN;
        }
        warnEnteredAt = 0;
        return DriftStatus.STABLE;
    }

    /** WARN 已持续多少样本 / How many samples since WARN was entered. */
    public long warnDuration() {
        return warnEnteredAt == 0 ? 0 : (n - warnEnteredAt);
    }

    @Override
    public boolean warnTimedOut() {
        return warnDuration() >= config.getWarnTimeoutSamples();
    }

    @Override
    public void reset() {
        window = new RingBuffer<>(windowSize);
        sum = 0.0;
        bestMean = 0.0;
        bestBound = 0.0;
        n = 0;
        warnEnteredAt = 0;
    }

    @Override
    public long sampleCount() {
        return n;
    }
}
