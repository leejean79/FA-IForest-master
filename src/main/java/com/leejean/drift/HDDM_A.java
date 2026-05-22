package com.leejean.drift;

/**
 * HDDM_A: Hoeffding Drift Detection Method, Average variant (single-sided upward).
 *
 * <p>检测均值上升的漂移。监控 sum 和 n，用 Hoeffding 不等式给出置信区间。
 * 如果当前均值 + 置信半径 显著低于历史最低均值 + 半径，更新最优。
 * 如果当前均值 - 置信半径 显著高于最优，告警/确认漂移。
 *
 * <p>Detects upward drift of the mean. Tracks running mean μ_t with Hoeffding bound ε_t.
 * Maintains the lowest observed (μ + ε) as best. When (μ_t - ε_t) significantly exceeds best,
 * signals warning or drift based on confidence levels.
 *
 * <p>Reference: Frias-Blanco et al. 2015, "Online and Non-parametric Drift Detection
 * Methods Based on Hoeffding's Bounds", IEEE TKDE.
 */
public class HDDM_A implements DriftDetector {
    private static final long serialVersionUID = 1L;

    private HDDM_AConfig config;

    // 累计统计 / cumulative statistics
    private long n;           // 当前样本数
    private double sum;       // 累计和

    // 最优值（最低 μ+ε 的位置） / best so far
    private double bestMean;  // μ_min
    private double bestBound; // ε at the time of bestMean
    private long bestN;       // n at the time of bestMean

    // WARN 进入时的样本计数（用于 timeout）
    private long warnEnteredAt;  // 0 表示未进入 WARN

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private HDDM_A() {}

    public HDDM_A(HDDM_AConfig config) {
        this.config = config;
        reset();
    }

    @Override
    public DriftStatus update(double value) {
        n++;
        sum += value;

        double mean = sum / n;
        double epsilon = Math.sqrt(Math.log(1.0 / config.getDriftConfidence()) / (2.0 * n));

        // 更新最优 / update best
        if (n == 1 || mean + epsilon < bestMean + bestBound) {
            bestMean = mean;
            bestBound = epsilon;
            bestN = n;
        }

        // 检测 / detection (single-sided upward)
        // drift 阈值
        double driftBound = bestBound * Math.sqrt(Math.log(1.0 / config.getDriftConfidence()) /
                                                   Math.log(1.0 / config.getWarnConfidence()));
        // warn 阈值
        double warnBound = bestBound * Math.sqrt(Math.log(1.0 / config.getWarnConfidence()) /
                                                  Math.log(1.0 / config.getDriftConfidence()));

        boolean isDrift = (mean - epsilon) > (bestMean + driftBound);
        boolean isWarn = (mean - epsilon) > (bestMean + warnBound);

        if (isDrift) {
            warnEnteredAt = 0;  // 退出 warn 计时
            return DriftStatus.DRIFT;
        }
        if (isWarn) {
            if (warnEnteredAt == 0) warnEnteredAt = n;
            // 检查 timeout：调用方根据 sampleCount 判断
            return DriftStatus.WARN;
        }
        // STABLE
        warnEnteredAt = 0;
        return DriftStatus.STABLE;
    }

    /** WARN 已持续多少样本（0 表示当前不在 WARN）。*/
    public long warnDuration() {
        return warnEnteredAt == 0 ? 0 : (n - warnEnteredAt);
    }

    @Override
    public boolean warnTimedOut() {
        return warnDuration() >= config.getWarnTimeoutSamples();
    }

    @Override
    public void reset() {
        n = 0;
        sum = 0;
        bestMean = 0;
        bestBound = 0;
        bestN = 0;
        warnEnteredAt = 0;
    }

    @Override
    public long sampleCount() {
        return n;
    }

    public HDDM_AConfig getConfig() {
        return config;
    }
}
