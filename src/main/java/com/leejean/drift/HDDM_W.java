package com.leejean.drift;

/**
 * HDDM_W: Hoeffding Drift Detection Method, Weighted variant (EWMA-based).
 *
 * <p>论文 Frias-Blanco et al. 2015 中 W-test 变体的实现，与本项目中
 * HDDM_A（cumulative 原版）和 HDDM_A_Windowed（v3.2 工程改造的滑动窗口版）并列。
 *
 * <p>关键算法区别：用 EWMA 递推标量 ewma = (1-λ)*ewma + λ*x 取代显式滑动窗口
 * 算术平均；用单样本 EWMA Hoeffding 边界 ε = sqrt(λ*ln(1/α)/(2(2-λ))) 取代
 * 窗口边界 sqrt(ln(1/α)/(2W))。状态机骨架（best 跟踪 + warn/drift 双阈值）
 * 与 HDDM_A_Windowed 完全一致。
 *
 * <p>EWMA 的有效记忆窗口约为 2/λ 个样本（λ=0.1 时约 20 个），这是"软窗口"——
 * 近期数据权重更高，旧数据指数衰减。
 *
 * <p>Implementation of the W-test variant from Frias-Blanco et al. 2015. Differs
 * from HDDM_A_Windowed only in update() internals: EWMA recursion replaces the
 * sliding-window arithmetic mean; EWMA single-sample Hoeffding bound replaces
 * the windowed bound. State machine (best tracking + warn/drift dual thresholds)
 * is identical.
 *
 * <p>Reference: Frias-Blanco et al. 2015, "Online and Non-parametric Drift
 * Detection Methods Based on Hoeffding's Bounds", IEEE TKDE. See Example 7 for
 * the EWMA single-sample bound derivation.
 */
public class HDDM_W implements DriftDetector {
    private static final long serialVersionUID = 1L;

    private final HDDM_AConfig config;
    private final double lambda;

    // EWMA 递推标量（取代滑动窗口）/ EWMA recursion scalar (replaces sliding window)
    private double ewma;

    // 最优值（最低 mean+bound）/ best so far (lowest mean+bound)
    private double bestMean;
    private double bestBound;

    // 总观察数 / total observations
    private long n;
    // WARN 进入时的样本计数（0 表示未进入 WARN）/ sample count when WARN was entered
    private long warnEnteredAt;

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private HDDM_W() {
        this.config = null;
        this.lambda = 0.0;
    }

    public HDDM_W(HDDM_AConfig config, double lambda) {
        if (lambda <= 0 || lambda > 1) {
            throw new IllegalArgumentException("lambda must be in (0, 1]");
        }
        this.config = config;
        this.lambda = lambda;
        reset();
    }

    @Override
    public DriftStatus update(double value) {
        n++;

        // ─── 改动 1：EWMA 递推取代滑动窗口算术平均 ───
        // EWMA recursion replaces sliding-window arithmetic mean
        if (n == 1) {
            // X̂_1 = X_1（论文 Example 7 初始化）
            // Initialization per paper Example 7
            ewma = value;
        } else {
            ewma = (1.0 - lambda) * ewma + lambda * value;
        }
        double mean = ewma;

        // ─── 改动 2：W-test 单样本 Hoeffding 边界（来自论文 Example 7）───
        // EWMA single-sample Hoeffding bound from paper Example 7:
        //   Pr{X̂_n - E[X̂_n] ≥ ε} ≤ exp(-2ε²(2-λ) / (λ(b-a)²))
        // 数据范围 [0,1] 所以 b-a=1; data range [0,1] so b-a=1
        double epsilon = Math.sqrt(
            lambda * Math.log(1.0 / config.getDriftConfidence()) / (2.0 * (2.0 - lambda))
        );

        // ─── 以下完全照搬 HDDM_A_Windowed，一字不改 ───
        // Following code is verbatim from HDDM_A_Windowed
        if (n == 1 || mean + epsilon < bestMean + bestBound) {
            bestMean = mean;
            bestBound = epsilon;
        }

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
        ewma = 0.0;
        bestMean = 0.0;
        bestBound = 0.0;
        n = 0;
        warnEnteredAt = 0;
    }

    @Override
    public long sampleCount() {
        return n;
    }

    public HDDM_AConfig getConfig() { return config; }
    public double getLambda() { return lambda; }
}
