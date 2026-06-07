package com.leejean.drift;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * IKS detector 测试,B1–B6 来自 v5.0-phase2 handover §5。
 *
 * <p>关于 B2 (桥接) 的容差说明:dos Reis 等的 IKS 算法用 (value, rnd, group)
 * 复合 key 做 lex 比较,treap 中每个节点存的 G = #{C lex≤} − #{R lex≤}。这是
 * 真实 value-based KS 的**严格上界**,但在 ref/cur 同值的"内部" lex 位置上
 * 可能比 value-KS 高出至多 1/W(经典的 lex bias)。故白盒断言写成
 * {@code value_KS ≤ detector.ks() ≤ value_KS + 1/W}——既验证算法不会低估
 * (它必须 ≥ value-KS,因为每个 value 桶的"末位 lex"恰好等于 value-KS
 * 在该点的取值),也验证它不会过度高估。
 */
class IKSTest {

    private static final double EPS = 1e-9;

    // ---------------- B1 warm-up ----------------

    @Test
    void b1_warmup_allStable_andSmallKsAfterFlush() {
        int W = 100;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        Random rng = new Random(42);

        for (int i = 0; i < W; i++) {
            double v = rng.nextDouble();
            DriftStatus s = detector.update(v);
            assertEquals(DriftStatus.STABLE, s, "warm-up must be STABLE (i=" + i + ")");
        }
        assertEquals((long) W, detector.sampleCount());

        // R = C 作为 multiset → 真实 value-KS = 0;算法上界 ≤ 1/W。
        double ks = detector.ks();
        assertTrue(ks >= -EPS, "ks must be ≥ 0, got " + ks);
        assertTrue(ks <= 1.0 / W + EPS,
                "ks after warm-up should be ≤ 1/W (lex bias), got " + ks);
        assertTrue(ks < detector.threshold(),
                "ks must be below threshold after warm-up, ks=" + ks + " thr=" + detector.threshold());
    }

    @Test
    void b1_warmup_acceptsVolatileValues() {
        int W = 50;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        Random rng = new Random(7);
        // 剧烈波动也应全 STABLE
        for (int i = 0; i < W; i++) {
            double v = rng.nextDouble();
            assertEquals(DriftStatus.STABLE, detector.update(v));
        }
        assertEquals((long) W, detector.sampleCount());
    }

    // ---------------- B2 bridge (most critical) ----------------

    @Test
    void b2_bridge_ksMatchesValueKSWithinLexBias() {
        int W = 30;
        IKS detector = new IKS(new IKSConfig(W, 0.001));

        // warm-up:W 个 [0, 0.5) 内 distinct 值
        Random rng = new Random(202606);
        List<Double> R = new ArrayList<>();
        TreeSet<Double> seen = new TreeSet<>();
        while (R.size() < W) {
            double v = rng.nextDouble() * 0.5;
            if (seen.add(v)) R.add(v);
        }
        Deque<Double> C = new ArrayDeque<>(R);
        for (double v : R) detector.update(v);

        // 一组已知 slide 值(混 in-range / out-of-range)
        double[] newValues = {
                0.6, 0.05, 0.7, 0.3, 0.9, 0.1, 0.55, 0.8, 0.25, 0.45,
                0.99, 0.0, 0.5, 0.75, 0.4, 0.2, 0.6, 0.1, 0.85, 0.35
        };
        for (int step = 0; step < newValues.length; step++) {
            double nv = newValues[step];
            C.pollFirst();
            C.addLast(nv);
            detector.update(nv);

            double bf = bruteForceValueKS(R, new ArrayList<>(C), W);
            double ks = detector.ks();
            assertTrue(ks >= bf - EPS,
                    "step " + step + ": ks=" + ks + " must be ≥ value_KS=" + bf);
            assertTrue(ks <= bf + 1.0 / W + EPS,
                    "step " + step + ": ks=" + ks + " must be ≤ value_KS+1/W=" + (bf + 1.0 / W));
        }
    }

    // ---------------- B3 DRIFT triggers under strong shift ----------------

    @Test
    void b3_disjointDistribution_triggersDrift() {
        int W = 100;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        Random rng = new Random(1);

        // warm-up R ⊂ [0, 0.5)
        for (int i = 0; i < W; i++) detector.update(rng.nextDouble() * 0.5);

        // slide W values in [0.5, 1.0):完全不相交
        boolean sawDrift = false;
        for (int i = 0; i < W; i++) {
            double v = 0.5 + rng.nextDouble() * 0.5;
            DriftStatus s = detector.update(v);
            if (s == DriftStatus.DRIFT) sawDrift = true;
            // 5a:绝不产 WARN
            assertNotEquals(DriftStatus.WARN, s, "IKS must never produce WARN");
        }
        assertTrue(sawDrift, "fully disjoint shift should trigger DRIFT");

        // 全部换完后:current ⊂ [0.5,1.0), reference ⊂ [0,0.5) → 真实 KS = 1.0
        assertTrue(detector.ks() >= 1.0 - EPS,
                "after full replacement ks should be 1.0, got " + detector.ks());
        // warnTimedOut 恒 false
        assertFalse(detector.warnTimedOut());
    }

    // ---------------- B4 no drift under repeats / constant stream ----------------

    @Test
    void b4_constantStream_noDrift_noException() {
        int W = 100;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        for (int i = 0; i < W; i++) detector.update(0.5);
        // 持续滑动同值;不应触发 DRIFT
        // (重复值压力:每个 cur(0.5) 节点用不同 rnd,Remove 必须精确命中插入时的那个)
        for (int i = 0; i < 5 * W; i++) {
            DriftStatus s = detector.update(0.5);
            assertNotEquals(DriftStatus.DRIFT, s,
                    "constant stream should never drift (i=" + i + ")");
        }
        // ks ≈ 0(粗略上界:随机游走 O(√W)/W)
        assertTrue(detector.ks() < detector.threshold(),
                "ks below threshold on constant stream, got " + detector.ks());
    }

    // ---------------- B5 reset ----------------

    @Test
    void b5_reset_returnsToFreshState_canReDetect() {
        int W = 80;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        Random rng = new Random(99);

        // 触发一次 DRIFT
        for (int i = 0; i < W; i++) detector.update(rng.nextDouble() * 0.5);
        boolean droveDriftBefore = false;
        for (int i = 0; i < W; i++) {
            if (detector.update(0.5 + rng.nextDouble() * 0.5) == DriftStatus.DRIFT) {
                droveDriftBefore = true;
            }
        }
        assertTrue(droveDriftBefore, "precondition: first phase must DRIFT");

        // reset → 全清
        detector.reset();
        assertEquals(0L, detector.sampleCount(), "sampleCount must be 0 after reset");
        assertEquals(0.0, detector.ks(), EPS, "ks must be 0 on empty tree after reset");

        // 重新 warm-up:头 W 条都应 STABLE
        for (int i = 0; i < W; i++) {
            assertEquals(DriftStatus.STABLE, detector.update(rng.nextDouble() * 0.5),
                    "post-reset warm-up must be STABLE");
        }
        // 再次能检出
        boolean droveDriftAfter = false;
        for (int i = 0; i < W; i++) {
            if (detector.update(0.5 + rng.nextDouble() * 0.5) == DriftStatus.DRIFT) {
                droveDriftAfter = true;
            }
        }
        assertTrue(droveDriftAfter, "should re-detect DRIFT after reset + re-warmup");
    }

    // ---------------- B6 boundary: W=1, heavy repeats ----------------

    @Test
    void b6_windowSizeOne_noException() {
        IKS detector = new IKS(new IKSConfig(1, 0.001));
        // warm-up = 1 条
        assertEquals(DriftStatus.STABLE, detector.update(0.3));
        assertEquals(1L, detector.sampleCount());

        // 后续若干 slide:不抛异常,D ∈ [0, 1]
        Random rng = new Random(11);
        for (int i = 0; i < 100; i++) {
            double v = rng.nextDouble();
            DriftStatus s = detector.update(v);
            assertNotNull(s);
            double ks = detector.ks();
            assertTrue(ks >= -EPS && ks <= 1.0 + EPS, "ks out of [0,1]: " + ks);
        }
    }

    @Test
    void b6_heavyRepeats_removeHitsCorrectKey() {
        // 全 0.5,大量重复值;Remove 必须借助复合 key 的 rnd tiebreak 命中
        // 当初插入的具体那一个 cur 节点(否则 G 累积错误)。
        int W = 40;
        IKS detector = new IKS(new IKSConfig(W, 0.001));
        for (int i = 0; i < W; i++) detector.update(0.5);
        for (int i = 0; i < 10 * W; i++) {
            DriftStatus s = detector.update(0.5);
            assertNotEquals(DriftStatus.DRIFT, s);
        }
        assertTrue(detector.ks() < detector.threshold());
    }

    // ---------------- 配置 / 边界 ----------------

    @Test
    void config_defaults_andValidation() {
        IKSConfig def = IKSConfig.defaults();
        assertEquals(2000, def.getWindowSize());
        assertEquals(0.001, def.getPValue(), EPS);
        assertEquals(Math.sqrt(-0.5 * Math.log(0.001)), def.getCa(), EPS);

        assertThrows(IllegalArgumentException.class, () -> new IKSConfig(0, 0.001));
        assertThrows(IllegalArgumentException.class, () -> new IKSConfig(-5, 0.001));
        assertThrows(IllegalArgumentException.class, () -> new IKSConfig(100, 0.0));
        assertThrows(IllegalArgumentException.class, () -> new IKSConfig(100, 1.0));
        assertThrows(IllegalArgumentException.class, () -> new IKSConfig(100, -0.1));
    }

    @Test
    void detector_rejectsNullConfig() {
        assertThrows(IllegalArgumentException.class, () -> new IKS(null));
    }

    @Test
    void detector_neverProducesWarn_warnTimedOutAlwaysFalse() {
        IKS detector = new IKS(new IKSConfig(20, 0.001));
        Random rng = new Random(3);
        for (int i = 0; i < 200; i++) {
            DriftStatus s = detector.update(rng.nextDouble());
            assertNotEquals(DriftStatus.WARN, s);
        }
        assertFalse(detector.warnTimedOut());
    }

    // ---------------- helper ----------------

    /**
     * Brute-force value-based KS statistic.
     * {@code D = max_{x∈R∪C} |#{c∈C: c≤x} − #{r∈R: r≤x}| / W}.
     */
    private static double bruteForceValueKS(List<Double> R, List<Double> C, int W) {
        TreeSet<Double> all = new TreeSet<>();
        all.addAll(R);
        all.addAll(C);
        int maxDiff = 0;
        for (double x : all) {
            int cR = 0, cC = 0;
            for (double r : R) if (r <= x) cR++;
            for (double c : C) if (c <= x) cC++;
            int d = Math.abs(cR - cC);
            if (d > maxDiff) maxDiff = d;
        }
        return maxDiff / (double) W;
    }
}
