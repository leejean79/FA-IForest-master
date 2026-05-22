package com.leejean.drift;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HDDM_A_Windowed 单元测试 / Unit tests for HDDM_A_Windowed.
 */
class HDDM_A_WindowedTest {

    private HDDM_AConfig cfg() {
        return HDDM_AConfig.defaults(); // warn=0.005, drift=0.001, timeout=2000
    }

    /** 喂稳定高斯数据，应全程 STABLE / Stable distribution should stay STABLE. */
    @Test
    void stableDistributionShouldNotTrigger() {
        HDDM_A_Windowed det = new HDDM_A_Windowed(cfg(), 2000);
        Random r = new Random(42);
        int driftCount = 0;
        for (int i = 0; i < 10000; i++) {
            double val = 0.5 + r.nextGaussian() * 0.05;
            DriftStatus s = det.update(val);
            if (s == DriftStatus.DRIFT) driftCount++;
        }
        assertEquals(0, driftCount, "stable data should not trigger DRIFT");
    }

    /**
     * 先稳定后漂移：窗口外的历史不影响检测 /
     * Drift after stable period: window forgets old history.
     */
    @Test
    void shouldDetectDriftAfterStablePeriod() {
        HDDM_A_Windowed det = new HDDM_A_Windowed(cfg(), 2000);
        Random r = new Random(42);

        // 5000 条稳定期 mean=0.5 / 5000 stable samples
        for (int i = 0; i < 5000; i++) {
            det.update(0.5 + r.nextGaussian() * 0.05);
        }

        // 1500 条漂移期 mean=0.7 / 1500 drift samples
        boolean driftDetected = false;
        for (int i = 0; i < 1500; i++) {
            DriftStatus s = det.update(0.7 + r.nextGaussian() * 0.05);
            if (s == DriftStatus.DRIFT) {
                driftDetected = true;
                break;
            }
        }
        assertTrue(driftDetected, "should detect drift when mean shifts 0.5→0.7");
    }

    /**
     * 不平衡场景：7000 稳定 + 2000 漂移 → HDDM_A 累积平均不触发，但 Windowed 应触发。
     * Imbalanced scenario where cumulative HDDM_A would fail to trigger.
     */
    @Test
    void shouldDetectDriftInImbalancedScenario() {
        HDDM_A_Windowed det = new HDDM_A_Windowed(cfg(), 2000);
        Random r = new Random(42);

        // 7000 条 mean=0.5 / 7000 stable
        for (int i = 0; i < 7000; i++) {
            det.update(0.5 + r.nextGaussian() * 0.05);
        }

        // 2000 条 mean=0.65 / 2000 drift (moderate shift)
        boolean driftDetected = false;
        for (int i = 0; i < 2000; i++) {
            DriftStatus s = det.update(0.65 + r.nextGaussian() * 0.05);
            if (s == DriftStatus.DRIFT) {
                driftDetected = true;
                break;
            }
        }
        assertTrue(driftDetected, "windowed detector should catch moderate drift even after long stable period");
    }

    /**
     * 窗口大小灵敏度：小窗口在强信号下触发更快（窗口更新更快）/
     * Smaller window triggers faster on strong signal due to quicker window turnover.
     */
    @Test
    void smallerWindowShouldTriggerFasterOnStrongShift() {
        Random r1 = new Random(42);
        Random r2 = new Random(42);
        HDDM_A_Windowed det500 = new HDDM_A_Windowed(cfg(), 500);
        HDDM_A_Windowed det2000 = new HDDM_A_Windowed(cfg(), 2000);

        // 3000 条稳定 / 3000 stable
        for (int i = 0; i < 3000; i++) {
            double v1 = 0.5 + r1.nextGaussian() * 0.05;
            double v2 = 0.5 + r2.nextGaussian() * 0.05;
            det500.update(v1);
            det2000.update(v2);
        }

        // 强信号 0.5 → 0.85（Hoeffding bound 在 W=500 时约 0.083，足以触发）
        // Strong shift to 0.85 so both windows can detect
        int trigger500 = -1, trigger2000 = -1;
        for (int i = 0; i < 3000; i++) {
            double v1 = 0.85 + r1.nextGaussian() * 0.05;
            double v2 = 0.85 + r2.nextGaussian() * 0.05;
            if (trigger500 < 0 && det500.update(v1) == DriftStatus.DRIFT) {
                trigger500 = i;
            }
            if (trigger2000 < 0 && det2000.update(v2) == DriftStatus.DRIFT) {
                trigger2000 = i;
            }
        }

        assertTrue(trigger500 >= 0, "window=500 should detect drift");
        assertTrue(trigger2000 >= 0, "window=2000 should detect drift");
        assertTrue(trigger500 < trigger2000,
                "window=500 (" + trigger500 + ") should trigger before window=2000 (" + trigger2000 + ")");
    }

    /** reset 后应回到 STABLE / After reset, should return to STABLE. */
    @Test
    void resetShouldClearState() {
        HDDM_A_Windowed det = new HDDM_A_Windowed(cfg(), 2000);
        Random r = new Random(42);

        // 触发 DRIFT / trigger drift
        for (int i = 0; i < 3000; i++) {
            det.update(0.5 + r.nextGaussian() * 0.05);
        }
        boolean drifted = false;
        for (int i = 0; i < 2000; i++) {
            if (det.update(0.8) == DriftStatus.DRIFT) { drifted = true; break; }
        }
        assertTrue(drifted, "should have drifted before reset");

        // reset + 稳定数据 / reset + stable data
        det.reset();
        assertEquals(0, det.sampleCount());
        int driftAfterReset = 0;
        for (int i = 0; i < 5000; i++) {
            double v = 0.5 + r.nextGaussian() * 0.05;
            if (det.update(v) == DriftStatus.DRIFT) driftAfterReset++;
        }
        assertEquals(0, driftAfterReset, "after reset + stable data, should be STABLE");
    }
}
