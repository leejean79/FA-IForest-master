package com.leejean.drift;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HDDM_A 五场景单元测试 / HDDM_A five-scenario unit test.
 *
 * 1. 静态分布不漂移 / static distribution, no drift
 * 2. 突然漂移 / sudden drift
 * 3. 渐进漂移 / gradual drift
 * 4. reset 后重新计数 / reset then stable
 * 5. warn timeout / warn timeout detection
 */
class HDDM_ATest {

    /**
     * 场景 1：静态分布不漂移
     * 喂 10000 个均值=0.5、std=0.05 的高斯分数 → 全程 STABLE
     */
    @Test
    void staticDistributionShouldRemainStable() {
        HDDM_A detector = new HDDM_A(HDDM_AConfig.defaults());
        Random rng = new Random(42);

        int driftCount = 0;
        for (int i = 0; i < 10000; i++) {
            double value = 0.5 + rng.nextGaussian() * 0.05;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.DRIFT) {
                driftCount++;
            }
        }
        assertEquals(0, driftCount, "Static distribution should not trigger DRIFT");
        assertEquals(10000, detector.sampleCount());
    }

    /**
     * 场景 2：突然漂移
     * 前 5000 条 mean=0.5，后 5000 条 mean=0.7 → 后期某点报 DRIFT
     */
    @Test
    void suddenDriftShouldTriggerDrift() {
        HDDM_A detector = new HDDM_A(HDDM_AConfig.defaults());
        Random rng = new Random(42);

        // Phase 1: 稳定分布 / stable distribution
        for (int i = 0; i < 5000; i++) {
            double value = 0.5 + rng.nextGaussian() * 0.05;
            detector.update(value);
        }

        // Phase 2: 漂移分布 / drifted distribution
        boolean driftDetected = false;
        for (int i = 0; i < 5000; i++) {
            double value = 0.7 + rng.nextGaussian() * 0.05;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.DRIFT) {
                driftDetected = true;
                break;
            }
        }
        assertTrue(driftDetected, "Sudden drift (0.5 → 0.7) should trigger DRIFT");
    }

    /**
     * 场景 3：渐进漂移
     * mean 从 0.5 线性涨到 0.7 → 至少触发一次 WARN
     */
    @Test
    void gradualDriftShouldTriggerAtLeastWarn() {
        HDDM_A detector = new HDDM_A(HDDM_AConfig.defaults());
        Random rng = new Random(42);

        int totalSamples = 20000;
        boolean warnDetected = false;

        for (int i = 0; i < totalSamples; i++) {
            // mean 从 0.5 线性涨到 0.7 / linear drift from 0.5 to 0.7
            double baseMean = 0.5 + 0.2 * ((double) i / totalSamples);
            double value = baseMean + rng.nextGaussian() * 0.05;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.WARN || status == DriftStatus.DRIFT) {
                warnDetected = true;
                break;
            }
        }
        assertTrue(warnDetected,
                "Gradual drift (0.5 → 0.7) should trigger at least WARN");
    }

    /**
     * 场景 4：reset 后重新计数
     * 触发 DRIFT → reset → 再喂稳定数据 → STABLE
     */
    @Test
    void resetShouldRestoreStableDetection() {
        HDDM_A detector = new HDDM_A(HDDM_AConfig.defaults());
        Random rng = new Random(42);

        // 先触发 DRIFT / trigger DRIFT first
        for (int i = 0; i < 5000; i++) {
            detector.update(0.5 + rng.nextGaussian() * 0.05);
        }
        boolean driftDetected = false;
        for (int i = 0; i < 5000; i++) {
            DriftStatus status = detector.update(0.7 + rng.nextGaussian() * 0.05);
            if (status == DriftStatus.DRIFT) {
                driftDetected = true;
                break;
            }
        }
        assertTrue(driftDetected, "Should have triggered DRIFT before reset");

        // reset
        detector.reset();
        assertEquals(0, detector.sampleCount(), "sampleCount should be 0 after reset");

        // 再喂稳定数据 / feed stable data again
        int driftAfterReset = 0;
        for (int i = 0; i < 5000; i++) {
            double value = 0.5 + rng.nextGaussian() * 0.05;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.DRIFT) {
                driftAfterReset++;
            }
        }
        assertEquals(0, driftAfterReset,
                "After reset, stable data should not trigger DRIFT");
        assertEquals(5000, detector.sampleCount());
    }

    /**
     * 场景 5：warn timeout
     * 构造长期 WARN 不升级的序列 → warnTimedOut() 返回 true
     *
     * 用一个微弱上升让 HDDM 进入 WARN 但不到 DRIFT。
     * 设置小 warnTimeoutSamples 加速测试。
     */
    @Test
    void warnTimeoutShouldBeDetected() {
        // 宽松 warnConfidence + 小 timeout 方便测试
        // Relaxed warnConfidence + small timeout for testing
        HDDM_AConfig cfg = new HDDM_AConfig(0.05, 0.001, 200);
        HDDM_A detector = new HDDM_A(cfg);
        Random rng = new Random(42);

        // 先建立低均值基线 / establish low baseline
        for (int i = 0; i < 2000; i++) {
            detector.update(0.3 + rng.nextGaussian() * 0.02);
        }

        // 偏移触发 WARN / shift to trigger WARN
        boolean warnSeen = false;
        boolean timedOut = false;

        for (int i = 0; i < 5000; i++) {
            double value = 0.38 + rng.nextGaussian() * 0.02;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.WARN) {
                warnSeen = true;
            }
            if (status == DriftStatus.DRIFT) {
                break;
            }
            if (detector.warnTimedOut()) {
                timedOut = true;
                break;
            }
        }

        assertTrue(warnSeen, "Should have seen WARN");
        assertTrue(timedOut, "WARN should have timed out after " + cfg.getWarnTimeoutSamples() + " samples");
    }
}
