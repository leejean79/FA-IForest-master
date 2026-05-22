package com.leejean.drift;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * HDDM_AConfig 参数校验测试 / HDDM_AConfig parameter validation test.
 */
class HDDM_AConfigTest {

    @Test
    void defaultsShouldCreateValidConfig() {
        HDDM_AConfig cfg = HDDM_AConfig.defaults();
        assertEquals(0.005, cfg.getWarnConfidence(), 1e-9);
        assertEquals(0.001, cfg.getDriftConfidence(), 1e-9);
        assertEquals(2000L, cfg.getWarnTimeoutSamples());
    }

    @Test
    void shouldRejectZeroWarnConfidence() {
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0, 0.001, 10000));
    }

    @Test
    void shouldRejectWarnConfidenceGeOne() {
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(1.0, 0.001, 10000));
    }

    @Test
    void shouldRejectDriftConfidenceGeWarnConfidence() {
        // driftConfidence must be < warnConfidence
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0.005, 0.005, 10000));
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0.005, 0.01, 10000));
    }

    @Test
    void shouldRejectZeroDriftConfidence() {
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0.005, 0, 10000));
    }

    @Test
    void shouldRejectNonPositiveTimeout() {
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0.005, 0.001, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new HDDM_AConfig(0.005, 0.001, -1));
    }
}
