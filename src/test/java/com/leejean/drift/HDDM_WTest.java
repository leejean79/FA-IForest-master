package com.leejean.drift;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.jupiter.api.Test;
import java.util.Random;
import static org.junit.jupiter.api.Assertions.*;

class HDDM_WTest {

    /**
     * checkpoint 下 ValueState&lt;HDDM_W&gt; 必须走 PojoSerializer 而非 Kryo GenericType，
     * 否则无限重启策略下从 Kryo 快照恢复可能导致 EWMA 状态错乱（DEV_FIXES 待修 1）。
     * 本测试用 Flink 的 TypeExtractor（与构建 ValueStateDescriptor 序列化器同一路径）
     * 断言 HDDM_W 与 HDDM_AConfig 均被识别为合法 POJO；若有人重新给字段加 final 或
     * 去掉 getter/setter，本测试会失败。
     */
    @Test
    void hddmWAndConfigAreFlinkPojoNotGenericType() {
        TypeInformation<HDDM_W> ti = TypeExtractor.createTypeInfo(HDDM_W.class);
        assertTrue(ti instanceof PojoTypeInfo,
                "HDDM_W must be a Flink POJO (PojoTypeInfo), got " + ti.getClass().getSimpleName());

        TypeInformation<HDDM_AConfig> cfgTi = TypeExtractor.createTypeInfo(HDDM_AConfig.class);
        assertTrue(cfgTi instanceof PojoTypeInfo,
                "HDDM_AConfig must be a Flink POJO (PojoTypeInfo), got " + cfgTi.getClass().getSimpleName());
    }

    private static HDDM_AConfig defaultCfg() {
        return new HDDM_AConfig(0.005, 0.001, 10000L);
    }

    private static final double DEFAULT_LAMBDA = 0.1;

    @Test
    void stableStreamShouldNotTriggerDrift() {
        HDDM_W detector = new HDDM_W(defaultCfg(), DEFAULT_LAMBDA);
        Random rng = new Random(42);

        int driftCount = 0;
        for (int i = 0; i < 5000; i++) {
            double value = 0.5 + rng.nextGaussian() * 0.02;
            if (detector.update(value) == DriftStatus.DRIFT) driftCount++;
        }
        assertEquals(0, driftCount, "stable N(0.5, 0.02) should never trigger DRIFT");
        assertEquals(5000, detector.sampleCount());
    }

    @Test
    void abruptLargeShiftShouldTriggerDrift() {
        HDDM_W detector = new HDDM_W(defaultCfg(), DEFAULT_LAMBDA);
        Random rng = new Random(42);

        // 第一段：建立低均值基线 / establish low-mean baseline
        for (int i = 0; i < 2000; i++) {
            detector.update(0.05 + rng.nextGaussian() * 0.02);
        }

        // 第二段：突跳到高均值，期待 DRIFT / abrupt shift, expect DRIFT
        boolean drifted = false;
        int driftAt = -1;
        for (int i = 0; i < 2000; i++) {
            double value = 0.95 + rng.nextGaussian() * 0.02;
            if (detector.update(value) == DriftStatus.DRIFT) {
                drifted = true;
                driftAt = i;
                break;
            }
        }
        assertTrue(drifted, "abrupt shift 0.05→0.95 should trigger DRIFT");
        assertTrue(driftAt < 1000,
            "DRIFT should be detected within 1000 post-shift samples, got " + driftAt);
    }

    @Test
    void gradualLargeShiftShouldTriggerDrift() {
        HDDM_W detector = new HDDM_W(defaultCfg(), DEFAULT_LAMBDA);
        Random rng = new Random(42);

        // 第一段：低均值基线 / low-mean baseline
        for (int i = 0; i < 1000; i++) {
            detector.update(0.05 + rng.nextGaussian() * 0.02);
        }

        // 第二段：渐变 0.05 → 0.95 (over 1500 steps)
        boolean drifted = false;
        for (int i = 0; i < 1500; i++) {
            double base = 0.05 + (0.95 - 0.05) * i / 1500.0;
            double value = base + rng.nextGaussian() * 0.02;
            if (detector.update(value) == DriftStatus.DRIFT) {
                drifted = true;
                break;
            }
        }

        // 第三段（兜底）：渐变结束后保持高均值 / post-ramp hold (allow late detection)
        if (!drifted) {
            for (int i = 0; i < 500; i++) {
                double value = 0.95 + rng.nextGaussian() * 0.02;
                if (detector.update(value) == DriftStatus.DRIFT) {
                    drifted = true;
                    break;
                }
            }
        }
        assertTrue(drifted, "gradual shift 0.05→0.95 should eventually trigger DRIFT");
    }

    @Test
    void resetShouldClearState() {
        HDDM_W detector = new HDDM_W(defaultCfg(), DEFAULT_LAMBDA);
        Random rng = new Random(42);

        // 触发 DRIFT / trigger DRIFT
        for (int i = 0; i < 2000; i++) {
            detector.update(0.05 + rng.nextGaussian() * 0.02);
        }
        boolean drifted = false;
        for (int i = 0; i < 2000; i++) {
            if (detector.update(0.95 + rng.nextGaussian() * 0.02) == DriftStatus.DRIFT) {
                drifted = true;
                break;
            }
        }
        assertTrue(drifted, "precondition: should have drifted before reset");

        // reset + 稳定数据 / reset + stable data
        detector.reset();
        assertEquals(0, detector.sampleCount(), "reset should zero sampleCount");

        int driftAfterReset = 0;
        for (int i = 0; i < 5000; i++) {
            double value = 0.5 + rng.nextGaussian() * 0.02;
            if (detector.update(value) == DriftStatus.DRIFT) driftAfterReset++;
        }
        assertEquals(0, driftAfterReset, "post-reset stable data should not trigger DRIFT");
        assertEquals(5000, detector.sampleCount());
    }

    @Test
    void warnTimeoutShouldBeDetected() {
        // 放宽 warnConfidence 让 WARN 阈值降低；shift 幅度选择使其不能到达 DRIFT
        // Relaxed warnConfidence + carefully chosen shift: triggers WARN but never DRIFT
        HDDM_AConfig cfg = new HDDM_AConfig(0.05, 0.001, 200L);
        HDDM_W detector = new HDDM_W(cfg, DEFAULT_LAMBDA);
        Random rng = new Random(42);

        // 建立低均值基线 / establish low baseline
        for (int i = 0; i < 1000; i++) {
            detector.update(0.05 + rng.nextGaussian() * 0.02);
        }

        // 偏移到 0.85：期待 WARN 但不到 DRIFT / shift to 0.85: WARN-stuck
        boolean warnSeen = false;
        boolean timedOut = false;
        for (int i = 0; i < 5000; i++) {
            double value = 0.85 + rng.nextGaussian() * 0.02;
            DriftStatus status = detector.update(value);
            if (status == DriftStatus.WARN) warnSeen = true;
            if (status == DriftStatus.DRIFT) {
                fail("unexpected DRIFT at sample " + i + "; check parameter assumptions");
            }
            if (detector.warnTimedOut()) { timedOut = true; break; }
        }

        assertTrue(warnSeen, "Should have seen WARN");
        assertTrue(timedOut, "WARN should time out after " + cfg.getWarnTimeoutSamples() + " samples");
    }
}
