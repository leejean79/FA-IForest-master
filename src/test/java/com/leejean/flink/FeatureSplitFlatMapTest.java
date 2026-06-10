package com.leejean.flink;

import com.leejean.beans.DataPoint;
import com.leejean.beans.FeatureValue;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FeatureSplitFlatMap 单元测试 — 一行 D 列 → D 条 FeatureValue。
 * Pure unit test (no Flink runtime).
 */
class FeatureSplitFlatMapTest {

    /** Captures collected items in-memory for assertions. */
    private static final class ListCollector implements Collector<FeatureValue> {
        final List<FeatureValue> items = new ArrayList<>();
        @Override public void collect(FeatureValue r) { items.add(r); }
        @Override public void close() {}
    }

    @Test
    void splits_row_into_D_feature_values() {
        FeatureSplitFlatMap fn = new FeatureSplitFlatMap();
        DataPoint dp = new DataPoint("42", 0L, new double[]{1.0, 2.0, 3.0, 4.0}, 0);
        dp.setOriginalSequence(42L);

        ListCollector out = new ListCollector();
        fn.flatMap(dp, out);

        assertEquals(4, out.items.size(), "should emit one FeatureValue per feature");
        for (int d = 0; d < 4; d++) {
            FeatureValue fv = out.items.get(d);
            assertEquals(d, fv.getFeatureId(), "featureId mismatch at index " + d);
            assertEquals(d + 1.0, fv.getValue(), 1e-12, "value mismatch at index " + d);
            assertEquals(42L, fv.getSeq(), "seq must propagate originalSequence");
        }
    }

    @Test
    void empty_features_emits_nothing() {
        FeatureSplitFlatMap fn = new FeatureSplitFlatMap();
        DataPoint dp = new DataPoint("0", 0L, new double[0], 0);
        dp.setOriginalSequence(0L);

        ListCollector out = new ListCollector();
        fn.flatMap(dp, out);

        assertTrue(out.items.isEmpty(), "empty features → no output");
    }

    @Test
    void null_features_emits_nothing() {
        FeatureSplitFlatMap fn = new FeatureSplitFlatMap();
        DataPoint dp = new DataPoint("0", 0L, null, 0);
        dp.setOriginalSequence(0L);

        ListCollector out = new ListCollector();
        fn.flatMap(dp, out);

        assertTrue(out.items.isEmpty(), "null features → no output (defensive)");
    }
}
