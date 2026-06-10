package com.leejean.flink;

import com.leejean.beans.DataPoint;
import com.leejean.beans.FeatureValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 检测面分流算子：每条 {@link DataPoint} 拆成 D 条 {@link FeatureValue}
 * （featureId = 列下标 0..D-1, value = features[d], seq = originalSequence）。
 *
 * <p>Detection-side splitter: row → D per-feature tuples (HANDOVER §2).
 * 配合下游 {@code keyBy(featureId)} 把 D 列分散到 P_d 个 subtask 做列并行 IKS。
 */
public class FeatureSplitFlatMap implements FlatMapFunction<DataPoint, FeatureValue> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(DataPoint dp, Collector<FeatureValue> out) {
        double[] f = dp.getFeatures();
        if (f == null) return;
        long seq = dp.getOriginalSequence();
        for (int d = 0; d < f.length; d++) {
            out.collect(new FeatureValue(d, f[d], seq));
        }
    }
}
