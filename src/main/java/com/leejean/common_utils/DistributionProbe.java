package com.leejean.common_utils;

import com.leejean.beans.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 分布探针：放在 keyBy 之后，每个 subtask 独立统计收到的数据条数，
 * 周期性打印本地计数，以实证验证 keyBy 是否真的把数据均匀分到了所有 subtask。
 * 这是局部漂移检测"每个 subtask 收到 IID 样本"假设的实证支撑。
 *
 * <p>Distribution probe placed after keyBy: each parallel subtask independently
 * counts records it receives and periodically logs its local count, providing
 * empirical evidence that records are distributed uniformly across subtasks —
 * the IID prerequisite for local-drift-detection correctness.
 *
 * <p>记录原样透传，不修改 / Records pass through unchanged.
 *
 * <p>预期输出 / Expected output: 在足够大的样本下，所有 subtask 的 localCount 应近似相等，
 * 误差 ~ √N（弱大数律）/ With enough samples, all subtasks' localCount values should
 * be approximately equal (deviation ~ √N by the law of large numbers).
 */
public class DistributionProbe extends RichMapFunction<DataPoint, DataPoint> {

    private static final long serialVersionUID = 1L;

    private final int parallelism;
    private final long reportEvery;

    private transient long count;
    private transient int subtaskIndex;

    /**
     * @param parallelism 算子并行度（仅用于日志展示） / operator parallelism (for logging only)
     * @param reportEvery 每收到多少条打印一次；&lt;= 0 禁用打印 / log every N records; &lt;= 0 disables logging
     */
    public DistributionProbe(int parallelism, long reportEvery) {
        this.parallelism = parallelism;
        this.reportEvery = reportEvery;
    }

    @Override
    public void open(Configuration cfg) {
        this.subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        this.count = 0L;
    }

    @Override
    public DataPoint map(DataPoint dp) {
        count++;
        if (reportEvery > 0 && count % reportEvery == 0) {
            double expectedShare = 1.0 / parallelism;
            // 同时打印理论期望占比，便于人工对比各 subtask 的 localCount 是否接近相等
            // Also log the theoretical share so subtasks' counts can be compared by eye
            System.out.printf(
                    "[probe subtask=%d/%d] localCount=%d expectedShare=1/%d=%.4f%n",
                    subtaskIndex, parallelism, count, parallelism, expectedShare);
        }
        return dp;
    }
}
