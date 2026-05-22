package com.leejean.common_utils;

import com.leejean.beans.DataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 将 CSV 行解析为 DataPoint 的 ProcessFunction
 * ProcessFunction that parses CSV lines into DataPoint
 *
 * 列布局根据 hasId / hasLabel 动态确定:
 * Column layout determined dynamically by hasId / hasLabel:
 *   hasId=true,  hasLabel=true  → [id, f0, f1, ..., fN, label]
 *   hasId=true,  hasLabel=false → [id, f0, f1, ..., fN]
 *   hasId=false, hasLabel=true  → [f0, f1, ..., fN, label]
 *   hasId=false, hasLabel=false → [f0, f1, ..., fN]
 *
 * 当 hasId=false 时，通过 autoIncrementId 控制是否自增生成 id:
 * When hasId=false, autoIncrementId controls whether to auto-generate incremental ids:
 *   autoIncrementId=true  → id 从 1 开始自增 / id auto-increments from 1
 *   autoIncrementId=false → id 固定为 "0" / id defaults to "0"
 */
public class CsvToDataPointFunction extends ProcessFunction<String, DataPoint> {

    private final boolean hasHeader;
    private final boolean hasId;
    private final boolean hasLabel;
    private final boolean autoIncrementId;
    private final SequenceSource sequenceSource;

    // 是否已跳过标题行 / whether header line has been skipped
    private transient boolean headerSkipped;
    // 自增 id 计数器 / Auto-increment id counter
    private transient long autoId;
    // 自增 sequence 计数器（仅 AUTO_INCREMENT 策略使用）
    // Auto-increment sequence counter (only used by AUTO_INCREMENT strategy)
    private transient long autoSequence;

    /**
     * 完整构造（含 sequenceSource）/ Full constructor with sequenceSource.
     *
     * @param hasHeader       数据是否包含标题行 / whether data has a header line
     * @param hasId           数据是否包含 id 列（第一列）/ whether data has an id column (1st col)
     * @param hasLabel        数据是否包含类标签列（最后一列）/ whether data has a label column (last col)
     * @param autoIncrementId 无 id 列时是否自增生成 id / whether to auto-increment id when no id column
     * @param sequenceSource  originalSequence 的提取策略 / strategy for extracting originalSequence
     */
    public CsvToDataPointFunction(boolean hasHeader, boolean hasId, boolean hasLabel,
                                  boolean autoIncrementId, SequenceSource sequenceSource) {
        this.hasHeader = hasHeader;
        this.hasId = hasId;
        this.hasLabel = hasLabel;
        this.autoIncrementId = autoIncrementId;
        this.sequenceSource = sequenceSource;
    }

    /**
     * v1 兼容构造（默认 AUTO_INCREMENT 策略）/ v1-compatible constructor, defaults to AUTO_INCREMENT.
     */
    public CsvToDataPointFunction(boolean hasHeader, boolean hasId, boolean hasLabel, boolean autoIncrementId) {
        this(hasHeader, hasId, hasLabel, autoIncrementId, SequenceSource.AUTO_INCREMENT);
    }

    /**
     * 最简构造 / Simplest constructor.
     */
    public CsvToDataPointFunction(boolean hasHeader, boolean hasId, boolean hasLabel) {
        this(hasHeader, hasId, hasLabel, true, SequenceSource.AUTO_INCREMENT);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        autoSequence = 0;
    }

    @Override
    public void processElement(String value, Context ctx, Collector<DataPoint> out) {
        String line = value.trim();
        if (line.isEmpty()) {
            return;
        }

        // 跳过标题行 / Skip header line
        if (hasHeader && !headerSkipped) {
            headerSkipped = true;
            System.out.println("[INFO] Header skipped: " + line);
            return;
        }

        try {
            String[] parts = line.split(",");

            // 确定 features 的起始和结束索引 / Determine feature start/end indices
            int featureStart = hasId ? 1 : 0;
            int featureEnd = hasLabel ? parts.length - 1 : parts.length;

            if (featureEnd <= featureStart) {
                System.err.println("[WARN] Not enough columns, skipping: " + line);
                return;
            }

            // 解析 id / Parse id
            String id;
            if (hasId) {
                id = parts[0].trim();
            } else if (autoIncrementId) {
                id = String.valueOf(++autoId);
            } else {
                id = "0";
            }

            // 解析 features / Parse features
            double[] features = new double[featureEnd - featureStart];
            for (int i = featureStart; i < featureEnd; i++) {
                features[i - featureStart] = Double.parseDouble(parts[i].trim());
            }

            // 解析 label / Parse label
            int label = 0;
            if (hasLabel) {
                label = Integer.parseInt(parts[parts.length - 1].trim());
            }

            DataPoint dp = new DataPoint(
                    id,
                    ctx.timestamp() != null ? ctx.timestamp() : System.currentTimeMillis(),
                    features,
                    label
            );

            // 提取 originalSequence / Extract originalSequence
            dp.setOriginalSequence(extractSequence(dp));

            out.collect(dp);
        } catch (IllegalStateException | UnsupportedOperationException e) {
            // sequence 策略错误不应静默吞掉 / sequence strategy errors must propagate
            throw e;
        } catch (Exception e) {
            System.err.println("[ERROR] Parse failed, skipping line: " + line + " | " + e.getMessage());
        }
    }

    /**
     * 根据策略提取 originalSequence / Extract originalSequence based on strategy.
     */
    private long extractSequence(DataPoint dp) {
        switch (sequenceSource) {
            case PARSE_ID:
                try {
                    return Long.parseLong(dp.getId());
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                            "PARSE_ID strategy requires numeric id, got: " + dp.getId(), e);
                }
            case AUTO_INCREMENT:
                return autoSequence++;
            case FROM_FIELD:
                throw new UnsupportedOperationException("FROM_FIELD not implemented in v2.1");
            default:
                throw new IllegalStateException("Unknown SequenceSource: " + sequenceSource);
        }
    }
}
