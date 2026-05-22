package com.leejean.common_utils;

/**
 * DataPoint.originalSequence 的提取策略 / Strategy for extracting DataPoint.originalSequence.
 */
public enum SequenceSource {
    /** 用 Long.parseLong(id)，要求 id 是数字字符串 / Parse id as long; requires numeric id */
    PARSE_ID,

    /** 用自增计数器 / Auto-increment counter (fallback when id is not numeric) */
    AUTO_INCREMENT,

    /** 从 CSV 的指定列读取（v2.1 暂未实现）/ Read from a specific CSV column (not implemented in v2.1) */
    FROM_FIELD
}
