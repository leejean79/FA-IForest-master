package com.leejean.flink;

/**
 * COOLDOWN / WAITING 期的样本处理模式。
 * Sample-handling mode while in COOLDOWN / WAITING (driven by COMMITTED).
 *
 * <ul>
 *   <li>{@link #USE_OLD_FOREST}：继续用旧森林打分输出</li>
 *   <li>{@link #BACKLOG_THEN_NEW_FOREST}：数据进 backlog，等新森林到来后再批量打分</li>
 * </ul>
 */
public enum PauseMode {
    USE_OLD_FOREST,
    BACKLOG_THEN_NEW_FOREST
}
