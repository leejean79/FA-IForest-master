package com.leejean.flink;

/**
 * v3.4 LOCAL_DRIFT_REPORTED 状态下的暂停模式
 * v3.4 pause mode during LOCAL_DRIFT_REPORTED state.
 *
 * <ul>
 *   <li>{@link #USE_OLD_FOREST}：继续用旧森林打分输出，HDDM 暂停</li>
 *   <li>{@link #BACKLOG_THEN_NEW_FOREST}：数据进 backlog，等新森林到来后再批量打分</li>
 * </ul>
 */
public enum PauseMode {
    USE_OLD_FOREST,
    BACKLOG_THEN_NEW_FOREST
}
