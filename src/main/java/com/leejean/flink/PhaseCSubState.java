package com.leejean.flink;

/**
 * Phase C 内部子状态 / Phase C internal sub-state.
 */
public enum PhaseCSubState {
    STABLE, WARN, LOCAL_DRIFT_REPORTED, COOLDOWN, WAITING
    // DRIFT 是瞬时状态，不会被持久化 / DRIFT is transient, never persisted
}
