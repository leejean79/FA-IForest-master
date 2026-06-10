package com.leejean.flink;

/**
 * Phase C 内部子状态 / Phase C internal sub-state.
 *
 * <p>方向二(a) Phase 3：检测移到独立检测面后，打分面不再有 WARN / LOCAL_DRIFT_REPORTED。
 * 状态机简化为 {@code STABLE → (COMMITTED) → COOLDOWN → WAITING → STABLE}。
 */
public enum PhaseCSubState {
    STABLE, COOLDOWN, WAITING
}
