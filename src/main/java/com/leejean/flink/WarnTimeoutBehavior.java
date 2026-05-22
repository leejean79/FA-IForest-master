package com.leejean.flink;

/**
 * WARN timeout 行为 / WARN timeout behavior.
 *
 * DISCARD: 超时后丢弃 candidateTrees，回 STABLE / discard candidates, return to STABLE
 * PROMOTE: 超时后将 candidateTrees 视为漂移确认，发出新树 / treat as drift confirmed, emit new trees
 */
public enum WarnTimeoutBehavior {
    DISCARD, PROMOTE
}
