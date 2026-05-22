package com.leejean.common_utils;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/**
 * 为给定并行度预生成"代理 Key"，保证 keys[i] 经 Flink keyBy 后恰好路由到 subtask i。
 * Pre-generates proxy keys such that a record keyed with keys[i] is routed to subtask i.
 *
 * <p>背景 / Why:
 * Flink keyBy 先把 key 做 murmur hash，再映射到 keyGroup，最后映射到 subtask。
 * 直接用 0..N-1 的整数作 key 经过这条链路后，subtask 分布会极不均匀（碰撞 + 空缺）。
 * 本工具用穷举法找出与每个 subtask 一一对应的 Key，使下游可"有限真随机"地均匀分配。
 *
 * <p>Naive {@code key = random.nextInt(parallelism)} cannot guarantee one key per subtask
 * because of the murmur-hash → keyGroup → subtask routing. This utility brute-forces a
 * one-key-per-subtask mapping so callers can pick any key uniformly at random and still
 * cover every parallel subtask.
 */
public final class ParallelismKeys {

    private ParallelismKeys() {
    }

    /**
     * 生成长度为 parallelism 的 Key 数组：keys[i] 唯一对应 subtask i。
     * Generate keys[] of length {@code parallelism}; keys[i] maps to subtask i.
     *
     * @param parallelism    算子并行度 / operator parallelism
     * @param maxParallelism keyBy 最大并行度（即 keyGroup 数）/ max parallelism (= keyGroup count)
     */
    public static String[] generate(int parallelism, int maxParallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        if (maxParallelism < parallelism) {
            throw new IllegalArgumentException(
                    "maxParallelism (" + maxParallelism + ") must be >= parallelism (" + parallelism + ")");
        }

        String[] keys = new String[parallelism];
        int filled = 0;
        long counter = 0;
        // 安全上限：理论上 maxParallelism 个候选已足够，这里给到 16 倍冗余
        // safety bound: 16x maxParallelism candidates, plenty in practice
        long limit = (long) maxParallelism * 16L;

        while (filled < parallelism && counter < limit) {
            String candidate = "k-" + counter++;
            int subtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                    candidate, maxParallelism, parallelism);
            if (keys[subtask] == null) {
                keys[subtask] = candidate;
                filled++;
            }
        }

        if (filled < parallelism) {
            throw new IllegalStateException(
                    "Could not find a key for every subtask within " + limit + " attempts "
                            + "(parallelism=" + parallelism + ", maxParallelism=" + maxParallelism + ")");
        }
        return keys;
    }
}
