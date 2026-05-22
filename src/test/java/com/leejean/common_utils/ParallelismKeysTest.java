package com.leejean.common_utils;

import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParallelismKeysTest {

    /**
     * 核心不变量：keys[i] 经 keyBy 路由到 subtask i，且 N 个 key 覆盖 N 个 subtask。
     * Core invariant: keys[i] routes to subtask i; the N keys cover all N subtasks.
     */
    @Test
    void keysMapOneToOneToSubtasks() {
        for (int parallelism : new int[]{1, 2, 4, 8, 16, 32}) {
            int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
            String[] keys = ParallelismKeys.generate(parallelism, maxParallelism);

            assertEquals(parallelism, keys.length);
            Set<Integer> coveredSubtasks = new HashSet<>();
            for (int i = 0; i < keys.length; i++) {
                assertNotNull(keys[i], "keys[" + i + "] should not be null");
                int subtask = KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        keys[i], maxParallelism, parallelism);
                assertEquals(i, subtask,
                        "keys[" + i + "]='" + keys[i] + "' should route to subtask " + i
                                + " but routed to " + subtask);
                coveredSubtasks.add(subtask);
            }
            assertEquals(parallelism, coveredSubtasks.size(),
                    "all subtasks should be covered for parallelism=" + parallelism);
        }
    }

    @Test
    void rejectsInvalidArguments() {
        assertThrows(IllegalArgumentException.class, () -> ParallelismKeys.generate(0, 128));
        assertThrows(IllegalArgumentException.class, () -> ParallelismKeys.generate(-1, 128));
        assertThrows(IllegalArgumentException.class, () -> ParallelismKeys.generate(8, 4));
    }
}
