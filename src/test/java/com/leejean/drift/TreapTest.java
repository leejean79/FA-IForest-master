package com.leejean.drift;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Treap 结构正确性测试。
 *
 * <p>priority 由 ThreadLocalRandom 生成，故每次树形随机；断言只针对
 * in-order 序列 / value / size / maxValue / minValue —— 绝不断言树形。
 *
 * <p>S1 顺序构建；S2 split/merge 往返；S3 区间加 + 聚合（多轮嵌套压测，最关键）；
 * S4 反复剥离极值。
 */
class TreapTest {

    // 用一棵 升序键、value=0 的 Treap 作起手
    // Build an ascending-key, value=0 Treap by merging single nodes left-to-right.
    private static Treap<Integer> buildAscending(int n) {
        Treap<Integer> root = null;
        for (int i = 0; i < n; i++) {
            root = Treap.merge(root, new Treap<>(i, 0L));
        }
        return root;
    }

    // ---------------- S1 ----------------

    @Test
    void s1_orderedBuild_keysAreAscending_sizeMatchesN() {
        int n = 64;
        Treap<Integer> root = buildAscending(n);

        assertNotNull(root);
        assertEquals(n, Treap.size(root));

        List<Integer> keys = Treap.keysToList(root);
        assertEquals(n, keys.size());
        for (int i = 0; i < n; i++) {
            assertEquals(i, keys.get(i));
        }

        // root 聚合：value 全 0 → max=min=0
        assertEquals(0L, root.maxValue);
        assertEquals(0L, root.minValue);
    }

    // ---------------- S2 ----------------

    @Test
    void s2_splitMergeRoundTrip_preservesKeysAndValues() {
        int n = 100;
        // build with non-trivial values: value_i = i (different per key)
        Treap<Integer> root = null;
        for (int i = 0; i < n; i++) {
            root = Treap.merge(root, new Treap<>(i, (long) i));
        }

        Random rng = new Random(2026);
        // 多次随机位置 split + merge
        for (int round = 0; round < 20; round++) {
            int k = rng.nextInt(n + 2) - 1; // 包含 -1 (全归右) 和 n (全归左) 边界
            Treap.SplitResult<Integer> sp = Treap.splitKeepRight(root, k);
            // 不变量：left 段全部 < k，right 段全部 >= k
            for (Integer key : Treap.keysToList(sp.left)) {
                if (key >= k) throw new AssertionError("left contains key >= k: " + key);
            }
            for (Integer key : Treap.keysToList(sp.right)) {
                if (key < k) throw new AssertionError("right contains key < k: " + key);
            }
            root = Treap.merge(sp.left, sp.right);
        }

        // 还原后键和值都不变
        List<Integer> keys = Treap.keysToList(root);
        List<Long> values = Treap.valuesToList(root);
        assertEquals(n, keys.size());
        assertEquals(n, values.size());
        for (int i = 0; i < n; i++) {
            assertEquals(i, keys.get(i));
            assertEquals((long) i, values.get(i).longValue());
        }
        assertEquals(n, Treap.size(root));
        assertEquals((long) (n - 1), root.maxValue);
        assertEquals(0L, root.minValue);
    }

    // ---------------- S3（最关键）----------------

    @Test
    void s3_rangeAddAndAggregates_multipleNestedRounds() {
        int n = 200;
        Treap<Integer> root = buildAscending(n);
        long[] oracle = new long[n]; // brute-force per-key effective value

        Random rng = new Random(20260605L);

        // 多轮嵌套 split + sumAll + merge，把 lazy 叠加压力拉满
        int rounds = 80;
        for (int r = 0; r < rounds; r++) {
            int k = rng.nextInt(n + 1); // 0..n
            long delta = rng.nextInt(21) - 10; // 允许 +/-，包含 0

            Treap.SplitResult<Integer> sp = Treap.splitKeepRight(root, k);
            Treap.sumAll(sp.right, delta);
            root = Treap.merge(sp.left, sp.right);

            // oracle: 对所有 key >= k 的位置 += delta
            for (int i = k; i < n; i++) oracle[i] += delta;
        }

        // 逐键 effective value 应与 oracle 一致
        List<Long> values = Treap.valuesToList(root);
        assertEquals(n, values.size());
        for (int i = 0; i < n; i++) {
            assertEquals(oracle[i], values.get(i).longValue(),
                    "value mismatch at key " + i);
        }

        // root 聚合：max/min 应与 oracle 暴力 max/min 一致
        long expMax = oracle[0], expMin = oracle[0];
        for (int i = 1; i < n; i++) {
            if (oracle[i] > expMax) expMax = oracle[i];
            if (oracle[i] < expMin) expMin = oracle[i];
        }
        assertEquals(expMax, root.maxValue);
        assertEquals(expMin, root.minValue);
        assertEquals(n, Treap.size(root));
    }

    @Test
    void s3_extra_splitInsideRightSegment_lazyPropagatesThroughDeepNesting() {
        // 在 right 段上再 split + sumAll 一次,然后逐级 merge 回去,
        // 强制让 lazy 在多层 split/merge 中被多次推动
        int n = 120;
        Treap<Integer> root = buildAscending(n);
        long[] oracle = new long[n];

        Random rng = new Random(7L);
        for (int r = 0; r < 30; r++) {
            int k1 = rng.nextInt(n + 1);
            long d1 = rng.nextInt(11) - 5;

            Treap.SplitResult<Integer> sp1 = Treap.splitKeepRight(root, k1);
            Treap.sumAll(sp1.right, d1);
            for (int i = k1; i < n; i++) oracle[i] += d1;

            // 在 right 段内再切一刀
            int k2 = k1 + rng.nextInt(Math.max(1, n - k1 + 1));
            long d2 = rng.nextInt(11) - 5;
            Treap.SplitResult<Integer> sp2 = Treap.splitKeepRight(sp1.right, k2);
            Treap.sumAll(sp2.right, d2);
            for (int i = k2; i < n; i++) oracle[i] += d2;

            // 三段 merge 回去（顺序必须保持 key 单调）
            Treap<Integer> rightMerged = Treap.merge(sp2.left, sp2.right);
            root = Treap.merge(sp1.left, rightMerged);
        }

        List<Long> values = Treap.valuesToList(root);
        for (int i = 0; i < n; i++) {
            assertEquals(oracle[i], values.get(i).longValue(),
                    "nested value mismatch at key " + i);
        }
        long expMax = oracle[0], expMin = oracle[0];
        for (int i = 1; i < n; i++) {
            if (oracle[i] > expMax) expMax = oracle[i];
            if (oracle[i] < expMin) expMin = oracle[i];
        }
        assertEquals(expMax, root.maxValue);
        assertEquals(expMin, root.minValue);
        assertEquals(n, Treap.size(root));
    }

    // ---------------- S4 ----------------

    @Test
    void s4_splitSmallest_yieldsAscendingKeys_andRemainingAggregatesCorrect() {
        int n = 50;
        // value_i = i*10（让 max/min 一眼可验）
        Treap<Integer> root = null;
        long[] originalValues = new long[n];
        for (int i = 0; i < n; i++) {
            originalValues[i] = i * 10L;
            root = Treap.merge(root, new Treap<>(i, originalValues[i]));
        }

        List<Integer> drained = new ArrayList<>();
        for (int step = 0; step < n; step++) {
            Treap.SplitResult<Integer> sp = Treap.splitSmallest(root);
            assertNotNull(sp.left, "smallest must exist while tree non-empty");
            // sp.left 应为单节点（key 最小、size=1）
            assertEquals(1, Treap.size(sp.left));
            drained.add(sp.left.key);
            assertEquals(originalValues[step], sp.left.value, "value preserved");

            root = sp.right;
            int remaining = n - step - 1;
            assertEquals(remaining, Treap.size(root));
            if (remaining > 0) {
                // 剩余 key 范围是 [step+1, n-1]
                assertEquals((long) (n - 1) * 10L, root.maxValue);
                assertEquals((long) (step + 1) * 10L, root.minValue);
            } else {
                assertNull(root);
            }
        }
        // 依次得升序最小 key
        for (int i = 0; i < n; i++) {
            assertEquals(i, drained.get(i));
        }
    }

    @Test
    void s4_splitGreatest_yieldsDescendingKeys_andRemainingAggregatesCorrect() {
        int n = 50;
        Treap<Integer> root = null;
        long[] originalValues = new long[n];
        for (int i = 0; i < n; i++) {
            originalValues[i] = i * 10L;
            root = Treap.merge(root, new Treap<>(i, originalValues[i]));
        }

        List<Integer> drained = new ArrayList<>();
        for (int step = 0; step < n; step++) {
            Treap.SplitResult<Integer> sp = Treap.splitGreatest(root);
            assertNotNull(sp.right, "greatest must exist while tree non-empty");
            assertEquals(1, Treap.size(sp.right));
            int expectedKey = n - 1 - step;
            drained.add(sp.right.key);
            assertEquals(originalValues[expectedKey], sp.right.value, "value preserved");

            root = sp.left;
            int remaining = n - step - 1;
            assertEquals(remaining, Treap.size(root));
            if (remaining > 0) {
                // 剩余 key 范围是 [0, n-2-step]
                assertEquals((long) (n - 2 - step) * 10L, root.maxValue);
                assertEquals(0L, root.minValue);
            } else {
                assertNull(root);
            }
        }
        List<Integer> expectedDesc = new ArrayList<>();
        for (int i = n - 1; i >= 0; i--) expectedDesc.add(i);
        assertEquals(expectedDesc, drained);
    }

    @Test
    void s4_splitSmallestAfterRangeAdd_extractsCorrectValue() {
        // 让 lazy 在 splitSmallest 路径上被强制下推：先 sumAll 整树，
        // 再 splitSmallest 应拿到带 delta 的值。
        int n = 30;
        Treap<Integer> root = buildAscending(n);
        long delta = 7L;
        Treap.sumAll(root, delta);

        for (int step = 0; step < n; step++) {
            Treap.SplitResult<Integer> sp = Treap.splitSmallest(root);
            assertEquals(delta, sp.left.value, "lazy should propagate through splitSmallest");
            assertEquals(step, sp.left.key.intValue());
            root = sp.right;
        }
        assertNull(root);
    }

    // ---------------- 边界 ----------------

    @Test
    void nullSafetyHelpers() {
        assertEquals(0, Treap.size(null));
        assertEquals(Collections.emptyList(), Treap.keysToList(null));
        assertEquals(Collections.emptyList(), Treap.valuesToList(null));

        // sumAll / unlazy / update on null are no-op
        Treap.sumAll(null, 5L);
        Treap.unlazy(null);
        Treap.update(null);

        Treap.SplitResult<Integer> sp1 = Treap.splitKeepRight(null, 0);
        assertNull(sp1.left);
        assertNull(sp1.right);

        Treap.SplitResult<Integer> sp2 = Treap.splitSmallest(null);
        assertNull(sp2.left);
        assertNull(sp2.right);

        Treap.SplitResult<Integer> sp3 = Treap.splitGreatest(null);
        assertNull(sp3.left);
        assertNull(sp3.right);

        // merge with null on either side returns the other
        Treap<Integer> a = new Treap<>(1, 99L);
        assertEquals(a, Treap.merge(null, a));
        assertEquals(a, Treap.merge(a, null));
        assertNull(Treap.merge(null, null));
    }

    @Test
    void splitKeepRight_equalKeysGoToRight() {
        // 单节点 key=5：split(5) → left=null, right=该节点
        Treap<Integer> root = new Treap<>(5, 42L);
        Treap.SplitResult<Integer> sp = Treap.splitKeepRight(root, 5);
        assertNull(sp.left);
        assertNotNull(sp.right);
        assertEquals(5, sp.right.key.intValue());
        assertEquals(42L, sp.right.value);
    }
}
