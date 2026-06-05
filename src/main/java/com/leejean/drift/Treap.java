package com.leejean.drift;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Treap with lazy range-add and subtree min/max aggregates.
 *
 * <p>BST 排序键 by {@code key}，最大堆排序 by 随机 {@code priority}。每个节点维护
 * 子树的 {@code maxValue}/{@code minValue}/{@code size}，并通过 {@code lazy}
 * 标记支持 O(1) 区间加。Phase 2 IKS 通过 {@code root.maxValue}/{@code root.minValue}
 * 读取 KS 统计量；通过 {@code splitKeepRight} + {@code sumAll} + {@code merge}
 * 完成单次 Add/Remove 后整段 +1/-1 的 ECDF 更新。
 *
 * <p>Tree-Heap data structure with lazy propagation. BST ordered by key,
 * max-heap ordered by random priority. Each node maintains subtree min/max
 * of {@code value} (long) and supports O(1) range-add via lazy markers.
 *
 * <p>Port of {@code Treap.py} for IKSSW drift detector (phase 2). This class
 * is package-private; its sole user is the upcoming {@code IKS} detector.
 */
class Treap<K extends Comparable<K>> {

    final K key;
    long value;
    final double priority;

    /** 待下推给左右子树的增量 / pending delta to push down to children. */
    long lazy;

    /** 子树内 value 最大值 / max value within subtree. */
    long maxValue;
    /** 子树内 value 最小值 / min value within subtree. */
    long minValue;
    /** 子树节点数 / number of nodes in subtree. */
    int size;

    Treap<K> left;
    Treap<K> right;

    Treap(K key, long value) {
        this.key = key;
        this.value = value;
        this.priority = ThreadLocalRandom.current().nextDouble();
        this.lazy = 0L;
        this.maxValue = value;
        this.minValue = value;
        this.size = 1;
        this.left = null;
        this.right = null;
    }

    Treap(K key) {
        this(key, 0L);
    }

    // ====================== aggregates / 聚合 ======================

    static <K extends Comparable<K>> int size(Treap<K> node) {
        return node == null ? 0 : node.size;
    }

    /**
     * O(1) 整子树加常数：直接更新 node 自身的 value/maxValue/minValue，
     * 并把 delta 累加到 lazy，留待后续操作下推到子节点。
     * O(1) range-add over an entire subtree.
     */
    static <K extends Comparable<K>> void sumAll(Treap<K> node, long delta) {
        if (node == null) return;
        node.value += delta;
        node.maxValue += delta;
        node.minValue += delta;
        node.lazy += delta;
    }

    /**
     * 将 node.lazy 下推给左右子（经 sumAll），再清零。
     * 结构性递归进入子树前必须调用，确保子树视图最新。
     * Push lazy down to children. MUST be called before structural recursion.
     */
    static <K extends Comparable<K>> void unlazy(Treap<K> node) {
        if (node == null || node.lazy == 0L) return;
        sumAll(node.left, node.lazy);
        sumAll(node.right, node.lazy);
        node.lazy = 0L;
    }

    /**
     * 由左右子重算 node 的 size/maxValue/minValue。先 unlazy 再聚合。
     * Recomputes aggregates from children. Calls unlazy first.
     */
    static <K extends Comparable<K>> void update(Treap<K> node) {
        if (node == null) return;
        unlazy(node);
        int s = 1;
        long mx = node.value;
        long mn = node.value;
        if (node.left != null) {
            s += node.left.size;
            if (node.left.maxValue > mx) mx = node.left.maxValue;
            if (node.left.minValue < mn) mn = node.left.minValue;
        }
        if (node.right != null) {
            s += node.right.size;
            if (node.right.maxValue > mx) mx = node.right.maxValue;
            if (node.right.minValue < mn) mn = node.right.minValue;
        }
        node.size = s;
        node.maxValue = mx;
        node.minValue = mn;
    }

    // ====================== split / merge ======================

    /**
     * 按 key 拆分：left = {key' &lt; key}，right = {key' &ge; key}。
     * 相等键归右半（phase 2 的 IKS.Add 依赖此边界）。
     * Equal keys go to the right half.
     */
    static <K extends Comparable<K>> SplitResult<K> splitKeepRight(Treap<K> node, K key) {
        if (node == null) return new SplitResult<>(null, null);
        unlazy(node);
        int cmp = key.compareTo(node.key);
        if (cmp <= 0) {
            // key <= node.key → node 进右半
            SplitResult<K> sub = splitKeepRight(node.left, key);
            node.left = sub.right;
            update(node);
            return new SplitResult<>(sub.left, node);
        } else {
            // key > node.key → node 进左半
            SplitResult<K> sub = splitKeepRight(node.right, key);
            node.right = sub.left;
            update(node);
            return new SplitResult<>(node, sub.right);
        }
    }

    /**
     * 前置：left 全部 key &lt; right 全部 key。
     * 按 priority 维持最大堆序（较高 priority 作根）。
     * Precondition: all keys in {@code left} are less than all keys in {@code right}.
     */
    static <K extends Comparable<K>> Treap<K> merge(Treap<K> left, Treap<K> right) {
        if (left == null) return right;
        if (right == null) return left;
        if (left.priority > right.priority) {
            unlazy(left);
            left.right = merge(left.right, right);
            update(left);
            return left;
        } else {
            unlazy(right);
            right.left = merge(left, right.left);
            update(right);
            return right;
        }
    }

    /** 剥离最小 key 的单节点（论文 SplitFirst）。Splits off the node with smallest key. */
    static <K extends Comparable<K>> SplitResult<K> splitSmallest(Treap<K> node) {
        if (node == null) return new SplitResult<>(null, null);
        unlazy(node);
        if (node.left == null) {
            Treap<K> rest = node.right;
            node.right = null;
            update(node);
            return new SplitResult<>(node, rest);
        }
        SplitResult<K> sub = splitSmallest(node.left);
        node.left = sub.right;
        update(node);
        return new SplitResult<>(sub.left, node);
    }

    /** 剥离最大 key 的单节点（论文 SplitLast）。Splits off the node with greatest key. */
    static <K extends Comparable<K>> SplitResult<K> splitGreatest(Treap<K> node) {
        if (node == null) return new SplitResult<>(null, null);
        unlazy(node);
        if (node.right == null) {
            Treap<K> rest = node.left;
            node.left = null;
            update(node);
            return new SplitResult<>(rest, node);
        }
        SplitResult<K> sub = splitGreatest(node.right);
        node.right = sub.left;
        update(node);
        return new SplitResult<>(node, sub.right);
    }

    // ====================== traversal helpers (tests) ======================

    /** in-order keys / 中序键序列。 */
    static <K extends Comparable<K>> List<K> keysToList(Treap<K> node) {
        List<K> out = new ArrayList<>();
        keysInOrder(node, out);
        return out;
    }

    /**
     * in-order effective values / 中序“有效值”序列。
     * 不修改树：用 carry 把未下推的 ancestor lazy 计入当前 effective value。
     * Does not mutate; carries ancestor lazy via parameter.
     */
    static <K extends Comparable<K>> List<Long> valuesToList(Treap<K> node) {
        List<Long> out = new ArrayList<>();
        valuesInOrder(node, 0L, out);
        return out;
    }

    private static <K extends Comparable<K>> void keysInOrder(Treap<K> node, List<K> out) {
        if (node == null) return;
        keysInOrder(node.left, out);
        out.add(node.key);
        keysInOrder(node.right, out);
    }

    private static <K extends Comparable<K>> void valuesInOrder(Treap<K> node, long inheritedLazy, List<Long> out) {
        if (node == null) return;
        // 当前节点 effective value：自身 value 已包含 sumAll 直接施加的部分，
        // 还需补上未下推到本层的 ancestor lazy。
        // 子节点继承的 lazy = inheritedLazy + 当前 node.lazy（node.lazy 尚未推下去）。
        long childCarry = inheritedLazy + node.lazy;
        valuesInOrder(node.left, childCarry, out);
        out.add(node.value + inheritedLazy);
        valuesInOrder(node.right, childCarry, out);
    }

    // ====================== split holder ======================

    static final class SplitResult<K extends Comparable<K>> {
        final Treap<K> left;
        final Treap<K> right;

        SplitResult(Treap<K> left, Treap<K> right) {
            this.left = left;
            this.right = right;
        }
    }
}
