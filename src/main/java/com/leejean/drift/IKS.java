package com.leejean.drift;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * IKS — Incremental Kolmogorov–Smirnov 漂移检测器(IKSSW 语义,5a 纯 STABLE/DRIFT)。
 *
 * <p>固定 reference 窗 + 滑动 current 窗,两窗等长 W。treap 内同时存两群:
 * reference 节点贡献 −1,current 节点贡献 +1;某 key 处的 G 值
 * = #{C≤key} − #{R≤key}。KS 统计量
 * {@code D = max(treap.maxValue, −treap.minValue) / W},阈值 {@code ca·√(2/W)}。
 *
 * <p>warm-up:头 W 条分数装入 reference + seed current,期间一律返回 STABLE。
 * Live 阶段每条新值替换 current 中最旧的一个,再做 KS 检验。
 * {@code reset()} = 全清 + 重新 warm-up(非 IKSSW.Update 再基准)。
 *
 * <p>Incremental KS drift detector with IKSSW semantics. Reference window is
 * frozen at warm-up; current window slides one observation at a time.
 * Reference contributes −1 to the treap, current contributes +1. KS statistic
 * is read directly from the root aggregates in O(log W).
 *
 * <p>See {@code IKS.py} (Add/Remove/KS/Test) and {@code IKSSW.py} (sliding) for
 * the reference Python implementation this class is ported from.
 */
public class IKS implements DriftDetector {
    private static final long serialVersionUID = 1L;

    // 群组约定:0 = current(贡献 +1),1 = reference(贡献 −1)
    // Group convention: 0 = current (+1 contribution), 1 = reference (−1).
    private static final int GROUP_CURRENT = 0;
    private static final int GROUP_REFERENCE = 1;

    private final IKSConfig config;

    private Treap<IKSKey> treap;
    private long nCurrent;
    private long nReference;

    /** current 群 key,按插入顺序;Increment 时弹首加尾。 */
    private Deque<IKSKey> sliding;

    /** warm-up 期暂存前 W 个分数。 */
    private List<Double> warmupBuffer;

    private boolean initialized;

    /** 自上次 reset 起总样本数 / total samples since last reset. */
    private long n;

    /** Flink Kryo 序列化需要无参构造 / No-arg constructor for Flink Kryo serialization. */
    private IKS() {
        this.config = null;
    }

    public IKS(IKSConfig config) {
        if (config == null) throw new IllegalArgumentException("config must not be null");
        this.config = config;
        reset();
    }

    public IKSConfig getConfig() { return config; }

    // ===================== DriftDetector =====================

    @Override
    public DriftStatus update(double value) {
        n++;

        if (!initialized) {
            warmupBuffer.add(value);
            if (warmupBuffer.size() == config.getWindowSize()) {
                // 一次性把暂存分数注入 reference(−1)与 current(+1)
                // Flush warm-up buffer: each value becomes one reference and one current node.
                for (double v : warmupBuffer) {
                    long rnd = ThreadLocalRandom.current().nextLong();
                    IKSKey refKey = new IKSKey(v, rnd, GROUP_REFERENCE);
                    add(refKey, GROUP_REFERENCE);

                    long rnd2 = ThreadLocalRandom.current().nextLong();
                    IKSKey curKey = new IKSKey(v, rnd2, GROUP_CURRENT);
                    add(curKey, GROUP_CURRENT);
                    sliding.addLast(curKey);
                }
                initialized = true;
                warmupBuffer.clear();
            }
            return DriftStatus.STABLE;
        }

        // Live:滑动一格 = remove 最旧 current + add 新 current
        IKSKey oldest = sliding.pollFirst();
        remove(oldest, GROUP_CURRENT);

        long rnd = ThreadLocalRandom.current().nextLong();
        IKSKey newKey = new IKSKey(value, rnd, GROUP_CURRENT);
        add(newKey, GROUP_CURRENT);
        sliding.addLast(newKey);

        return ks() > threshold() ? DriftStatus.DRIFT : DriftStatus.STABLE;
    }

    @Override
    public void reset() {
        treap = null;
        nCurrent = 0L;
        nReference = 0L;
        sliding = new ArrayDeque<>();
        warmupBuffer = new ArrayList<>();
        initialized = false;
        n = 0L;
    }

    @Override
    public long sampleCount() { return n; }

    /** 5a:IKS 永不产 WARN / IKS never produces WARN under semantics 5a. */
    @Override
    public boolean warnTimedOut() { return false; }

    // ===================== core ops (IKS.py 移植) =====================

    /**
     * IKS.Add 移植:把 key 插入 treap,并对 key 及其后所有 key 的 G 值施加该群贡献。
     * Port of IKS.Add. Inserts {@code key} (taking value from in-order predecessor)
     * and applies the group contribution (+1 for current, −1 for reference) to the
     * new node and everything ≥ key in tree order.
     */
    void add(IKSKey key, int group) {
        if (group == GROUP_CURRENT) nCurrent++;
        else nReference++;

        Treap.SplitResult<IKSKey> sp1 = Treap.splitKeepRight(treap, key);
        Treap<IKSKey> left = sp1.left;
        Treap<IKSKey> right = sp1.right;

        // 剥前驱:left 段最大 key 即前驱(若 left 非空)
        // Pop the in-order predecessor; splitGreatest pushes lazy down to it so its
        // .value is up to date.
        Treap.SplitResult<IKSKey> sp2 = Treap.splitGreatest(left);
        left = sp2.left;
        Treap<IKSKey> leftG = sp2.right;
        long val = (leftG == null) ? 0L : leftG.value;
        left = Treap.merge(left, leftG);

        // 新节点初值 = 前驱 G 值,插到 right 段最前
        // New node starts at predecessor's G value; sumAll will then bump it by ±1.
        Treap<IKSKey> fresh = new Treap<>(key, val);
        right = Treap.merge(fresh, right);
        Treap.sumAll(right, (group == GROUP_CURRENT) ? +1L : -1L);

        treap = Treap.merge(left, right);
    }

    /**
     * IKS.Remove 移植:用 splitSmallest 精确剥掉那个 key,再对其后所有 key 施反向 ±1。
     * Port of IKS.Remove.
     */
    void remove(IKSKey key, int group) {
        if (group == GROUP_CURRENT) nCurrent--;
        else nReference--;

        Treap.SplitResult<IKSKey> sp1 = Treap.splitKeepRight(treap, key);
        Treap<IKSKey> left = sp1.left;
        Treap<IKSKey> right = sp1.right;

        // right 段最小 key 应正是 key(复合 key 全序 + 我们存的就是当初插入的那个)
        // The smallest in right should be exactly the target key (composite key uniqueness).
        Treap.SplitResult<IKSKey> sp2 = Treap.splitSmallest(right);
        Treap<IKSKey> rightL = sp2.left;
        right = sp2.right;

        if (rightL != null && rightL.key.equals(key)) {
            // 命中即丢弃 rightL,对其后段施反向 ±1 抵消该点贡献
            // Discard rightL; counter-act its contribution on what follows.
            Treap.sumAll(right, (group == GROUP_CURRENT) ? -1L : +1L);
        } else {
            // 理论上不该发生;原样并回
            // Should not happen in steady state; merge back unchanged.
            right = Treap.merge(rightL, right);
        }

        treap = Treap.merge(left, right);
    }

    /**
     * IKS.KS 移植:D = max(treap.maxValue, −treap.minValue) / W。
     * Package-private for white-box testing.
     */
    double ks() {
        if (nCurrent != nReference)
            throw new IllegalStateException("ks() requires nCurrent == nReference");
        long W = nCurrent;
        if (W == 0L || treap == null) return 0.0;
        long peak = Math.max(treap.maxValue, -treap.minValue);
        return peak / (double) W;
    }

    /** ca·√(2/W)。Package-private for tests. */
    double threshold() {
        return config.getCa() * Math.sqrt(2.0 / config.getWindowSize());
    }

    // ===================== composite key =====================

    /**
     * 复合 key:(value, rnd, group) 全序。
     * <p>rnd 用于给重复值做 tiebreak,保证 Remove 命中当初插入的那个节点。
     * <p>Composite ordering for KS treap: value first, then random tiebreak, then
     * group. The random component is essential because anomaly scores ∈ [0,1] may
     * repeat exactly.
     */
    public static final class IKSKey implements Comparable<IKSKey>, Serializable {
        private static final long serialVersionUID = 1L;

        private final double value;
        private final long rnd;
        private final int group;

        public IKSKey(double value, long rnd, int group) {
            this.value = value;
            this.rnd = rnd;
            this.group = group;
        }

        public double getValue() { return value; }
        public long getRnd() { return rnd; }
        public int getGroup() { return group; }

        @Override
        public int compareTo(IKSKey other) {
            int c = Double.compare(this.value, other.value);
            if (c != 0) return c;
            c = Long.compare(this.rnd, other.rnd);
            if (c != 0) return c;
            return Integer.compare(this.group, other.group);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof IKSKey)) return false;
            IKSKey k = (IKSKey) o;
            return Double.compare(value, k.value) == 0 && rnd == k.rnd && group == k.group;
        }

        @Override
        public int hashCode() {
            int h = Double.hashCode(value);
            h = 31 * h + Long.hashCode(rnd);
            h = 31 * h + group;
            return h;
        }

        @Override
        public String toString() {
            return "IKSKey(v=" + value + ",rnd=" + rnd + ",g=" + group + ")";
        }
    }
}
