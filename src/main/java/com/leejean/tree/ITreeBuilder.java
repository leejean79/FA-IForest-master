package com.leejean.tree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 经典 iForest iTree 构建器 / Classic iForest iTree builder.
 *
 * <p>算法 / Algorithm (Liu et al. 2008):
 * <ol>
 *   <li>从数据集中无放回抽样 ψ 条作为训练集 / Sub-sample ψ rows without replacement.</li>
 *   <li>从根节点开始递归：随机选一维 q，在该维 [min, max] 区间内随机选阈值 p，按 p 切左右两份。
 *       Recursively split: pick a random dimension q, then a random threshold p in [min, max] of q.</li>
 *   <li>停止条件 / Stop when:
 *     <ul>
 *       <li>样本数 ≤ 1 / sample count ≤ 1</li>
 *       <li>深度 ≥ ceil(log2(ψ)) / depth ≥ ceil(log2(ψ))</li>
 *       <li>选中维度上所有值相同（无法切分）/ all values equal on the chosen dimension (cannot split)</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>线程安全 / Thread-safety: 不是线程安全的；每个调用方持有自己的 builder 实例。
 * Not thread-safe; each caller owns its own builder instance.
 */
public class ITreeBuilder {

    private final Random random;

    /** 默认构造：使用系统时间 seed / Default constructor seeds from system time. */
    public ITreeBuilder() {
        this(new Random());
    }

    /** 测试用：固定 seed / For tests: fixed seed for reproducibility. */
    public ITreeBuilder(long seed) {
        this(new Random(seed));
    }

    public ITreeBuilder(Random random) {
        this.random = random;
    }

    /**
     * 构建一棵 iTree / Build one iTree.
     *
     * @param data           训练数据（每行一个样本，已按 [N][D] 组织）/ training data, [N rows][D dims]
     * @param subsampleSize  期望的子采样数 ψ / target subsample size ψ
     * @return 一棵 ITree（节点已扁平化）/ a fully built ITree (flat node list)
     */
    public ITree build(double[][] data, int subsampleSize) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("data must be non-empty");
        }
        if (subsampleSize <= 0) {
            throw new IllegalArgumentException("subsampleSize must be > 0, got " + subsampleSize);
        }

        int dimension = data[0].length;
        // 子采样：实际 ψ 取 min(请求量, 可用量) / actual ψ = min(requested, available)
        int actualSubsample = Math.min(subsampleSize, data.length);
        double[][] sample = subsample(data, actualSubsample);

        // 深度上限 = ceil(log2(ψ))，与论文一致 / depth limit per the paper
        int depthLimit = (int) Math.ceil(Math.log(actualSubsample) / Math.log(2.0));
        if (depthLimit < 1) {
            depthLimit = 1; // ψ=1 时给 1 层兜底 / floor to 1 when ψ=1
        }

        List<ITreeNode> nodes = new ArrayList<>();
        // 递归构建，根节点 id = 0 / recursive build, root id = 0
        buildRecursive(sample, 0, depthLimit, dimension, nodes);

        return new ITree(nodes, actualSubsample, depthLimit, dimension);
    }

    /**
     * 递归构建 / Recursive build. 返回新创建节点的 id。
     *
     * @return id of the node just appended to {@code nodes}
     */
    private int buildRecursive(double[][] data,
                               int currentDepth,
                               int depthLimit,
                               int dimension,
                               List<ITreeNode> nodes) {
        int n = data.length;

        // 停止条件 1+2 / stop conditions 1 & 2
        if (currentDepth >= depthLimit || n <= 1) {
            return appendLeaf(nodes, n);
        }

        // 随机选一维 / pick a random dimension
        int feature = random.nextInt(dimension);

        // 找该维度上的 [min, max] / find min/max on this dimension
        double min = data[0][feature];
        double max = data[0][feature];
        for (int i = 1; i < n; i++) {
            double v = data[i][feature];
            if (v < min) min = v;
            else if (v > max) max = v;
        }

        // 停止条件 3：该维度全相同 → 叶子 / cannot split if all equal on this dim
        if (min == max) {
            return appendLeaf(nodes, n);
        }

        // 在 (min, max) 区间随机选阈值 / random threshold in (min, max)
        double threshold = min + (max - min) * random.nextDouble();

        // 切分 / partition into left (< threshold) and right (>= threshold)
        List<double[]> leftList = new ArrayList<>();
        List<double[]> rightList = new ArrayList<>();
        for (double[] row : data) {
            if (row[feature] < threshold) {
                leftList.add(row);
            } else {
                rightList.add(row);
            }
        }

        // 极端情况：阈值正好等于 max，rightList 为空（左闭右开切分）
        // 或者所有值靠近 min/max 边界导致一侧为空。退化为叶子。
        // Edge case: a side is empty (all values lumped together). Fall back to leaf.
        if (leftList.isEmpty() || rightList.isEmpty()) {
            return appendLeaf(nodes, n);
        }

        // 先占位当前节点（占据当前 id），再递归填左右
        // Reserve this node's id first, then recurse into children.
        // 这样保证父节点 id < 子节点 id（前序），便于反序列化与遍历。
        int myId = nodes.size();
        nodes.add(null); // placeholder

        double[][] leftArr = leftList.toArray(new double[0][]);
        double[][] rightArr = rightList.toArray(new double[0][]);

        int leftId = buildRecursive(leftArr, currentDepth + 1, depthLimit, dimension, nodes);
        int rightId = buildRecursive(rightArr, currentDepth + 1, depthLimit, dimension, nodes);

        nodes.set(myId, ITreeNode.internal(myId, feature, threshold, leftId, rightId, n));
        return myId;
    }

    private int appendLeaf(List<ITreeNode> nodes, int size) {
        int id = nodes.size();
        nodes.add(ITreeNode.leaf(id, size));
        return id;
    }

    /**
     * v3.1 新增：从一个数据池子随机采样 sampleSize 条，构建一棵 iTree。
     * 用于环形缓冲场景——pool 通常大于 sampleSize，做无放回采样。
     *
     * v3.1: Build an iTree by randomly sampling sampleSize rows from a pool.
     * For ring buffer scenarios where pool > sampleSize; uses sampling without replacement.
     *
     * @param pool       数据池（每个元素是一个 double[] 特征向量）/ data pool (each element is a feature vector)
     * @param sampleSize 期望的子采样数 ψ / target subsample size ψ
     * @return 一棵 ITree / a fully built ITree
     */
    public ITree buildFromPool(List<double[]> pool, int sampleSize) {
        if (pool == null || pool.isEmpty()) {
            throw new IllegalArgumentException("pool must be non-empty");
        }
        int actualN = Math.min(sampleSize, pool.size());
        // 复用同一个 random（seed 已初始化）/ reuse the same random instance
        List<double[]> shuffled = new ArrayList<>(pool);
        Collections.shuffle(shuffled, random);
        double[][] sample = new double[actualN][];
        for (int i = 0; i < actualN; i++) {
            sample[i] = shuffled.get(i);
        }
        return build(sample, actualN);
    }

    /**
     * Fisher-Yates 部分洗牌实现无放回抽样 / Reservoir-style partial Fisher-Yates for subsampling.
     *
     * <p>避免 shuffle 整个 N 行，时间 O(ψ) / shuffles only the first ψ slots.
     */
    private double[][] subsample(double[][] data, int sampleSize) {
        int n = data.length;
        if (sampleSize >= n) {
            // 返回拷贝避免外部修改影响构建过程 / defensive copy
            return Arrays.copyOf(data, n);
        }
        // 索引数组 0..n-1 / index array
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        // 部分 Fisher-Yates：把前 sampleSize 个位置填上随机选中的索引
        // partial F-Y: fill the first sampleSize slots with random picks
        List<Integer> idxList = Arrays.asList(idx);
        // 注意 Collections.shuffle 是全量 shuffle；这里手写部分 shuffle 节省时间
        for (int i = 0; i < sampleSize; i++) {
            int j = i + random.nextInt(n - i);
            Collections.swap(idxList, i, j);
        }
        double[][] out = new double[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            out[i] = data[idxList.get(i)];
        }
        return out;
    }
}
