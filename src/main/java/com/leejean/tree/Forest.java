package com.leejean.tree;

import java.io.Serializable;
import java.util.List;

/**
 * 经典 iForest 全局森林，持有多棵 ITree 并实现打分公式 / Classic iForest global forest with scoring.
 *
 * <p>打分公式（Liu et al. 2008 §3）/ Scoring formula:
 * <pre>
 *   H(i)   = ln(i) + 0.5772156649   (Euler's constant γ)
 *   c(n)   = 2*H(n-1) - 2*(n-1)/n   for n &gt; 1; c(1) = 0; c(0) = 0
 *
 *   对单点 x / for a single point x:
 *     h_t(x)   = pathLength(x, tree_t)  + c(leaf.size)
 *     E[h(x)]  = mean of h_t(x) over all trees
 *     score(x) = 2 ^ ( -E[h(x)] / c(subsampleSize) )
 * </pre>
 * 分数范围 [0,1]，越大越异常 / Score in [0,1], higher = more anomalous.
 */
public class Forest implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 欧拉常数 / Euler-Mascheroni constant γ */
    private static final double EULER_GAMMA = 0.5772156649;

    private final List<ITree> trees;
    private final int subsampleSize;
    private final long version;

    /**
     * @param trees         森林中的所有 iTree / all iTrees in the forest
     * @param subsampleSize 所有树共用的 ψ / shared subsample size ψ
     * @param version       森林版本号 / forest version number
     */
    public Forest(List<ITree> trees, int subsampleSize, long version) {
        this.trees = trees;
        this.subsampleSize = subsampleSize;
        this.version = version;
    }

    public List<ITree> getTrees() { return trees; }
    public int getSubsampleSize() { return subsampleSize; }
    public long getVersion() { return version; }

    /**
     * 单点打分 / Score a single point.
     *
     * @param features 特征向量 / feature vector
     * @return 异常分数 [0,1] / anomaly score [0,1]
     */
    public double score(double[] features) {
        double cPsi = averagePathLength(subsampleSize);
        if (cPsi == 0.0) {
            return 0.0; // 退化情况：subsampleSize ≤ 1 / degenerate: subsampleSize ≤ 1
        }

        double sumH = 0.0;
        for (ITree tree : trees) {
            sumH += pathLength(features, tree);
        }
        double meanH = sumH / trees.size();
        return Math.pow(2.0, -meanH / cPsi);
    }

    /**
     * 计算点在单棵树上的路径长度（含叶子修正项）
     * Compute path length of a point on a single tree (including leaf correction)
     */
    private double pathLength(double[] features, ITree tree) {
        ITreeNode node = tree.getRoot();
        int depth = 0;

        while (!node.isLeaf()) {
            if (features[node.getFeature()] < node.getThreshold()) {
                node = tree.getNode(node.getLeft());
            } else {
                node = tree.getNode(node.getRight());
            }
            depth++;
        }

        // depth = 从 root 到叶子的边数 + c(leaf.size) 修正
        // depth = edges from root to leaf + c(leaf.size) correction
        return depth + averagePathLength(node.getSize());
    }

    /**
     * 平均路径长度修正项 c(n) / Average path length correction c(n).
     *
     * <p>c(n) = 2*H(n-1) - 2*(n-1)/n，其中 H(i) = ln(i) + γ
     * c(1) = 0, c(0) = 0（约定）/ c(1) = 0, c(0) = 0 (by convention)
     */
    static double averagePathLength(int n) {
        if (n <= 1) {
            return 0.0;
        }
        double hn = Math.log(n - 1.0) + EULER_GAMMA;
        return 2.0 * hn - 2.0 * (n - 1.0) / n;
    }
}
