package com.leejean.tree;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Forest 打分逻辑测试 / Forest scoring tests
 */
public class ForestTest {

    /**
     * 手动构造已知小树，验证 path length 正确
     * Manually build a known small tree, verify path length
     *
     * 树结构 / Tree structure:
     *        node0 (feature=0, threshold=5.0)
     *       /    \
     *   leaf1    leaf2
     *   size=3   size=7
     */
    @Test
    public void testPathLengthOnKnownTree() {
        List<ITreeNode> nodes = Arrays.asList(
                ITreeNode.internal(0, 0, 5.0, 1, 2, 10),
                ITreeNode.leaf(1, 3),
                ITreeNode.leaf(2, 7)
        );
        ITree tree = new ITree(nodes, 10, 4, 2);

        Forest forest = new Forest(Arrays.asList(tree), 10, 1L);

        // x[0]=3.0 < 5.0 → 走左子 leaf1(size=3)
        // pathLength = 1 (depth) + c(3)
        // c(3) = 2*(ln(2)+γ) - 2*2/3 ≈ 2*(0.6931+0.5772) - 1.3333 ≈ 1.2073
        // E[h] = 1 + 1.2073 = 2.2073
        // c(10) = 2*(ln(9)+γ) - 2*9/10 ≈ 2*(2.1972+0.5772) - 1.8 ≈ 3.7488
        // score = 2^(-2.2073/3.7488)
        double scoreLeft = forest.score(new double[]{3.0, 0.0});
        assertTrue(scoreLeft > 0 && scoreLeft < 1, "Score should be in (0,1), got " + scoreLeft);

        // x[0]=8.0 >= 5.0 → 走右子 leaf2(size=7)
        double scoreRight = forest.score(new double[]{8.0, 0.0});
        assertTrue(scoreRight > 0 && scoreRight < 1, "Score should be in (0,1), got " + scoreRight);

        // leaf1 size=3 < leaf2 size=7 → left 应更异常（更短路径+更少修正 → 更高分）
        // leaf1 有更少样本 → path length 更短 → 分数更高
        assertTrue(scoreLeft > scoreRight,
                "Point hitting smaller leaf should score higher (more anomalous)");
    }

    /**
     * c(n) 边界值测试 / averagePathLength boundary tests
     */
    @Test
    public void testAveragePathLengthBoundary() {
        assertEquals(0.0, Forest.averagePathLength(0), 1e-10);
        assertEquals(0.0, Forest.averagePathLength(1), 1e-10);
        // c(2) = 2*(ln(1)+γ) - 2*1/2 = 2*γ - 1 ≈ 0.1544
        assertTrue(Forest.averagePathLength(2) > 0);
        // c(256) = 2*(ln(255)+γ) - 2*255/256 ≈ 10.24（与 sklearn 一致）
        // c(256) = 2*(ln(255)+γ) - 2*255/256 ≈ 10.24 (matches sklearn)
        double c256 = Forest.averagePathLength(256);
        assertTrue(c256 > 10.0 && c256 < 10.5,
                "c(256) should be ~10.24, got " + c256);
    }

    /**
     * 异常点（远离训练数据）分数 > 0.7
     * Anomalous point (far from training data) should score > 0.7
     */
    @Test
    public void testAnomalousPointScoresHigh() {
        // 训练 100 棵树，数据集中在 [0,10] 范围
        // Train 100 trees, data concentrated in [0,10]
        int numTrees = 100;
        int subsampleSize = 256;
        int dim = 2;
        Random rng = new Random(42);

        // 生成正常数据 / Generate normal data
        double[][] data = new double[500][dim];
        for (int i = 0; i < 500; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = rng.nextGaussian() * 2 + 5; // 均值 5，标准差 2
            }
        }

        ITreeBuilder builder = new ITreeBuilder(42);
        List<ITree> trees = new ArrayList<>();
        for (int t = 0; t < numTrees; t++) {
            trees.add(builder.build(data, subsampleSize));
        }

        Forest forest = new Forest(trees, subsampleSize, 1L);

        // 异常点：远离中心 / Anomalous point: far from center
        double scoreAnomaly = forest.score(new double[]{100.0, 100.0});
        assertTrue(scoreAnomaly > 0.7,
                "Anomalous point should score > 0.7, got " + scoreAnomaly);
    }

    /**
     * 正常点（在训练数据中心）分数 < 0.6
     * Normal point (near training data center) should score < 0.6
     */
    @Test
    public void testNormalPointScoresLow() {
        int numTrees = 100;
        int subsampleSize = 256;
        int dim = 2;
        Random rng = new Random(42);

        double[][] data = new double[500][dim];
        for (int i = 0; i < 500; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = rng.nextGaussian() * 2 + 5;
            }
        }

        ITreeBuilder builder = new ITreeBuilder(42);
        List<ITree> trees = new ArrayList<>();
        for (int t = 0; t < numTrees; t++) {
            trees.add(builder.build(data, subsampleSize));
        }

        Forest forest = new Forest(trees, subsampleSize, 1L);

        // 正常点：数据集中心附近 / Normal point: near center of training data
        double scoreNormal = forest.score(new double[]{5.0, 5.0});
        assertTrue(scoreNormal < 0.6,
                "Normal point should score < 0.6, got " + scoreNormal);
    }

    /**
     * 100 棵树能区分注入的离群点 / 100-tree forest distinguishes injected outliers
     */
    @Test
    public void testForestDistinguishesOutliers() {
        int numTrees = 100;
        int subsampleSize = 256;
        int dim = 9; // shuttle 维度
        Random rng = new Random(123);

        // 正常数据：均值 50，标准差 10 / Normal data: mean 50, std 10
        double[][] data = new double[1000][dim];
        for (int i = 0; i < 1000; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = rng.nextGaussian() * 10 + 50;
            }
        }

        ITreeBuilder builder = new ITreeBuilder(123);
        List<ITree> trees = new ArrayList<>();
        for (int t = 0; t < numTrees; t++) {
            trees.add(builder.build(data, subsampleSize));
        }

        Forest forest = new Forest(trees, subsampleSize, 1L);

        // 正常点分数 / Normal point scores
        double scoreNormal1 = forest.score(new double[]{50, 50, 50, 50, 50, 50, 50, 50, 50});
        double scoreNormal2 = forest.score(new double[]{55, 45, 52, 48, 50, 53, 47, 51, 49});

        // 离群点分数 / Outlier scores
        double scoreOutlier1 = forest.score(new double[]{200, 200, 200, 200, 200, 200, 200, 200, 200});
        double scoreOutlier2 = forest.score(new double[]{-100, -100, -100, -100, -100, -100, -100, -100, -100});

        // 离群点应比正常点分数高 / Outliers should score higher than normals
        assertTrue(scoreOutlier1 > scoreNormal1,
                "Outlier1 (" + scoreOutlier1 + ") should score > Normal1 (" + scoreNormal1 + ")");
        assertTrue(scoreOutlier2 > scoreNormal2,
                "Outlier2 (" + scoreOutlier2 + ") should score > Normal2 (" + scoreNormal2 + ")");
        assertTrue(scoreOutlier1 > 0.6, "Far outlier should score > 0.6, got " + scoreOutlier1);
    }
}
