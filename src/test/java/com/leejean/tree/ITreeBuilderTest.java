package com.leejean.tree;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ITreeBuilder 单元测试 / Unit tests for ITreeBuilder.
 */
class ITreeBuilderTest {

    /** 生成 9 维高斯数据，模拟 shuttle 数据集的形态 / 9-dim Gaussian, mimics shuttle features. */
    private double[][] generateData(int n, int dim, long seed) {
        Random r = new Random(seed);
        double[][] data = new double[n][dim];
        for (int i = 0; i < n; i++) {
            for (int d = 0; d < dim; d++) {
                data[i][d] = r.nextGaussian() * 10.0;
            }
        }
        return data;
    }

    @Test
    void shouldBuildTreeWithCorrectMetadata() {
        double[][] data = generateData(256, 9, 42L);
        ITreeBuilder builder = new ITreeBuilder(123L);

        ITree tree = builder.build(data, 256);

        assertEquals(256, tree.getSubsampleSize());
        assertEquals(9, tree.getDimension());
        // ceil(log2(256)) = 8
        assertEquals(8, tree.getDepthLimit());
        assertFalse(tree.getNodes().isEmpty(), "tree should have at least one node");
    }

    @Test
    void rootShouldBeNodeZero() {
        double[][] data = generateData(256, 9, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        assertEquals(0, tree.getRoot().getId());
    }

    @Test
    void leafSampleSizesShouldSumToSubsample() {
        // iForest 性质：所有叶子的 size 之和应等于 ψ
        // iForest invariant: leaf sizes sum to ψ
        double[][] data = generateData(500, 9, 11L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        int leafSum = tree.getNodes().stream()
                .filter(ITreeNode::isLeaf)
                .mapToInt(ITreeNode::getSize)
                .sum();

        assertEquals(256, leafSum, "sum of leaf sizes should equal subsample size");
    }

    @Test
    void allInternalNodesShouldHaveValidChildIds() {
        double[][] data = generateData(256, 9, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        for (ITreeNode node : tree.getNodes()) {
            if (!node.isLeaf()) {
                assertTrue(node.getLeft() >= 0 && node.getLeft() < tree.size(),
                        "left child id out of range: " + node);
                assertTrue(node.getRight() >= 0 && node.getRight() < tree.size(),
                        "right child id out of range: " + node);
                // 子节点 id 必须大于父节点（前序构建保证）/ child id > parent id (preorder build)
                assertTrue(node.getLeft() > node.getId(), "left child should have greater id");
                assertTrue(node.getRight() > node.getId(), "right child should have greater id");
            }
        }
    }

    @Test
    void shouldRespectDepthLimit() {
        double[][] data = generateData(256, 9, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        // 检查每个节点的实际深度都 ≤ depthLimit
        // walk from root, ensure no path exceeds depthLimit
        int maxDepth = computeMaxDepth(tree, 0, 0);
        assertTrue(maxDepth <= tree.getDepthLimit(),
                "max depth " + maxDepth + " exceeded limit " + tree.getDepthLimit());
    }

    private int computeMaxDepth(ITree tree, int nodeId, int currentDepth) {
        ITreeNode node = tree.getNode(nodeId);
        if (node.isLeaf()) {
            return currentDepth;
        }
        int leftDepth = computeMaxDepth(tree, node.getLeft(), currentDepth + 1);
        int rightDepth = computeMaxDepth(tree, node.getRight(), currentDepth + 1);
        return Math.max(leftDepth, rightDepth);
    }

    @Test
    void fixedSeedShouldGiveReproducibleTrees() {
        double[][] data = generateData(256, 9, 42L);

        ITree t1 = new ITreeBuilder(99L).build(data, 256);
        ITree t2 = new ITreeBuilder(99L).build(data, 256);

        assertEquals(t1.size(), t2.size(), "same seed should give same node count");
        for (int i = 0; i < t1.size(); i++) {
            ITreeNode n1 = t1.getNode(i);
            ITreeNode n2 = t2.getNode(i);
            assertEquals(n1.getFeature(), n2.getFeature());
            assertEquals(n1.getThreshold(), n2.getThreshold(), 1e-12);
            assertEquals(n1.getLeft(), n2.getLeft());
            assertEquals(n1.getRight(), n2.getRight());
            assertEquals(n1.getSize(), n2.getSize());
        }
    }

    @Test
    void shouldHandleSingleSample() {
        double[][] data = {{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0}};
        ITree tree = new ITreeBuilder(7L).build(data, 1);

        assertEquals(1, tree.size(), "single sample should produce a single leaf");
        assertTrue(tree.getRoot().isLeaf());
        assertEquals(1, tree.getRoot().getSize());
    }

    @Test
    void shouldHandleAllIdenticalSamples() {
        // 全相同的样本任何维度都无法切分 → 根节点直接是叶子
        // identical rows: cannot split on any dim → root is a leaf
        double[][] data = new double[10][9];
        for (int i = 0; i < 10; i++) {
            for (int d = 0; d < 9; d++) {
                data[i][d] = 5.0;
            }
        }
        ITree tree = new ITreeBuilder(7L).build(data, 10);

        assertEquals(1, tree.size());
        assertTrue(tree.getRoot().isLeaf());
        assertEquals(10, tree.getRoot().getSize());
    }

    @Test
    void shouldSubsampleWhenDataLargerThanPsi() {
        // 给 1000 条但 ψ=256，叶子样本和应为 256
        double[][] data = generateData(1000, 9, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        int leafSum = tree.getNodes().stream()
                .filter(ITreeNode::isLeaf)
                .mapToInt(ITreeNode::getSize)
                .sum();

        assertEquals(256, leafSum);
        assertEquals(256, tree.getSubsampleSize());
    }

    // ===== v3.1 buildFromPool tests =====

    private List<double[]> generatePool(int n, int dim, long seed) {
        Random r = new Random(seed);
        List<double[]> pool = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            double[] row = new double[dim];
            for (int d = 0; d < dim; d++) {
                row[d] = r.nextGaussian() * 10.0;
            }
            pool.add(row);
        }
        return pool;
    }

    @Test
    void buildFromPoolBasicFunctionality() {
        List<double[]> pool = generatePool(1000, 9, 42L);
        ITreeBuilder builder = new ITreeBuilder(123L);

        ITree tree = builder.buildFromPool(pool, 256);

        assertEquals(256, tree.getSubsampleSize());
        assertEquals(9, tree.getDimension());
        int leafSum = tree.getNodes().stream()
                .filter(ITreeNode::isLeaf)
                .mapToInt(ITreeNode::getSize)
                .sum();
        assertEquals(256, leafSum);
    }

    @Test
    void buildFromPoolSmallerThanSampleSize() {
        // 池子 100 条，sampleSize 256 → 实际 subsampleSize=100
        List<double[]> pool = generatePool(100, 9, 42L);
        ITreeBuilder builder = new ITreeBuilder(123L);

        ITree tree = builder.buildFromPool(pool, 256);

        assertEquals(100, tree.getSubsampleSize());
        int leafSum = tree.getNodes().stream()
                .filter(ITreeNode::isLeaf)
                .mapToInt(ITreeNode::getSize)
                .sum();
        assertEquals(100, leafSum);
    }

    @Test
    void buildFromPoolReproducibility() {
        List<double[]> pool = generatePool(1000, 9, 42L);

        ITree t1 = new ITreeBuilder(99L).buildFromPool(pool, 256);
        ITree t2 = new ITreeBuilder(99L).buildFromPool(pool, 256);

        assertEquals(t1.size(), t2.size(), "same seed should give same node count");
        for (int i = 0; i < t1.size(); i++) {
            ITreeNode n1 = t1.getNode(i);
            ITreeNode n2 = t2.getNode(i);
            assertEquals(n1.getFeature(), n2.getFeature());
            assertEquals(n1.getThreshold(), n2.getThreshold(), 1e-12);
            assertEquals(n1.getLeft(), n2.getLeft());
            assertEquals(n1.getRight(), n2.getRight());
            assertEquals(n1.getSize(), n2.getSize());
        }
    }

    @Test
    void buildFromPoolEmptyPoolShouldThrow() {
        ITreeBuilder builder = new ITreeBuilder(123L);
        assertThrows(IllegalArgumentException.class,
                () -> builder.buildFromPool(new ArrayList<>(), 256));
    }

    @Test
    void shouldSerializeToJsonAndBack() throws Exception {
        double[][] data = generateData(256, 9, 42L);
        ITree original = new ITreeBuilder(7L).build(data, 256);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(original);
        ITree roundTrip = mapper.readValue(json, ITree.class);

        assertEquals(original.size(), roundTrip.size());
        assertEquals(original.getDepthLimit(), roundTrip.getDepthLimit());
        assertEquals(original.getDimension(), roundTrip.getDimension());
        assertEquals(original.getSubsampleSize(), roundTrip.getSubsampleSize());

        for (int i = 0; i < original.size(); i++) {
            ITreeNode a = original.getNode(i);
            ITreeNode b = roundTrip.getNode(i);
            assertEquals(a.getId(), b.getId());
            assertEquals(a.getFeature(), b.getFeature());
            assertEquals(a.getThreshold(), b.getThreshold(), 1e-12);
            assertEquals(a.getLeft(), b.getLeft());
            assertEquals(a.getRight(), b.getRight());
            assertEquals(a.getSize(), b.getSize());
        }
    }
}
