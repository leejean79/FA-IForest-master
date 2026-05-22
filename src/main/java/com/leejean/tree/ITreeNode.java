package com.leejean.tree;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * iTree 的扁平节点结构 / Flat node structure of an iTree.
 *
 * <p>设计说明 / Design notes:
 * <ul>
 *   <li>节点用整数 id 而非嵌套对象引用，方便 Jackson 序列化为 JSON 数组、避免递归栈风险。</li>
 *   <li>一个节点要么是内部节点（feature ≥ 0, left/right ≥ 0），要么是叶子（feature = -1, left = -1, right = -1）。</li>
 *   <li>{@link #size} 字段：内部节点保存子树的样本总数（=训练时该节点见过的样本数）；
 *       叶子节点保存其样本数（用于打分时的路径长度修正项 c(size)）。</li>
 * </ul>
 *
 * <p>Nodes use integer ids (not nested object references) so the tree serializes cleanly
 * as a JSON array and avoids recursive-stack pitfalls. Internal nodes carry a split
 * feature index and threshold; leaves use sentinel values (-1) and report their sample
 * size for the c(size) correction in iForest path-length scoring.
 */
public class ITreeNode implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 叶子节点哨兵值 / Sentinel value for leaves. */
    public static final int LEAF_SENTINEL = -1;

    private int id;
    private int feature;     // 内部节点: 切分特征索引; 叶子: -1
    private double threshold; // 内部节点: 切分阈值; 叶子: 0.0
    private int left;        // 内部节点: 左子节点 id; 叶子: -1
    private int right;       // 内部节点: 右子节点 id; 叶子: -1
    private int size;        // 该节点上的样本数 / sample count at this node

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public ITreeNode() {
    }

    private ITreeNode(int id, int feature, double threshold, int left, int right, int size) {
        this.id = id;
        this.feature = feature;
        this.threshold = threshold;
        this.left = left;
        this.right = right;
        this.size = size;
    }

    /** 构造内部节点 / Build an internal node. */
    public static ITreeNode internal(int id, int feature, double threshold, int left, int right, int size) {
        if (feature < 0) {
            throw new IllegalArgumentException("internal node feature must be >= 0, got " + feature);
        }
        return new ITreeNode(id, feature, threshold, left, right, size);
    }

    /** 构造叶子节点 / Build a leaf node. */
    public static ITreeNode leaf(int id, int size) {
        return new ITreeNode(id, LEAF_SENTINEL, 0.0, LEAF_SENTINEL, LEAF_SENTINEL, size);
    }

    @JsonProperty
    public int getId() { return id; }
    public void setId(int id) { this.id = id; }

    @JsonProperty
    public int getFeature() { return feature; }
    public void setFeature(int feature) { this.feature = feature; }

    @JsonProperty
    public double getThreshold() { return threshold; }
    public void setThreshold(double threshold) { this.threshold = threshold; }

    @JsonProperty
    public int getLeft() { return left; }
    public void setLeft(int left) { this.left = left; }

    @JsonProperty
    public int getRight() { return right; }
    public void setRight(int right) { this.right = right; }

    @JsonProperty
    public int getSize() { return size; }
    public void setSize(int size) { this.size = size; }

    /** 是否为叶子节点 / Is this a leaf? */
    @JsonIgnore
    public boolean isLeaf() {
        return feature == LEAF_SENTINEL;
    }

    @Override
    public String toString() {
        if (isLeaf()) {
            return String.format("Leaf{id=%d, size=%d}", id, size);
        }
        return String.format("Internal{id=%d, feature=%d, threshold=%.4f, left=%d, right=%d, size=%d}",
                id, feature, threshold, left, right, size);
    }
}
