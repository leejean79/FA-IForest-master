package com.leejean.tree;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 一棵经典 iForest iTree 的容器 / Container for a single classic iForest iTree.
 *
 * <p>仅承载结构化数据：节点列表 + 元信息。构建逻辑在 {@link ITreeBuilder}。
 * Holds the tree as a flat list of nodes with metadata. The build algorithm
 * lives in {@link ITreeBuilder} so that this class stays serialization-friendly.
 *
 * <p>节点 id 0 始终是根节点 / Node id 0 is always the root.
 */
public class ITree implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<ITreeNode> nodes;
    private int subsampleSize;   // 训练样本数 ψ
    private int depthLimit;      // 深度上限 = ceil(log2(ψ))
    private int dimension;       // 特征维度 D

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public ITree() {
        this.nodes = new ArrayList<>();
    }

    public ITree(List<ITreeNode> nodes, int subsampleSize, int depthLimit, int dimension) {
        this.nodes = Objects.requireNonNull(nodes, "nodes");
        this.subsampleSize = subsampleSize;
        this.depthLimit = depthLimit;
        this.dimension = dimension;
    }

    @JsonProperty
    public List<ITreeNode> getNodes() { return nodes; }
    public void setNodes(List<ITreeNode> nodes) { this.nodes = nodes; }

    @JsonProperty
    public int getSubsampleSize() { return subsampleSize; }
    public void setSubsampleSize(int subsampleSize) { this.subsampleSize = subsampleSize; }

    @JsonProperty
    public int getDepthLimit() { return depthLimit; }
    public void setDepthLimit(int depthLimit) { this.depthLimit = depthLimit; }

    @JsonProperty
    public int getDimension() { return dimension; }
    public void setDimension(int dimension) { this.dimension = dimension; }

    @JsonIgnore
    public ITreeNode getRoot() {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("tree has no nodes");
        }
        return nodes.get(0);
    }

    public ITreeNode getNode(int id) {
        return nodes.get(id);
    }

    public int size() {
        return nodes.size();
    }

    @Override
    public String toString() {
        return String.format("ITree{nodes=%d, subsampleSize=%d, depthLimit=%d, dimension=%d}",
                nodes.size(), subsampleSize, depthLimit, dimension);
    }
}
