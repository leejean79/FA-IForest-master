package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.leejean.tree.ITree;

import java.io.Serializable;

/**
 * 发送到 Kafka tree-topic 的 iTree 消息载体 / Message wrapper for sending an iTree to Kafka tree-topic.
 */
public class ITreeMessage implements Serializable {
    private static final long serialVersionUID = 2L;

    private String treeId;          // UUID
    private int producerSubtask;    // 来自哪个 subtask / which subtask produced this tree
    private long createdAt;         // 创建时间戳 / creation timestamp
    /**
     * 该树在该 subtask 内的位置索引，范围 [0, localTreeCount-1]。
     * 协调器用 (producerSubtask, slotIndex) 作为唯一 key 索引森林。
     *
     * Slot index of this tree within the producing subtask, range [0, localTreeCount-1].
     * The coordinator uses (producerSubtask, slotIndex) as the unique key for forest indexing.
     */
    private int slotIndex;

    /**
     * 批次 ID，标识本树属于哪次训练批次。
     * 编码：(subtaskIndex << 32) | driftRoundCount
     * - v1 Phase B 训练：driftRoundCount = 0
     * - 每次 COOLDOWN 触发的重训：driftRoundCount += 1
     *
     * Batch ID identifying which training round this tree belongs to.
     * Encoded as (subtaskIndex << 32) | driftRoundCount.
     */
    private long batchId;

    /**
     * 是否是本批次的最后一棵。协调器仅在收到 batchEnd=true 时才考虑触发新版本。
     * Whether this is the last tree of the batch. Coordinator triggers a new
     * forest version only when batchEnd=true.
     */
    private boolean batchEnd;

    private ITree tree;             // 树本身 / the tree itself

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public ITreeMessage() {
    }

    public ITreeMessage(String treeId, int producerSubtask, long createdAt, ITree tree) {
        this(treeId, producerSubtask, createdAt, 0, 0L, false, tree);
    }

    public ITreeMessage(String treeId, int producerSubtask, long createdAt,
                        int slotIndex, ITree tree) {
        this(treeId, producerSubtask, createdAt, slotIndex, 0L, false, tree);
    }

    public ITreeMessage(String treeId, int producerSubtask, long createdAt,
                        int slotIndex, long batchId, boolean batchEnd, ITree tree) {
        this.treeId = treeId;
        this.producerSubtask = producerSubtask;
        this.createdAt = createdAt;
        this.slotIndex = slotIndex;
        this.batchId = batchId;
        this.batchEnd = batchEnd;
        this.tree = tree;
    }

    @JsonProperty
    public String getTreeId() { return treeId; }
    public void setTreeId(String treeId) { this.treeId = treeId; }

    @JsonProperty
    public int getProducerSubtask() { return producerSubtask; }
    public void setProducerSubtask(int producerSubtask) { this.producerSubtask = producerSubtask; }

    @JsonProperty
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    @JsonProperty
    public int getSlotIndex() { return slotIndex; }
    public void setSlotIndex(int slotIndex) { this.slotIndex = slotIndex; }

    @JsonProperty
    public long getBatchId() { return batchId; }
    public void setBatchId(long batchId) { this.batchId = batchId; }

    @JsonProperty
    public boolean isBatchEnd() { return batchEnd; }
    public void setBatchEnd(boolean batchEnd) { this.batchEnd = batchEnd; }

    @JsonProperty
    public ITree getTree() { return tree; }
    public void setTree(ITree tree) { this.tree = tree; }

    @Override
    public String toString() {
        return String.format("ITreeMessage{treeId='%s', subtask=%d, slot=%d, batchId=%d, batchEnd=%s, createdAt=%d}",
                treeId, producerSubtask, slotIndex, batchId, batchEnd, createdAt);
    }
}
