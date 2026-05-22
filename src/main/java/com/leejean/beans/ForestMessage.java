package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

/**
 * 发送到 Kafka model-topic 的全局森林消息载体
 * Message wrapper for sending a global forest to Kafka model-topic.
 */
public class ForestMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private String forestId;            // UUID
    private long version;               // 单调递增版本号 / monotonically increasing version
    private long createdAt;             // 创建时间戳 / creation timestamp
    private int subsampleSize;          // 所有树共用的 ψ / shared subsample size ψ
    private List<ITreeMessage> trees;   // 直接复用 v1 结构 / reuses v1 ITreeMessage structure

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public ForestMessage() {
    }

    public ForestMessage(String forestId, long version, long createdAt,
                         int subsampleSize, List<ITreeMessage> trees) {
        this.forestId = forestId;
        this.version = version;
        this.createdAt = createdAt;
        this.subsampleSize = subsampleSize;
        this.trees = trees;
    }

    @JsonProperty
    public String getForestId() { return forestId; }
    public void setForestId(String forestId) { this.forestId = forestId; }

    @JsonProperty
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }

    @JsonProperty
    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    @JsonProperty
    public int getSubsampleSize() { return subsampleSize; }
    public void setSubsampleSize(int subsampleSize) { this.subsampleSize = subsampleSize; }

    @JsonProperty
    public List<ITreeMessage> getTrees() { return trees; }
    public void setTrees(List<ITreeMessage> trees) { this.trees = trees; }

    @Override
    public String toString() {
        return String.format("ForestMessage{forestId='%s', version=%d, trees=%d, subsampleSize=%d}",
                forestId, version, trees != null ? trees.size() : 0, subsampleSize);
    }
}
