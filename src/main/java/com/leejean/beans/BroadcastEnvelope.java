package com.leejean.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * v3.4 广播流合并包装：将 ForestMessage 和 DriftRoundMessage 统一为一种消息类型
 * v3.4 broadcast stream wrapper: unifies ForestMessage and DriftRoundMessage into one type.
 *
 * <p>LocalProcessor 端从 model-topic 和 drift-round-topic 分别反序列化后包装为 BroadcastEnvelope，
 * 再 union 成一条广播流。接收端通过 {@link #getType()} 判断载荷类型。
 */
public class BroadcastEnvelope implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Type { FOREST, DRIFT_ROUND }

    private Type type;
    private ForestMessage forestMessage;
    private DriftRoundMessage driftRoundMessage;

    /** Jackson 反序列化需要无参构造 / No-arg constructor required by Jackson. */
    public BroadcastEnvelope() {
    }

    public BroadcastEnvelope(Type type, ForestMessage forestMessage, DriftRoundMessage driftRoundMessage) {
        this.type = type;
        this.forestMessage = forestMessage;
        this.driftRoundMessage = driftRoundMessage;
    }

    public static BroadcastEnvelope forest(ForestMessage fm) {
        return new BroadcastEnvelope(Type.FOREST, fm, null);
    }

    public static BroadcastEnvelope driftRound(DriftRoundMessage drm) {
        return new BroadcastEnvelope(Type.DRIFT_ROUND, null, drm);
    }

    @JsonProperty
    public Type getType() { return type; }
    public void setType(Type type) { this.type = type; }

    @JsonProperty
    public ForestMessage getForestMessage() { return forestMessage; }
    public void setForestMessage(ForestMessage forestMessage) { this.forestMessage = forestMessage; }

    @JsonProperty
    public DriftRoundMessage getDriftRoundMessage() { return driftRoundMessage; }
    public void setDriftRoundMessage(DriftRoundMessage driftRoundMessage) { this.driftRoundMessage = driftRoundMessage; }

    @Override
    public String toString() {
        if (type == Type.FOREST) {
            return "BroadcastEnvelope{FOREST, " + forestMessage + "}";
        } else {
            return "BroadcastEnvelope{DRIFT_ROUND, " + driftRoundMessage + "}";
        }
    }
}
