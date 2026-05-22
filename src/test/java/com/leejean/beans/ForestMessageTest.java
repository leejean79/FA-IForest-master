package com.leejean.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.tree.ITree;
import com.leejean.tree.ITreeNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ForestMessage JSON 序列化往返测试
 * ForestMessage JSON round-trip test
 */
public class ForestMessageTest {

    @Test
    public void testJsonRoundTrip() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // 构造一棵小树 / Build a small tree
        ITree tree = new ITree(
                Arrays.asList(
                        ITreeNode.internal(0, 0, 5.0, 1, 2, 10),
                        ITreeNode.leaf(1, 3),
                        ITreeNode.leaf(2, 7)
                ), 10, 4, 2);

        ITreeMessage treeMsg = new ITreeMessage(UUID.randomUUID().toString(), 0, System.currentTimeMillis(), tree);

        ForestMessage original = new ForestMessage(
                UUID.randomUUID().toString(),
                1L,
                System.currentTimeMillis(),
                256,
                Collections.singletonList(treeMsg)
        );

        // 序列化 → 反序列化 / serialize → deserialize
        String json = mapper.writeValueAsString(original);
        ForestMessage restored = mapper.readValue(json, ForestMessage.class);

        assertEquals(original.getForestId(), restored.getForestId());
        assertEquals(original.getVersion(), restored.getVersion());
        assertEquals(original.getCreatedAt(), restored.getCreatedAt());
        assertEquals(original.getSubsampleSize(), restored.getSubsampleSize());
        assertEquals(1, restored.getTrees().size());

        // 验证内嵌的 ITreeMessage / verify nested ITreeMessage
        ITreeMessage restoredTree = restored.getTrees().get(0);
        assertEquals(treeMsg.getTreeId(), restoredTree.getTreeId());
        assertEquals(3, restoredTree.getTree().getNodes().size());
    }

    @Test
    public void testScoreResultJsonRoundTrip() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        ScoreResult original = new ScoreResult(
                42L, "dp-42", System.currentTimeMillis(),
                new double[]{1.0, 2.0, 3.0}, 1, 0.85, 1L, "C"
        );

        String json = mapper.writeValueAsString(original);
        ScoreResult restored = mapper.readValue(json, ScoreResult.class);

        assertEquals(original.getOriginalSequence(), restored.getOriginalSequence());
        assertEquals(original.getDataPointId(), restored.getDataPointId());
        assertEquals(original.getTimestamp(), restored.getTimestamp());
        assertArrayEquals(original.getFeatures(), restored.getFeatures());
        assertEquals(original.getLabel(), restored.getLabel());
        assertEquals(original.getScore(), restored.getScore(), 1e-10);
        assertEquals(original.getForestVersion(), restored.getForestVersion());
        assertEquals(original.getPhase(), restored.getPhase());
    }
}
