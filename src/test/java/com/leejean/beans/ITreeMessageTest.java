package com.leejean.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.tree.ITree;
import com.leejean.tree.ITreeBuilder;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ITreeMessage JSON 序列化往返测试 / Round-trip serialization test for ITreeMessage.
 */
class ITreeMessageTest {

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
    void shouldSerializeToJsonAndBack() throws Exception {
        double[][] data = generateData(256, 9, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 256);

        String treeId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        long batchId = ((long) 2 << 32) | 3L;
        ITreeMessage original = new ITreeMessage(treeId, 2, now, 7, batchId, true, tree);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(original);
        ITreeMessage roundTrip = mapper.readValue(json, ITreeMessage.class);

        assertEquals(original.getTreeId(), roundTrip.getTreeId());
        assertEquals(original.getProducerSubtask(), roundTrip.getProducerSubtask());
        assertEquals(original.getSlotIndex(), roundTrip.getSlotIndex());
        assertEquals(original.getCreatedAt(), roundTrip.getCreatedAt());
        assertEquals(original.getBatchId(), roundTrip.getBatchId());
        assertEquals(original.isBatchEnd(), roundTrip.isBatchEnd());

        // 验证 batchId 值 / verify batchId encoding
        assertEquals(batchId, roundTrip.getBatchId());
        assertTrue(roundTrip.isBatchEnd());

        ITree rt = roundTrip.getTree();
        assertNotNull(rt);
        assertEquals(tree.size(), rt.size());
        assertEquals(tree.getSubsampleSize(), rt.getSubsampleSize());
        assertEquals(tree.getDimension(), rt.getDimension());
        assertEquals(tree.getDepthLimit(), rt.getDepthLimit());

        for (int i = 0; i < tree.size(); i++) {
            assertEquals(tree.getNode(i).getId(), rt.getNode(i).getId());
            assertEquals(tree.getNode(i).getFeature(), rt.getNode(i).getFeature());
            assertEquals(tree.getNode(i).getThreshold(), rt.getNode(i).getThreshold(), 1e-12);
            assertEquals(tree.getNode(i).getLeft(), rt.getNode(i).getLeft());
            assertEquals(tree.getNode(i).getRight(), rt.getNode(i).getRight());
            assertEquals(tree.getNode(i).getSize(), rt.getNode(i).getSize());
        }
    }

    /** 缺少 batchId/batchEnd 的旧 JSON 应反序列化为默认值 / Old JSON without batchId/batchEnd → defaults. */
    @Test
    void shouldDeserializeOldJsonWithoutBatchFields() throws Exception {
        double[][] data = generateData(64, 3, 42L);
        ITree tree = new ITreeBuilder(7L).build(data, 64);

        // 手工构造不含 batchId/batchEnd 的旧格式 JSON / manually build old format JSON
        ObjectMapper mapper = new ObjectMapper();
        String treeJson = mapper.writeValueAsString(tree);
        String oldJson = String.format(
                "{\"treeId\":\"old-id\",\"producerSubtask\":1,\"createdAt\":12345,\"slotIndex\":5,\"tree\":%s}",
                treeJson);

        ITreeMessage msg = mapper.readValue(oldJson, ITreeMessage.class);

        assertEquals("old-id", msg.getTreeId());
        assertEquals(1, msg.getProducerSubtask());
        assertEquals(5, msg.getSlotIndex());
        assertEquals(0L, msg.getBatchId(), "missing batchId should default to 0");
        assertFalse(msg.isBatchEnd(), "missing batchEnd should default to false");
    }
}
