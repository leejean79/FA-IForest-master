package com.leejean.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DriftRoundMessage JSON 往返测试
 * DriftRoundMessage JSON round-trip tests.
 */
public class DriftRoundMessageTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldSerializeAndDeserialize() throws Exception {
        DriftRoundMessage drm = new DriftRoundMessage(7L, 3000L,
                DriftRoundMessage.RoundStatus.ABORTED, 1, 2, 1);

        String json = mapper.writeValueAsString(drm);
        DriftRoundMessage restored = mapper.readValue(json, DriftRoundMessage.class);

        assertEquals(7L, restored.getRoundId());
        assertEquals(3000L, restored.getTimestamp());
        assertEquals(DriftRoundMessage.RoundStatus.ABORTED, restored.getStatus());
        assertEquals(1, restored.getVotesYes());
        assertEquals(2, restored.getVotesNo());
        assertEquals(1, restored.getVotesAbstain());
    }

    @Test
    public void shouldDeserializeWithMissingFields() throws Exception {
        // 模拟只有 roundId 和 status 的 JSON / simulate partial JSON
        String json = "{\"roundId\":1,\"status\":\"VOTING\"}";
        DriftRoundMessage restored = mapper.readValue(json, DriftRoundMessage.class);

        assertEquals(1L, restored.getRoundId());
        assertEquals(DriftRoundMessage.RoundStatus.VOTING, restored.getStatus());
        assertEquals(0, restored.getVotesYes());
        assertEquals(0, restored.getVotesNo());
        assertEquals(0, restored.getVotesAbstain());
    }
}
