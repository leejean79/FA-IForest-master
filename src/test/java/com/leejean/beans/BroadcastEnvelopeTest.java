package com.leejean.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * BroadcastEnvelope JSON 往返测试
 * BroadcastEnvelope JSON round-trip tests.
 */
public class BroadcastEnvelopeTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void shouldRoundTripForestType() throws Exception {
        ForestMessage fm = new ForestMessage("f-1", 3L, 1000L, 256, null);
        BroadcastEnvelope env = BroadcastEnvelope.forest(fm);

        String json = mapper.writeValueAsString(env);
        BroadcastEnvelope restored = mapper.readValue(json, BroadcastEnvelope.class);

        assertEquals(BroadcastEnvelope.Type.FOREST, restored.getType());
        assertNotNull(restored.getForestMessage());
        assertEquals("f-1", restored.getForestMessage().getForestId());
        assertEquals(3L, restored.getForestMessage().getVersion());
        assertNull(restored.getDriftRoundMessage());
    }

    @Test
    public void shouldRoundTripDriftRoundType() throws Exception {
        DriftRoundMessage drm = new DriftRoundMessage(5L, 2000L,
                DriftRoundMessage.RoundStatus.COMMITTED, 3, 1, 0);
        BroadcastEnvelope env = BroadcastEnvelope.driftRound(drm);

        String json = mapper.writeValueAsString(env);
        BroadcastEnvelope restored = mapper.readValue(json, BroadcastEnvelope.class);

        assertEquals(BroadcastEnvelope.Type.DRIFT_ROUND, restored.getType());
        assertNull(restored.getForestMessage());
        assertNotNull(restored.getDriftRoundMessage());
        assertEquals(5L, restored.getDriftRoundMessage().getRoundId());
        assertEquals(DriftRoundMessage.RoundStatus.COMMITTED, restored.getDriftRoundMessage().getStatus());
        assertEquals(3, restored.getDriftRoundMessage().getVotesYes());
        assertEquals(1, restored.getDriftRoundMessage().getVotesNo());
        assertEquals(0, restored.getDriftRoundMessage().getVotesAbstain());
    }
}
