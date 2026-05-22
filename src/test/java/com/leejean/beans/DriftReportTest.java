package com.leejean.beans;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.drift.DriftStatus;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DriftReport JSON 往返测试 / Round-trip serialization test for DriftReport.
 */
class DriftReportTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldSerializeToJsonAndBack() throws Exception {
        DriftReport original = new DriftReport(3, 1700000000000L, DriftStatus.WARN, 5000);
        original.setSignal(0.85);

        String json = mapper.writeValueAsString(original);
        DriftReport roundTrip = mapper.readValue(json, DriftReport.class);

        assertEquals(original.getSubtask(), roundTrip.getSubtask());
        assertEquals(original.getTimestamp(), roundTrip.getTimestamp());
        assertEquals(original.getStatus(), roundTrip.getStatus());
        assertEquals(original.getSampleCount(), roundTrip.getSampleCount());
        assertEquals(original.getSignal(), roundTrip.getSignal());
    }

    @Test
    void shouldHandleNullSignal() throws Exception {
        DriftReport original = new DriftReport(0, 1700000000000L, DriftStatus.DRIFT, 10000);

        String json = mapper.writeValueAsString(original);
        DriftReport roundTrip = mapper.readValue(json, DriftReport.class);

        assertEquals(original.getStatus(), roundTrip.getStatus());
        assertNull(roundTrip.getSignal());
    }

    /** v3.4: INITIATE vote round-trip / v3.4 INITIATE 投票往返测试 */
    @Test
    void shouldRoundTripWithVoteFields() throws Exception {
        DriftReport original = new DriftReport(2, 1700000000000L, DriftStatus.DRIFT, 0L,
                DriftReport.DriftVote.INITIATE);

        String json = mapper.writeValueAsString(original);
        DriftReport roundTrip = mapper.readValue(json, DriftReport.class);

        assertEquals(2, roundTrip.getSubtask());
        assertEquals(DriftStatus.DRIFT, roundTrip.getStatus());
        assertEquals(0L, roundTrip.getRoundId());
        assertEquals(DriftReport.DriftVote.INITIATE, roundTrip.getVote());
    }

    /** v3.4: 老 JSON（无 roundId/vote）反序列化兼容 / old JSON without v3.4 fields */
    @Test
    void shouldDeserializeOldJsonWithoutVoteFields() throws Exception {
        String oldJson = "{\"subtask\":1,\"timestamp\":100,\"status\":\"WARN\",\"sampleCount\":500}";
        DriftReport restored = mapper.readValue(oldJson, DriftReport.class);

        assertEquals(1, restored.getSubtask());
        assertEquals(DriftStatus.WARN, restored.getStatus());
        assertEquals(0L, restored.getRoundId());
        assertNull(restored.getVote());
    }
}
