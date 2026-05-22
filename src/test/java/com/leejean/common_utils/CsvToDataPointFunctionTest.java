package com.leejean.common_utils;

import com.leejean.beans.DataPoint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * CsvToDataPointFunction 测试，覆盖 SequenceSource 策略
 * Tests for CsvToDataPointFunction, covering SequenceSource strategies
 */
public class CsvToDataPointFunctionTest {

    @BeforeEach
    public void setUp() {
        CollectSink.values.clear();
    }

    /**
     * PARSE_ID 策略：id="42" → originalSequence=42
     */
    @Test
    public void testParseIdStrategy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        // CSV: id, f0, f1, label
        DataStream<String> source = env.fromCollection(Arrays.asList(
                "42,1.0,2.0,0",
                "100,3.0,4.0,1"
        ));

        source.process(new CsvToDataPointFunction(false, true, true, true, SequenceSource.PARSE_ID))
                .addSink(new CollectSink());

        env.execute("PARSE_ID test");

        assertEquals(2, CollectSink.values.size());
        assertEquals(42L, CollectSink.values.get(0).getOriginalSequence());
        assertEquals("42", CollectSink.values.get(0).getId());
        assertEquals(100L, CollectSink.values.get(1).getOriginalSequence());
    }

    /**
     * PARSE_ID 策略：非数字 id → 抛 IllegalStateException
     */
    @Test
    public void testParseIdWithNonNumericThrows() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> source = env.fromCollection(Collections.singletonList(
                "abc,1.0,2.0,0"
        ));

        source.process(new CsvToDataPointFunction(false, true, true, true, SequenceSource.PARSE_ID))
                .addSink(new CollectSink());

        // Flink 把 IllegalStateException 包成 JobExecutionException
        // Flink wraps IllegalStateException in JobExecutionException
        assertThrows(Exception.class, () -> env.execute("PARSE_ID non-numeric test"));
    }

    /**
     * AUTO_INCREMENT 策略：连续三条 → originalSequence = 0, 1, 2
     */
    @Test
    public void testAutoIncrementStrategy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> source = env.fromCollection(Arrays.asList(
                "a,1.0,2.0,0",
                "b,3.0,4.0,1",
                "c,5.0,6.0,0"
        ));

        source.process(new CsvToDataPointFunction(false, true, true, true, SequenceSource.AUTO_INCREMENT))
                .addSink(new CollectSink());

        env.execute("AUTO_INCREMENT test");

        assertEquals(3, CollectSink.values.size());
        assertEquals(0L, CollectSink.values.get(0).getOriginalSequence());
        assertEquals(1L, CollectSink.values.get(1).getOriginalSequence());
        assertEquals(2L, CollectSink.values.get(2).getOriginalSequence());
    }

    /**
     * FROM_FIELD 策略 → 抛 UnsupportedOperationException
     */
    @Test
    public void testFromFieldThrows() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> source = env.fromCollection(Collections.singletonList(
                "1,1.0,2.0,0"
        ));

        source.process(new CsvToDataPointFunction(false, true, true, true, SequenceSource.FROM_FIELD))
                .addSink(new CollectSink());

        assertThrows(Exception.class, () -> env.execute("FROM_FIELD test"));
    }

    /**
     * v1 兼容：4 参数构造默认使用 AUTO_INCREMENT
     */
    @Test
    public void testV1CompatibleConstructor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<String> source = env.fromCollection(Arrays.asList(
                "10,1.0,2.0,0",
                "20,3.0,4.0,1"
        ));

        // v1 的 4 参数构造 / v1's 4-arg constructor
        source.process(new CsvToDataPointFunction(false, true, true, true))
                .addSink(new CollectSink());

        env.execute("v1 compatible test");

        assertEquals(2, CollectSink.values.size());
        // AUTO_INCREMENT → 0, 1
        assertEquals(0L, CollectSink.values.get(0).getOriginalSequence());
        assertEquals(1L, CollectSink.values.get(1).getOriginalSequence());
    }

    /**
     * 线程安全的收集 Sink / Thread-safe collecting sink
     */
    private static class CollectSink implements SinkFunction<DataPoint> {
        static final List<DataPoint> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(DataPoint value, Context context) {
            values.add(value);
        }
    }
}
