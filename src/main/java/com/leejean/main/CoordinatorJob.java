package com.leejean.main;

import com.leejean.flink.CoordinatorFunction;
import com.leejean.flink.DriftAggregatorFunction;

import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.FeatureDrift;
import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.UUID;

/**
 * 协调器作业入口：森林聚合 + 检测面聚合器（方向二(a) Phase 3）。
 * Coordinator job: forest assembly + cross-feature drift aggregation.
 *
 * <p>两条独立管线 / Two independent pipelines:
 * <ul>
 *   <li>tree-topic → CoordinatorFunction → model-topic（森林聚合，不动）</li>
 *   <li>feature-drift-topic → DriftAggregatorFunction → drift-round-topic
 *       （k 个不同特征确认 onset 落在 aggWin 内 → 合成 COMMITTED）</li>
 * </ul>
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --treeTopic tree-topic --modelTopic model-topic
 *   --featureDriftTopic feature-drift-topic --driftRoundTopic drift-round-topic
 *   --parallelism 4 --totalTrees 100 --aggK 2 --aggWin 2000 --refractory 5000
 */
public class CoordinatorJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("broker", "localhost:9092");
        String treeTopic = params.get("treeTopic", "tree-topic");
        String modelTopic = params.get("modelTopic", "model-topic");
        String featureDriftTopic = params.get("featureDriftTopic", "feature-drift-topic");
        String driftRoundTopic = params.get("driftRoundTopic", "drift-round-topic");
        int parallelism = params.getInt("parallelism", 4);
        int totalTrees = params.getInt("totalTrees", 100);

        // 聚合器参数 / aggregator parameters
        int aggK = params.getInt("aggK", 2);
        long aggWin = params.getLong("aggWin", 2000L);
        long refractory = params.getLong("refractory", 5000L);

        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        int expectedSlots = parallelism * localTreeCount;

        System.out.println("========================================");
        System.out.println("Coordinator Job (phase3)");
        System.out.println("Broker: " + brokers);
        System.out.println("Tree topic: " + treeTopic);
        System.out.println("Model topic: " + modelTopic);
        System.out.println("Feature drift topic: " + featureDriftTopic);
        System.out.println("Drift round topic: " + driftRoundTopic);
        System.out.println("LocalProcessor parallelism: " + parallelism);
        System.out.println("Total trees: " + totalTrees);
        System.out.println("Local tree count: " + localTreeCount);
        System.out.println("Expected slots: " + expectedSlots);
        System.out.println("Aggregator k: " + aggK);
        System.out.println("Aggregator window: " + aggWin);
        System.out.println("Refractory: " + refractory);
        System.out.println("========================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 协调器始终 parallelism=1
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        // ===== Pipeline 1: tree-topic → CoordinatorFunction → model-topic =====
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id",
                "coordinator-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> treeConsumer = new FlinkKafkaConsumer<>(
                treeTopic, new SimpleStringSchema(), consumerProps);
        treeConsumer.setStartFromEarliest();

        DataStream<ITreeMessage> treeStream = env.addSource(treeConsumer)
                .name("Tree Topic Source [" + treeTopic + "]")
                .map(new ITreeMessageDeserializer())
                .name("Parse ITreeMessage");

        DataStream<ForestMessage> forestStream = treeStream
                .keyBy((KeySelector<ITreeMessage, String>) t -> "global")
                .process(new CoordinatorFunction(parallelism, totalTrees))
                .name("Coordinator Function");

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);
        producerProps.setProperty("max.request.size", "5242880");
        producerProps.setProperty("compression.type", "gzip");

        FlinkKafkaProducer<ForestMessage> modelProducer = new FlinkKafkaProducer<>(
                modelTopic,
                new ForestMessageSerializationSchema(modelTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        forestStream.addSink(modelProducer)
                .name("Model Topic Sink [" + modelTopic + "]");

        forestStream.print("ForestMessage emitted");

        // ===== Pipeline 2: feature-drift-topic → DriftAggregatorFunction → drift-round-topic =====
        Properties aggConsumerProps = new Properties();
        aggConsumerProps.setProperty("bootstrap.servers", brokers);
        aggConsumerProps.setProperty("group.id",
                "drift-aggregator-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> fdConsumer = new FlinkKafkaConsumer<>(
                featureDriftTopic, new SimpleStringSchema(), aggConsumerProps);
        fdConsumer.setStartFromEarliest();

        DataStream<FeatureDrift> fdStream = env.addSource(fdConsumer)
                .name("Feature Drift Topic Source [" + featureDriftTopic + "]")
                .map(new FeatureDriftDeserializer())
                .name("Parse FeatureDrift");

        DataStream<DriftRoundMessage> roundStream = fdStream
                .keyBy((KeySelector<FeatureDrift, String>) f -> "global")
                .process(new DriftAggregatorFunction(aggK, aggWin, refractory))
                .name("Drift Aggregator Function");

        FlinkKafkaProducer<DriftRoundMessage> roundProducer = new FlinkKafkaProducer<>(
                driftRoundTopic,
                new DriftRoundMessageSerializationSchema(driftRoundTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        roundStream.addSink(roundProducer)
                .name("Drift Round Topic Sink [" + driftRoundTopic + "]");

        roundStream.print("DriftRoundMessage emitted");

        env.execute("Coordinator - Forest Assembly + Cross-Feature Aggregation (phase3)");
    }

    // ===== 序列化/反序列化 =====

    private static class ITreeMessageDeserializer implements MapFunction<String, ITreeMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public ITreeMessage map(String json) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.readValue(json, ITreeMessage.class);
        }
    }

    private static class ForestMessageSerializationSchema
            implements KafkaSerializationSchema<ForestMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ForestMessageSerializationSchema(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ForestMessage msg, @Nullable Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ForestMessage to JSON", e);
            }
        }
    }

    private static class FeatureDriftDeserializer implements MapFunction<String, FeatureDrift> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public FeatureDrift map(String json) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.readValue(json, FeatureDrift.class);
        }
    }

    private static class DriftRoundMessageSerializationSchema
            implements KafkaSerializationSchema<DriftRoundMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        DriftRoundMessageSerializationSchema(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DriftRoundMessage msg, @Nullable Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DriftRoundMessage to JSON", e);
            }
        }
    }
}
