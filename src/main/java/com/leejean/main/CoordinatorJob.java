package com.leejean.main;

import com.leejean.flink.CoordinatorFunction;
import com.leejean.flink.DriftVoterFunction;

import com.leejean.beans.DriftReport;
import com.leejean.beans.DriftRoundMessage;
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
 * v3.4 协调器作业入口：森林聚合 + 漂移投票
 * v3.4 Coordinator job entry: forest assembly + drift voting
 *
 * <p>两条独立处理管线 / Two independent processing pipelines:
 * <ul>
 *   <li>tree-topic → CoordinatorFunction → model-topic（森林聚合）</li>
 *   <li>drift-topic → DriftVoterFunction → drift-round-topic（漂移投票）</li>
 * </ul>
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --treeTopic tree-topic --modelTopic model-topic
 *   --driftTopic drift-topic --driftRoundTopic drift-round-topic
 *   --parallelism 4 --totalTrees 100 --votingTimeoutMs 5000
 *
 * 参数说明 / Parameters:
 *   --broker            Kafka broker 地址，默认 localhost:9092
 *   --treeTopic         iTree 输入 topic，默认 tree-topic
 *   --modelTopic        全局森林输出 topic，默认 model-topic
 *   --driftTopic        漂移上报 topic（上行），默认 drift-topic
 *   --driftRoundTopic   漂移决议 topic（下行），默认 drift-round-topic
 *   --parallelism       LocalProcessor 的并行度（协调器自身始终 parallelism=1），默认 4
 *   --totalTrees        全局森林总树数，默认 100
 *   --votingTimeoutMs   投票超时毫秒数，默认 5000
 */
public class CoordinatorJob {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("broker", "localhost:9092");
        String treeTopic = params.get("treeTopic", "tree-topic");
        String modelTopic = params.get("modelTopic", "model-topic");
        String driftTopic = params.get("driftTopic", "drift-topic");
        String driftRoundTopic = params.get("driftRoundTopic", "drift-round-topic");
        int parallelism = params.getInt("parallelism", 4);
        int totalTrees = params.getInt("totalTrees", 100);
        long votingTimeoutMs = params.getLong("votingTimeoutMs", 5000L);

        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);
        int expectedSlots = parallelism * localTreeCount;

        System.out.println("========================================");
        System.out.println("Coordinator Job (v3.4)");
        System.out.println("Broker: " + brokers);
        System.out.println("Tree topic: " + treeTopic);
        System.out.println("Model topic: " + modelTopic);
        System.out.println("Drift topic: " + driftTopic);
        System.out.println("Drift round topic: " + driftRoundTopic);
        System.out.println("LocalProcessor parallelism: " + parallelism);
        System.out.println("Total trees: " + totalTrees);
        System.out.println("Local tree count: " + localTreeCount);
        System.out.println("Expected slots: " + expectedSlots);
        System.out.println("Voting timeout ms: " + votingTimeoutMs);
        System.out.println("========================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 协调器始终 parallelism=1 / coordinator always runs at parallelism=1
        env.setParallelism(1);
        env.enableCheckpointing(10000);  // 开启 checkpoint，使 AT_LEAST_ONCE Producer 在 checkpoint 时 flush+等 ack，确保单条森林可靠落盘


        // ===== Source: 订阅 tree-topic =====
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", brokers);
        consumerProps.setProperty("group.id",
                "coordinator-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> treeConsumer = new FlinkKafkaConsumer<>(
                treeTopic, new SimpleStringSchema(), consumerProps);
        // 从 earliest 读：重启后能恢复完整树状态 / from-earliest: recover full tree state on restart
        treeConsumer.setStartFromEarliest();

        DataStream<ITreeMessage> treeStream = env.addSource(treeConsumer)
                .name("Tree Topic Source [" + treeTopic + "]")
                .map(new ITreeMessageDeserializer())
                .name("Parse ITreeMessage");

        // ===== Process: 协调聚合 =====
        DataStream<ForestMessage> forestStream = treeStream
                .keyBy((KeySelector<ITreeMessage, String>) t -> "global")
                .process(new CoordinatorFunction(parallelism, totalTrees))
                .name("Coordinator Function");

        // ===== Sink: 发到 model-topic =====
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

        // ===== Pipeline 2: 漂移投票 drift-topic → DriftVoterFunction → drift-round-topic =====
        Properties driftConsumerProps = new Properties();
        driftConsumerProps.setProperty("bootstrap.servers", brokers);
        driftConsumerProps.setProperty("group.id",
                "drift-voter-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> driftConsumer = new FlinkKafkaConsumer<>(
                driftTopic, new SimpleStringSchema(), driftConsumerProps);
        driftConsumer.setStartFromEarliest();

        DataStream<DriftReport> driftStream = env.addSource(driftConsumer)
                .name("Drift Topic Source [" + driftTopic + "]")
                .map(new DriftReportDeserializer())
                .name("Parse DriftReport");

        DataStream<DriftRoundMessage> roundStream = driftStream
                .keyBy((KeySelector<DriftReport, String>) r -> "global")
                .process(new DriftVoterFunction(parallelism, votingTimeoutMs))
                .name("Drift Voter Function");

        FlinkKafkaProducer<DriftRoundMessage> roundProducer = new FlinkKafkaProducer<>(
                driftRoundTopic,
                new DriftRoundMessageSerializationSchema(driftRoundTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        roundStream.addSink(roundProducer)
                .name("Drift Round Topic Sink [" + driftRoundTopic + "]");

        roundStream.print("DriftRoundMessage emitted");

        env.execute("Coordinator - Forest Assembly + Drift Voting (v3.4)");
    }

    // ===== 序列化/反序列化 =====

    /**
     * ITreeMessage JSON 反序列化器 / ITreeMessage JSON deserializer
     */
    private static class ITreeMessageDeserializer implements MapFunction<String, ITreeMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public ITreeMessage map(String json) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(json, ITreeMessage.class);
        }
    }

    /**
     * ForestMessage → Kafka ProducerRecord 的序列化器
     * Serializer: ForestMessage → Kafka ProducerRecord (JSON)
     */
    private static class ForestMessageSerializationSchema
            implements KafkaSerializationSchema<ForestMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ForestMessageSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ForestMessage msg, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ForestMessage to JSON", e);
            }
        }
    }

    /**
     * DriftReport JSON 反序列化器 / DriftReport JSON deserializer
     */
    private static class DriftReportDeserializer implements MapFunction<String, DriftReport> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public DriftReport map(String json) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(json, DriftReport.class);
        }
    }

    /**
     * DriftRoundMessage → Kafka ProducerRecord 的序列化器
     * Serializer: DriftRoundMessage → Kafka ProducerRecord (JSON)
     */
    private static class DriftRoundMessageSerializationSchema
            implements KafkaSerializationSchema<DriftRoundMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        DriftRoundMessageSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DriftRoundMessage msg, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DriftRoundMessage to JSON", e);
            }
        }
    }
}
