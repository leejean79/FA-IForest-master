package com.leejean.main;

import com.leejean.beans.BroadcastEnvelope;
import com.leejean.beans.DataPoint;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.FeatureDrift;
import com.leejean.beans.FeatureValue;
import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.leejean.beans.ScoreResult;
import com.leejean.common_utils.CsvToDataPointFunction;
import com.leejean.common_utils.ParallelismKeys;
import com.leejean.common_utils.SequenceSource;
import com.leejean.flink.FeatureSplitFlatMap;
import com.leejean.flink.LocalProcessorFunction;
import com.leejean.flink.PerFeatureHDDMFunction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * 方向二(a) Phase 3 本地 Flink 流处理入口：打分面 + 检测面双轨。
 * Local entrypoint with split scoring + detection pipelines.
 *
 * <p>主流分流点 = {@code dataPointStream}（CsvToDataPointFunction 之后）：
 * <ul>
 *   <li>打分面（行并行，沿用）：keyBy(uniform key) → LocalProcessorFunction → output-scores</li>
 *   <li>检测面（列并行，新增）：flatMap(FeatureSplit) → keyBy(featureId)
 *       → PerFeatureHDDMFunction → feature-drift-topic</li>
 * </ul>
 * 打分面广播路径不变，聚合器经 drift-round-topic 推 COMMITTED 进
 * {@link LocalProcessorFunction}，驱动 STABLE → COOLDOWN → WAITING → STABLE。
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --topic source-topic --treeTopic tree-topic
 *   --modelTopic model-topic --scoreTopic output-scores
 *   --featureDriftTopic feature-drift-topic --driftRoundTopic drift-round-topic
 *   --hasHeader true --hasId true --hasLabel true --autoIncrementId true
 *   --totalTrees 100 --subsampleSize 256 --ringBufferSize 1000 --parallelism 4
 *   --hddmWarmup 2000 --hddmLambda 0.005 --hddmWarnConf 0.005 --hddmDriftConf 0.001
 *   --hddmWarnTimeout 2000 --hddmScaleMode p99
 *   --cooldownSamples 5000 --zThresholdK 1.0
 */
public class LocalProcessor {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.get("broker", "localhost:9092");
        String topic = params.get("topic", "source-topic");
        boolean hasHeader = params.getBoolean("hasHeader", true);
        boolean hasId = params.getBoolean("hasId", true);
        boolean hasLabel = params.getBoolean("hasLabel", true);
        boolean autoIncrementId = params.getBoolean("autoIncrementId", true);
        int parallelism = params.getInt("parallelism", 4);
        int totalTrees = params.getInt("totalTrees", 100);
        int subsampleSize = params.getInt("subsampleSize", 256);
        int ringBufferSize = params.getInt("ringBufferSize", 1000);
        String treeTopic = params.get("treeTopic", "tree-topic");
        String modelTopic = params.get("modelTopic", "model-topic");
        String scoreTopic = params.get("scoreTopic", "output-scores");
        String featureDriftTopic = params.get("featureDriftTopic", "feature-drift-topic");
        String driftRoundTopic = params.get("driftRoundTopic", "drift-round-topic");

        // 检测面参数 / detection-side parameters
        int iksWindowSize = params.getInt("iksWindowSize", 2000);   // 复用为 HDDM warm-up 默认值

        // HDDM_W 检测面参数 / HDDM_W detection-side parameters
        int    hddmWarmup      = params.getInt("hddmWarmup", iksWindowSize);   // 复用 W=2000
        double hddmLambda      = params.getDouble("hddmLambda", 0.005);   // 离线验证修正（原 0.1），见 devspec §4/§7
        double hddmWarnConf    = params.getDouble("hddmWarnConf", 0.005);
        double hddmDriftConf   = params.getDouble("hddmDriftConf", 0.001);
        long   hddmWarnTimeout = params.getLong("hddmWarnTimeout", 2000L);
        String hddmScaleMode   = params.get("hddmScaleMode", "p99");      // 离线验证修正（原 maxdev），见 devspec §3

        // 检测面并行度（Fork 1 v1：1；Fork 2 EXP3：P_d）
        int detectionParallelism = params.getInt("detectionParallelism", 1);

        // COOLDOWN 参数 / COOLDOWN parameters
        int cooldownSamples = params.getInt("cooldownSamples", 2000);
        double zThresholdK = params.getDouble("zThresholdK", 1.0);

        int maxParallelism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism);
        int localTreeCount = (int) Math.ceil((double) totalTrees / parallelism);

        System.out.println("========================================");
        System.out.println("Broker: " + brokers);
        System.out.println("Source topic: " + topic);
        System.out.println("Tree topic: " + treeTopic);
        System.out.println("Model topic: " + modelTopic);
        System.out.println("Score topic: " + scoreTopic);
        System.out.println("Feature drift topic: " + featureDriftTopic);
        System.out.println("Drift round topic: " + driftRoundTopic);
        System.out.println("Has header: " + hasHeader);
        System.out.println("Has id column: " + hasId);
        System.out.println("Has label column: " + hasLabel);
        System.out.println("Auto increment id: " + autoIncrementId);
        System.out.println("Parallelism: " + parallelism);
        System.out.println("Max parallelism: " + maxParallelism);
        System.out.println("Total trees: " + totalTrees);
        System.out.println("Subsample size: " + subsampleSize);
        System.out.println("Ring buffer size: " + ringBufferSize);
        System.out.println("Local tree count per subtask: " + localTreeCount);
        System.out.println("--- detection side (HDDM_W) ---");
        System.out.println("HDDM warmup W: " + hddmWarmup);
        System.out.println("HDDM lambda: " + hddmLambda);
        System.out.println("HDDM warnConf/driftConf: " + hddmWarnConf + " / " + hddmDriftConf);
        System.out.println("HDDM warnTimeout: " + hddmWarnTimeout);
        System.out.println("HDDM scaleMode: " + hddmScaleMode);
        System.out.println("Detection parallelism P_d: " + detectionParallelism);
        System.out.println("--- scoring side / COOLDOWN ---");
        System.out.println("Cooldown samples: " + cooldownSamples);
        System.out.println("Z-threshold k: " + zThresholdK);
        System.out.println("========================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(10000);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        kafkaProps.setProperty("group.id", "local-processor-" + UUID.randomUUID().toString().substring(0, 8));
        kafkaProps.setProperty("auto.offset.reset", "latest");

        // ===== 主流：从 Kafka source-topic 读取原始数据 =====
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProps
        );
        kafkaConsumer.setStartFromEarliest();

        DataStream<String> rawStream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>noWatermarks()
                                .withTimestampAssigner((event, kafkaTimestamp) -> kafkaTimestamp)
                )
                .name("Kafka Source [" + topic + "]");

        DataStream<DataPoint> dataPointStream = rawStream
                .process(new CsvToDataPointFunction(hasHeader, hasId, hasLabel,
                        autoIncrementId, SequenceSource.PARSE_ID))
                .name("CSV to DataPoint");

        // ===== 打分面：行并行 keyBy =====
        final String[] randomKeys = ParallelismKeys.generate(parallelism, maxParallelism);
        System.out.println("Generated keys: " + Arrays.toString(randomKeys));

        KeyedStream<DataPoint, String> keyedStream = dataPointStream.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        randomKeys[Math.abs(dp.getId().hashCode() % randomKeys.length)]);

        // ===== 检测面：列并行 keyBy(featureId) → PerFeatureIKS → feature-drift-topic =====
        DataStream<FeatureValue> featureStream = dataPointStream
                .flatMap(new FeatureSplitFlatMap())
                .name("Feature Split (row → D)");

        DataStream<FeatureDrift> featureDriftStream = featureStream
                .keyBy((KeySelector<FeatureValue, Integer>) FeatureValue::getFeatureId)
                .process(new PerFeatureHDDMFunction(hddmWarmup, hddmLambda,
                        hddmWarnConf, hddmDriftConf, hddmWarnTimeout, hddmScaleMode))
                .setParallelism(detectionParallelism)
                .name("Per-Feature HDDM_W");

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);

        FlinkKafkaProducer<FeatureDrift> featureDriftProducer = new FlinkKafkaProducer<>(
                featureDriftTopic,
                new FeatureDriftSerializationSchema(featureDriftTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
        featureDriftStream.addSink(featureDriftProducer)
                .name("Feature Drift Sink [" + featureDriftTopic + "]");

        // ===== 广播流：合并 model-topic（森林）和 drift-round-topic（COMMITTED 决议）=====
        Properties modelKafkaProps = new Properties();
        modelKafkaProps.setProperty("bootstrap.servers", brokers);
        modelKafkaProps.setProperty("group.id", "model-consumer-" + UUID.randomUUID().toString().substring(0, 8));
        modelKafkaProps.setProperty("max.partition.fetch.bytes", "5242880");

        FlinkKafkaConsumer<String> modelConsumer = new FlinkKafkaConsumer<>(
                modelTopic,
                new SimpleStringSchema(),
                modelKafkaProps
        );
        modelConsumer.setStartFromEarliest();

        DataStream<BroadcastEnvelope> forestEnvelopeStream = env
                .addSource(modelConsumer)
                .name("Model Source [" + modelTopic + "]")
                .map(new ForestMessageDeserializer())
                .name("Parse ForestMessage")
                .map(BroadcastEnvelope::forest)
                .returns(BroadcastEnvelope.class)
                .name("Wrap Forest → BroadcastEnvelope");

        Properties driftRoundKafkaProps = new Properties();
        driftRoundKafkaProps.setProperty("bootstrap.servers", brokers);
        driftRoundKafkaProps.setProperty("group.id", "drift-round-consumer-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> driftRoundConsumer = new FlinkKafkaConsumer<>(
                driftRoundTopic,
                new SimpleStringSchema(),
                driftRoundKafkaProps
        );
        driftRoundConsumer.setStartFromEarliest();

        DataStream<BroadcastEnvelope> driftRoundEnvelopeStream = env
                .addSource(driftRoundConsumer)
                .name("Drift Round Source [" + driftRoundTopic + "]")
                .map(new DriftRoundMessageDeserializer())
                .name("Parse DriftRoundMessage")
                .map(BroadcastEnvelope::driftRound)
                .returns(BroadcastEnvelope.class)
                .name("Wrap DriftRound → BroadcastEnvelope");

        DataStream<BroadcastEnvelope> mergedBroadcast = forestEnvelopeStream.union(driftRoundEnvelopeStream);

        BroadcastStream<BroadcastEnvelope> broadcastStream =
                mergedBroadcast.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        // ===== 打分面三阶段状态机 =====
        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor (Phase A/B/C)");

        DataStream<ITreeMessage> treeStream = processed.getSideOutput(LocalProcessorFunction.TREE_TAG);

        // ===== Sink 1: iTree → Kafka tree-topic =====
        FlinkKafkaProducer<ITreeMessage> treeProducer = new FlinkKafkaProducer<>(
                treeTopic,
                new ITreeMessageSerializationSchema(treeTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        treeStream.addSink(treeProducer)
                .name("Kafka Sink [" + treeTopic + "]");

        // ===== Sink 2: ScoreResult → Kafka output-scores =====
        FlinkKafkaProducer<ScoreResult> scoreProducer = new FlinkKafkaProducer<>(
                scoreTopic,
                new ScoreResultSerializationSchema(scoreTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        processed.addSink(scoreProducer)
                .name("Kafka Sink [" + scoreTopic + "]");

        treeStream.print("ITreeMessage");
        processed.print("ScoreResult");
        featureDriftStream.print("FeatureDrift emitted");

        env.execute("LocalProcessor - Per-Feature HDDM_W Detection + Aggregator-Driven Retrain (phase3)");
    }

    // ===== 序列化/反序列化工具类 =====

    private static class ForestMessageDeserializer implements MapFunction<String, ForestMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public ForestMessage map(String json) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.readValue(json, ForestMessage.class);
        }
    }

    private static class ITreeMessageSerializationSchema
            implements KafkaSerializationSchema<ITreeMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ITreeMessageSerializationSchema(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ITreeMessage msg, @Nullable Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ITreeMessage to JSON", e);
            }
        }
    }

    private static class DriftRoundMessageDeserializer implements MapFunction<String, DriftRoundMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public DriftRoundMessage map(String json) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.readValue(json, DriftRoundMessage.class);
        }
    }

    private static class FeatureDriftSerializationSchema
            implements KafkaSerializationSchema<FeatureDrift> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        FeatureDriftSerializationSchema(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(FeatureDrift msg, @Nullable Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize FeatureDrift to JSON", e);
            }
        }
    }

    private static class ScoreResultSerializationSchema
            implements KafkaSerializationSchema<ScoreResult> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ScoreResultSerializationSchema(String topic) { this.topic = topic; }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ScoreResult msg, @Nullable Long timestamp) {
            if (mapper == null) mapper = new ObjectMapper();
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ScoreResult to JSON", e);
            }
        }
    }
}
