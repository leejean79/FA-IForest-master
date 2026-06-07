package com.leejean.main;

import com.leejean.beans.BroadcastEnvelope;
import com.leejean.beans.DataPoint;
import com.leejean.beans.DriftReport;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.ForestMessage;
import com.leejean.beans.ITreeMessage;
import com.leejean.beans.ScoreResult;
import com.leejean.common_utils.CsvToDataPointFunction;
import com.leejean.common_utils.ParallelismKeys;
import com.leejean.common_utils.SequenceSource;
import com.leejean.drift.HDDM_AConfig;
import com.leejean.flink.LocalProcessorFunction;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

/**
 * v3.2 本地 Flink 流处理入口：三阶段状态机（Phase B/A/C）+ 滑动窗口 HDDM 漂移检测 + COOLDOWN 重训
 * v3.2 Local Flink streaming entry point: three-phase state machine + windowed HDDM + COOLDOWN retrain.
 *
 * <p>从 Kafka source-topic 读取原始数据，从 model-topic 接收全局森林广播：
 * <ul>
 *   <li>Phase B（冷启动）：环形缓冲区填满后分散训树 → side output iTree 到 tree-topic</li>
 *   <li>Phase A（积压消化）：全局模型到达后，给积压数据打分</li>
 *   <li>Phase C（正常预测 + 漂移检测）：STABLE → WARN → COOLDOWN → WAITING → STABLE</li>
 * </ul>
 *
 * <p>v3.2 改进 / v3.2 improvements:
 * <ul>
 *   <li>HDDM_A_Windowed 滑动窗口检测器替代累积 HDDM_A，避免漂移信号被历史稀释</li>
 *   <li>COOLDOWN 子状态：DRIFT 后收集数据，z-score 阈值筛选正常值写入环形缓冲，重训新森林</li>
 *   <li>Phase C 概率写入环形缓冲：STABLE 期 p=0.3，WARN 期 p=0.1，异常数据不写入</li>
 * </ul>
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --topic source-topic --treeTopic tree-topic
 *   --modelTopic model-topic --scoreTopic output-scores
 *   --hasHeader true --hasId true --hasLabel true --autoIncrementId true
 *   --totalTrees 100 --subsampleSize 256 --ringBufferSize 1000 --parallelism 4
 *   --detector HDDM_A_Windowed --hddmWindowSize 2000
 *   --warnConfidence 0.005 --driftConfidence 0.001
 *   --warnTimeoutSamples 2000 --warnTimeoutBehavior DISCARD
 *   --cooldownSamples 5000 --zThresholdK 1.0
 *
 * 参数说明 / Parameters:
 *   --broker               Kafka broker 地址，默认 localhost:9092
 *   --topic                数据源 topic，默认 source-topic
 *   --treeTopic            iTree 输出 topic，默认 tree-topic
 *   --modelTopic           全局森林输入 topic，默认 model-topic
 *   --scoreTopic           异常分数输出 topic，默认 output-scores
 *   --hasHeader             数据是否包含标题行，默认 true
 *   --hasId                 数据是否包含 id 列（第一列），默认 true
 *   --hasLabel              数据是否包含类标签列（最后一列），默认 true
 *   --autoIncrementId       无 id 列时是否自增生成 id，默认 true
 *   --totalTrees            全局森林总树数，默认 100
 *   --subsampleSize         每棵树的训练样本数 ψ，默认 256
 *   --ringBufferSize        环形缓冲区大小，默认 1000
 *   --parallelism           并行度，默认 4
 *   --seed                  可选，固定随机种子
 *   --detector              漂移检测器类型：HDDM_A_Windowed（默认）| HDDM_A | HDDM_W | IKS
 *   --hddmWindowSize        HDDM_A_Windowed 窗口大小，默认 2000
 *   --hddmLambda            HDDM_W 的 EWMA 遗忘因子，范围 (0, 1]，默认 0.1
 *   --iksWindowSize         IKS reference/current 窗口大小 W，默认 2000
 *   --iksPValue             IKS KS 检验显著性水平 p-value，范围 (0, 1)，默认 0.001
 *   --warnConfidence        HDDM WARN 置信度，默认 0.005
 *   --driftConfidence       HDDM DRIFT 置信度，默认 0.001
 *   --warnTimeoutSamples    WARN 超时样本数，默认 2000
 *   --warnTimeoutBehavior   WARN 超时行为：DISCARD|PROMOTE，默认 DISCARD
 *   --cooldownSamples       COOLDOWN 期采集样本数，默认 5000
 *   --zThresholdK           COOLDOWN 期 z-score 阈值系数 k，默认 1.0
 */
public class LocalProcessor {

    public static void main(String[] args) throws Exception {
        // 解析命令行参数 / Parse command line args
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
        String driftTopic = params.get("driftTopic", "drift-topic");
        String driftRoundTopic = params.get("driftRoundTopic", "drift-round-topic");

        // v3.2 HDDM + COOLDOWN 参数 / v3.2 HDDM + COOLDOWN parameters
        HDDM_AConfig hddmDefaults = HDDM_AConfig.defaults();
        String detector = params.get("detector", "HDDM_A_Windowed");
        int hddmWindowSize = params.getInt("hddmWindowSize", 2000);
        double hddmLambda = params.getDouble("hddmLambda", 0.1);
        int iksWindowSize = params.getInt("iksWindowSize", 2000);
        double iksPValue = params.getDouble("iksPValue", 0.001);
        double warnConfidence = params.getDouble("warnConfidence", hddmDefaults.getWarnConfidence());
        double driftConfidence = params.getDouble("driftConfidence", hddmDefaults.getDriftConfidence());
        long warnTimeoutSamples = params.getLong("warnTimeoutSamples", hddmDefaults.getWarnTimeoutSamples());
        String warnTimeoutBehavior = params.get("warnTimeoutBehavior", "DISCARD");
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
        System.out.println("Drift topic: " + driftTopic);
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
        System.out.println("--- v3.2 HDDM + COOLDOWN ---");
        System.out.println("Detector: " + detector);
        System.out.println("HDDM window size: " + hddmWindowSize);
        System.out.println("HDDM lambda: " + hddmLambda);
        System.out.println("IKS window size: " + iksWindowSize);
        System.out.println("IKS pValue: " + iksPValue);
        System.out.println("Warn confidence: " + warnConfidence);
        System.out.println("Drift confidence: " + driftConfidence);
        System.out.println("Warn timeout samples: " + warnTimeoutSamples);
        System.out.println("Warn timeout behavior: " + warnTimeoutBehavior);
        System.out.println("Cooldown samples: " + cooldownSamples);
        System.out.println("Z-threshold k: " + zThresholdK);
        System.out.println("========================================");

        // 创建 Flink 执行环境 / Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.setMaxParallelism(maxParallelism);
        env.getConfig().setGlobalJobParameters(params);
        env.enableCheckpointing(10000);  // 开启 checkpoint，使 AT_LEAST_ONCE Producer 在 checkpoint 时 flush+等 ack，确保单条森林可靠落盘


        // 配置 Kafka 属性 / Configure Kafka properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", brokers);
        kafkaProps.setProperty("group.id", "local-processor-" + UUID.randomUUID().toString().substring(0, 8));
        kafkaProps.setProperty("auto.offset.reset", "latest");

        // ===== 主流：从 Kafka source-topic 读取原始数据 =====
        // Main stream: read raw data from Kafka source-topic
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                kafkaProps
        );

        kafkaConsumer.setStartFromEarliest();  // 从头消费 source-topic，避免 producer 抢跑导致漏掉开头数据

        DataStream<String> rawStream = env.addSource(kafkaConsumer)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>noWatermarks()
                                .withTimestampAssigner((event, kafkaTimestamp) -> kafkaTimestamp)
                )
                .name("Kafka Source [" + topic + "]");

        // 将原始 CSV 数据封装为 DataPoint（使用 PARSE_ID 策略提取 originalSequence）
        // Parse CSV into DataPoint (use PARSE_ID strategy for originalSequence)
        DataStream<DataPoint> dataPointStream = rawStream
                .process(new CsvToDataPointFunction(hasHeader, hasId, hasLabel,
                        autoIncrementId, SequenceSource.PARSE_ID))
                .name("CSV to DataPoint");

        // 预生成均匀 Key / Pre-generate uniform keys
        final String[] randomKeys = ParallelismKeys.generate(parallelism, maxParallelism);
        System.out.println("Generated keys: " + Arrays.toString(randomKeys));

        // 确定性均匀 keyBy / Deterministic uniform keyBy
        KeyedStream<DataPoint, String> keyedStream = dataPointStream.keyBy(
                (KeySelector<DataPoint, String>) dp ->
                        randomKeys[Math.abs(dp.getId().hashCode() % randomKeys.length)]);

        // ===== 广播流：合并 model-topic（森林）和 drift-round-topic（投票决议）=====
        // Broadcast stream: merge model-topic (forest) and drift-round-topic (voting decisions)
        Properties modelKafkaProps = new Properties();
        modelKafkaProps.setProperty("bootstrap.servers", brokers);
        modelKafkaProps.setProperty("group.id", "model-consumer-" + UUID.randomUUID().toString().substring(0, 8));
        modelKafkaProps.setProperty("max.partition.fetch.bytes", "5242880");

        FlinkKafkaConsumer<String> modelConsumer = new FlinkKafkaConsumer<>(
                modelTopic,
                new SimpleStringSchema(),
                modelKafkaProps
        );
//        modelConsumer.setStartFromLatest();  // 不消费历史森林 / skip historical forests
        modelConsumer.setStartFromEarliest();

        DataStream<BroadcastEnvelope> forestEnvelopeStream = env
                .addSource(modelConsumer)
                .name("Model Source [" + modelTopic + "]")
                .map(new ForestMessageDeserializer())
                .name("Parse ForestMessage")
                .map(fm -> BroadcastEnvelope.forest(fm))
                .returns(BroadcastEnvelope.class)
                .name("Wrap Forest → BroadcastEnvelope");

        // v3.4: drift-round-topic 订阅
        Properties driftRoundKafkaProps = new Properties();
        driftRoundKafkaProps.setProperty("bootstrap.servers", brokers);
        driftRoundKafkaProps.setProperty("group.id", "drift-round-consumer-" + UUID.randomUUID().toString().substring(0, 8));

        FlinkKafkaConsumer<String> driftRoundConsumer = new FlinkKafkaConsumer<>(
                driftRoundTopic,
                new SimpleStringSchema(),
                driftRoundKafkaProps
        );
//        driftRoundConsumer.setStartFromLatest();
        driftRoundConsumer.setStartFromEarliest();

        DataStream<BroadcastEnvelope> driftRoundEnvelopeStream = env
                .addSource(driftRoundConsumer)
                .name("Drift Round Source [" + driftRoundTopic + "]")
                .map(new DriftRoundMessageDeserializer())
                .name("Parse DriftRoundMessage")
                .map(drm -> BroadcastEnvelope.driftRound(drm))
                .returns(BroadcastEnvelope.class)
                .name("Wrap DriftRound → BroadcastEnvelope");

        // union 两条流 → 统一广播 / union both streams → unified broadcast
        DataStream<BroadcastEnvelope> mergedBroadcast = forestEnvelopeStream.union(driftRoundEnvelopeStream);

        BroadcastStream<BroadcastEnvelope> broadcastStream =
                mergedBroadcast.broadcast(LocalProcessorFunction.FOREST_DESC, LocalProcessorFunction.DRIFT_ROUND_DESC);

        // ===== 三阶段状态机处理 =====
        // Three-phase state machine processing
        SingleOutputStreamOperator<ScoreResult> processed = keyedStream
                .connect(broadcastStream)
                .process(new LocalProcessorFunction())
                .name("Local Processor (Phase A/B/C)");

        // 两路输出 / Two output streams
        DataStream<ITreeMessage> treeStream = processed.getSideOutput(LocalProcessorFunction.TREE_TAG);
        // processed 本身就是 ScoreResult 主流 / processed itself is the ScoreResult main stream

        // ===== Sink 1: iTree → Kafka tree-topic =====
        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", brokers);

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

        // ===== v3.4.1 修复：DriftReport side output → drift-topic =====
        DataStream<DriftReport> driftReportStream = processed.getSideOutput(LocalProcessorFunction.DRIFT_REPORT_TAG);

        FlinkKafkaProducer<DriftReport> driftReportProducer = new FlinkKafkaProducer<>(
                driftTopic,
                new DriftReportSerializationSchema(driftTopic),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        driftReportStream.addSink(driftReportProducer)
                .name("Drift Topic Sink [" + driftTopic + "]");

        // 控制台调试输出 / Console debug output
        treeStream.print("ITreeMessage");
        processed.print("ScoreResult");
        driftReportStream.print("DriftReport emitted");

        // 启动 Flink 任务 / Start Flink job
        env.execute("LocalProcessor - Three-Phase iForest + Federated Drift Voting (v3.4)");
    }

    // ===== 序列化/反序列化工具类 =====
    // Serialization / Deserialization helpers

    /**
     * ForestMessage JSON 反序列化器 / ForestMessage JSON deserializer
     */
    private static class ForestMessageDeserializer implements MapFunction<String, ForestMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public ForestMessage map(String json) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(json, ForestMessage.class);
        }
    }

    /**
     * ITreeMessage → Kafka ProducerRecord 的序列化器
     * Serializer: ITreeMessage → Kafka ProducerRecord (JSON)
     */
    private static class ITreeMessageSerializationSchema
            implements KafkaSerializationSchema<ITreeMessage> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ITreeMessageSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ITreeMessage msg, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ITreeMessage to JSON", e);
            }
        }
    }

    /**
     * DriftRoundMessage JSON 反序列化器 / DriftRoundMessage JSON deserializer
     */
    private static class DriftRoundMessageDeserializer implements MapFunction<String, DriftRoundMessage> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper mapper;

        @Override
        public DriftRoundMessage map(String json) throws Exception {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            return mapper.readValue(json, DriftRoundMessage.class);
        }
    }

    /**
     * DriftReport JSON 序列化器
     * v3.4.1: DriftReport side output → Kafka drift-topic
     */
    private static class DriftReportSerializationSchema
            implements KafkaSerializationSchema<DriftReport> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        DriftReportSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(DriftReport msg, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize DriftReport to JSON", e);
            }
        }
    }

    /**
     * ScoreResult → Kafka ProducerRecord 的序列化器
     * Serializer: ScoreResult → Kafka ProducerRecord (JSON)
     */
    private static class ScoreResultSerializationSchema
            implements KafkaSerializationSchema<ScoreResult> {

        private static final long serialVersionUID = 1L;
        private final String topic;
        private transient ObjectMapper mapper;

        ScoreResultSerializationSchema(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(ScoreResult msg, @Nullable Long timestamp) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            try {
                byte[] value = mapper.writeValueAsBytes(msg);
                return new ProducerRecord<>(topic, null, value);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize ScoreResult to JSON", e);
            }
        }
    }
}
