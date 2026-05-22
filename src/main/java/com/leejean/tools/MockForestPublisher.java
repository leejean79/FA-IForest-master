package com.leejean.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.beans.ForestMessage;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.util.Properties;

/**
 * 端到端手动测试工具：从本地 JSON 文件读 ForestMessage 投到 Kafka model-topic
 * End-to-end manual testing tool: reads ForestMessage from local JSON file and publishes to Kafka model-topic
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --topic model-topic --file forest.json
 *
 * JSON 文件准备方式 / How to prepare the JSON file:
 * 1. 跑 v1 一段时间，从 tree-topic 拉够 ITreeMessage 存到本地
 *    Run v1 for a while, pull enough ITreeMessages from tree-topic to local
 * 2. 用脚本把多条 ITreeMessage 拼成一条 ForestMessage 写到文件
 *    Combine multiple ITreeMessages into one ForestMessage and write to file
 * 3. 用本工具投到 model-topic
 *    Use this tool to publish to model-topic
 */
public class MockForestPublisher {

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String brokers = params.getRequired("broker");
        String topic = params.get("topic", "model-topic");
        String filePath = params.getRequired("file");

        // 读取并验证 JSON 文件 / Read and validate JSON file
        ObjectMapper mapper = new ObjectMapper();
        File jsonFile = new File(filePath);
        if (!jsonFile.exists()) {
            System.err.println("File not found: " + filePath);
            System.exit(1);
        }

        ForestMessage forest = mapper.readValue(jsonFile, ForestMessage.class);
        System.out.printf("Loaded ForestMessage: forestId=%s, version=%d, trees=%d, subsampleSize=%d%n",
                forest.getForestId(), forest.getVersion(),
                forest.getTrees() != null ? forest.getTrees().size() : 0,
                forest.getSubsampleSize());

        // 序列化为 JSON 字符串 / Serialize to JSON string
        String json = mapper.writeValueAsString(forest);

        // 发送到 Kafka / Send to Kafka
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, json);
            producer.send(record).get();
            producer.flush();
        }

        System.out.printf("Successfully published ForestMessage (version=%d, %d trees) to topic '%s'%n",
                forest.getVersion(),
                forest.getTrees() != null ? forest.getTrees().size() : 0,
                topic);
    }
}
