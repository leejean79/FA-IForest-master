package com.leejean.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Kafka Consumer 测试工具，用于快速查看指定 topic 的数据
 * Kafka Consumer test utility for quickly inspecting data in a given topic
 *
 * 使用方式 / Usage:
 *   mvn test -Dtest=KafkaConsumerTest#consumeTopic -Dbroker=localhost:9092 -Dtopic=test
 *
 * 可选参数 / Optional:
 *   -DfromBeginning=true  从头消费 / consume from earliest
 *   -Dmax=200             最大读取条数 / max messages to read (default 100, 0=unlimited)
 */
public class KafkaConsumerTest {

    /**
     * 消费指定 topic 并打印消息
     * Consumes the specified topic and prints messages
     */
    @Test
    public void consumeTopic() {
        // 通过 System Properties 读取参数（mvn -D 传入）
        // Read params from System Properties (passed via mvn -D)
        ParameterTool params = ParameterTool.fromSystemProperties();

        String brokers = params.getRequired("broker");
        String topic = params.getRequired("topic");
        boolean fromBeginning = params.getBoolean("fromBeginning", false);
        int maxMessages = params.getInt("max", 100);

        // 配置 Consumer / Configure Consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-test-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("========================================");
        System.out.println("Consuming topic: " + topic);
        System.out.println("Broker: " + brokers);
        System.out.println("Offset reset: " + props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        System.out.println("Max messages: " + (maxMessages == 0 ? "unlimited" : maxMessages));
        System.out.println("========================================");

        int count = 0;
        int emptyPolls = 0;
        // 连续空轮询次数上限，超过则退出 / Exit after consecutive empty polls
        int maxEmptyPolls = 10;

        try {
            while (maxMessages == 0 || count < maxMessages) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    if (emptyPolls >= maxEmptyPolls) {
                        System.out.println("[INFO] No more messages after " + maxEmptyPolls + " empty polls, exiting.");
                        break;
                    }
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : records) {
                    count++;
                    System.out.printf("[#%d] partition=%d, offset=%d, key=%s, value=%s%n",
                            count, record.partition(), record.offset(), record.key(), record.value());
                    if (maxMessages > 0 && count >= maxMessages) {
                        break;
                    }
                }
            }
        } finally {
            consumer.close();
        }

        System.out.println("========================================");
        System.out.println("Total messages consumed: " + count);
        System.out.println("========================================");
    }
}
