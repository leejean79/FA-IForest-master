package com.leejean.common_utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Kafka test utility class.
 */
public class KafkaHelper implements AutoCloseable {

    private final String bootstrapServers;
    private final AdminClient adminClient;

    public KafkaHelper(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(props);
    }

    /**
     * 专门用于清理测试产生的消费者组
     */
    public void cleanupConsumerGroups(String groupPrefix) {
        try {
            Collection<ConsumerGroupListing> groups = adminClient.listConsumerGroups().all().get();
            List<String> testGroups = groups.stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(g -> g.startsWith(groupPrefix))
                    .collect(Collectors.toList());

            if (!testGroups.isEmpty()) {
                System.out.println("🗑️ 正在删除消费者组: " + testGroups);
                adminClient.deleteConsumerGroups(testGroups).all().get();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            System.err.println("清理消费者组失败 (可能是因为 Flink 任务还在运行未释放): " + e.getMessage());
        }
    }

    /**
     * 重建主题（带容错重试机制）
     */
    public void recreateTopic(String topic, int partitions, short replicationFactor) {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try {
                // 1. 如果存在，先删除
                if (topicExists(topic)) {
                    System.out.println("检测到旧主题，正在删除: " + topic);
                    adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
                    // 多给 Kafka 一点时间清理元数据
                    Thread.sleep(3000);
                }

                // 2. 创建新主题
                NewTopic newTopic = new NewTopic(topic, partitions, replicationFactor);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println("✅ 主题已成功重建: " + topic);
                return; // 成功则直接退出方法

            } catch (Exception e) {
                // 捕获常见的 "Topic is marked for deletion" 异常并重试
                if (e.getMessage() != null && (e.getMessage().contains("marked for deletion") || e.getMessage().contains("TopicExistsException"))) {
                    System.err.println("⚠️ 主题 [" + topic + "] 处于待删除中间状态，等待 3 秒后重试 (" + (i + 1) + "/" + maxRetries + ")...");
                    try { Thread.sleep(3000); } catch (InterruptedException ie) {}
                } else {
                    // 其他不可恢复的严重错误
                    throw new RuntimeException("重建主题彻底失败: " + topic, e);
                }
            }
        }
        throw new RuntimeException("超过最大重试次数，重建主题失败: " + topic);
    }

    /**
     * 检查主题是否存在
     */
    public boolean topicExists(String topic) {
        try {
            return adminClient.listTopics().names().get().contains(topic);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 创建测试用的 Consumer
     */
    public <K, V> KafkaConsumer<K, V> createTestConsumer(
            String keyDeserializer,
            String valueDeserializer) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }

    /**
     * 创建测试用的 Producer
     */
    public <K, V> KafkaProducer<K, V> createTestProducer(
            String keySerializer,
            String valueSerializer) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return new KafkaProducer<>(props);
    }

    @Override
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}
