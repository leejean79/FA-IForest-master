package com.leejean.common_utils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leejean.beans.DriftRoundMessage;
import com.leejean.beans.FeatureDrift;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class JacksonKafkaConsumer {

    // 全局复用 ObjectMapper，保证线程安全和极高的性能
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {
        // 1. 配置 Kafka 消费者参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fa-iforest-multi-group"); // 你的消费者组

        // 依然使用 String 接收，把 JSON 解析工作放在业务代码里防崩溃
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 2. 创建消费者实例 (try-with-resources 保证最后被关闭)
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            // 3. 定义并精准订阅多个固定主题 (List 方式)
            List<String> targetTopics = Arrays.asList("drift-round-topic","feature-drift-topic");
            consumer.subscribe(targetTopics);

            System.out.println("✅ 消费者启动成功！正在监听以下主题: " + targetTopics);
            System.out.println("--------------------------------------------------");

            // 4. 无限循环拉取数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    String currentTopic = record.topic();
                    String rawJson = record.value();

                    // 5. 路由分发机制：根据主题名执行不同的处理逻辑
                    switch (currentTopic) {

                        case "drift-round-topic":
                            try {
                                // 处理异常打分结果
                                DriftRoundMessage driftRoundMessage = mapper.readValue(rawJson, DriftRoundMessage.class);
                                System.out.println(driftRoundMessage);

                            } catch (Exception e) {
                                System.err.println("[打分流] 脏数据跳过 -> " + rawJson);
                            }
                            break;

                        case "feature-drift-topic":
                            // 检测面每特征确认 onset 上报 / per-feature confirmed onset
                            FeatureDrift featureDrift = mapper.readValue(rawJson, FeatureDrift.class);
                            System.out.println(featureDrift);
                            break;

                        default:
                            System.out.println("⚠️ 收到未注册的主题数据 [" + currentTopic + "]: " + rawJson);
                            break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("❌ 消费者发生严重错误: ");
            e.printStackTrace();
        }
    }

}