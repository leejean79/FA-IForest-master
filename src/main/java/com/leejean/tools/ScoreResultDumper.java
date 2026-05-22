package com.leejean.tools;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

/**
 * 独立的 Kafka 消费者：把 output-scores 里的 ScoreResult JSON 全部 dump 到本地 jsonl 文件。
 * Standalone Kafka consumer that dumps every ScoreResult on output-scores to a local jsonl file.
 *
 * <p>设计与 TreeTopicDumper 一致 / Same design as TreeTopicDumper:
 * <ul>
 *   <li>不起 Flink，纯 KafkaConsumer + 文件写入</li>
 *   <li>用随机 group id 强制 from-beginning</li>
 *   <li>poll 循环：连续 N 毫秒没数据才退出（idle timeout）</li>
 * </ul>
 *
 * <p>用法 / Usage:
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=com.leejean.tools.ScoreResultDumper \
 *       -Dexec.args="localhost:9092 output-scores scores-stable.jsonl 10000"
 * </pre>
 *
 * <p>参数 / Args (按顺序):
 * <ol>
 *   <li>kafka bootstrap (默认 localhost:9092)</li>
 *   <li>topic name (默认 output-scores)</li>
 *   <li>output file path (默认 scores-stable.jsonl)</li>
 *   <li>idle timeout in ms (默认 10000)</li>
 * </ol>
 */
public class ScoreResultDumper {

    public static void main(String[] args) throws IOException {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "output-scores";
        String outputPath = args.length > 2 ? args[2] : "scores-stable.jsonl";
        long idleTimeoutMs = args.length > 3 ? Long.parseLong(args[3]) : 10_000L;

        System.out.printf("[ScoreResultDumper] bootstrap=%s topic=%s output=%s idleTimeout=%dms%n",
                bootstrap, topic, outputPath, idleTimeoutMs);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // 随机 group id，避免和已有消费者冲突 / random group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "score-dumper-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Path output = Paths.get(outputPath);
        Path parent = output.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        long count = 0;
        long lastMessageAt = System.currentTimeMillis();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {

            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        writer.write(record.value());
                        writer.newLine();
                        count++;
                    }
                    writer.flush();
                    lastMessageAt = System.currentTimeMillis();
                    // 每 1000 条打印一次进度，避免日志噪音
                    if (count % 1000 == 0) {
                        System.out.printf("[ScoreResultDumper] dumped %d records so far%n", count);
                    }
                } else {
                    long idle = System.currentTimeMillis() - lastMessageAt;
                    if (idle >= idleTimeoutMs) {
                        System.out.printf("[ScoreResultDumper] no new messages for %dms, exiting%n", idle);
                        break;
                    }
                }
            }
        }

        System.out.printf("[ScoreResultDumper] done. total=%d, output=%s%n", count, output.toAbsolutePath());

        if (count == 0) {
            System.err.println("[ScoreResultDumper] WARNING: 0 messages dumped. " +
                    "Is the topic name correct? Has LocalProcessor run and produced scores?");
            System.exit(1);
        }
    }
}
