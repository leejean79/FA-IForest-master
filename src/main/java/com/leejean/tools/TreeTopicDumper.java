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
 * 独立的 Kafka 消费者：把 tree-topic 里的 ITreeMessage JSON 全部 dump 到本地 jsonl 文件。
 * Standalone Kafka consumer that dumps every ITreeMessage on tree-topic to a local jsonl file.
 *
 * <p>设计 / Design:
 * <ul>
 *   <li>不起 Flink，纯 KafkaConsumer + 文件写入。Pure KafkaConsumer + file writer, no Flink.</li>
 *   <li>用 group id = 随机 UUID，强制 from-beginning。Random group id forces read from earliest.</li>
 *   <li>poll 循环：连续 N 次 poll 都没数据才退出（idle timeout）。Polling exits after N empty polls.</li>
 * </ul>
 *
 * <p>用法 / Usage:
 * <pre>
 *   mvn -q exec:java -Dexec.mainClass=com.leejean.tools.TreeTopicDumper \
 *       -Dexec.args="localhost:9092 tree-topic trees.jsonl 10000"
 * </pre>
 *
 * <p>参数 / Args (按顺序，all positional):
 * <ol>
 *   <li>kafka bootstrap (默认 localhost:9092)</li>
 *   <li>topic name (默认 tree-topic)</li>
 *   <li>output file path (默认 trees.jsonl)</li>
 *   <li>idle timeout in ms (默认 10000，连续这么久没新消息就退出)</li>
 * </ol>
 */
public class TreeTopicDumper {

    public static void main(String[] args) throws IOException {
        // 解析参数 / parse args
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "tree-topic";
        String outputPath = args.length > 2 ? args[2] : "trees.jsonl";
        long idleTimeoutMs = args.length > 3 ? Long.parseLong(args[3]) : 10_000L;

        System.out.printf("[TreeTopicDumper] bootstrap=%s topic=%s output=%s idleTimeout=%dms%n",
                bootstrap, topic, outputPath, idleTimeoutMs);

        // KafkaConsumer 配置 / consumer config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // 随机 group id，避免和已有消费者冲突 / random group id to avoid conflict with existing consumers
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tree-topic-dumper-" + UUID.randomUUID());
        // 从最早消息开始读 / start from beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 不需要提交 offset（dumper 是一次性的）/ no need to commit offsets
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Path output = Paths.get(outputPath);
        // 如果文件已存在直接覆盖 / overwrite existing file
        Files.createDirectories(output.toAbsolutePath().getParent() == null
                ? Paths.get(".") : output.toAbsolutePath().getParent());

        long count = 0;
        long lastMessageAt = System.currentTimeMillis();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {

            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        // 一行一条原始 JSON / one raw JSON per line
                        writer.write(record.value());
                        writer.newLine();
                        count++;
                    }
                    writer.flush();
                    lastMessageAt = System.currentTimeMillis();
                    System.out.printf("[TreeTopicDumper] dumped %d records so far%n", count);
                } else {
                    // 空 poll：检查是否超过 idle timeout
                    // empty poll: check idle timeout
                    long idle = System.currentTimeMillis() - lastMessageAt;
                    if (idle >= idleTimeoutMs) {
                        System.out.printf("[TreeTopicDumper] no new messages for %dms, exiting%n", idle);
                        break;
                    }
                }
            }
        }

        System.out.printf("[TreeTopicDumper] done. total=%d, output=%s%n", count, output.toAbsolutePath());

        if (count == 0) {
            System.err.println("[TreeTopicDumper] WARNING: 0 messages dumped. " +
                    "Is the topic name correct? Has v1 LocalProcessor produced any trees?");
            System.exit(1);
        }
    }
}
