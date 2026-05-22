package com.leejean.source;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 从本地文件读取数据并限速发送到 Kafka topic，支持断点续传
 * Reads data from a local file and sends to Kafka topic with rate limiting, supports resuming
 *
 * 使用方式 / Usage:
 *   --broker localhost:9092 --topic test --file /path/to/data.csv --rate 100
 *   --fromBeginning false   从上次位置继续读取 / resume from last position
 *
 * 进度文件保存在源文件同目录下，名为 <filename>.offset
 * Offset file is saved alongside the source file as <filename>.offset
 */
public class FileToKafkaProducer {

    // 每隔多少行打印一次进度 / Print progress every N lines
    private static final int PROGRESS_INTERVAL = 1000;

    public static void main(String[] args) {
        // 通过 ParameterTool 解析命令行参数 / Parse command line args via ParameterTool
        ParameterTool params = ParameterTool.fromArgs(args);

        String brokers = params.getRequired("broker");
        String topic = params.getRequired("topic");
        String filePath = params.getRequired("file");
        double rate = params.getDouble("rate", 1000.0);
        boolean noRateLimit = params.getBoolean("no-rate-limit", false);
        boolean fromBeginning = params.getBoolean("fromBeginning", true);

        // 进度文件路径 / Offset file path
        Path offsetPath = Paths.get(filePath + ".offset");

        // 读取上次进度 / Load previous offset
        int skipLines = 0;
        if (!fromBeginning && Files.exists(offsetPath)) {
            try {
                String content = new String(Files.readAllBytes(offsetPath), StandardCharsets.UTF_8).trim();
                skipLines = Integer.parseInt(content);
                System.out.println("Resuming from line: " + skipLines);
            } catch (IOException | NumberFormatException e) {
                System.err.println("Failed to read offset file, starting from beginning.");
            }
        }

        System.out.println("========================================");
        System.out.println("Broker: " + brokers);
        System.out.println("Topic: " + topic);
        System.out.println("File: " + filePath);
        System.out.println("Rate: " + (noRateLimit ? "UNLIMITED" : rate + " msg/s"));
        System.out.println("From beginning: " + fromBeginning);
        if (skipLines > 0) {
            System.out.println("Skipping first " + skipLines + " lines");
        }
        System.out.println("========================================");

        // 配置 Kafka Producer / Configure Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // v3.4.6: optional unlimited mode for throughput testing
        RateLimiter rateLimiter = noRateLimit ? null : RateLimiter.create(rate);

        int totalRead = 0;  // 当前文件已读行数 / total lines read in file
        int sent = 0;       // 实际发送条数 / actually sent count

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                totalRead++;

                // 跳过已发送的行 / Skip already-sent lines
                if (totalRead <= skipLines) {
                    continue;
                }

                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
                producer.send(new ProducerRecord<>(topic, null, line));
                sent++;

                // 定期输出进度 / Periodic progress report
                if (sent % PROGRESS_INTERVAL == 0) {
                    System.out.println("[Progress] Sent: " + sent + ", File line: " + totalRead);
                    saveOffset(offsetPath, totalRead);
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
            // 最终保存进度 / Save final offset
            saveOffset(offsetPath, totalRead);
        }

        System.out.println("========================================");
        System.out.println("Finished. Lines sent this run: " + sent);
        System.out.println("Total file lines read: " + totalRead);
        System.out.println("Offset saved to: " + offsetPath);
        System.out.println("========================================");
    }

    /**
     * 保存已读行数到进度文件 / Save current line offset to file
     */
    private static void saveOffset(Path offsetPath, int lineNumber) {
        try {
            Files.write(offsetPath, String.valueOf(lineNumber).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("Failed to save offset: " + e.getMessage());
        }
    }
}
