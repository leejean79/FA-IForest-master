package com.leejean.common_utils;

public class KafkaMainCleaner {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // 使用 try-with-resources 保证 adminClient 最终被关闭
        try (KafkaHelper helper = new KafkaHelper(BOOTSTRAP_SERVERS)) {

            System.out.println("========== 1. 清理遗留的消费者组 ==========");
            // 修复了之前的拼写错误 cosumer -> consumer
            helper.cleanupConsumerGroups("kafka-consumer-");
            helper.cleanupConsumerGroups("model-consumer-");
            helper.cleanupConsumerGroups("drift-round-consumer-");
            helper.cleanupConsumerGroups("drift-aggregator-");


            System.out.println("\n========== 2. 开始重建业务主题 ==========");
            // Flink 异常检测框架所需的所有 Topic
            helper.recreateTopic("source-topic", 1, (short) 1);
            helper.recreateTopic("tree-topic", 1, (short) 1);
            helper.recreateTopic("model-topic", 1, (short) 1);
            helper.recreateTopic("output-scores", 1, (short) 1);
            helper.recreateTopic("drift-round-topic", 1, (short) 1);
            helper.recreateTopic("feature-drift-topic", 1, (short) 1);

            System.out.println("\n🎉 所有 Kafka 环境清理及重建完毕！");

        } catch (Exception e) {
            System.err.println("\n❌ 脚本执行中断，发生严重错误: ");
            e.printStackTrace(); // 这样以后哪怕断了，你也能一眼看出是哪一行报错
        }
    }
}