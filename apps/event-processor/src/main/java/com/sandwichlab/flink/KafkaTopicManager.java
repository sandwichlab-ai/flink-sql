package com.sandwichlab.flink;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Topic 管理器
 *
 * 在 Flink Job 启动前自动创建所需的 Topics
 * 支持 MSK Serverless IAM 认证
 */
public class KafkaTopicManager {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicManager.class);

    private final String bootstrapServers;
    private final int numPartitions;
    private final short replicationFactor;

    public KafkaTopicManager(String bootstrapServers) {
        this(bootstrapServers, 3, (short) 3);
    }

    public KafkaTopicManager(String bootstrapServers, int numPartitions, short replicationFactor) {
        this.bootstrapServers = bootstrapServers;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    /**
     * 确保 Topics 存在，不存在则创建
     */
    public void ensureTopicsExist(String... topicNames) {
        ensureTopicsExist(Arrays.asList(topicNames));
    }

    /**
     * 确保 Topics 存在，不存在则创建
     */
    public void ensureTopicsExist(List<String> topicNames) {
        Properties props = createAdminClientConfig();

        try (AdminClient adminClient = AdminClient.create(props)) {
            // 获取已存在的 topics
            Set<String> existingTopics = adminClient.listTopics().names().get();
            LOG.info("Existing topics: {}", existingTopics);

            // 找出需要创建的 topics
            List<NewTopic> topicsToCreate = new ArrayList<>();
            for (String topicName : topicNames) {
                if (!existingTopics.contains(topicName)) {
                    LOG.info("Topic '{}' does not exist, will create", topicName);
                    topicsToCreate.add(new NewTopic(topicName, numPartitions, replicationFactor));
                } else {
                    LOG.info("Topic '{}' already exists", topicName);
                }
            }

            // 创建 topics
            if (!topicsToCreate.isEmpty()) {
                createTopics(adminClient, topicsToCreate);
            }

        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to manage Kafka topics", e);
            throw new RuntimeException("Failed to ensure topics exist", e);
        }
    }

    private void createTopics(AdminClient adminClient, List<NewTopic> topics) {
        LOG.info("Creating {} topic(s)...", topics.size());

        try {
            adminClient.createTopics(topics).all().get();
            for (NewTopic topic : topics) {
                LOG.info("Created topic: {} (partitions={}, replication={})",
                        topic.name(), topic.numPartitions(), topic.replicationFactor());
            }
        } catch (ExecutionException e) {
            // 处理部分 topic 已存在的情况
            if (e.getCause() instanceof TopicExistsException) {
                LOG.warn("Some topics already exist: {}", e.getMessage());
            } else {
                throw new RuntimeException("Failed to create topics", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Topic creation interrupted", e);
        }
    }

    private Properties createAdminClientConfig() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // MSK IAM 认证配置
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "AWS_MSK_IAM");
        props.put("sasl.jaas.config",
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        props.put("sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        // 超时配置
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        return props;
    }
}