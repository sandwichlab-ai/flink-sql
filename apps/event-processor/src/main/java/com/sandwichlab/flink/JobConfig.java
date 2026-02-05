package com.sandwichlab.flink;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Job 配置管理
 *
 * 支持从以下来源读取配置（优先级从高到低）：
 * 1. AWS Managed Flink EnvironmentProperties (通过 KinesisAnalyticsRuntime)
 * 2. 系统属性 / 环境变量
 * 3. 默认值
 */
public class JobConfig {

    private static final Logger LOG = LoggerFactory.getLogger(JobConfig.class);
    private static final String PROPERTY_GROUP = "FlinkApplicationProperties";

    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final String attributedTopic;
    private final String awsRegion;
    private final String ddbTableName;
    private final int kafkaPartitions;
    private final String maxPollRecords;

    public JobConfig(String[] args) {
        Map<String, String> props = loadProperties();

        this.bootstrapServers = props.getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        this.inputTopic = props.getOrDefault("INPUT_TOPIC", "raw-events");
        this.outputTopic = props.getOrDefault("OUTPUT_TOPIC", "processed-events");
        this.attributedTopic = props.getOrDefault("ATTRIBUTED_TOPIC", "attributed-events");
        this.awsRegion = props.getOrDefault("AWS_REGION", "us-west-2");
        this.ddbTableName = props.getOrDefault("DDB_TABLE_NAME", "click_events");
        this.kafkaPartitions = Integer.parseInt(props.getOrDefault("KAFKA_PARTITIONS", "3"));
        this.maxPollRecords = props.getOrDefault("MAX_POLL_RECORDS", "100");
    }

    private Map<String, String> loadProperties() {
        Map<String, String> props = new HashMap<>();

        // 1. 尝试从 AWS Managed Flink 读取
        try {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties flinkProps = applicationProperties.get(PROPERTY_GROUP);
            if (flinkProps != null) {
                LOG.info("Loaded properties from AWS Managed Flink: {}", flinkProps);
                for (String key : flinkProps.stringPropertyNames()) {
                    props.put(key, flinkProps.getProperty(key));
                }
                return props;
            }
        } catch (IOException e) {
            LOG.warn("Failed to load AWS Managed Flink properties, falling back to env/system properties: {}", e.getMessage());
        }

        // 2. 从环境变量和系统属性读取
        props.put("BOOTSTRAP_SERVERS", getEnvOrProperty("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put("INPUT_TOPIC", getEnvOrProperty("INPUT_TOPIC", "raw-events"));
        props.put("OUTPUT_TOPIC", getEnvOrProperty("OUTPUT_TOPIC", "processed-events"));
        props.put("ATTRIBUTED_TOPIC", getEnvOrProperty("ATTRIBUTED_TOPIC", "attributed-events"));
        props.put("AWS_REGION", getEnvOrProperty("AWS_REGION", "us-west-2"));
        props.put("DDB_TABLE_NAME", getEnvOrProperty("DDB_TABLE_NAME", "click_events"));
        props.put("KAFKA_PARTITIONS", getEnvOrProperty("KAFKA_PARTITIONS", "3"));
        props.put("MAX_POLL_RECORDS", getEnvOrProperty("MAX_POLL_RECORDS", "100"));

        LOG.info("Loaded properties from env/system: {}", props);
        return props;
    }

    private String getEnvOrProperty(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return defaultValue;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public String getAttributedTopic() {
        return attributedTopic;
    }

    public int getKafkaPartitions() {
        return kafkaPartitions;
    }

    public Map<String, String> toSqlVariables() {
        Map<String, String> variables = new HashMap<>();
        variables.put("BOOTSTRAP_SERVERS", bootstrapServers);
        variables.put("INPUT_TOPIC", inputTopic);
        variables.put("OUTPUT_TOPIC", outputTopic);
        variables.put("ATTRIBUTED_TOPIC", attributedTopic);
        variables.put("AWS_REGION", awsRegion);
        variables.put("DDB_TABLE_NAME", ddbTableName);
        variables.put("MAX_POLL_RECORDS", maxPollRecords);

        return variables;
    }

    public String getMaxPollRecords() {
        return maxPollRecords;
    }

    @Override
    public String toString() {
        return String.format(
            "JobConfig{bootstrapServers='%s', inputTopic='%s', outputTopic='%s', attributedTopic='%s', awsRegion='%s', ddbTableName='%s', kafkaPartitions=%d, maxPollRecords='%s'}",
            bootstrapServers, inputTopic, outputTopic, attributedTopic, awsRegion, ddbTableName, kafkaPartitions, maxPollRecords
        );
    }
}
