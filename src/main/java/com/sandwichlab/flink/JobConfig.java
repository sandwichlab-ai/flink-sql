package com.sandwichlab.flink;

import java.util.HashMap;
import java.util.Map;

/**
 * Job 配置管理
 *
 * 从环境变量或系统属性读取配置
 */
public class JobConfig {

    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;

    public JobConfig() {
        this.bootstrapServers = getConfig("BOOTSTRAP_SERVERS", "localhost:9092");
        this.inputTopic = getConfig("INPUT_TOPIC", "raw-events");
        this.outputTopic = getConfig("OUTPUT_TOPIC", "processed-events");
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

    /**
     * 获取 SQL 变量映射，用于替换 SQL 文件中的 ${VARIABLE}
     */
    public Map<String, String> toSqlVariables() {
        Map<String, String> variables = new HashMap<>();
        variables.put("BOOTSTRAP_SERVERS", bootstrapServers);
        variables.put("INPUT_TOPIC", inputTopic);
        variables.put("OUTPUT_TOPIC", outputTopic);
        return variables;
    }

    @Override
    public String toString() {
        return String.format(
            "JobConfig{bootstrapServers='%s', inputTopic='%s', outputTopic='%s'}",
            bootstrapServers, inputTopic, outputTopic
        );
    }

    private static String getConfig(String key, String defaultValue) {
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
}
