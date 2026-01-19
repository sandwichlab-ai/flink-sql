package com.sandwichlab.flink.udf.config;

import org.apache.flink.table.functions.FunctionContext;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Operator 配置
 *
 * 支持从多种来源加载配置（优先级从高到低）：
 * 1. Flink Job Parameter
 * 2. 系统环境变量
 * 3. 默认值
 */
public class OperatorConfig implements Serializable {

    private final Map<String, String> properties;

    private OperatorConfig(Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
    }

    /**
     * 从 FunctionContext 加载配置
     */
    public static OperatorConfig fromContext(FunctionContext context) {
        Map<String, String> props = new HashMap<>();

        // DynamoDB 配置
        props.put("ddb.table.name", getConfig(context, "DDB_TABLE_NAME", ""));
        props.put("ddb.region", getConfig(context, "AWS_REGION", "us-west-2"));

        return new OperatorConfig(props);
    }

    /**
     * 从 Map 创建配置
     */
    public static OperatorConfig fromMap(Map<String, String> properties) {
        return new OperatorConfig(properties);
    }

    /**
     * 获取配置值
     */
    public String get(String key) {
        return properties.get(key);
    }

    /**
     * 获取配置值，带默认值
     */
    public String get(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    /**
     * 获取整数配置
     */
    public int getInt(String key, int defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    /**
     * 获取布尔配置
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.get(key);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value);
    }

    /**
     * 从 FunctionContext 或环境变量获取配置
     */
    private static String getConfig(FunctionContext context, String key, String defaultValue) {
        // 1. 尝试从 Job Parameter 获取
        if (context != null) {
            String value = context.getJobParameter(key, null);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }

        // 2. 尝试从环境变量获取
        String envValue = System.getenv(key);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }

        // 3. 尝试从系统属性获取
        String propValue = System.getProperty(key);
        if (propValue != null && !propValue.isEmpty()) {
            return propValue;
        }

        // 4. 返回默认值
        return defaultValue;
    }

    @Override
    public String toString() {
        return "OperatorConfig{" + properties + "}";
    }
}
