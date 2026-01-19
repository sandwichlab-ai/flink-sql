package com.sandwichlab.flink.udf.operator;

import com.sandwichlab.flink.udf.config.OperatorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.HashMap;
import java.util.Map;

/**
 * DynamoDB Operator
 *
 * 将数据写入 DynamoDB
 */
public class DdbOperator implements Operator {

    private static final Logger LOG = LoggerFactory.getLogger(DdbOperator.class);

    private String tableName;
    private String region;
    private transient DynamoDbClient client;

    @Override
    public String getType() {
        return "dynamodb";
    }

    @Override
    public void open(OperatorConfig config) {
        this.tableName = config.get("ddb.table.name");
        this.region = config.get("ddb.region", "us-west-2");

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("DDB table name is required (ddb.table.name or DDB_TABLE_NAME)");
        }

        this.client = DynamoDbClient.builder()
                .region(Region.of(region))
                .build();

        LOG.info("DdbOperator initialized: table={}, region={}", tableName, region);
    }

    @Override
    public boolean execute(Map<String, Object> keys, Map<String, Object> values) {
        if (client == null) {
            LOG.error("DdbOperator not initialized");
            return false;
        }

        try {
            Map<String, AttributeValue> item = new HashMap<>();

            // 添加 keys
            for (Map.Entry<String, Object> entry : keys.entrySet()) {
                AttributeValue av = toAttributeValue(entry.getValue());
                if (av != null) {
                    item.put(entry.getKey(), av);
                }
            }

            // 添加 values
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                AttributeValue av = toAttributeValue(entry.getValue());
                if (av != null) {
                    item.put(entry.getKey(), av);
                }
            }

            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();

            client.putItem(request);
            return true;

        } catch (DynamoDbException e) {
            LOG.error("DynamoDB write failed: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 将 Java 对象转换为 DynamoDB AttributeValue
     */
    private AttributeValue toAttributeValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            String s = (String) value;
            if (s.isEmpty()) {
                return null;
            }
            return AttributeValue.builder().s(s).build();
        }

        if (value instanceof Number) {
            return AttributeValue.builder().n(value.toString()).build();
        }

        if (value instanceof Boolean) {
            return AttributeValue.builder().bool((Boolean) value).build();
        }

        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, AttributeValue> avMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                AttributeValue av = toAttributeValue(entry.getValue());
                if (av != null) {
                    avMap.put(entry.getKey(), av);
                }
            }
            if (avMap.isEmpty()) {
                return null;
            }
            return AttributeValue.builder().m(avMap).build();
        }

        // 其他类型转为字符串
        return AttributeValue.builder().s(value.toString()).build();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            LOG.info("DdbOperator closed");
        }
    }
}
