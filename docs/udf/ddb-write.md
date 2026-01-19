# DynamoDB Write UDF

在 Flink SQL 中间节点写入 DynamoDB，保证写入完成后数据再流向下游。

## 使用场景

当需要确保 DynamoDB 在 Kafka/S3 sink 之前写入时，使用此 UDF 在 View 中完成写入。

## SQL 语法

```sql
ddb_write(
    fingerprint,    -- STRING: Partition Key
    click_time,     -- BIGINT: Sort Key
    user_id,        -- STRING: 用户 ID
    click_id,       -- STRING: 点击 ID (gclid, fbclid 等)
    click_id_name,  -- STRING: 点击 ID 类型名称
    utm_params      -- MAP<STRING, STRING>: UTM 参数 (可选)
) RETURNS STRING    -- 返回 fingerprint (透传)
```

## 使用示例

```sql
-- 1. 创建中间 View，在此处写入 DynamoDB
CREATE TEMPORARY VIEW events_with_ddb AS
SELECT
    *,
    ddb_write(
        anonymous_id,
        CAST(clid_params['gclid_timestamp'] AS BIGINT),
        user_id,
        clid_params['gclid'],
        'gclid',
        utm_params
    ) AS ddb_written
FROM raw_events
WHERE clid_params['gclid'] IS NOT NULL;

-- 2. 下游从 View 读取时，DynamoDB 已写入
INSERT INTO processed_events
SELECT * FROM events_with_ddb;
```

## 配置

通过环境变量或 Flink Job 参数配置：

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| ddb.table.name | DDB_TABLE_NAME | (必填) | DynamoDB 表名 |
| ddb.region | AWS_REGION | us-west-2 | AWS 区域 |

### AWS Managed Flink 配置示例

```json
{
  "PropertyGroups": [
    {
      "PropertyGroupId": "FlinkApplicationProperties",
      "PropertyMap": {
        "DDB_TABLE_NAME": "click_events",
        "AWS_REGION": "us-west-2"
      }
    }
  ]
}
```

## DynamoDB 表结构

```
Table: click_events
├── fingerprint (PK) - String
├── click_time (SK)  - Number
├── user_id          - String
├── click_id         - String
├── click_id_name    - String
└── utm_json         - Map
```

## 数据流

```
raw_events
    │
    ▼
┌─────────────────────────┐
│  View: ddb_write UDF    │ ──► DynamoDB (同步写入)
└─────────────────────────┘
    │
    ▼
┌─────────────────────────┐
│    STATEMENT SET        │
│  ├─► Kafka sink         │
│  └─► S3 Iceberg sink    │
└─────────────────────────┘
```

## 注意事项

1. **写入是同步的** - UDF 会阻塞直到 DynamoDB 写入完成
2. **错误处理** - 写入失败会记录 WARN 日志，但不会中断数据流
3. **性能** - 每条记录一次 PutItem 调用，高吞吐场景建议评估
4. **幂等性** - 依赖 DynamoDB 的 PutItem 覆盖语义

## 模块结构

```
libs/flink-udf/
├── pom.xml
└── src/main/java/com/sandwichlab/flink/udf/
    ├── DdbWriteUdf.java           # UDF 入口
    ├── config/
    │   └── OperatorConfig.java    # 配置加载
    └── operator/
        ├── Operator.java          # 接口
        └── DdbOperator.java       # DynamoDB 实现
```

## 扩展

实现 `Operator` 接口可添加其他存储支持：

```java
public interface Operator extends Serializable, AutoCloseable {
    void open(OperatorConfig config);
    boolean execute(Map<String, Object> keys, Map<String, Object> values);
    default void flush() {}
    String getType();
}
```
