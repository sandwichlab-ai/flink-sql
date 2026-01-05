-- ============================================================
-- Job: event_processor
-- 主 Job 文件 - 组合 DDL 和 DML
--
-- 注意: 部署时会自动合并 ddl/*.sql 和 dml/*.sql
-- 此文件仅作为完整示例参考
-- ============================================================

-- ====================
-- 1. 配置设置
-- ====================

-- 设置 checkpoint 间隔 (毫秒)
SET 'execution.checkpointing.interval' = '60000';

-- 设置 checkpoint 模式
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 设置状态后端
SET 'state.backend' = 'rocksdb';

-- 设置并行度
SET 'parallelism.default' = '${PARALLELISM}';


-- ====================
-- 2. Source 表定义
-- ====================

CREATE TABLE raw_events (
    event_id STRING,
    user_id STRING,
    device_id STRING,
    event_type STRING,
    event_name STRING,
    properties MAP<STRING, STRING>,
    app_version STRING,
    platform STRING,
    event_time TIMESTAMP(3),
    server_time TIMESTAMP(3),
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${INPUT_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-event-processor',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);


-- ====================
-- 3. Sink 表定义
-- ====================

CREATE TABLE processed_events (
    event_id STRING,
    user_id STRING,
    device_id STRING,
    event_type STRING,
    event_name STRING,
    properties MAP<STRING, STRING>,
    app_version STRING,
    platform STRING,
    event_time TIMESTAMP(3),
    server_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    processing_status STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '${OUTPUT_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
    'key.format' = 'json',
    'value.format' = 'json'
);


-- ====================
-- 4. 业务逻辑 - 去重处理
-- ====================

INSERT INTO processed_events
SELECT
    event_id,
    user_id,
    device_id,
    event_type,
    event_name,
    properties,
    app_version,
    platform,
    event_time,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    'success' AS processing_status
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time DESC
        ) AS row_num
    FROM raw_events
)
WHERE row_num = 1;
