-- ============================================================
-- Sink Table: processed_events
-- 处理后的事件写入 MSK Kafka
-- ============================================================

CREATE TABLE processed_events (
    -- 事件标识
    event_id STRING,

    -- 用户信息
    user_id STRING,
    device_id STRING,

    -- 事件信息
    event_type STRING,
    event_name STRING,

    -- 处理后的属性
    properties MAP<STRING, STRING>,

    -- 上下文信息
    app_version STRING,
    platform STRING,

    -- 时间戳
    event_time TIMESTAMP(3),
    server_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),

    -- 处理元数据
    processing_status STRING,    -- success, filtered, deduplicated

    -- 主键 (用于 Upsert 语义)
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = '${OUTPUT_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',

    -- MSK IAM 认证
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',

    -- 格式配置
    'key.format' = 'json',
    'value.format' = 'json'
);

