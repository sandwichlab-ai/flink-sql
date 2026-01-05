-- ============================================================
-- Source Table: raw_events
-- 从 MSK Kafka 读取原始事件数据
-- ============================================================

CREATE TABLE raw_events (
    -- 事件标识
    event_id STRING,

    -- 用户信息
    user_id STRING,
    device_id STRING,

    -- 事件信息
    event_type STRING,
    event_name STRING,

    -- 事件属性 (JSON 格式)
    properties MAP<STRING, STRING>,

    -- 上下文信息
    app_version STRING,
    platform STRING,         -- ios, android, web

    -- 时间戳
    event_time TIMESTAMP(3),
    server_time TIMESTAMP(3),

    -- Kafka 元数据
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL,

    -- Watermark 定义 (允许 5 秒乱序)
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${INPUT_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-event-processor',

    -- MSK IAM 认证
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',

    -- 消费配置
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
