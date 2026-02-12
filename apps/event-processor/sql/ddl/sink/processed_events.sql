-- ============================================================
-- Sink Table: processed_events
-- 处理后的事件写入 MSK Kafka (upsert 模式)
-- 主键: (event_id, anonymous_id) 用于去重
-- ============================================================

CREATE TABLE processed_events (
    -- ========== 事件标识 ==========
    event_id STRING,
    event_type STRING,

    -- ========== 用户标识 ==========
    user_id STRING,
    anonymous_id STRING,

    -- ========== 嵌套数据 ==========
    utm_params MAP<STRING, STRING>,
    clid_params MAP<STRING, STRING>,
    page_context MAP<STRING, STRING>,
    user_data MAP<STRING, STRING>,
    event_properties MAP<STRING, STRING>,
    tracking_cookies MAP<STRING, STRING>,
    retrieval_source MAP<STRING, STRING>,
    ext_props MAP<STRING, STRING>,       -- 扩展属性

    -- ========== 设备指纹 ==========
    fingerprint STRING,                 -- 设备指纹

    -- ========== 时间戳（BIGINT 存储 Unix 秒级时间戳）==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,            -- GTM Server Preview 调试参数

    -- ========== 处理元数据 ==========
    processing_status STRING,     -- success, filtered, deduplicated

    -- ========== 主键（用于 upsert 去重）==========
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

    -- 连接保活，防止 MSK Serverless 断开空闲连接
    'properties.connections.max.idle.ms' = '60000',
    'properties.metadata.max.age.ms' = '60000',
    'properties.reconnect.backoff.ms' = '1000',
    'properties.reconnect.backoff.max.ms' = '10000',

    -- Key 和 Value 格式
    'key.format' = 'json',
    'value.format' = 'json'
);
