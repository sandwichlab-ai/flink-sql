-- ============================================================
-- Sink Table: processed_events
-- 处理后的事件写入 MSK Kafka
-- 分区键: anonymous_id（确保同一用户的事件在同一分区，保证顺序）
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

    -- ========== 时间戳（BIGINT 存储 Unix 秒级时间戳）==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,            -- GTM Server Preview 调试参数

    -- ========== 处理元数据 ==========
    processing_status STRING,    -- success, filtered, deduplicated

    -- ========== 主键 (用于 Kafka 分区) ==========
    -- 使用 anonymous_id 作为主键，确保同一用户的事件落在同一分区
    PRIMARY KEY (anonymous_id) NOT ENFORCED
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
