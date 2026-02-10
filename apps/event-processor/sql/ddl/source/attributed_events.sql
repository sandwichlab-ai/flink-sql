-- ============================================================
-- Source Table: attributed_events
-- 从 Data Gather 读取归因后的事件数据
-- ============================================================

CREATE TABLE attributed_events (
    -- ========== 事件标识 ==========
    event_id STRING,
    event_type STRING,

    -- ========== 用户标识 ==========
    user_id STRING,
    anonymous_id STRING,

    -- ========== 嵌套数据 (MAP 类型) ==========
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

    -- ========== 时间字段 ==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,

    -- ========== 归因字段 ==========
    source STRING,                      -- 归因来源: google, facebook 等
    click_id STRING,                    -- 点击 ID
    click_time BIGINT,                  -- 点击时间 (Unix 秒级时间戳)
    click_id_name STRING,               -- 点击 ID 类型: gclid, fbclid 等
    is_attributed INT,                  -- 是否归因: 0=未归因, 1=已归因
    attribution_model STRING,           -- 归因策略/模型: last_click
    attribution_window STRING,          -- 归因窗口: 7d

    -- ========== Event Time + Watermark ==========
    event_timestamp AS TO_TIMESTAMP_LTZ(event_time, 0),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND,

    -- ========== Kafka 元数据 ==========
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = '${ATTRIBUTED_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-attribution-processor',

    -- MSK IAM 认证
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',

    -- 消费配置
    'scan.startup.mode' = 'latest-offset',
    'properties.max.poll.records' = '${MAX_POLL_RECORDS}',  -- 每次 poll 最大记录数（控制消费速度）
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'json.map-null-key.mode' = 'LITERAL',
    'json.map-null-key.literal' = 'null'
);
