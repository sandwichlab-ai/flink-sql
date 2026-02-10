-- ============================================================
-- Source Table: raw_events
-- 从 MSK Kafka 读取原始事件数据
-- ============================================================

CREATE TABLE raw_events (
    -- ========== 事件标识 ==========
    event_id STRING,                    -- 事件唯一ID（用于去重）
    event_type STRING,                  -- 事件类型（page, purchase, add_to_cart 等）
    
    -- ========== 用户标识 ==========
    user_id STRING,                     -- 登录用户ID
    anonymous_id STRING,                -- 匿名用户ID（未登录用户）
    
    -- ========== 嵌套数据 (MAP 类型) ==========
    utm_params MAP<STRING, STRING>,     -- 完整 UTM 参数（utm_medium, utm_content, utm_term 等）
    clid_params MAP<STRING, STRING>,    -- 完整 CLID 参数（tracking_id, ttclid 等）
    page_context MAP<STRING, STRING>,   -- 页面上下文（url, title, referrer, path, search）
    user_data MAP<STRING, STRING>,      -- 用户数据（email, phone 等）
    event_properties MAP<STRING, STRING>, -- 事件属性（order_id, value, currency 等）
    tracking_cookies MAP<STRING, STRING>, -- 广告追踪 Cookies（_fbp, _fbc, _ga, _gid, _gcl_aw 等）
    retrieval_source MAP<STRING, STRING>, -- SDK 数据源信息（sdk_key, sdk_version 等）
    ext_props MAP<STRING, STRING>,       -- 扩展属性

    -- ========== 设备指纹 ==========
    fingerprint STRING,                 -- 设备指纹

    -- ========== 时间字段（BIGINT 存储 Unix 秒级时间戳）==========
    event_time BIGINT,                  -- 事件发生时间（Unix 秒级时间戳）
    report_time BIGINT,                 -- 事件上报时间（Unix 秒级时间戳）
    server_time BIGINT,                 -- 服务器接收时间（Unix 秒级时间戳）

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,            -- GTM Server Preview 调试参数

    -- ========== Event Time + Watermark ==========
    event_timestamp AS TO_TIMESTAMP_LTZ(event_time, 0),  -- 将秒级时间戳转换为 TIMESTAMP
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND,  -- 允许 5 秒乱序

    -- ========== Kafka 元数据 ==========
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL
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
    'properties.max.poll.records' = '${MAX_POLL_RECORDS}',  -- 每次 poll 最大记录数（控制消费速度）
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true',
    'json.map-null-key.mode' = 'LITERAL',
    'json.map-null-key.literal' = 'null'
);
