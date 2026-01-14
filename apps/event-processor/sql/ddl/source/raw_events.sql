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
    
    -- ========== 营销归因 (顶层字段，便于查询) ==========
    utm_source STRING,                  -- 流量来源（google, facebook, email 等）
    utm_campaign STRING,                -- 营销活动名称
    gclid STRING,                       -- Google Ads Click ID
    fbclid STRING,                      -- Facebook Click ID
    
    -- ========== 嵌套数据 (MAP 类型) ==========
    utm_params MAP<STRING, STRING>,     -- 完整 UTM 参数（utm_medium, utm_content, utm_term 等）
    clid_params MAP<STRING, STRING>,    -- 完整 CLID 参数（tracking_id, ttclid 等）
    page_context MAP<STRING, STRING>,   -- 页面上下文（url, title, referrer, path, search）
    user_data MAP<STRING, STRING>,      -- 用户数据（email, phone 等）
    event_properties MAP<STRING, STRING>, -- 事件属性（order_id, value, currency 等）
    tracking_cookies MAP<STRING, STRING>, -- 广告追踪 Cookies（_fbp, _fbc, _ga, _gid, _gcl_aw 等）
    
    -- ========== 时间字段 ==========
    event_time TIMESTAMP(3),            -- 事件发生时间（客户端时间戳转换）
    sent_at TIMESTAMP(3),               -- 事件发送时间（客户端）
    server_time TIMESTAMP(3),           -- 服务器接收时间
    
    -- ========== Kafka 元数据 ==========
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
    
    -- ========== Watermark (允许 5 秒乱序) ==========
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
