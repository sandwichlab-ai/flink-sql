-- ============================================================
-- Source Table: exposure_events
-- 从 MSK Kafka 读取曝光事件数据
-- ============================================================

CREATE TABLE exposure_events (
    -- ========== 事件标识 ==========
    event_id STRING,                    -- 事件唯一ID（服务端生成）

    -- ========== 曝光核心字段 ==========
    page_url STRING,                    -- 页面 URL
    product_id STRING,                  -- 商品 ID
    store_id STRING,                    -- 店铺 ID
    scm STRING,                         -- 来源追踪参数

    -- ========== 曝光状态 ==========
    visible STRING,                     -- impression=有效曝光(1s), impression_end=曝光结束
    visible_duration_ms BIGINT,         -- 曝光时长 (ms)
    visible_ratio DOUBLE,               -- 可见面积比例

    -- ========== 用户标识 ==========
    session_id STRING,                  -- 会话 ID
    anonymous_id STRING,                -- 匿名用户 ID

    -- ========== 扩展数据 ==========
    ext_json STRING,                    -- 扩展属性（原始 JSON 字符串）

    -- ========== 时间字段 ==========
    event_time BIGINT,                  -- 客户端事件时间（Unix 秒级时间戳）
    report_time BIGINT,                 -- 客户端发送时间（Unix 秒级时间戳）
    server_time BIGINT,                 -- 服务器接收时间（Unix 秒级时间戳）

    -- ========== Event Time + Watermark ==========
    event_timestamp AS TO_TIMESTAMP_LTZ(event_time, 0),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND,

    -- ========== Kafka 元数据 ==========
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = '${EXPOSURE_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-exposure-processor',

    -- MSK IAM 认证
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',

    -- 消费配置
    'scan.startup.mode' = 'latest-offset',
    'properties.max.poll.records' = '${MAX_POLL_RECORDS}',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
