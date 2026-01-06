-- ============================================================
-- Sink Table: events_s3 (Iceberg)
-- 归档事件到 S3 Landing Zone，支持 Upsert 去重
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.raw_events.events_s3 (
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
    processing_status STRING,

    -- 分区字段
    dt STRING,
    hr STRING,

    -- 主键 (用于 Upsert 去重, 必须包含分区字段)
    PRIMARY KEY (event_id, dt, hr) NOT ENFORCED
) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
)
