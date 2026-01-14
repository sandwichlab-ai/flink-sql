-- ============================================================
-- Sink Table: events_s3 (Iceberg)
-- 归档事件到 S3 Landing Zone，支持 Upsert 去重
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.raw_events.events_s3 (
    -- ========== 事件标识 ==========
    event_id STRING,
    event_type STRING,
    
    -- ========== 用户标识 ==========
    user_id STRING,
    anonymous_id STRING,
    
    -- ========== 营销归因（顶层字段，便于查询） ==========
    utm_source STRING,
    utm_campaign STRING,
    gclid STRING,
    fbclid STRING,
    
    -- ========== 嵌套数据 ==========
    utm_params MAP<STRING, STRING>,
    clid_params MAP<STRING, STRING>,
    page_context MAP<STRING, STRING>,
    user_data MAP<STRING, STRING>,
    event_properties MAP<STRING, STRING>,
    tracking_cookies MAP<STRING, STRING>,
    
    -- ========== 时间字段 ==========
    event_time TIMESTAMP(3),
    sent_at TIMESTAMP(3),
    server_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),
    
    -- ========== 处理元数据 ==========
    processing_status STRING,
    
    -- ========== 分区字段 ==========
    dt STRING,
    hr STRING,
    
    -- ========== 主键 (用于 Upsert 去重, 必须包含分区字段) ==========
    PRIMARY KEY (event_id, dt, hr) NOT ENFORCED
) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
)
