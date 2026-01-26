-- ============================================================
-- Sink Table: attributed_events (Iceberg)
-- 归因事件归档到 S3，支持 Upsert 去重
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.raw_events.attributed_events (
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

    -- ========== 时间字段 ==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,

    -- ========== 归因字段 (新增) ==========
    source STRING,                      -- 归因来源: google, facebook 等
    click_id STRING,                    -- 点击 ID
    click_time BIGINT,                  -- 点击时间
    click_id_name STRING,               -- 点击 ID 类型

    -- ========== 处理元数据 ==========
    processing_status STRING,

    -- ========== 分区字段 ==========
    dt STRING,
    hr STRING,

    -- ========== 主键 ==========
    PRIMARY KEY (event_id, dt, hr) NOT ENFORCED
) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);
