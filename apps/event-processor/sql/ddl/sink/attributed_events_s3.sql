-- ============================================================
-- Sink Table: attributed_events (Iceberg)
-- 归因事件归档到 S3，支持 Upsert 去重
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.${ICEBERG_DATABASE}.attributed_events_v3 (
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
    fingerprint STRING,                  -- 设备指纹

    -- ========== 时间字段 ==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== GTM 调试参数 ==========
    gtm_preview_code STRING,

    -- ========== 归因字段 ==========
    source STRING,                      -- 归因来源: google, facebook 等
    click_id STRING,                    -- 点击 ID
    click_time BIGINT,                  -- 点击时间
    click_id_name STRING,               -- 点击 ID 类型
    is_attributed INT,                  -- 是否归因: 0=未归因, 1=已归因
    attribution_model STRING,           -- 归因策略/模型: last_click
    attribution_window STRING,          -- 归因窗口: 7d

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
