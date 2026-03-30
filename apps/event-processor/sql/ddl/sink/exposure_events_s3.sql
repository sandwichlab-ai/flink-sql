-- ============================================================
-- Sink Table: exposure_events (Iceberg)
-- 曝光事件归档到 S3，append-only（服务端已保证唯一性）
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.${ICEBERG_DATABASE}.exposure_events_v1 (
    -- ========== 事件标识 ==========
    event_id STRING,

    -- ========== 曝光核心字段 ==========
    page_url STRING,
    product_id STRING,
    store_id STRING,
    scm STRING,

    -- ========== 曝光状态 ==========
    visible STRING,
    visible_duration_ms BIGINT,
    visible_ratio DOUBLE,

    -- ========== 用户标识 ==========
    session_id STRING,
    anonymous_id STRING,

    -- ========== 扩展数据 ==========
    ext_json STRING,

    -- ========== 时间字段 ==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== 分区字段 ==========
    dt STRING,
    hr STRING

) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2'
);
