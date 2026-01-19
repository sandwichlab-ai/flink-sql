-- ============================================================
-- DML: 事件去重
-- Kafka: 去重后的数据 (upsert)
-- S3 Iceberg: 原始数据 (upsert 去重)
-- ============================================================

-- 使用 STATEMENT SET 同时写入多个 Sink
EXECUTE STATEMENT SET
BEGIN

-- Sink 1: 写入 Kafka (去重后)
-- 注意：显式指定列名以确保列顺序正确
INSERT INTO processed_events (
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    retrieval_source,
    event_time,
    report_time,
    server_time,
    processed_time,
    gtm_preview_code,
    processing_status
)
SELECT
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    retrieval_source,
    event_time,
    report_time,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    gtm_preview_code,
    'deduplicated' AS processing_status
FROM (
    SELECT
        event_id,
        event_type,
        user_id,
        anonymous_id,
        utm_params,
        clid_params,
        page_context,
        user_data,
        event_properties,
        tracking_cookies,
        retrieval_source,
        event_time,
        report_time,
        server_time,
        gtm_preview_code,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time DESC
        ) AS row_num
    FROM raw_events
)
WHERE row_num = 1;

-- Sink 2: 写入 S3 Iceberg (原始数据，upsert 去重)
-- 注意：显式指定列名以避免 schema evolution 导致的列顺序问题
INSERT INTO iceberg_catalog.raw_events.events_s3_v5 (
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    retrieval_source,
    event_time,
    report_time,
    server_time,
    processed_time,
    gtm_preview_code,
    processing_status,
    dt,
    hr
)
SELECT
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    retrieval_source,
    event_time,
    report_time,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    gtm_preview_code,
    'raw' AS processing_status,
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'yyyy-MM-dd') AS dt,  -- 北京时间 UTC+8
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'HH') AS hr           -- 北京时间 UTC+8
FROM raw_events;

-- Sink 3: 写入 DynamoDB (点击事件去重)
-- 直接按点击维度 (fingerprint, user_id, click_id, click_time) 去重
INSERT INTO click_events_ddb
SELECT
    fingerprint,
    click_time,
    user_id,
    click_id,
    click_id_name,
    utm_json
FROM (
    SELECT
        anonymous_id AS fingerprint,
        user_id,
        t.v AS click_id,
        t.k AS click_id_name,
        CAST(clid_params[t.k || '_timestamp'] AS BIGINT) AS click_time,
        utm_params AS utm_json,
        ROW_NUMBER() OVER (
            PARTITION BY anonymous_id, user_id, t.v, clid_params[t.k || '_timestamp']
            ORDER BY event_time DESC
        ) AS row_num
    FROM raw_events
    CROSS JOIN UNNEST(raw_events.clid_params) AS t(k, v)
    WHERE t.k NOT LIKE '%_timestamp' AND t.v IS NOT NULL AND t.v <> ''
)
WHERE row_num = 1;

END;
