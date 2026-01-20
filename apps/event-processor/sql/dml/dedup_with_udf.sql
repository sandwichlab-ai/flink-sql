-- ============================================================
-- DML: 事件去重
-- 1. 有 clid_params 的事件：从 click_events_with_ddb View 读取（已通过 UDF 写入 DDB）
-- 2. 无 clid_params 的事件：直接从 raw_events 读取
-- 两种事件合并后写入 Kafka 和 S3
-- 注意：需要先执行 click_events_with_ddb_view.sql 创建 View
-- ============================================================

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
    FROM (
        -- 有 clid_params 的事件（已通过 UDF 写入 DDB）
        SELECT
            event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
            page_context, user_data, event_properties, tracking_cookies,
            retrieval_source, event_time, report_time, server_time, gtm_preview_code
        FROM click_events_with_ddb

        UNION ALL

        -- 无 clid_params 的事件（直接透传）
        SELECT
            event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
            page_context, user_data, event_properties, tracking_cookies,
            retrieval_source, event_time, report_time, server_time, gtm_preview_code
        FROM raw_events
        WHERE CARDINALITY(clid_params) = 0 OR clid_params IS NULL
    ) AS all_events
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
FROM (
    -- 有 clid_params 的事件（已通过 UDF 写入 DDB）
    SELECT
        event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
        page_context, user_data, event_properties, tracking_cookies,
        retrieval_source, event_time, report_time, server_time, gtm_preview_code
    FROM click_events_with_ddb

    UNION ALL

    -- 无 clid_params 的事件（直接透传）
    SELECT
        event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
        page_context, user_data, event_properties, tracking_cookies,
        retrieval_source, event_time, report_time, server_time, gtm_preview_code
    FROM raw_events
    WHERE CARDINALITY(clid_params) = 0 OR clid_params IS NULL
) AS all_events;

END;
