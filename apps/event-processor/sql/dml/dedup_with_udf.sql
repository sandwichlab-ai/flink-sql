-- ============================================================
-- DML: 事件去重与推送
-- 处理逻辑：
-- 1. 从 raw_events 读取所有事件
-- 2. 按 event_id 去重（保留最早的一条）
-- 3. 推送到 processed_events (Kafka) 和 S3 (Iceberg)
--
-- 注意：DDB 写入逻辑已移至 data_gather worker 处理
-- ============================================================

EXECUTE STATEMENT SET
BEGIN

-- Sink 1: 写入 Kafka (去重后)
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
            ORDER BY event_time ASC
        ) AS row_num
    FROM raw_events
) AS deduped
WHERE row_num = 1;

-- Sink 2: 写入 S3 Iceberg (原始数据，按 event_id 去重)
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
            ORDER BY event_time ASC
        ) AS row_num
    FROM raw_events
) AS deduped
WHERE row_num = 1;

-- Sink 3: 写入归因事件到 Iceberg (从 Data Gather 回写)
INSERT INTO iceberg_catalog.raw_events.attributed_events_v3 (
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
    source,
    click_id,
    click_time,
    click_id_name,
    is_attributed,
    attribution_model,
    attribution_window,
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
    source,
    click_id,
    click_time,
    click_id_name,
    is_attributed,
    attribution_model,
    attribution_window,
    'attributed' AS processing_status,
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'yyyy-MM-dd') AS dt,  -- 北京时间 UTC+8
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'HH') AS hr           -- 北京时间 UTC+8
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
        source,
        click_id,
        click_time,
        click_id_name,
        is_attributed,
        attribution_model,
        attribution_window,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time ASC
        ) AS row_num
    FROM attributed_events
) AS deduped
WHERE row_num = 1;

END;
