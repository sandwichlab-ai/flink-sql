-- ============================================================
-- DML: 事件去重与推送
-- 处理逻辑：
-- 1. event_type = "page" + 有 clid + clid 新的 → 写 DDB + 推送 Kafka (Source 1)
-- 2. event_type = "page" + 有 clid + clid 不是新的 → 不推送
-- 3. event_type = "page" + 无 clid → 不推送
-- 4. event_type != "page" + 有 clid + clid 新的 → 写 DDB + 推送 Kafka (Source 1)
-- 5. event_type != "page" + 有 clid + clid 不是新的 → 只推送 Kafka (Source 1, 不写 DDB)
-- 6. event_type != "page" + 无 clid → 直接推送 Kafka (Source 2)
--
-- 数据来源设计（确保有 clid 的事件先写 DDB 再推 Kafka）：
-- Source 1: 通过 View 处理有 clid 的事件，row_num=1 写 DDB，row_num=2 不写
-- Source 2: 直接从 raw_events 读取无 clid 的非 page 事件
-- 注意：需要先执行 click_events_with_clid_view.sql 创建 View
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
        ddb_written,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time ASC
        ) AS event_row_num
    FROM (
        -- 来源 1: 有 clid 的事件（通过 View，确保 DDB 先写入）
        -- row_num = 1: 新 clid，任意 event_type，已调用 ddb_write UDF
        -- row_num = 2 且非 page: 旧 clid，不写 DDB 但需推 Kafka
        SELECT
            event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
            page_context, user_data, event_properties, tracking_cookies,
            retrieval_source, event_time, report_time, server_time, gtm_preview_code,
            ddb_written
        FROM click_events_with_clid
        WHERE row_num = 1
           OR (row_num = 2 AND event_type <> 'page')

        UNION ALL

        -- 来源 2: 无 clid 的非 page 事件（直接从 raw_events）
        -- 这些事件不会出现在 View 中，直接推 Kafka
        SELECT
            event_id, event_type, user_id, anonymous_id, utm_params, clid_params,
            page_context, user_data, event_properties, tracking_cookies,
            retrieval_source, event_time, report_time, server_time, gtm_preview_code,
            CAST(NULL AS STRING) AS ddb_written
        FROM raw_events
        WHERE event_type <> 'page'
          AND (clid_params IS NULL OR CARDINALITY(clid_params) = 0)
    ) AS all_events
)
WHERE event_row_num = 1;

-- Sink 2: 写入 S3 Iceberg (原始数据，按 event_id 去重)
-- 注意：直接从 raw_events 读取，不走 view，避免重复触发 UDF
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

END;
