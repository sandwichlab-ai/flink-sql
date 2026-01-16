-- ============================================================
-- DML: 事件去重
-- Kafka: 去重后的数据 (upsert)
-- S3 Iceberg: 原始数据 (upsert 去重)
-- ============================================================

-- 使用 STATEMENT SET 同时写入多个 Sink
EXECUTE STATEMENT SET
BEGIN

-- Sink 1: 写入 Kafka (去重后)
INSERT INTO processed_events
SELECT
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_source,
    utm_campaign,
    gclid,
    fbclid,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    event_time,
    sent_at,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    'deduplicated' AS processing_status
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time DESC
        ) AS row_num
    FROM raw_events
)
WHERE row_num = 1;

-- Sink 2: 写入 S3 Iceberg (原始数据，upsert 去重)
INSERT INTO iceberg_catalog.raw_events.events_s3_v4
SELECT
    event_id,
    event_type,
    user_id,
    anonymous_id,
    utm_source,
    utm_campaign,
    gclid,
    fbclid,
    utm_params,
    clid_params,
    page_context,
    user_data,
    event_properties,
    tracking_cookies,
    event_time,
    sent_at,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    'raw' AS processing_status,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'HH') AS hr
FROM raw_events;

END;
