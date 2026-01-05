-- ============================================================
-- DML: 事件去重
-- Kafka: 去重后的数据 (upsert)
-- S3: 原始数据 (append-only)
-- ============================================================

-- 使用 STATEMENT SET 同时写入多个 Sink
EXECUTE STATEMENT SET
BEGIN

-- Sink 1: 写入 Kafka (去重后)
INSERT INTO processed_events
SELECT
    event_id,
    user_id,
    device_id,
    event_type,
    event_name,
    properties,
    app_version,
    platform,
    event_time,
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

-- Sink 2: 写入 S3 (原始数据，append-only)
INSERT INTO events_s3
SELECT
    event_id,
    user_id,
    device_id,
    event_type,
    event_name,
    properties,
    app_version,
    platform,
    event_time,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    'raw' AS processing_status,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'HH') AS hr
FROM raw_events;

END;
