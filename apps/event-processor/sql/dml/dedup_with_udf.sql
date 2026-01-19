-- ============================================================
-- DML: 事件去重（使用 UDF 保证 DDB 先写入）
-- 1. 先通过 UDF 写入 DynamoDB
-- 2. 再写入 Kafka 和 S3
-- ============================================================

-- 中间 View: 通过 UDF 写入 DynamoDB，同时透传数据
CREATE TEMPORARY VIEW click_events_with_ddb AS
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
    -- 调用 UDF 写入 DynamoDB（透传 fingerprint）
    ddb_write(
        anonymous_id,
        CAST(clid_params[t.k || '_timestamp'] AS BIGINT),
        user_id,
        t.v,
        t.k,
        utm_params
    ) AS ddb_written
FROM (
    SELECT
        raw_events.*,
        t.k,
        t.v,
        ROW_NUMBER() OVER (
            PARTITION BY anonymous_id, user_id, t.v, clid_params[t.k || '_timestamp']
            ORDER BY event_time DESC
        ) AS row_num
    FROM raw_events
    CROSS JOIN UNNEST(raw_events.clid_params) AS t(k, v)
    WHERE t.k NOT LIKE '%_timestamp' AND t.v IS NOT NULL AND t.v <> ''
) AS deduped
WHERE row_num = 1;

-- 从已写入 DDB 的 View 读取，写入 Kafka 和 S3
EXECUTE STATEMENT SET
BEGIN

-- Sink 1: 写入 Kafka (去重后)
INSERT INTO processed_events
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
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY event_time DESC
        ) AS row_num
    FROM click_events_with_ddb
)
WHERE row_num = 1;

-- Sink 2: 写入 S3 Iceberg
INSERT INTO iceberg_catalog.raw_events.events_s3_v5
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
    'raw' AS processing_status,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'HH') AS hr
FROM click_events_with_ddb;

END;
