-- ============================================================
-- DML: 创建中间 View，通过 UDF 写入 DynamoDB
-- 该 View 确保 DDB 先于下游 Sink 写入
-- 只处理有 clid_params 的事件
-- ============================================================

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
    gtm_preview_code,
    -- 调用 UDF 写入 DynamoDB（透传 fingerprint）
    ddb_write(
        anonymous_id,
        CAST(clid_params[k || '_timestamp'] AS BIGINT),
        user_id,
        v,
        k,
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
WHERE row_num = 1
