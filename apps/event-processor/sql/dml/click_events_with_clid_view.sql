-- ============================================================
-- DML: 创建中间 View，处理有 clid 的事件
-- 使用 row_num <= 2 满足 Flink TopN 要求
-- row_num = 1: 新 clid → 调用 UDF 写 DDB
-- row_num = 2: 旧 clid → 不调用 UDF
-- ============================================================

CREATE TEMPORARY VIEW click_events_with_clid AS
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
    row_num,
    -- 只有 row_num = 1（新 clid）时才调用 UDF
    CASE
        WHEN row_num = 1 THEN ddb_write(
            anonymous_id,
            CAST(clid_params[k || '_timestamp'] AS BIGINT),
            user_id,
            v,
            k,
            utm_params,
            event_id
        )
        ELSE CAST(NULL AS STRING)
    END AS ddb_written
FROM (
    SELECT
        raw_events.*,
        t.k,
        t.v,
        ROW_NUMBER() OVER (
            PARTITION BY anonymous_id, t.k, t.v
            ORDER BY CAST(clid_params[t.k || '_timestamp'] AS BIGINT) DESC, event_time ASC
        ) AS row_num
    FROM raw_events
    CROSS JOIN UNNEST(raw_events.clid_params) AS t(k, v)
    WHERE t.k NOT LIKE '%_timestamp' AND t.v IS NOT NULL AND t.v <> ''
) AS deduped
WHERE row_num <= 1
