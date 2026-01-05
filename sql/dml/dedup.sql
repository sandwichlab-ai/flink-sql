-- ============================================================
-- DML: 事件去重
-- 基于 event_id 在 5 分钟窗口内去重
-- ============================================================

-- 方案 1: 使用 ROW_NUMBER 去重
-- 适用于需要保留最新事件的场景

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


-- 方案 2: 使用 FIRST_VALUE 聚合去重 (注释保留作为备选)
-- 适用于需要保留最早事件的场景

-- INSERT INTO processed_events
-- SELECT
--     event_id,
--     FIRST_VALUE(user_id) AS user_id,
--     FIRST_VALUE(device_id) AS device_id,
--     FIRST_VALUE(event_type) AS event_type,
--     FIRST_VALUE(event_name) AS event_name,
--     FIRST_VALUE(properties) AS properties,
--     FIRST_VALUE(app_version) AS app_version,
--     FIRST_VALUE(platform) AS platform,
--     FIRST_VALUE(event_time) AS event_time,
--     FIRST_VALUE(server_time) AS server_time,
--     CURRENT_TIMESTAMP AS processed_time,
--     'deduplicated' AS processing_status
-- FROM raw_events
-- GROUP BY
--     event_id,
--     TUMBLE(event_time, INTERVAL '5' MINUTE);
