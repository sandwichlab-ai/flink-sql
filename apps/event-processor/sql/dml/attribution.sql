-- ============================================================
-- DML: 事件归因
-- 将转化事件归因到来源渠道
-- ============================================================

-- 需要先创建用户会话表和归因结果表

-- 会话表定义 (用于存储用户首次来源)
-- CREATE TABLE user_sessions (
--     user_id STRING,
--     session_id STRING,
--     first_touch_channel STRING,
--     first_touch_campaign STRING,
--     session_start TIMESTAMP(3),
--     PRIMARY KEY (user_id, session_id) NOT ENFORCED
-- ) WITH (...);

-- 归因结果表
-- CREATE TABLE attributed_conversions (
--     conversion_id STRING,
--     user_id STRING,
--     conversion_type STRING,
--     conversion_time TIMESTAMP(3),
--     attributed_channel STRING,
--     attributed_campaign STRING,
--     attribution_model STRING,  -- first_touch, last_touch, linear
--     PRIMARY KEY (conversion_id) NOT ENFORCED
-- ) WITH (...);


-- 首次触达归因 (First Touch Attribution)
-- INSERT INTO attributed_conversions
-- SELECT
--     e.event_id AS conversion_id,
--     e.user_id,
--     e.event_type AS conversion_type,
--     e.event_time AS conversion_time,
--     s.first_touch_channel AS attributed_channel,
--     s.first_touch_campaign AS attributed_campaign,
--     'first_touch' AS attribution_model
-- FROM raw_events e
-- JOIN user_sessions s
--     ON e.user_id = s.user_id
-- WHERE e.event_type = 'conversion';


-- 最后触达归因 (Last Touch Attribution)
-- INSERT INTO attributed_conversions
-- SELECT
--     e.event_id AS conversion_id,
--     e.user_id,
--     e.event_type AS conversion_type,
--     e.event_time AS conversion_time,
--     e.properties['utm_source'] AS attributed_channel,
--     e.properties['utm_campaign'] AS attributed_campaign,
--     'last_touch' AS attribution_model
-- FROM raw_events e
-- WHERE e.event_type = 'conversion';
