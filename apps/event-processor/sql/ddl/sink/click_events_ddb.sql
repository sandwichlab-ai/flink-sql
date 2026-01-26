-- ============================================================
-- Sink Table: click_events_ddb
-- 将点击参数写入 DynamoDB
-- ============================================================

CREATE TABLE click_events_ddb (
    fingerprint STRING,                 -- Partition Key (anonymous_id)
    click_id_name STRING,               -- Sort Key: 点击参数名称 (如 'gclid', 'fbclid')

    click_time BIGINT,                  -- 点击时间戳
    user_id STRING,
    click_id STRING,                    -- 点击参数值
    utm_json MAP<STRING, STRING>,       -- UTM 参数 (Original Map)
    event_id STRING,                    -- 事件 ID（用于链路追踪）
    updated_at BIGINT,                  -- 记录写入/更新时间戳
    expire_at BIGINT,                   -- TTL 过期时间（click_time + 30天，秒级）

    PRIMARY KEY (fingerprint, click_id_name) NOT ENFORCED
) WITH (
    'connector' = 'dynamodb',
    'table-name' = '${DDB_TABLE_NAME}',
    'aws.region' = '${AWS_REGION}',
    'write.request-base.enabled' = 'true'
);
