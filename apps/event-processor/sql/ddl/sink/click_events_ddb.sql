-- ============================================================
-- Sink Table: click_events_ddb
-- 将点击参数写入 DynamoDB
-- ============================================================

CREATE TABLE click_events_ddb (
    fingerprint STRING,                 -- Partition Key (anonymous_id)
    click_time BIGINT,                  -- Sort Key (秒级时间戳，如 1737000000)

    user_id STRING,
    click_id STRING,                    -- 点击参数值
    click_id_name STRING,               -- 点击参数名称 (如 'gclid', 'fbclid')
    utm_json MAP<STRING, STRING>,       -- UTM 参数 (Original Map)

    PRIMARY KEY (fingerprint, click_time) NOT ENFORCED
) WITH (
    'connector' = 'dynamodb',
    'table-name' = '${DDB_TABLE_NAME}',
    'aws.region' = '${AWS_REGION}',
    'write.request-base.enabled' = 'true'
);
