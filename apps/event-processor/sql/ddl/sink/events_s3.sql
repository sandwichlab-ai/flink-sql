-- ============================================================
-- Sink Table: events_s3 (Hudi)
-- 归档事件到 S3 Landing Zone，支持 Upsert 去重
-- ============================================================

CREATE TABLE events_s3 (
    -- 事件标识
    event_id STRING,

    -- 用户信息
    user_id STRING,
    device_id STRING,

    -- 事件信息
    event_type STRING,
    event_name STRING,

    -- 处理后的属性
    properties MAP<STRING, STRING>,

    -- 上下文信息
    app_version STRING,
    platform STRING,

    -- 时间戳
    event_time TIMESTAMP(3),
    server_time TIMESTAMP(3),
    processed_time TIMESTAMP(3),

    -- 处理元数据
    processing_status STRING,

    -- 分区字段
    dt STRING,
    hr STRING,

    -- 主键 (用于 Upsert 去重)
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (dt, hr) WITH (
    'connector' = 'hudi',
    'path' = 's3://dataware-landing-zone-dev/raw_signal/',

    -- Hudi 表类型: MERGE_ON_READ (写入快，后台合并)
    'table.type' = 'MERGE_ON_READ',

    -- 写入操作: upsert (按 event_id 去重)
    'write.operation' = 'upsert',

    -- 记录键和分区路径
    'hoodie.datasource.write.recordkey.field' = 'event_id',
    'hoodie.datasource.write.partitionpath.field' = 'dt,hr',

    -- AWS Managed Flink 必须禁用 timeline server
    'hoodie.embed.timeline.server' = 'false',

    -- Compaction 配置
    'compaction.async.enabled' = 'true',
    'compaction.schedule.enabled' = 'true',
    'compaction.trigger.strategy' = 'num_commits',
    'compaction.delta_commits' = '5'
);
