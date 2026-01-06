# Iceberg Upsert 与 Checkpoint 机制

## 概述

本项目使用 Apache Iceberg 作为 S3 数据湖的表格式，支持流式写入和 upsert 去重。

## 架构

```
Kafka (MSK) → Flink SQL Deduplicate → Iceberg (S3 + Glue Catalog) → Athena
```

## Iceberg Upsert 原理

### 配置

```sql
CREATE TABLE iceberg_catalog.raw_events.events_s3 (
    ...
    PRIMARY KEY (event_id, dt, hr) NOT ENFORCED
) WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
)
```

### 写入机制

当配置 `write.upsert.enabled = true` 时，每次 Checkpoint 提交会写入两种文件：

| 文件类型 | 说明 |
|---------|------|
| Data Files | 包含新/更新的记录 |
| Delete Files | 标记同 Primary Key 的旧记录为删除 (Equality Delete) |

### Equality Delete 示例

```
Snapshot 1:
  data-001.parquet: {event_id=abc, value=100}

Snapshot 2 (同 event_id 重复写入):
  delete-001.parquet: {event_id=abc}  ← 标记旧记录删除
  data-002.parquet: {event_id=abc, value=200}

查询时: 合并所有 data files，应用 delete files 过滤 = 最新状态
```

## Checkpoint 依赖

Iceberg 提交与 Flink Checkpoint 绑定，实现 Exactly-Once 语义。

### 提交流程

```
1. 处理阶段 (Checkpoint 之间)
   IcebergStreamWriter → 写入临时 Parquet 文件到 S3 (未提交)

2. Checkpoint 触发
   IcebergFilesCommitter.snapshotState() → 保存待提交文件列表

3. Checkpoint 完成
   IcebergFilesCommitter.notifyCheckpointComplete() → 创建 Iceberg Snapshot
```

### 配置参数

| 参数 | 当前值 | 影响 |
|-----|-------|------|
| CheckpointInterval | 60s | 数据可见延迟、S3 文件数量 |
| MinPauseBetweenCheckpoints | 5s | Checkpoint 最小间隔 |

## Job 结构变更与 Checkpoint 兼容性

### 兼容性规则

| 变更类型 | 能否恢复 | 解决方案 |
|---------|---------|---------|
| 修改算子逻辑（不改结构） | ✅ | 正常恢复 |
| 增加/删除算子 | ⚠️ | AllowNonRestoredState=true |
| 修改算子 UID | ❌ | SKIP_RESTORE_FROM_SNAPSHOT |
| 修改 Key 结构 | ❌ | SKIP_RESTORE_FROM_SNAPSHOT |
| 修改并行度 | ✅ | State 自动 rescale |

### Checkpoint 不兼容时的处理

```bash
# 跳过 Checkpoint 恢复
aws kinesisanalyticsv2 start-application \
  --application-name event-processor-dev \
  --run-configuration '{
    "ApplicationRestoreConfiguration": {
      "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
    }
  }'
```

### 数据一致性保证

即使 Checkpoint 不可用，Iceberg Upsert 也能保证数据正确性：

```
Job 结构变更 → Checkpoint 不兼容
       ↓
SKIP_RESTORE_FROM_SNAPSHOT
       ↓
Kafka 重新消费 (offset 重置)
       ↓
重复事件到达
       ↓
Iceberg Upsert: 同 PK 覆盖写入 → 数据最终一致
```

## 去重策略

本项目采用双层去重策略：

| 层级 | 组件 | 作用 |
|-----|------|------|
| 实时去重 | Flink SQL Deduplicate | 窗口内去重，减少写入量 |
| 持久化去重 | Iceberg Upsert | 跨 Checkpoint 去重，兜底保证 |

## 跨账户配置

| 资源 | 账户 | 说明 |
|-----|------|------|
| Flink Application | Dev-US | 运行 Flink Job |
| MSK Serverless | Dev-US | Kafka 消息队列 |
| S3 Landing Zone | Data Platform | Iceberg 数据存储 |
| Glue Catalog | Data Platform | Iceberg 元数据 |

### Iceberg Catalog 配置

```sql
CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'warehouse' = 's3://dataware-landing-zone-dev/iceberg/',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'glue.id' = '<data-platform-account-id>'  -- 跨账户 Glue Catalog
)
```

## 依赖版本

| 组件 | 版本 | 说明 |
|-----|------|------|
| Flink | 1.20 | AWS Managed Flink |
| Iceberg | 1.7.1 | 支持 Flink 1.20 |
| iceberg-aws-bundle | 1.7.1 | 包含 Glue + S3FileIO |

> **注意**: 不要对 AWS SDK 进行 shade relocation，会导致 S3FileIO lambda 序列化失败。
