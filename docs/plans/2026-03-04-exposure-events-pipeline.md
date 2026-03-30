# Exposure Events Pipeline Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增 `exposure-events` Kafka topic 的监听与处理流，将曝光事件写入 Iceberg S3 表。

**Architecture:** 与 `attributed_events` 模式完全一致——新建 Kafka source DDL 和 Iceberg sink DDL，在现有 `dedup_with_udf.sql` 的 `EXECUTE STATEMENT SET` 中追加一条 INSERT，保持单 Flink Job。不做去重（服务端保证唯一性），使用 append-only Iceberg 表。

**Tech Stack:** Flink SQL 1.20, Iceberg (format-version=2, append-only), AWS MSK IAM 认证, Java 11

---

### Task 1: 创建 exposure_events Kafka Source DDL

**Files:**
- Create: `apps/event-processor/sql/ddl/source/exposure_events.sql`

**Step 1: 创建文件，内容如下**

```sql
-- ============================================================
-- Source Table: exposure_events
-- 从 MSK Kafka 读取曝光事件数据
-- ============================================================

CREATE TABLE exposure_events (
    -- ========== 事件标识 ==========
    event_id STRING,                    -- 事件唯一ID（服务端生成）

    -- ========== 曝光核心字段 ==========
    page_url STRING,                    -- 页面 URL
    product_id STRING,                  -- 商品 ID
    store_id STRING,                    -- 店铺 ID
    scm STRING,                         -- 来源追踪参数

    -- ========== 曝光状态 ==========
    visible STRING,                     -- impression=有效曝光(1s), impression_end=曝光结束
    visible_duration_ms BIGINT,         -- 曝光时长 (ms)
    visible_ratio DOUBLE,               -- 可见面积比例

    -- ========== 用户标识 ==========
    session_id STRING,                  -- 会话 ID
    anonymous_id STRING,                -- 匿名用户 ID

    -- ========== 扩展数据 ==========
    ext_json STRING,                    -- 扩展属性（原始 JSON 字符串）

    -- ========== 时间字段 ==========
    event_time BIGINT,                  -- 客户端事件时间（Unix 秒级时间戳）
    report_time BIGINT,                 -- 客户端发送时间（Unix 秒级时间戳）
    server_time BIGINT,                 -- 服务器接收时间（Unix 秒级时间戳）

    -- ========== Event Time + Watermark ==========
    event_timestamp AS TO_TIMESTAMP_LTZ(event_time, 0),
    WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND,

    -- ========== Kafka 元数据 ==========
    `partition` INT METADATA FROM 'partition' VIRTUAL,
    `offset` BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = '${EXPOSURE_TOPIC}',
    'properties.bootstrap.servers' = '${BOOTSTRAP_SERVERS}',
    'properties.group.id' = 'flink-exposure-processor',

    -- MSK IAM 认证
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism' = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',

    -- 消费配置
    'scan.startup.mode' = 'latest-offset',
    'properties.max.poll.records' = '${MAX_POLL_RECORDS}',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
```

**Step 2: Commit**

```bash
git add apps/event-processor/sql/ddl/source/exposure_events.sql
git commit -m "feat: add exposure_events Kafka source DDL"
```

---

### Task 2: 创建 exposure_events_s3 Iceberg Sink DDL

**Files:**
- Create: `apps/event-processor/sql/ddl/sink/exposure_events_s3.sql`

**Step 1: 创建文件，内容如下**

```sql
-- ============================================================
-- Sink Table: exposure_events (Iceberg)
-- 曝光事件归档到 S3，append-only（服务端已保证唯一性）
-- ============================================================

CREATE TABLE IF NOT EXISTS iceberg_catalog.${ICEBERG_DATABASE}.exposure_events_v1 (
    -- ========== 事件标识 ==========
    event_id STRING,

    -- ========== 曝光核心字段 ==========
    page_url STRING,
    product_id STRING,
    store_id STRING,
    scm STRING,

    -- ========== 曝光状态 ==========
    visible STRING,
    visible_duration_ms BIGINT,
    visible_ratio DOUBLE,

    -- ========== 用户标识 ==========
    session_id STRING,
    anonymous_id STRING,

    -- ========== 扩展数据 ==========
    ext_json STRING,

    -- ========== 时间字段 ==========
    event_time BIGINT,
    report_time BIGINT,
    server_time BIGINT,
    processed_time TIMESTAMP(3),

    -- ========== 分区字段 ==========
    dt STRING,
    hr STRING

) PARTITIONED BY (dt, hr) WITH (
    'format-version' = '2'
);
```

**Step 2: Commit**

```bash
git add apps/event-processor/sql/ddl/sink/exposure_events_s3.sql
git commit -m "feat: add exposure_events_s3 Iceberg sink DDL"
```

---

### Task 3: 在 dedup_with_udf.sql 追加 exposure events INSERT

**Files:**
- Modify: `apps/event-processor/sql/dml/dedup_with_udf.sql`

**Step 1: 在文件末尾 `END;` 之前追加以下 SQL**

在现有的 Sink 3（归因事件）之后、`END;` 之前，添加：

```sql
-- Sink 4: 写入曝光事件到 Iceberg (append-only)
INSERT INTO iceberg_catalog.${ICEBERG_DATABASE}.exposure_events_v1 (
    event_id,
    page_url,
    product_id,
    store_id,
    scm,
    visible,
    visible_duration_ms,
    visible_ratio,
    session_id,
    anonymous_id,
    ext_json,
    event_time,
    report_time,
    server_time,
    processed_time,
    dt,
    hr
)
SELECT
    event_id,
    page_url,
    product_id,
    store_id,
    scm,
    visible,
    visible_duration_ms,
    visible_ratio,
    session_id,
    anonymous_id,
    ext_json,
    event_time,
    report_time,
    server_time,
    CURRENT_TIMESTAMP AS processed_time,
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'yyyy-MM-dd') AS dt,  -- 北京时间 UTC+8
    DATE_FORMAT(CURRENT_TIMESTAMP + INTERVAL '8' HOUR, 'HH') AS hr           -- 北京时间 UTC+8
FROM exposure_events;
```

**Step 2: Commit**

```bash
git add apps/event-processor/sql/dml/dedup_with_udf.sql
git commit -m "feat: add exposure_events INSERT to statement set"
```

---

### Task 4: 更新 JobConfig.java 添加 exposureTopic

**Files:**
- Modify: `apps/event-processor/src/main/java/com/sandwichlab/flink/JobConfig.java`

**Step 1: 添加字段和读取逻辑**

在 `private final String attributedTopic;` 下方添加：
```java
private final String exposureTopic;
```

在构造函数中 `this.attributedTopic = ...` 下方添加：
```java
this.exposureTopic = props.getOrDefault("EXPOSURE_TOPIC", "exposure-events");
```

在 `loadProperties()` 的 env/system 读取块中 `ATTRIBUTED_TOPIC` 下方添加：
```java
props.put("EXPOSURE_TOPIC", getEnvOrProperty("EXPOSURE_TOPIC", "exposure-events"));
```

添加 getter 方法：
```java
public String getExposureTopic() {
    return exposureTopic;
}
```

在 `toSqlVariables()` 中 `ATTRIBUTED_TOPIC` 下方添加：
```java
variables.put("EXPOSURE_TOPIC", exposureTopic);
```

在 `toString()` 的格式字符串中添加 `exposureTopic='%s'`，并在对应位置添加 `exposureTopic` 参数。

**Step 2: Commit**

```bash
git add apps/event-processor/src/main/java/com/sandwichlab/flink/JobConfig.java
git commit -m "feat: add EXPOSURE_TOPIC config to JobConfig"
```

---

### Task 5: 更新 EventProcessorJob.java 加载 exposure DDL 并注册 topic

**Files:**
- Modify: `apps/event-processor/src/main/java/com/sandwichlab/flink/EventProcessorJob.java`

**Step 1: 更新 topicManager.ensureTopicsExist() 调用**

将：
```java
topicManager.ensureTopicsExist(config.getInputTopic(), config.getOutputTopic(), config.getAttributedTopic());
```

改为：
```java
topicManager.ensureTopicsExist(config.getInputTopic(), config.getOutputTopic(), config.getAttributedTopic(), config.getExposureTopic());
```

**Step 2: 在 executeJob() 方法中追加 exposure DDL 加载**

在归因事件 Sink 表创建之后（`attributed_events_s3.sql` 加载后），追加：

```java
// DDL: 创建曝光事件源表 (Kafka)
LOG.info("Creating source table: exposure_events (Kafka)");
tableEnv.executeSql(sqlLoader.load("sql/ddl/source/exposure_events.sql"));

// DDL: 创建曝光事件 Sink 表 (Iceberg)
LOG.info("Creating sink table: exposure_events (Iceberg)");
tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/exposure_events_s3.sql"));
```

**Step 3: Commit**

```bash
git add apps/event-processor/src/main/java/com/sandwichlab/flink/EventProcessorJob.java
git commit -m "feat: register exposure_events source and sink tables in job"
```

---

### Task 6: 更新配置文件

**Files:**
- Modify: `apps/event-processor/config/dev.yaml`
- Modify: `apps/event-processor/config/prod.yaml`

**Step 1: 在两个文件的 `kafka:` 块中，`attributed_topic` 下方各添加一行**

```yaml
  exposure_topic: exposure-events
```

**Step 2: Commit**

```bash
git add apps/event-processor/config/dev.yaml apps/event-processor/config/prod.yaml
git commit -m "feat: add exposure_topic to dev and prod configs"
```

---

### Task 7: 构建验证

**Step 1: 执行 Maven 构建**

```bash
cd apps/event-processor
mvn clean package -DskipTests
```

Expected: `BUILD SUCCESS`，生成 `target/event-processor-1.0.0.jar`

**Step 2: 验证 SQL 文件被打包进 jar**

```bash
jar tf target/event-processor-1.0.0.jar | grep exposure
```

Expected: 输出包含
```
sql/ddl/source/exposure_events.sql
sql/ddl/sink/exposure_events_s3.sql
```
