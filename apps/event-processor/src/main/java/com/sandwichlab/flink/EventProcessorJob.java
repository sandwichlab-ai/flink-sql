package com.sandwichlab.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Flink Event Processor Job
 *
 * 从 MSK 读取事件，去重后写入 MSK
 */
public class EventProcessorJob {

    private static final Logger LOG = LoggerFactory.getLogger(EventProcessorJob.class);

    public static void main(String[] args) {
        LOG.info("Starting Flink Event Processor Job");

        // 加载配置
        JobConfig config = new JobConfig(args);
        LOG.info("Config: {}", config);

        // 确保 Kafka Topics 存在（在启动 Flink SQL 前创建）
        LOG.info("Ensuring Kafka topics exist with {} partitions...", config.getKafkaPartitions());
        KafkaTopicManager topicManager = new KafkaTopicManager(
            config.getBootstrapServers(),
            config.getKafkaPartitions(),
            (short) 3
        );
        topicManager.ensureTopicsExist(config.getInputTopic(), config.getOutputTopic(), config.getAttributedTopic());

        // 创建 SQL 加载器
        SqlLoader sqlLoader = new SqlLoader(config.toSqlVariables());

        // 创建 Flink 环境（传递配置给 UDF）
        StreamTableEnvironment tableEnv = createTableEnvironment(config);

        // 执行 SQL
        executeJob(tableEnv, sqlLoader);

        LOG.info("Job started successfully");
    }

    private static StreamTableEnvironment createTableEnvironment(JobConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        // 将配置传递给 GlobalJobParameters，供 UDF 通过 FunctionContext.getJobParameter() 获取
        org.apache.flink.configuration.Configuration globalParams = new org.apache.flink.configuration.Configuration();
        for (Map.Entry<String, String> entry : config.toSqlVariables().entrySet()) {
            globalParams.setString(entry.getKey(), entry.getValue());
            LOG.info("Setting GlobalJobParameter: {} = {}", entry.getKey(), entry.getValue());
        }
        env.getConfig().setGlobalJobParameters(globalParams);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 设置 State TTL 为 2 分钟，超过 2 分钟未更新的状态会被清理
        tableEnv.getConfig().set("table.exec.state.ttl", "2min");
        LOG.info("State TTL set to 2 minutes");

        return tableEnv;
    }

    private static void executeJob(StreamTableEnvironment tableEnv, SqlLoader sqlLoader) {
        // DDL: 创建源表
        LOG.info("Creating source table: raw_events");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/source/raw_events.sql"));

        // DDL: 创建 Kafka Sink 表
        LOG.info("Creating sink table: processed_events (Kafka)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/processed_events.sql"));

        // DDL: 创建 Iceberg Catalog
        LOG.info("Creating Iceberg catalog");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/catalog/iceberg_catalog.sql"));

        // DDL: 创建 Iceberg Database
        LOG.info("Creating Iceberg database");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/catalog/iceberg_database.sql"));

        // DDL: 创建 S3 Sink 表 (Iceberg)
        LOG.info("Creating sink table: events_s3 (Iceberg)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/events_s3.sql"));

        // DDL: 创建归因事件源表 (Kafka)
        LOG.info("Creating source table: attributed_events (Kafka)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/source/attributed_events.sql"));

        // DDL: 创建归因事件 Sink 表 (Iceberg)
        LOG.info("Creating sink table: attributed_events (Iceberg)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/attributed_events_s3.sql"));

        // DML: 执行去重逻辑，写入 Kafka、S3 和归因事件
        LOG.info("Executing deduplication (Kafka + S3 + Attribution sinks)");
        tableEnv.executeSql(sqlLoader.load("sql/dml/dedup_with_udf.sql"));
    }
}
