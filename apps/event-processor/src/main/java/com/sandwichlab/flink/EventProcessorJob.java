package com.sandwichlab.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("Ensuring Kafka topics exist...");
        KafkaTopicManager topicManager = new KafkaTopicManager(config.getBootstrapServers());
        topicManager.ensureTopicsExist(config.getInputTopic(), config.getOutputTopic());

        // 创建 SQL 加载器
        SqlLoader sqlLoader = new SqlLoader(config.toSqlVariables());

        // 创建 Flink 环境
        StreamTableEnvironment tableEnv = createTableEnvironment();

        // 执行 SQL
        executeJob(tableEnv, sqlLoader);

        LOG.info("Job started successfully");
    }

    private static StreamTableEnvironment createTableEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        return StreamTableEnvironment.create(env, settings);
    }

    private static void executeJob(StreamTableEnvironment tableEnv, SqlLoader sqlLoader) {
        // DDL: 创建源表
        LOG.info("Creating source table: raw_events");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/source/raw_events.sql"));

        // DDL: 创建 Kafka Sink 表
        LOG.info("Creating sink table: processed_events (Kafka)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/processed_events.sql"));

        // DDL: 创建 S3 Sink 表
        LOG.info("Creating sink table: events_s3 (S3)");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink/events_s3.sql"));

        // DML: 执行去重逻辑，同时写入 Kafka 和 S3
        LOG.info("Executing deduplication (dual sink: Kafka + S3)");
        tableEnv.executeSql(sqlLoader.load("sql/dml/dedup.sql"));
    }
}
