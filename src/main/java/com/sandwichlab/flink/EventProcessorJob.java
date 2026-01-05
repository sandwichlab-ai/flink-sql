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
        JobConfig config = new JobConfig();
        LOG.info("Config: {}", config);

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
        tableEnv.executeSql(sqlLoader.load("sql/ddl/source_raw_events.sql"));

        // DDL: 创建目标表
        LOG.info("Creating sink table: processed_events");
        tableEnv.executeSql(sqlLoader.load("sql/ddl/sink_processed_events.sql"));

        // DML: 执行去重逻辑
        LOG.info("Executing deduplication");
        tableEnv.executeSql(sqlLoader.load("sql/dml/dedup.sql"));
    }
}
