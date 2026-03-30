# dev 工作流项目配置
# 此文件由 /dev、/dev-design、/dev-impl、/dev-test 命令读取，覆盖默认行为

## 项目信息

- **项目名称**：flink-sql（event-processor）
- **项目类型**：Apache Flink 实时数据处理（Java 11 + Flink SQL 1.20 + Maven Monorepo）
- **包管理器**：Maven 3
- **测试框架**：JUnit 5（命令：`mvn test`）
- **辅助脚本**：Python 3（`scripts/` 目录）

## 关键文档（key_docs）

设计阶段必须按序阅读：

1. `README.md` — 架构图、数据流、配置说明、Makefile 命令参考（必读）
2. `docs/iceberg-upsert.md` — Iceberg Upsert 机制、Checkpoint 策略、跨账户配置
3. `docs/udf/ddb-write.md` — DynamoDB UDF 使用说明和扩展方式
4. `docs/plans/` — 历史实现计划，了解已有决策背景

## 目录约定

```
apps/
  event-processor/
    src/main/java/com/sandwichlab/flink/
      EventProcessorJob.java  # Flink 主程序（DDL 加载顺序在此定义）
      JobConfig.java          # 配置加载（三层：Managed Flink → 环境变量 → 默认值）
      SqlLoader.java          # SQL 文件加载 + 变量替换 (${VAR} 格式)
      KafkaTopicManager.java  # 启动时自动创建 Kafka Topics
    sql/
      ddl/
        source/   # Kafka source 表定义
        sink/     # Kafka + Iceberg + DynamoDB sink 表定义
        catalog/  # Iceberg Catalog 和 Database 定义
      dml/        # 数据处理逻辑（去重、归因、聚合）
    config/
      dev.yaml    # 开发环境配置
      prod.yaml   # 生产环境配置
libs/
  flink-udf/
    src/main/java/com/sandwichlab/flink/udf/
      DdbWriteUdf.java     # DynamoDB 写入 ScalarFunction
      operator/            # Operator 接口和实现
scripts/                   # Python 测试和运维脚本
docs/                      # 项目文档
```

## 技术规范

- 语言：Java 11（严格）
- SQL：Flink SQL 1.20 风格，变量用 `${VARIABLE_NAME}` 占位符
- 构建：`mvn package`（Shade 插件打 fat jar，排除 Flink 和 SLF4J）
- 代码风格：遵循现有 Java 代码风格（无 Lombok，直接手写 getter/setter）
- 日志：SLF4J（`LoggerFactory.getLogger()`）

## 常用命令参考

```bash
# 构建
make build                          # mvn package
mvn compile                         # 仅编译，快速验证

# 本地运行（需要 AWS 配置）
make start APP=event-processor-dev ENV=dev

# 部署
make deploy APP=event-processor-dev ENV=dev

# 状态和日志
make status APP=event-processor-dev ENV=dev
make logs APP=event-processor-dev ENV=dev

# 测试工具
python3 scripts/send_test_events.py   # 发送测试事件到 Kafka
python3 scripts/consume_events.py     # 消费并查看事件
python3 scripts/check_kafka_messages.py
```

## 设计阶段注意事项

1. **DDL 加载顺序**：新增 source/sink 表须在 `EventProcessorJob.java` 中按顺序注册（Catalog/Database 必须在 sink 之前）；设计方案须明确新增 DDL 的插入位置

2. **配置扩展**：新增配置字段须同时修改：
   - `JobConfig.java`：字段定义 + 三层加载逻辑 + `toSqlVariables()` 映射
   - `config/dev.yaml` 和 `config/prod.yaml`：添加对应配置项

3. **SQL 变量**：SQL 文件中用 `${VARIABLE_NAME}` 引用配置，变量名须与 `JobConfig.toSqlVariables()` 的 key 一致

4. **Iceberg vs Append-only**：
   - 需要幂等写入（去重）→ Upsert 模式（Primary Key + `write.upsert.enabled=true`）
   - 服务端已保证唯一性 → Append-only（如 exposure_events）

5. **多 Sink 写入**：新增 sink 须加入 `dedup_with_udf.sql` 的 `EXECUTE STATEMENT SET`，不单独起 Job

6. **UDF 扩展**：新增外部存储操作实现 `Operator` 接口，不要直接在 UDF 里堆逻辑

7. **跨账户权限**：Iceberg sink 写入 Data Platform 账户的 Glue Catalog + S3，涉及 IAM Role 配置须在方案中说明

## 实现阶段注意事项

- SQL 文件修改后，用 `mvn compile` 快速验证 Java 部分编译通过
- 新增 Java 类放在对应模块（`apps/event-processor` 或 `libs/flink-udf`）下
- 修改 `pom.xml` 依赖时注意 scope：Flink 相关依赖用 `provided`，UDF 依赖用 `compile`
- 不要修改 `dedup.sql`（简化版，仅留作参考），DML 修改都在 `dedup_with_udf.sql`

## 测试阶段注意事项

- 单元测试放在 `libs/flink-udf/src/test/`（使用 JUnit 5）
- UDF 测试：mock `FunctionContext` 和 AWS DynamoDB 客户端
- SQL 逻辑验证：用 `scripts/send_test_events.py` 发送测试事件，用 `scripts/consume_events.py` 验证输出
- 编译检查作为最低验证标准：`mvn compile` 必须通过
- 集成测试依赖 AWS 环境（dev），无法在纯本地执行；单元测试须能在本地跑通
