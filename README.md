# Lexi2 Flink Event Processor

实时数据处理项目，使用 Java Flink Table API + Amazon Managed Service for Apache Flink。

## 架构

```
SDK 上报 → MSK (raw-events) → Flink (去重) → MSK (processed-events)
                                   ↓
                              下游订阅消费
```

## 目录结构

```
.
├── Makefile                     # 部署命令入口
├── pom.xml                      # Maven 构建配置
├── config/
│   ├── dev.env                  # 开发环境配置
│   └── prod.env                 # 生产环境配置
├── src/main/java/
│   └── com/sandwichlab/flink/
│       └── EventProcessorJob.java   # Flink Table API 主程序
├── sql/                         # SQL 参考文件 (嵌入到 Java 代码中)
│   ├── ddl/
│   │   ├── source_raw_events.sql
│   │   └── sink_processed_events.sql
│   └── dml/
│       ├── dedup.sql
│       ├── attribution.sql
│       └── aggregation.sql
├── scripts/
│   └── manage-app.sh            # 应用管理脚本
└── README.md
```

## 前置条件

1. **Java 11** + **Maven**
   ```bash
   brew install openjdk@11 maven
   export JAVA_HOME="/opt/homebrew/opt/openjdk@11"
   export PATH="$JAVA_HOME/bin:$PATH"
   ```

2. **AWS CLI** 配置 `dev-us` / `prod-us` profile

3. **CDK 部署的资源** (在 `sandwichlab_cdk_app` 中):
   - MSK Serverless 集群
   - Flink 代码 S3 桶
   - Flink 执行角色 (IAM)

## 快速开始

### 1. 首次部署

```bash
# 构建 + 上传 + 创建应用
make create-app ENV=dev
```

### 2. 启动应用

```bash
make start ENV=dev

# 查看状态
make status ENV=dev
```

### 3. 更新代码

```bash
# 修改 Java 代码后
make deploy ENV=dev

# 重启应用使更改生效
make stop ENV=dev
make start ENV=dev
```

## Makefile 命令

| 命令 | 说明 |
|------|------|
| `make help` | 显示帮助信息 |
| `make build` | 构建 JAR (mvn package) |
| `make deploy` | 构建 + 上传 + 更新应用代码 |
| `make create-app` | 首次创建 Flink 应用 (含 VPC 配置) |
| `make delete-app` | 删除 Flink 应用 |
| `make update-app` | 更新应用代码 (不重新构建) |
| `make start` | 启动应用 |
| `make stop` | 停止应用 |
| `make status` | 查看应用状态 |
| `make logs` | 查看 CloudWatch 日志 |
| `make clean` | 清理构建产物 |

## 脚本方式

```bash
# 使用管理脚本
ENV=dev ./scripts/manage-app.sh create   # 创建应用
ENV=dev ./scripts/manage-app.sh start    # 启动
ENV=dev ./scripts/manage-app.sh status   # 查看状态
ENV=dev ./scripts/manage-app.sh stop     # 停止
ENV=dev ./scripts/manage-app.sh update   # 更新代码
ENV=dev ./scripts/manage-app.sh delete   # 删除
ENV=dev ./scripts/manage-app.sh logs     # 查看日志
```

## 配置说明

### config/dev.env

```bash
# AWS
AWS_PROFILE=dev-us
AWS_REGION=us-west-2
AWS_ACCOUNT_ID=856969325542

# Flink 应用
APP_NAME=lexi2-flink-dev
PARALLELISM=1
EXECUTION_ROLE_ARN=arn:aws:iam::856969325542:role/lexi2-dev-usw2-856969325542-flink-execution-role

# VPC (Flink 需要 VPC 访问 MSK)
VPC_ID=vpc-0f456807dee4060a5
SUBNET_IDS=subnet-01c18c77cca5dde43,subnet-0ce1f0d7755a1d2df
SECURITY_GROUP_ID=sg-0cbf7b01b8bc9b3b3

# MSK
BOOTSTRAP_SERVERS=boot-7zbc9cuu.c1.kafka-serverless.us-west-2.amazonaws.com:9098

# S3
CODE_BUCKET=lexi2-dev-usw2-856969325542-flink-code

# Topics
INPUT_TOPIC=raw-events
OUTPUT_TOPIC=processed-events
```

## Java 代码结构

`EventProcessorJob.java` 使用 Flink Table API：

```java
// 1. 创建环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// 2. 创建 Source 表 (Kafka)
tableEnv.executeSql("CREATE TABLE raw_events (...) WITH ('connector'='kafka', ...)");

// 3. 创建 Sink 表 (Upsert Kafka)
tableEnv.executeSql("CREATE TABLE processed_events (...) WITH ('connector'='upsert-kafka', ...)");

// 4. 执行去重逻辑
tableEnv.executeSql("INSERT INTO processed_events SELECT ... FROM raw_events ...");
```

### 去重逻辑

使用 `ROW_NUMBER()` 基于 `event_id` 去重，保留最新事件：

```sql
INSERT INTO processed_events
SELECT event_id, user_id, ...
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY event_time DESC
    ) AS row_num
    FROM raw_events
)
WHERE row_num = 1;
```

## CDK 管理的资源

以下资源由 CDK 管理 (`sandwichlab_cdk_app`):

| 资源 | Stack | 说明 |
|------|-------|------|
| MSK Serverless | `StreamingStack` | Kafka 集群 |
| S3 Bucket | `StreamingStack` | Flink 代码桶 |
| IAM Role | `StreamingStack` | Flink 执行角色 |
| VPC/Subnets | `VpcStack` | 网络资源 |

### Flink 执行角色权限

- MSK: 读写 Topic、消费组管理
- S3: 读取代码桶
- CloudWatch: 写入日志
- VPC: 创建/删除 ENI
- 跨账户 S3: 写入 Data Platform Landing Zone

## 监控和调试

### 查看应用状态

```bash
make status ENV=dev
```

输出示例：
```
-----------------------------------------------------
|              DescribeApplication                  |
+-------+----------+----------+---------+-----------+
| Name  | Status   | Version  | Runtime | Parallelism|
+-------+----------+----------+---------+-----------+
| lexi2 | RUNNING  | 1        | FLINK   | 1         |
+-------+----------+----------+---------+-----------+
```

### 查看实时日志

```bash
make logs ENV=dev
```

### Flink Dashboard

应用运行后，可在 AWS Console 查看 Flink Dashboard：
1. 打开 Kinesis Data Analytics 控制台
2. 选择应用 `lexi2-flink-dev`
3. 点击 "Open Apache Flink dashboard"

## 成本估算

| 组件 | 计费方式 | 估算 (dev) |
|------|----------|-----------|
| Managed Flink | $0.11/KPU-hour | ~$80/月 (1 KPU) |
| MSK Serverless | 按流量计费 | ~$10/月 |
| S3 | 存储 + 请求 | <$1/月 |

## 注意事项

1. **代码更新需要重启**: Flink 应用更新代码后需要 `stop` + `start`
2. **VPC 配置**: Flink 必须部署在与 MSK 相同的 VPC 中
3. **Checkpoint**: 默认 60 秒 checkpoint，重启后从 checkpoint 恢复
4. **MSK IAM 认证**: 使用 `AWS_MSK_IAM` 机制，无需管理密码

## 常见问题

### Q: 应用启动失败

检查：
1. IAM 角色权限是否正确
2. VPC 安全组是否允许访问 MSK (端口 9098)
3. 子网是否有 NAT Gateway (访问 S3)

### Q: 无法连接 MSK

确认：
1. 安全组 `sg-0cbf7b01b8bc9b3b3` 允许入站 9098
2. Flink 应用使用相同的安全组

### Q: 日志在哪里

CloudWatch Log Group: `/aws/kinesis-analytics/lexi2-flink-dev`

```bash
aws logs tail /aws/kinesis-analytics/lexi2-flink-dev --follow --profile dev-us --region us-west-2
```
