# Flink Apps Monorepo
# 使用: make <target> APP=<app-name> ENV=<dev|prod>
#
# 示例:
#   make deploy APP=event-processor ENV=dev
#   make start APP=event-processor ENV=dev

# ==================== 参数 ====================
APP ?= event-processor
ENV ?= dev

# App 路径
APP_DIR := apps/$(APP)
CONFIG_FILE := $(APP_DIR)/config/$(ENV).yaml

# 从 YAML 读取配置
AWS_PROFILE := $(shell yq '.aws.profile' $(CONFIG_FILE))
AWS_REGION := $(shell yq '.aws.region' $(CONFIG_FILE))
AWS_ACCOUNT_ID := $(shell yq '.aws.account_id' $(CONFIG_FILE))
APP_NAME := $(shell yq '.app.name' $(CONFIG_FILE))
RUNTIME := $(shell yq '.flink.runtime' $(CONFIG_FILE))
PARALLELISM := $(shell yq '.flink.parallelism' $(CONFIG_FILE))
PARALLELISM_PER_KPU := $(shell yq '.flink.parallelism_per_kpu' $(CONFIG_FILE))
AUTOSCALING_ENABLED := $(shell yq '.flink.autoscaling_enabled' $(CONFIG_FILE))
EXECUTION_ROLE_ARN := $(shell yq '.iam.execution_role_arn' $(CONFIG_FILE))
SUBNET_IDS := $(shell yq '.vpc.subnet_ids | join(",")' $(CONFIG_FILE))
SECURITY_GROUP_ID := $(shell yq '.vpc.security_group_id' $(CONFIG_FILE))
BOOTSTRAP_SERVERS := $(shell yq '.msk.bootstrap_servers' $(CONFIG_FILE))
CODE_BUCKET := $(shell yq '.s3.code_bucket' $(CONFIG_FILE))
INPUT_TOPIC := $(shell yq '.kafka.input_topic' $(CONFIG_FILE))
OUTPUT_TOPIC := $(shell yq '.kafka.output_topic' $(CONFIG_FILE))
ATTRIBUTED_TOPIC := $(shell yq '.kafka.attributed_topic' $(CONFIG_FILE))
KAFKA_PARTITIONS := $(shell yq '.kafka.partitions // 3' $(CONFIG_FILE))
MAX_POLL_RECORDS := $(shell yq '.kafka.max_poll_records // 100' $(CONFIG_FILE))
DDB_TABLE_NAME := $(shell yq '.dynamodb.table_name' $(CONFIG_FILE))
CLOUDWATCH_LOG_GROUP := $(shell yq '.logging.log_group' $(CONFIG_FILE))

# ==================== 构建配置 ====================
JAR_NAME := $(APP)-1.0.0.jar
JAR_PATH := $(APP_DIR)/target/$(JAR_NAME)
S3_JAR_KEY := flink-jars/$(APP)/$(JAR_NAME)

# ==================== 颜色 ====================
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
CYAN := \033[0;36m
NC := \033[0m

.PHONY: help list build deploy create-app delete-app update-app start stop status logs clean config create-topics

# ==================== 帮助 ====================

help: ## 显示帮助
	@echo ""
	@echo "$(CYAN)Flink Apps Monorepo$(NC)"
	@echo ""
	@echo "$(YELLOW)命令:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)参数:$(NC)"
	@echo "  APP=<name>    App 名称 (默认: event-processor)"
	@echo "  ENV=<env>     环境 (默认: dev)"
	@echo ""
	@echo "$(YELLOW)示例:$(NC)"
	@echo "  make list"
	@echo "  make config APP=event-processor ENV=dev"
	@echo "  make create-app APP=event-processor ENV=dev"
	@echo ""

list: ## 列出所有 Apps 和环境
	@echo "$(CYAN)Apps:$(NC)"
	@for app in apps/*/; do \
		app_name=$$(basename $$app); \
		echo "  $(GREEN)$$app_name$(NC)"; \
		for cfg in $$app/config/*.yaml; do \
			if [ -f "$$cfg" ]; then \
				env_name=$$(basename $$cfg .yaml); \
				echo "    - $$env_name"; \
			fi; \
		done; \
	done

config: ## 显示当前配置
	@echo "$(CYAN)[$(APP)/$(ENV)] 配置:$(NC)"
	@echo "  App Name:      $(APP_NAME)"
	@echo "  AWS Profile:   $(AWS_PROFILE)"
	@echo "  AWS Region:    $(AWS_REGION)"
	@echo "  Runtime:       $(RUNTIME)"
	@echo "  Parallelism:   $(PARALLELISM)"
	@echo "  Code Bucket:   $(CODE_BUCKET)"
	@echo "  Input Topic:   $(INPUT_TOPIC)"
	@echo "  Output Topic:  $(OUTPUT_TOPIC)"
	@echo "  DDB Table:     $(DDB_TABLE_NAME)"

# ==================== 构建 ====================

build: ## 构建指定 App
	@echo "$(YELLOW)[$(APP)] 构建中...$(NC)"
	mvn clean package -DskipTests -q -pl $(APP_DIR) -am
	@echo "$(GREEN)[$(APP)] 构建完成: $(JAR_PATH)$(NC)"

build-all: ## 构建所有 Apps
	@echo "$(YELLOW)构建所有 Apps...$(NC)"
	mvn clean package -DskipTests -q
	@echo "$(GREEN)全部构建完成$(NC)"

# ==================== 部署 ====================

deploy: build upload-jar update-app ## 构建并部署
	@echo "$(GREEN)[$(APP)/$(ENV)] 部署完成$(NC)"

upload-jar: ## 上传 JAR 到 S3
	@echo "$(YELLOW)[$(APP)/$(ENV)] 上传 JAR...$(NC)"
	aws s3 cp $(JAR_PATH) s3://$(CODE_BUCKET)/$(S3_JAR_KEY) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)已上传: s3://$(CODE_BUCKET)/$(S3_JAR_KEY)$(NC)"

# ==================== 应用管理 ====================

create-app: build upload-jar create-log-group ## 创建 Flink 应用
	@echo "$(YELLOW)[$(APP)/$(ENV)] 创建应用: $(APP_NAME)$(NC)"
	@SUBNET_JSON=$$(echo '$(SUBNET_IDS)' | sed 's/,/","/g' | sed 's/^/["/;s/$$/"]/' ) && \
	aws kinesisanalyticsv2 create-application \
		--application-name $(APP_NAME) \
		--runtime-environment $(RUNTIME) \
		--service-execution-role $(EXECUTION_ROLE_ARN) \
		--application-configuration "{ \
			\"FlinkApplicationConfiguration\": { \
				\"ParallelismConfiguration\": { \
					\"ConfigurationType\": \"CUSTOM\", \
					\"Parallelism\": $(PARALLELISM), \
					\"ParallelismPerKPU\": $(PARALLELISM_PER_KPU), \
					\"AutoScalingEnabled\": $(AUTOSCALING_ENABLED) \
				}, \
				\"CheckpointConfiguration\": { \
					\"ConfigurationType\": \"CUSTOM\", \
					\"CheckpointingEnabled\": true, \
					\"CheckpointInterval\": 60000, \
					\"MinPauseBetweenCheckpoints\": 5000 \
				}, \
				\"MonitoringConfiguration\": { \
					\"ConfigurationType\": \"CUSTOM\", \
					\"MetricsLevel\": \"APPLICATION\", \
					\"LogLevel\": \"INFO\" \
				} \
			}, \
			\"ApplicationCodeConfiguration\": { \
				\"CodeContent\": { \
					\"S3ContentLocation\": { \
						\"BucketARN\": \"arn:aws:s3:::$(CODE_BUCKET)\", \
						\"FileKey\": \"$(S3_JAR_KEY)\" \
					} \
				}, \
				\"CodeContentType\": \"ZIPFILE\" \
			}, \
			\"EnvironmentProperties\": { \
				\"PropertyGroups\": [{ \
					\"PropertyGroupId\": \"FlinkApplicationProperties\", \
					\"PropertyMap\": { \
						\"BOOTSTRAP_SERVERS\": \"$(BOOTSTRAP_SERVERS)\", \
						\"INPUT_TOPIC\": \"$(INPUT_TOPIC)\", \
						\"OUTPUT_TOPIC\": \"$(OUTPUT_TOPIC)\", \
						\"ATTRIBUTED_TOPIC\": \"$(ATTRIBUTED_TOPIC)\", \
						\"AWS_REGION\": \"$(AWS_REGION)\", \
						\"DDB_TABLE_NAME\": \"$(DDB_TABLE_NAME)\", \
						\"KAFKA_PARTITIONS\": \"$(KAFKA_PARTITIONS)\", \
						\"MAX_POLL_RECORDS\": \"$(MAX_POLL_RECORDS)\" \
					} \
				}] \
			}, \
			\"VpcConfigurations\": [{ \
				\"SubnetIds\": $$SUBNET_JSON, \
				\"SecurityGroupIds\": [\"$(SECURITY_GROUP_ID)\"] \
			}] \
		}" \
		--cloud-watch-logging-options '[{"LogStreamARN":"arn:aws:logs:$(AWS_REGION):$(AWS_ACCOUNT_ID):log-group:$(CLOUDWATCH_LOG_GROUP):log-stream:flink-logs"}]' \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)[$(APP)/$(ENV)] 应用已创建$(NC)"

delete-app: ## 删除 Flink 应用
	@echo "$(YELLOW)[$(APP)/$(ENV)] 删除应用: $(APP_NAME)$(NC)"
	@CREATE_TS=$$(aws kinesisanalyticsv2 describe-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--query 'ApplicationDetail.CreateTimestamp' \
		--output text) && \
	aws kinesisanalyticsv2 delete-application \
		--application-name $(APP_NAME) \
		--create-timestamp $$CREATE_TS \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)[$(APP)/$(ENV)] 应用已删除$(NC)"

update-app: ## 更新应用代码
	@echo "$(YELLOW)[$(APP)/$(ENV)] 更新代码...$(NC)"
	@VERSION=$$(aws kinesisanalyticsv2 describe-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--query 'ApplicationDetail.ApplicationVersionId' \
		--output text) && \
	aws kinesisanalyticsv2 update-application \
		--application-name $(APP_NAME) \
		--current-application-version-id $$VERSION \
		--application-configuration-update '{ \
			"ApplicationCodeConfigurationUpdate": { \
				"CodeContentTypeUpdate": "ZIPFILE", \
				"CodeContentUpdate": { \
					"S3ContentLocationUpdate": { \
						"BucketARNUpdate": "arn:aws:s3:::$(CODE_BUCKET)", \
						"FileKeyUpdate": "$(S3_JAR_KEY)" \
					} \
				} \
			} \
		}' \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)[$(APP)/$(ENV)] 更新完成$(NC)"

# ==================== 应用控制 ====================

start: ## 启动应用
	@echo "$(YELLOW)[$(APP)/$(ENV)] 启动: $(APP_NAME)$(NC)"
	aws kinesisanalyticsv2 start-application \
		--application-name $(APP_NAME) \
		--run-configuration '{"FlinkRunConfiguration":{"AllowNonRestoredState":true}}' \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)启动命令已发送$(NC)"

stop: ## 停止应用
	@echo "$(YELLOW)[$(APP)/$(ENV)] 停止: $(APP_NAME)$(NC)"
	aws kinesisanalyticsv2 stop-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)停止命令已发送$(NC)"

status: ## 查看状态
	@echo "$(CYAN)[$(APP)/$(ENV)] $(APP_NAME)$(NC)"
	@aws kinesisanalyticsv2 describe-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--query 'ApplicationDetail.{Status:ApplicationStatus,Version:ApplicationVersionId}' \
		--output table 2>/dev/null || echo "$(RED)应用不存在$(NC)"

logs: ## 查看日志
	aws logs tail $(CLOUDWATCH_LOG_GROUP) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--follow

# ==================== 日志 ====================

create-log-group: ## 创建 CloudWatch 日志组
	@aws logs create-log-group \
		--log-group-name $(CLOUDWATCH_LOG_GROUP) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) 2>/dev/null || true
	@aws logs create-log-stream \
		--log-group-name $(CLOUDWATCH_LOG_GROUP) \
		--log-stream-name flink-logs \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) 2>/dev/null || true
	@echo "$(GREEN)日志组已创建: $(CLOUDWATCH_LOG_GROUP)$(NC)"

# ==================== Kafka ====================

create-topics: ## 创建 Kafka Topics
	@echo "$(YELLOW)[$(APP)/$(ENV)] 创建 Kafka Topics...$(NC)"
	@echo "  Bootstrap: $(BOOTSTRAP_SERVERS)"
	@echo "  Topics: $(INPUT_TOPIC), $(OUTPUT_TOPIC)"
	@AWS_PROFILE=$(AWS_PROFILE) python3 scripts/create_topics.py \
		--bootstrap-servers $(BOOTSTRAP_SERVERS) \
		--topics $(INPUT_TOPIC),$(OUTPUT_TOPIC) \
		--region $(AWS_REGION)
	@echo "$(GREEN)[$(APP)/$(ENV)] Topics 创建完成$(NC)"

send-test-events: ## 发送测试事件
	@echo "$(YELLOW)[$(APP)/$(ENV)] 发送测试事件...$(NC)"
	@AWS_PROFILE=$(AWS_PROFILE) python3 scripts/send_test_events.py \
		--bootstrap-servers $(BOOTSTRAP_SERVERS) \
		--topic $(INPUT_TOPIC) \
		--region $(AWS_REGION) \
		--count $(or $(COUNT),10)
	@echo "$(GREEN)[$(APP)/$(ENV)] 测试事件发送完成$(NC)"

# ==================== 清理 ====================

clean: ## 清理构建
	mvn clean -q
	@echo "$(GREEN)清理完成$(NC)"
