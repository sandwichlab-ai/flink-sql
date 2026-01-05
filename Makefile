# Flink Event Processor
# 使用: make <target> ENV=<dev|prod>

ENV ?= dev
include config/$(ENV).env
export

JAR_NAME := flink-event-processor-1.0.0.jar
JAR_PATH := target/$(JAR_NAME)
S3_JAR_KEY := flink-jars/$(JAR_NAME)

GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m

.PHONY: help build deploy create-app delete-app update start stop status logs clean

help: ## 显示帮助
	@echo "Flink Event Processor 命令:"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "使用示例:"
	@echo "  make build              # 构建 JAR"
	@echo "  make deploy ENV=dev     # 构建并部署"
	@echo "  make create-app ENV=dev # 首次创建应用"

# ==================== 构建 ====================

build: ## 构建 JAR
	@echo "$(YELLOW)构建 JAR...$(NC)"
	mvn clean package -DskipTests
	@echo "$(GREEN)构建完成: $(JAR_PATH)$(NC)"

# ==================== 部署 ====================

deploy: build upload-jar update-app ## 构建并部署
	@echo "$(GREEN)部署完成$(NC)"

upload-jar: ## 上传 JAR 到 S3
	@echo "$(YELLOW)上传 JAR 到 S3...$(NC)"
	aws s3 cp $(JAR_PATH) s3://$(CODE_BUCKET)/$(S3_JAR_KEY) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)已上传: s3://$(CODE_BUCKET)/$(S3_JAR_KEY)$(NC)"

create-app: build upload-jar ## 首次创建 Flink 应用
	@echo "$(YELLOW)创建 Flink 应用: $(APP_NAME)$(NC)"
	@# 将逗号分隔的子网转换为 JSON 数组
	@SUBNET_JSON=$$(echo '$(SUBNET_IDS)' | sed 's/,/","/g' | sed 's/^/["/;s/$$/"]/' ) && \
	aws kinesisanalyticsv2 create-application \
		--application-name $(APP_NAME) \
		--runtime-environment FLINK-1_20 \
		--service-execution-role $(EXECUTION_ROLE_ARN) \
		--application-configuration "{ \
			\"FlinkApplicationConfiguration\": { \
				\"ParallelismConfiguration\": { \
					\"ConfigurationType\": \"CUSTOM\", \
					\"Parallelism\": $(PARALLELISM), \
					\"ParallelismPerKPU\": 1, \
					\"AutoScalingEnabled\": false \
				}, \
				\"CheckpointConfiguration\": { \
					\"ConfigurationType\": \"CUSTOM\", \
					\"CheckpointingEnabled\": true, \
					\"CheckpointInterval\": 60000, \
					\"MinPauseBetweenCheckpoints\": 5000 \
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
						\"OUTPUT_TOPIC\": \"$(OUTPUT_TOPIC)\" \
					} \
				}] \
			}, \
			\"VpcConfigurations\": [{ \
				\"SubnetIds\": $$SUBNET_JSON, \
				\"SecurityGroupIds\": [\"$(SECURITY_GROUP_ID)\"] \
			}] \
		}" \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)应用已创建$(NC)"

delete-app: ## 删除 Flink 应用
	@echo "$(YELLOW)删除 Flink 应用: $(APP_NAME)$(NC)"
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
	@echo "$(GREEN)应用已删除$(NC)"

update-app: ## 更新应用代码
	@echo "$(YELLOW)更新应用代码...$(NC)"
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
	@echo "$(GREEN)更新完成$(NC)"

# ==================== 应用控制 ====================

start: ## 启动应用
	@echo "$(YELLOW)启动应用: $(APP_NAME)$(NC)"
	aws kinesisanalyticsv2 start-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)启动命令已发送$(NC)"

stop: ## 停止应用
	@echo "$(YELLOW)停止应用: $(APP_NAME)$(NC)"
	aws kinesisanalyticsv2 stop-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE)
	@echo "$(GREEN)停止命令已发送$(NC)"

status: ## 查看状态
	@aws kinesisanalyticsv2 describe-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--query 'ApplicationDetail.{Name:ApplicationName,Status:ApplicationStatus,Version:ApplicationVersionId}' \
		--output table

logs: ## 查看日志
	aws logs tail /aws/kinesis-analytics/$(APP_NAME) \
		--region $(AWS_REGION) \
		--profile $(AWS_PROFILE) \
		--follow

# ==================== 清理 ====================

clean: ## 清理构建
	mvn clean
	rm -rf .build
	@echo "$(GREEN)清理完成$(NC)"
