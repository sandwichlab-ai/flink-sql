#!/bin/bash
# Flink 应用管理脚本
# 用法: ./scripts/manage-app.sh <command> [ENV=dev|prod]

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# 默认环境
ENV=${ENV:-dev}

# 加载配置
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
source "$PROJECT_DIR/config/${ENV}.env"

# 设置 AWS Profile
export AWS_PROFILE
export AWS_REGION

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 将逗号分隔的子网转换为 JSON 数组
subnets_to_json() {
    echo "$SUBNET_IDS" | sed 's/,/","/g' | sed 's/^/["/;s/$/"]/'
}

# 创建应用
create_app() {
    log_info "创建 Flink 应用: $APP_NAME"

    SUBNET_JSON=$(subnets_to_json)

    aws kinesisanalyticsv2 create-application \
        --application-name "$APP_NAME" \
        --runtime-environment FLINK-1_20 \
        --service-execution-role "$EXECUTION_ROLE_ARN" \
        --application-configuration "{
            \"FlinkApplicationConfiguration\": {
                \"ParallelismConfiguration\": {
                    \"ConfigurationType\": \"CUSTOM\",
                    \"Parallelism\": $PARALLELISM,
                    \"ParallelismPerKPU\": 1,
                    \"AutoScalingEnabled\": false
                },
                \"CheckpointConfiguration\": {
                    \"ConfigurationType\": \"CUSTOM\",
                    \"CheckpointingEnabled\": true,
                    \"CheckpointInterval\": 60000,
                    \"MinPauseBetweenCheckpoints\": 5000
                }
            },
            \"ApplicationCodeConfiguration\": {
                \"CodeContent\": {
                    \"S3ContentLocation\": {
                        \"BucketARN\": \"arn:aws:s3:::$CODE_BUCKET\",
                        \"FileKey\": \"flink-jars/flink-event-processor-1.0.0.jar\"
                    }
                },
                \"CodeContentType\": \"ZIPFILE\"
            },
            \"EnvironmentProperties\": {
                \"PropertyGroups\": [{
                    \"PropertyGroupId\": \"FlinkApplicationProperties\",
                    \"PropertyMap\": {
                        \"BOOTSTRAP_SERVERS\": \"$BOOTSTRAP_SERVERS\",
                        \"INPUT_TOPIC\": \"$INPUT_TOPIC\",
                        \"OUTPUT_TOPIC\": \"$OUTPUT_TOPIC\"
                    }
                }]
            },
            \"VpcConfigurations\": [{
                \"SubnetIds\": $SUBNET_JSON,
                \"SecurityGroupIds\": [\"$SECURITY_GROUP_ID\"]
            }]
        }" \
        --region "$AWS_REGION"

    log_info "应用创建成功"
}

# 删除应用
delete_app() {
    log_warn "删除 Flink 应用: $APP_NAME"
    read -p "确认删除? (y/N): " confirm
    if [[ "$confirm" =~ ^[Yy]$ ]]; then
        # 先获取创建时间戳
        CREATE_TIMESTAMP=$(aws kinesisanalyticsv2 describe-application \
            --application-name "$APP_NAME" \
            --region "$AWS_REGION" \
            --query 'ApplicationDetail.CreateTimestamp' \
            --output text)

        aws kinesisanalyticsv2 delete-application \
            --application-name "$APP_NAME" \
            --create-timestamp "$CREATE_TIMESTAMP" \
            --region "$AWS_REGION"

        log_info "应用已删除"
    else
        log_info "取消删除"
    fi
}

# 启动应用
start_app() {
    log_info "启动应用: $APP_NAME"

    aws kinesisanalyticsv2 start-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION"

    log_info "启动命令已发送，请使用 'status' 查看状态"
}

# 停止应用
stop_app() {
    log_info "停止应用: $APP_NAME"

    aws kinesisanalyticsv2 stop-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION"

    log_info "停止命令已发送，请使用 'status' 查看状态"
}

# 查看状态
show_status() {
    log_info "应用状态: $APP_NAME"

    aws kinesisanalyticsv2 describe-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" \
        --query 'ApplicationDetail.{
            Name:ApplicationName,
            Status:ApplicationStatus,
            Version:ApplicationVersionId,
            Runtime:RuntimeEnvironment,
            Parallelism:ApplicationConfigurationDescription.FlinkApplicationConfigurationDescription.ParallelismConfigurationDescription.CurrentParallelism
        }' \
        --output table
}

# 更新代码
update_code() {
    log_info "更新应用代码..."

    VERSION=$(aws kinesisanalyticsv2 describe-application \
        --application-name "$APP_NAME" \
        --region "$AWS_REGION" \
        --query 'ApplicationDetail.ApplicationVersionId' \
        --output text)

    aws kinesisanalyticsv2 update-application \
        --application-name "$APP_NAME" \
        --current-application-version-id "$VERSION" \
        --application-configuration-update "{
            \"ApplicationCodeConfigurationUpdate\": {
                \"CodeContentTypeUpdate\": \"ZIPFILE\",
                \"CodeContentUpdate\": {
                    \"S3ContentLocationUpdate\": {
                        \"BucketARNUpdate\": \"arn:aws:s3:::$CODE_BUCKET\",
                        \"FileKeyUpdate\": \"flink-jars/flink-event-processor-1.0.0.jar\"
                    }
                }
            }
        }" \
        --region "$AWS_REGION"

    log_info "代码更新完成"
}

# 查看日志
show_logs() {
    log_info "查看日志: $CLOUDWATCH_LOG_GROUP"

    aws logs tail "$CLOUDWATCH_LOG_GROUP" \
        --region "$AWS_REGION" \
        --follow
}

# 帮助信息
show_help() {
    echo "Flink 应用管理脚本"
    echo ""
    echo "用法: $0 <command> [ENV=dev|prod]"
    echo ""
    echo "命令:"
    echo "  create    创建 Flink 应用"
    echo "  delete    删除 Flink 应用"
    echo "  start     启动应用"
    echo "  stop      停止应用"
    echo "  status    查看状态"
    echo "  update    更新应用代码"
    echo "  logs      查看日志"
    echo "  help      显示帮助"
    echo ""
    echo "示例:"
    echo "  ENV=dev $0 create"
    echo "  ENV=prod $0 status"
}

# 主入口
case "${1:-help}" in
    create)
        create_app
        ;;
    delete)
        delete_app
        ;;
    start)
        start_app
        ;;
    stop)
        stop_app
        ;;
    status)
        show_status
        ;;
    update)
        update_code
        ;;
    logs)
        show_logs
        ;;
    help|*)
        show_help
        ;;
esac
