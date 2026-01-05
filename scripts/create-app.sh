#!/bin/bash
# 创建 Managed Flink 应用
set -euo pipefail

echo "创建 Flink 应用: ${APP_NAME}"

# 获取 VPC 配置 (从 SSM)
VPC_ID=$(aws ssm get-parameter \
    --name "/lexi2/${ENV:-dev}/network/vpc_id" \
    --query 'Parameter.Value' \
    --output text \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}" 2>/dev/null || echo "")

SUBNET_IDS=$(aws ssm get-parameter \
    --name "/lexi2/${ENV:-dev}/network/private_subnet_ids" \
    --query 'Parameter.Value' \
    --output text \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}" 2>/dev/null || echo "")

SECURITY_GROUP_ID=$(aws ssm get-parameter \
    --name "/lexi2/${ENV:-dev}/infra-streaming/msk_security_group_id" \
    --query 'Parameter.Value' \
    --output text \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}" 2>/dev/null || echo "")

# 创建应用
aws kinesisanalyticsv2 create-application \
    --application-name "${APP_NAME}" \
    --runtime-environment "${RUNTIME}" \
    --service-execution-role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${APP_NAME}-execution-role" \
    --application-configuration '{
        "FlinkApplicationConfiguration": {
            "ParallelismConfiguration": {
                "ConfigurationType": "CUSTOM",
                "Parallelism": '"${PARALLELISM:-1}"',
                "ParallelismPerKPU": '"${PARALLELISM_PER_KPU:-1}"',
                "AutoScalingEnabled": '"${AUTOSCALING_ENABLED:-false}"'
            },
            "CheckpointConfiguration": {
                "ConfigurationType": "CUSTOM",
                "CheckpointingEnabled": true,
                "CheckpointInterval": 60000,
                "MinPauseBetweenCheckpoints": 5000
            }
        },
        "ApplicationCodeConfiguration": {
            "CodeContent": {
                "S3ContentLocation": {
                    "BucketARN": "arn:aws:s3:::'"${CODE_BUCKET}"'",
                    "FileKey": "'"${SQL_KEY}"'"
                }
            },
            "CodeContentType": "PLAINTEXT"
        },
        "EnvironmentProperties": {
            "PropertyGroups": [
                {
                    "PropertyGroupId": "FlinkApplicationProperties",
                    "PropertyMap": {
                        "INPUT_TOPIC": "'"${INPUT_TOPIC}"'",
                        "OUTPUT_TOPIC": "'"${OUTPUT_TOPIC}"'",
                        "BOOTSTRAP_SERVERS": "'"${BOOTSTRAP_SERVERS}"'"
                    }
                }
            ]
        },
        "VpcConfigurations": [
            {
                "SubnetIds": ['"$(echo $SUBNET_IDS | sed 's/,/","/g')"'],
                "SecurityGroupIds": ["'"${SECURITY_GROUP_ID}"'"]
            }
        ]
    }' \
    --cloud-watch-logging-options '[{
        "LogStreamARN": "arn:aws:logs:'"${AWS_REGION}"':'"${AWS_ACCOUNT_ID}"':log-group:'"${CLOUDWATCH_LOG_GROUP}"':log-stream:flink-app-log"
    }]' \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}"

echo "应用创建成功: ${APP_NAME}"
echo ""
echo "下一步:"
echo "  1. 确保 IAM 执行角色已创建: ${APP_NAME}-execution-role"
echo "  2. 启动应用: make start ENV=${ENV:-dev}"
