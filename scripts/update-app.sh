#!/bin/bash
# 更新 Managed Flink 应用代码
set -euo pipefail

echo "更新 Flink 应用: ${APP_NAME}"

# 获取当前应用版本
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name "${APP_NAME}" \
    --query 'ApplicationDetail.ApplicationVersionId' \
    --output text \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}")

echo "当前版本: ${CURRENT_VERSION}"

# 更新应用代码位置
aws kinesisanalyticsv2 update-application \
    --application-name "${APP_NAME}" \
    --current-application-version-id "${CURRENT_VERSION}" \
    --application-configuration-update '{
        "ApplicationCodeConfigurationUpdate": {
            "CodeContentTypeUpdate": "PLAINTEXT",
            "CodeContentUpdate": {
                "S3ContentLocationUpdate": {
                    "BucketARNUpdate": "arn:aws:s3:::'"${CODE_BUCKET}"'",
                    "FileKeyUpdate": "'"${SQL_KEY}"'"
                }
            }
        }
    }' \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}"

# 获取更新后版本
NEW_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name "${APP_NAME}" \
    --query 'ApplicationDetail.ApplicationVersionId' \
    --output text \
    --region "${AWS_REGION}" \
    --profile "${AWS_PROFILE}")

echo "更新完成: 版本 ${CURRENT_VERSION} -> ${NEW_VERSION}"
