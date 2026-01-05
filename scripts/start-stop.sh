#!/bin/bash
# Flink 应用启停控制
set -euo pipefail

ACTION="${1:-status}"

case "${ACTION}" in
    start)
        echo "启动应用: ${APP_NAME}"
        aws kinesisanalyticsv2 start-application \
            --application-name "${APP_NAME}" \
            --region "${AWS_REGION}" \
            --profile "${AWS_PROFILE}"
        echo "启动命令已发送"
        ;;

    stop)
        echo "停止应用: ${APP_NAME}"
        aws kinesisanalyticsv2 stop-application \
            --application-name "${APP_NAME}" \
            --region "${AWS_REGION}" \
            --profile "${AWS_PROFILE}"
        echo "停止命令已发送"
        ;;

    status)
        echo "应用状态: ${APP_NAME}"
        aws kinesisanalyticsv2 describe-application \
            --application-name "${APP_NAME}" \
            --region "${AWS_REGION}" \
            --profile "${AWS_PROFILE}" \
            --query 'ApplicationDetail.{
                Name:ApplicationName,
                Status:ApplicationStatus,
                Version:ApplicationVersionId,
                Runtime:RuntimeEnvironment,
                Parallelism:ApplicationConfigurationDescription.FlinkApplicationConfigurationDescription.ParallelismConfigurationDescription.Parallelism,
                AutoScaling:ApplicationConfigurationDescription.FlinkApplicationConfigurationDescription.ParallelismConfigurationDescription.AutoScalingEnabled
            }' \
            --output table
        ;;

    restart)
        echo "重启应用: ${APP_NAME}"
        $0 stop
        echo "等待停止..."
        sleep 30
        $0 start
        ;;

    *)
        echo "用法: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac
