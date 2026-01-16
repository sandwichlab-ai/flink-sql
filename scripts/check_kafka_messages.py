#!/usr/bin/env python3
"""
检查 Kafka Topic 的消息数量（使用 boto3 调用 MSK API）
"""
import boto3
import json
from datetime import datetime, timedelta

def check_kafka_lag():
    """检查 Kafka 消费者 lag"""
    try:
        # 使用 boto3 客户端
        client = boto3.client('kafka', region_name='us-west-2')
        
        # 获取集群 ARN（Serverless）
        # 注意：MSK Serverless 没有直接的 API 来查看 consumer lag
        print("⚠️  MSK Serverless 无法直接通过 boto3 查询消费者 lag")
        print("需要使用 Kafka 客户端工具（kafka-console-consumer）")
        
        # 建议使用 CloudWatch Metrics
        cloudwatch = boto3.client('cloudwatch', region_name='us-west-2')
        
        # 查询最近 10 分钟的 Flink 应用 Records In 指标
        response = cloudwatch.get_metric_statistics(
            Namespace='AWS/KinesisAnalytics',
            MetricName='millisBehindLatest',
            Dimensions=[
                {
                    'Name': 'Application',
                    'Value': 'event-processor-dev'
                }
            ],
            StartTime=datetime.utcnow() - timedelta(minutes=10),
            EndTime=datetime.utcnow(),
            Period=60,
            Statistics=['Average', 'Maximum']
        )
        
        print("\n📊 Flink Application Lag (millisBehindLatest):")
        print(json.dumps(response['Datapoints'], indent=2, default=str))
        
        # 查询 Records In 指标
        response2 = cloudwatch.get_metric_statistics(
            Namespace='AWS/KinesisAnalytics',
            MetricName='numRecordsInPerSecond',
            Dimensions=[
                {
                    'Name': 'Application',
                    'Value': 'event-processor-dev'
                }
            ],
            StartTime=datetime.utcnow() - timedelta(minutes=10),
            EndTime=datetime.utcnow(),
            Period=60,
            Statistics=['Sum', 'Average']
        )
        
        print("\n📊 Flink Application Records In Per Second:")
        if response2['Datapoints']:
            for dp in sorted(response2['Datapoints'], key=lambda x: x['Timestamp'], reverse=True)[:10]:
                print(f"  {dp['Timestamp']}: {dp.get('Sum', 0):.2f} records/sec (avg: {dp.get('Average', 0):.2f})")
        else:
            print("  No data found")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_kafka_lag()
