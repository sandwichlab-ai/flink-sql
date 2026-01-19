#!/usr/bin/env python3
"""
创建 DynamoDB 表用于存储点击事件

表结构:
- Partition Key: fingerprint (匿名用户 ID)
- Sort Key: click_id (点击参数值)
- 属性: user_id, click_time, utm_json

使用:
    python3 scripts/create_dynamodb_table.py \
        --table-name click_events_dev \
        --region us-west-2 \
        --profile dev-us
"""

import argparse
import boto3
import sys
from botocore.exceptions import ClientError


def create_table(table_name: str, region: str, profile: str = None):
    """创建 DynamoDB 表"""

    # 创建 DynamoDB 客户端
    session = boto3.Session(profile_name=profile, region_name=region)
    dynamodb = session.client('dynamodb')

    try:
        # 检查表是否已存在
        try:
            response = dynamodb.describe_table(TableName=table_name)
            print(f"✓ 表 '{table_name}' 已存在")
            print(f"  状态: {response['Table']['TableStatus']}")
            print(f"  ARN: {response['Table']['TableArn']}")
            return
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise

        print(f"创建表: {table_name}")

        # 创建表
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'fingerprint',
                    'KeyType': 'HASH'  # Partition Key
                },
                {
                    'AttributeName': 'click_id',
                    'KeyType': 'RANGE'  # Sort Key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'fingerprint',
                    'AttributeType': 'S'  # String
                },
                {
                    'AttributeName': 'click_id',
                    'AttributeType': 'S'  # String
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # On-Demand 计费
            Tags=[
                {'Key': 'Application', 'Value': 'event-processor'},
                {'Key': 'ManagedBy', 'Value': 'flink-sql'},
                {'Key': 'Environment', 'Value': 'dev' if 'dev' in table_name else 'prod'}
            ]
        )

        print(f"✓ 表创建请求已提交")
        print(f"  ARN: {response['TableDescription']['TableArn']}")
        print(f"  状态: {response['TableDescription']['TableStatus']}")
        print(f"\n等待表变为 ACTIVE 状态...")

        # 等待表创建完成
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(
            TableName=table_name,
            WaiterConfig={'Delay': 5, 'MaxAttempts': 40}
        )

        # 获取最终状态
        response = dynamodb.describe_table(TableName=table_name)
        print(f"\n✓ 表创建成功!")
        print(f"  表名: {table_name}")
        print(f"  状态: {response['Table']['TableStatus']}")
        print(f"  Partition Key: fingerprint (S)")
        print(f"  Sort Key: click_id (S)")
        print(f"  计费模式: On-Demand")

    except ClientError as e:
        print(f"✗ 创建表失败: {e.response['Error']['Message']}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"✗ 错误: {str(e)}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='创建 DynamoDB 表用于存储点击事件',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 创建 dev 环境的表
  python3 scripts/create_dynamodb_table.py \\
      --table-name click_events_dev \\
      --region us-west-2 \\
      --profile dev-us

  # 创建 prod 环境的表
  python3 scripts/create_dynamodb_table.py \\
      --table-name click_events_prod \\
      --region us-west-2 \\
      --profile prod-us
        """
    )

    parser.add_argument(
        '--table-name',
        required=True,
        help='DynamoDB 表名 (例如: click_events_dev)'
    )
    parser.add_argument(
        '--region',
        required=True,
        help='AWS Region (例如: us-west-2)'
    )
    parser.add_argument(
        '--profile',
        help='AWS Profile (可选)'
    )

    args = parser.parse_args()

    print(f"DynamoDB 表创建工具")
    print(f"==================")
    print(f"表名: {args.table_name}")
    print(f"Region: {args.region}")
    if args.profile:
        print(f"Profile: {args.profile}")
    print()

    create_table(args.table_name, args.region, args.profile)


if __name__ == '__main__':
    main()
