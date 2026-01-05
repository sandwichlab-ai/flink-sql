#!/usr/bin/env python3
"""
创建 Kafka Topics (支持 MSK Serverless IAM 认证)

使用:
    python scripts/create_topics.py --bootstrap-servers <servers> --topics raw-events,processed-events

依赖:
    pip install kafka-python aws-msk-iam-sasl-signer-python
"""

import argparse
import socket
import sys
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


def get_msk_token(region: str):
    """获取 MSK IAM 认证 token"""
    def token_provider():
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token
    return token_provider


def create_topics(bootstrap_servers: str, topics: list[str], region: str, num_partitions: int = 3):
    """创建 Kafka topics"""

    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Topics to create: {topics}")
    print(f"Region: {region}")

    # MSK IAM 认证配置
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=get_msk_token(region),
        client_id=socket.gethostname(),
    )

    # 创建 topic 列表
    new_topics = [
        NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=3,  # MSK Serverless 固定为 3
        )
        for topic in topics
    ]

    # 创建 topics
    for topic in new_topics:
        try:
            admin_client.create_topics([topic], validate_only=False)
            print(f"Created topic: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"Topic already exists: {topic.name}")
        except Exception as e:
            print(f"Failed to create topic {topic.name}: {e}")
            sys.exit(1)

    # 列出所有 topics
    print("\nExisting topics:")
    for topic in admin_client.list_topics():
        print(f"  - {topic}")

    admin_client.close()
    print("\nDone!")


def main():
    parser = argparse.ArgumentParser(description="Create Kafka topics on MSK Serverless")
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="MSK bootstrap servers (e.g., boot-xxx.kafka-serverless.us-west-2.amazonaws.com:9098)",
    )
    parser.add_argument(
        "--topics",
        required=True,
        help="Comma-separated list of topics to create",
    )
    parser.add_argument(
        "--region",
        default="us-west-2",
        help="AWS region (default: us-west-2)",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=3,
        help="Number of partitions per topic (default: 3)",
    )

    args = parser.parse_args()
    topics = [t.strip() for t in args.topics.split(",")]

    create_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        region=args.region,
        num_partitions=args.partitions,
    )


if __name__ == "__main__":
    main()