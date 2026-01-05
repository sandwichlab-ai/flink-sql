#!/usr/bin/env python3
"""
发送测试事件到 Kafka

使用:
    python scripts/send_test_events.py --bootstrap-servers <servers> --topic raw-events --count 10
"""

import argparse
import json
import random
import socket
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKTokenProvider(AbstractTokenProvider):
    """MSK IAM Token Provider for kafka-python"""

    def __init__(self, region: str):
        self.region = region

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


def generate_event():
    """生成随机测试事件"""
    event_types = ["page_view", "click", "purchase", "signup", "login"]
    platforms = ["ios", "android", "web"]

    now = datetime.now(timezone.utc)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 1000)}",
        "device_id": f"device_{random.randint(1, 500)}",
        "event_type": random.choice(event_types),
        "event_name": f"test_event_{random.randint(1, 100)}",
        "properties": {
            "source": random.choice(["organic", "paid", "referral"]),
            "campaign": f"campaign_{random.randint(1, 10)}",
            "value": str(random.randint(1, 1000)),
        },
        "app_version": f"1.{random.randint(0, 9)}.{random.randint(0, 99)}",
        "platform": random.choice(platforms),
        "event_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
        "server_time": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


def send_events(bootstrap_servers: str, topic: str, region: str, count: int, interval: float):
    """发送测试事件"""

    print(f"Connecting to: {bootstrap_servers}")
    print(f"Topic: {topic}")
    print(f"Sending {count} events...")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(region),
        client_id=socket.gethostname(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    try:
        for i in range(count):
            event = generate_event()

            # 使用 event_id 作为 key（用于分区）
            future = producer.send(
                topic,
                key=event["event_id"],
                value=event,
            )

            # 等待发送完成
            record_metadata = future.get(timeout=10)

            print(f"[{i+1}/{count}] Sent: {event['event_type']} | user={event['user_id']} | partition={record_metadata.partition}")

            if interval > 0 and i < count - 1:
                time.sleep(interval)

    finally:
        producer.close()

    print(f"\nDone! Sent {count} events to {topic}")


def main():
    parser = argparse.ArgumentParser(description="Send test events to Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="raw-events",
        help="Topic name (default: raw-events)",
    )
    parser.add_argument(
        "--region",
        default="us-west-2",
        help="AWS region (default: us-west-2)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to send (default: 10)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Interval between events in seconds (default: 0.5)",
    )

    args = parser.parse_args()

    send_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        region=args.region,
        count=args.count,
        interval=args.interval,
    )


if __name__ == "__main__":
    main()