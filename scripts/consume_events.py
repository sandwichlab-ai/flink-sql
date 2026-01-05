#!/usr/bin/env python3
"""
消费 Kafka Topic 中的事件

使用:
    python scripts/consume_events.py --bootstrap-servers <servers> --topic processed-events --count 10
"""

import argparse
import json
import socket

from kafka import KafkaConsumer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider


class MSKTokenProvider(AbstractTokenProvider):
    """MSK IAM Token Provider for kafka-python"""

    def __init__(self, region: str):
        self.region = region

    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


def consume_events(bootstrap_servers: str, topic: str, region: str, count: int):
    """消费事件"""

    print(f"Connecting to: {bootstrap_servers}")
    print(f"Topic: {topic}")
    print(f"Consuming up to {count} events...")
    print("-" * 60)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(region),
        client_id=socket.gethostname(),
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,  # 10秒超时
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    try:
        received = 0
        for message in consumer:
            received += 1
            print(f"[{received}] partition={message.partition} offset={message.offset}")
            print(f"    {json.dumps(message.value, indent=2, ensure_ascii=False)}")
            print()

            if received >= count:
                break

        if received == 0:
            print("No messages found in topic (within timeout)")
        else:
            print(f"\nDone! Received {received} events from {topic}")

    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Consume events from Kafka")
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="processed-events",
        help="Topic name (default: processed-events)",
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
        help="Max number of events to consume (default: 10)",
    )

    args = parser.parse_args()

    consume_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        region=args.region,
        count=args.count,
    )


if __name__ == "__main__":
    main()
