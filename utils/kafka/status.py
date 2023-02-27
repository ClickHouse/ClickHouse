#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# `pip install …`
import kafka  # … kafka-python

import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Kafka client to get groups and topics status"
    )
    parser.add_argument(
        "--server",
        type=str,
        metavar="HOST",
        default="localhost",
        help="Kafka bootstrap-server address",
    )
    parser.add_argument(
        "--port",
        type=int,
        metavar="PORT",
        default=9092,
        help="Kafka bootstrap-server port",
    )
    parser.add_argument(
        "--client",
        type=str,
        default="ch-kafka-python",
        help="custom client id for this producer",
    )

    args = parser.parse_args()
    config = {
        "bootstrap_servers": f"{args.server}:{args.port}",
        "client_id": args.client,
    }

    client = kafka.KafkaAdminClient(**config)
    consumer = kafka.KafkaConsumer(**config)
    cluster = client._client.cluster

    topics = cluster.topics()
    for topic in topics:
        print(f'Topic "{topic}":', end="")
        for partition in cluster.partitions_for_topic(topic):
            tp = kafka.TopicPartition(topic, partition)
            print(
                f" {partition} (begin: {consumer.beginning_offsets([tp])[tp]}, end: {consumer.end_offsets([tp])[tp]})",
                end="",
            )
        print()

    groups = client.list_consumer_groups()
    for group in groups:
        print(f'Group "{group[0]}" ({group[1]}):')

        consumer = kafka.KafkaConsumer(**config, group_id=group[0])
        offsets = client.list_consumer_group_offsets(group[0])
        for topic, offset in offsets.items():
            print(
                f"\t{topic.topic}[{topic.partition}]: {consumer.beginning_offsets([topic])[topic]}, {offset.offset}, {consumer.end_offsets([topic])[topic]}"
            )
        consumer.close()

    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
