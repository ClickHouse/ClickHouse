#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# `pip install …`
import kafka  # … kafka-python

import argparse
from pprint import pprint


def main():
    parser = argparse.ArgumentParser(description='Kafka Producer client')
    parser.add_argument('--server', type=str, metavar='HOST', default='localhost',
        help='Kafka bootstrap-server address')
    parser.add_argument('--port', type=int, metavar='PORT', default=9092,
        help='Kafka bootstrap-server port')
    parser.add_argument('--client', type=str, default='ch-kafka-python',
        help='custom client id for this producer')
    parser.add_argument('--topic', type=str, required=True,
        help='name of Kafka topic to store in')
    parser.add_argument('--group', type=str, required=True,
        help='name of the consumer group')

    args = parser.parse_args()
    config = {
        'bootstrap_servers': f'{args.server}:{args.port}',
        'client_id': args.client,
        'group_id': args.group,
        'auto_offset_reset': 'earliest',
    }
    client = kafka.KafkaConsumer(**config)

    client.subscribe([args.topic])
    pprint(client.poll(10000))
    client.unsubscribe()
    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
