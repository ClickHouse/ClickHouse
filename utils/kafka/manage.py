#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

# `pip install …`
import kafka  # … kafka-python


def main():
    parser = argparse.ArgumentParser(description="Kafka Topic manager")
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

    commands = parser.add_mutually_exclusive_group()
    commands.add_argument(
        "--create",
        type=str,
        metavar="TOPIC",
        nargs="+",
        help="create new topic(s) in the cluster",
    )
    commands.add_argument(
        "--delete",
        type=str,
        metavar="TOPIC",
        nargs="+",
        help="delete existing topic(s) from the cluster",
    )

    args = parser.parse_args()
    config = {
        "bootstrap_servers": f"{args.server}:{args.port}",
        "client_id": args.client,
    }

    client = kafka.KafkaAdminClient(**config)
    if args.create:
        print((client.create_topics(args.create)))
    elif args.delete:
        print((client.delete_topics(args.delete)))

    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
