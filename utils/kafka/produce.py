#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import enum
import multiprocessing
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# `pip install …`
import kafka  # … kafka-python


class Sync(enum.Enum):
    NONE = "none"
    LEAD = "leader"
    ALL = "all"

    def __str__(self):
        return self.value

    def convert(self):
        values = {
            str(Sync.NONE): "0",
            str(Sync.LEAD): "1",
            str(Sync.ALL): "all",
        }
        return values[self.value]


def main():
    parser = argparse.ArgumentParser(
        description="Produce a single message taken from input"
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
    parser.add_argument(
        "--topic", type=str, required=True, help="name of Kafka topic to store in"
    )
    parser.add_argument(
        "--retries", type=int, default=0, help="number of retries to send on failure"
    )
    parser.add_argument(
        "--multiply", type=int, default=1, help="multiplies incoming string many times"
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="send same (multiplied) message many times",
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--jobs",
        type=int,
        default=multiprocessing.cpu_count(),
        help="number of concurrent jobs",
    )
    mode_group.add_argument(
        "--delay",
        type=int,
        metavar="SECONDS",
        default=0,
        help="delay before sending next message",
    )

    args = parser.parse_args()
    config = {
        "bootstrap_servers": f"{args.server}:{args.port}",
        "client_id": args.client,
        "retries": args.retries,
    }
    client = kafka.KafkaProducer(**config)

    message = sys.stdin.buffer.read() * args.multiply

    def send(num):
        if args.delay > 0:
            time.sleep(args.delay)
        client.send(topic=args.topic, value=message)
        print(f"iteration {num}: sent a message multiplied {args.multiply} times")

    if args.delay > 0:
        args.jobs = 1
    pool = ThreadPoolExecutor(max_workers=args.jobs)
    for num in range(args.repeat):
        pool.submit(send, num)
    pool.shutdown()

    client.flush()
    client.close()
    return 0


if __name__ == "__main__":
    exit(main())
