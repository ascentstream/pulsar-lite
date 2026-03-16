#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time

import pulsar


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--consumer-name", required=True)
    args = parser.parse_args()

    client = pulsar.Client(args.url)
    consumer = client.subscribe(
        args.topic,
        args.subscription,
        consumer_name=args.consumer_name,
        consumer_type=pulsar.ConsumerType.Shared,
        initial_position=pulsar.InitialPosition.Earliest,
        receiver_queue_size=1,
    )

    try:
        print("READY", flush=True)
        while True:
            time.sleep(1)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
