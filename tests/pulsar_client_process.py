#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import time

import pulsar

READY_SENTINEL = "__PULSAR_LITE_READY__"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--consumer-name", required=True)
    parser.add_argument("--ready-file")
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
        if args.ready_file:
            Path(args.ready_file).write_text(f"{READY_SENTINEL}\n", encoding="utf-8")
        while True:
            time.sleep(1)
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
