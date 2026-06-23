#!/usr/bin/env python3
"""High-level producer smoke tests against the Pulsar Lite broker."""

from __future__ import annotations

import pulsar
from test_support import persistent_topic


def test_producer(broker_url, unique_name):
    topic = persistent_topic(unique_name("binary-producer"))
    subscription = unique_name("binary-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="binary-consumer",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        payloads = [f"Message {index}".encode("utf-8") for index in range(5)]
        message_ids = [producer.send(payload) for payload in payloads]

        assert all(msg_id is not None for msg_id in message_ids)

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert received == payloads
    finally:
        client.close()
