#!/usr/bin/env python3
"""Basic Shared subscription smoke test using the official Python client."""

from __future__ import annotations

import pulsar

from test_support import persistent_topic


def test_simple_shared(broker_url, unique_name):
    topic = persistent_topic(unique_name("simple-shared"))
    subscription = unique_name("simple-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        producer.send(b"hello-world")

        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"hello-world"
        consumer.acknowledge(message)
    finally:
        client.close()
