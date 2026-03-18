#!/usr/bin/env python3
"""Producer behavior tests using the official Python client."""

from __future__ import annotations

import threading

import pulsar
import pytest

from test_support import persistent_topic


def test_async_send_delivers_all_messages(broker_url, unique_name):
    topic = persistent_topic(unique_name("async-send"))
    subscription = unique_name("async-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="async-consumer",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False, block_if_queue_full=True)

        payloads = [f"async-{index}".encode() for index in range(5)]
        callback_results = []
        callback_errors = []
        done = threading.Event()

        def callback(result, msg_id):
            if result != pulsar.Result.Ok:
                callback_errors.append(result)
            else:
                callback_results.append(msg_id)
            if len(callback_results) + len(callback_errors) == len(payloads):
                done.set()

        for payload in payloads:
            producer.send_async(payload, callback)

        producer.flush()
        assert done.wait(10), "timed out waiting for async send callbacks"
        assert not callback_errors
        assert len(callback_results) == len(payloads)

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert set(received) == set(payloads)
    finally:
        client.close()


@pytest.mark.skip(reason="pulsar-lite message metadata for batched payload decoding is not implemented yet")
def test_batched_send_delivers_all_messages(broker_url, unique_name):
    topic = persistent_topic(unique_name("batched-send"))
    subscription = unique_name("batched-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="batch-consumer",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(
            topic,
            batching_enabled=True,
            batching_max_messages=5,
            batching_max_publish_delay_ms=50,
        )

        payloads = [f"batch-{index}".encode() for index in range(10)]
        for payload in payloads:
            producer.send(payload)
        producer.flush()

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert set(received) == set(payloads)
    finally:
        client.close()


@pytest.mark.skip(reason="pulsar-lite message metadata for batched payload decoding is not implemented yet")
def test_compressed_send_preserves_payload(broker_url, unique_name):
    topic = persistent_topic(unique_name("compressed-send"))
    subscription = unique_name("compressed-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="compressed-consumer",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(
            topic,
            compression_type=pulsar.CompressionType.ZLib,
            batching_enabled=False,
        )

        payload = (b"compressible-payload-" * 64)
        producer.send(payload)

        message = consumer.receive(timeout_millis=5000)
        assert message.data() == payload
        consumer.acknowledge(message)
    finally:
        client.close()
