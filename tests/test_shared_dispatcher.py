#!/usr/bin/env python3
"""Shared subscription dispatcher tests using the official Python client."""

from __future__ import annotations

import threading
from collections import defaultdict

import pulsar

from test_support import persistent_topic


def _consume_messages_until_done(
    consumer,
    consumer_name,
    target_total,
    received,
    errors,
    lock,
    done,
):
    try:
        while not done.is_set():
            try:
                message = consumer.receive(timeout_millis=1000)
            except pulsar.Timeout:
                with lock:
                    if sum(len(messages) for messages in received.values()) >= target_total:
                        done.set()
                        return
                continue

            consumer.acknowledge(message)
            with lock:
                received[consumer_name].append(message.data())
                if sum(len(messages) for messages in received.values()) >= target_total:
                    done.set()
                    return
    except Exception as exc:  # pragma: no cover - surfaced in assertions below
        errors.append((consumer_name, exc))
        done.set()


def test_shared_round_robin_distribution(broker_url, unique_name):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-round-robin"))
    subscription = unique_name("shared-sub")
    consumers = []
    producer = None

    try:
        for index in range(3):
            consumers.append(
                client.subscribe(
                    topic,
                    subscription,
                    consumer_name=f"consumer-{index}",
                    consumer_type=pulsar.ConsumerType.Shared,
                    initial_position=pulsar.InitialPosition.Earliest,
                    receiver_queue_size=1,
                )
            )

        producer = client.create_producer(topic)
        payloads = [f"message-{index}".encode() for index in range(30)]
        for payload in payloads:
            producer.send(payload)

        received = defaultdict(list)
        errors = []
        lock = threading.Lock()
        done = threading.Event()
        threads = []
        for index, consumer in enumerate(consumers):
            thread = threading.Thread(
                target=_consume_messages_until_done,
                args=(consumer, f"consumer-{index}", len(payloads), received, errors, lock, done),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join(timeout=20)

        assert done.is_set(), "timed out waiting for all round-robin messages to be consumed"
        assert not errors, f"consumer receive errors: {errors}"
        total_received = sum(len(messages) for messages in received.values())
        assert total_received == len(payloads)

        all_payloads = [payload for messages in received.values() for payload in messages]
        assert len(set(all_payloads)) == len(payloads),f"message duplication"

        distribution = [len(received[f"consumer-{index}"]) for index in range(3)]
        assert min(distribution) >= 5
        assert max(distribution) - min(distribution) <= 10
    finally:
        for consumer in consumers:
            consumer.close()
        if producer is not None:
            producer.close()
        client.close()


def test_shared_multiple_consumers_concurrent(broker_url, unique_name):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-concurrent"))
    subscription = unique_name("shared-sub")
    consumers = []
    producer = None

    try:
        for index in range(5):
            consumers.append(
                client.subscribe(
                    topic,
                    subscription,
                    consumer_name=f"consumer-{index}",
                    consumer_type=pulsar.ConsumerType.Shared,
                    initial_position=pulsar.InitialPosition.Earliest,
                    receiver_queue_size=1,
                )
            )

        producer = client.create_producer(topic)
        payloads = [f"message-{index}".encode() for index in range(100)]
        for payload in payloads:
            producer.send(payload)

        received = []
        errors = []
        lock = threading.Lock()

        def worker(consumer):
            try:
                while True:
                    message = consumer.receive(timeout_millis=2000)
                    consumer.acknowledge(message)
                    with lock:
                        received.append(message.data())
                        if len(received) >= len(payloads):
                            return
            except pulsar.Timeout:
                return
            except Exception as exc:  # pragma: no cover - surfaced in assertions below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(consumer,), daemon=True) for consumer in consumers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert not errors, f"consumer receive errors: {errors}"
        assert len(received) == len(payloads)
        assert len(set(received)) == len(payloads)
    finally:
        for consumer in consumers:
            consumer.close()
        if producer is not None:
            producer.close()
        client.close()
