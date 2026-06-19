#!/usr/bin/env python3

from __future__ import annotations

import time

import pulsar

from non_persist.support import assert_no_message, non_persistent_runtime_topic
from non_persist.test_non_persist_dynamic_consumers import _find_key_in_range


def test_non_persist_shared_negative_ack_does_not_trigger_redelivery(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        return

    topic = non_persistent_runtime_topic(unique_name, "np-negative-ack")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-negative-ack",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
            negative_ack_redelivery_delay_ms=500,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"negative-ack-once")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"negative-ack-once"
        consumer.negative_acknowledge(message)

        time.sleep(1.0)
        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_shared_ack_timeout_does_not_trigger_redelivery(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        return

    topic = non_persistent_runtime_topic(unique_name, "np-ack-timeout")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-ack-timeout",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
            unacked_messages_timeout_ms=10001,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"timeout-once")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"timeout-once"

        time.sleep(12.0)
        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_shared_explicit_redelivery_command_does_not_redeliver(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        return

    topic = non_persistent_runtime_topic(unique_name, "np-explicit-redelivery")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-explicit-redelivery",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"redelivery-once")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"redelivery-once"

        consumer.redeliver_unacknowledged_messages()
        time.sleep(1.0)
        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_key_shared_explicit_redelivery_command_does_not_redeliver(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        return

    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-explicit-redelivery")
    subscription = unique_name("np-sub")
    sticky_key = _find_key_in_range("np-ks-redelivery", 0, 32767)
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="key-shared-explicit-redelivery",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                sticky_ranges=[(0, 32767)],
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"key-shared-redelivery-once", ordering_key=sticky_key)
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"key-shared-redelivery-once"

        consumer.redeliver_unacknowledged_messages()
        time.sleep(1.0)
        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()
