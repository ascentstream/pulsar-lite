#!/usr/bin/env python3

from __future__ import annotations

import pulsar
import pytest

from non_persist.support import non_persistent_runtime_topic, receive_from_any


def test_non_persist_shared_disconnect_drops_unacked_message(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        pytest.skip(
            "non-persistent shared ack semantics tests require a non-partitioned broker; "
            "set default_partitions = 0 before running them."
        )
    topic = non_persistent_runtime_topic(unique_name, "np-shared-redelivery")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-owner",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-survivor",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"acked-first")
        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(first)

        producer.send(b"redeliver-second")
        owner, second = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        survivor = consumer_2 if owner is consumer_1 else consumer_1
        owner.close()

        try:
            unexpected = survivor.receive(timeout_millis=1500)
            raise AssertionError(
                f"unexpected non-persistent redelivery after disconnect: {unexpected.data()!r}"
            )
        except pulsar.Timeout:
            pass
    finally:
        client.close()


def test_non_persist_shared_acked_message_is_not_redelivered_after_owner_closes(
    broker_url, broker_timing, unique_name
):
    if broker_timing.get("default_partitions", 0) > 0:
        pytest.skip(
            "non-persistent shared ack semantics tests require a non-partitioned broker; "
            "set default_partitions = 0 before running them."
        )
    topic = non_persistent_runtime_topic(unique_name, "np-shared-no-redelivery")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-owner",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-survivor",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"acked-once")
        owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(message)
        survivor = consumer_2 if owner is consumer_1 else consumer_1
        owner.close()

        try:
            unexpected = survivor.receive(timeout_millis=1500)
            raise AssertionError(
                f"unexpected redelivery after ack: {unexpected.data()!r}"
            )
        except pulsar.Timeout:
            pass
    finally:
        client.close()
