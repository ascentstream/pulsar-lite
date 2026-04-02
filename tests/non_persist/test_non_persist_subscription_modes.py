#!/usr/bin/env python3

from __future__ import annotations

import pulsar
import pytest

from non_persist.support import (
    assert_no_message,
    non_persistent_runtime_topic,
    receive_from_any,
    wait_for_delivery_ready,
)


def test_non_persist_exclusive_rejects_second_consumer(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-exclusive")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        first = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-1",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        with pytest.raises(Exception):
            client.subscribe(
                topic,
                subscription,
                consumer_name="exclusive-2",
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )

        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()
        producer.send(b"exclusive-message")
        message = first.receive(timeout_millis=5000)
        assert message.data() == b"exclusive-message"
        first.acknowledge(message)
    finally:
        client.close()


def test_non_persist_failover_promotes_standby_after_active_closes(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-failover")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="failover-a",
            consumer_type=pulsar.ConsumerType.Failover,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="failover-b",
            consumer_type=pulsar.ConsumerType.Failover,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"before-failover")
        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(first)
        standby = consumer_2 if owner is consumer_1 else consumer_1
        assert_no_message(standby, timeout_millis=1000)

        owner.close()

        producer.send(b"after-failover")
        promoted = standby.receive(timeout_millis=10000)
        assert promoted.data() == b"after-failover"
        standby.acknowledge(promoted)
    finally:
        client.close()


def test_non_persist_shared_distributes_messages_across_consumers(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-shared")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [b"shared-0", b"shared-1", b"shared-2", b"shared-3"]
        for payload in payloads:
            producer.send(payload)

        owners = set()
        seen = set()
        for _ in payloads:
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            owners.add(owner.consumer_name())
            seen.add(message.data())
            owner.acknowledge(message)

        assert seen == set(payloads)
        assert owners == {"shared-1", "shared-2"}
    finally:
        client.close()


def test_non_persist_key_shared_routes_same_key_to_same_consumer(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="ks-1",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="ks-2",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"alpha-1", ordering_key="alpha")
        producer.send(b"alpha-2", ordering_key="alpha")

        owner_1, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner_2, second = receive_from_any([consumer_1, consumer_2], timeout_secs=10)

        assert owner_1.consumer_name() == owner_2.consumer_name()
        assert {first.data(), second.data()} == {b"alpha-1", b"alpha-2"}
        owner_1.acknowledge(first)
        owner_2.acknowledge(second)
    finally:
        client.close()
