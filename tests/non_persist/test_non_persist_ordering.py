#!/usr/bin/env python3

from __future__ import annotations

import pulsar

from non_persist.support import (
    close_quietly,
    non_persistent_runtime_topic,
    receive_from_any,
    wait_for_delivery_ready,
)
from non_persist.test_non_persist_dynamic_consumers import _find_key_in_range


def _receive_payloads(consumer: pulsar.Consumer, count: int) -> list[bytes]:
    received = []
    for _ in range(count):
        message = consumer.receive(timeout_millis=5000)
        received.append(message.data())
        consumer.acknowledge(message)
    return received


def test_non_persist_exclusive_preserves_send_order(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-exclusive-order")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-order",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"exclusive-{index}".encode() for index in range(5)]
        for payload in payloads:
            producer.send(payload)

        assert _receive_payloads(consumer, len(payloads)) == payloads
    finally:
        client.close()


def test_non_persist_exclusive_handoff_preserves_order_after_rejoin(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-exclusive-handoff-order")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-a",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        before_payloads = [f"before-handoff-{index}".encode() for index in range(3)]
        for payload in before_payloads:
            producer.send(payload)

        assert _receive_payloads(consumer_1, len(before_payloads)) == before_payloads

        consumer_1.close()
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-b",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        wait_for_delivery_ready()

        after_payloads = [f"after-handoff-{index}".encode() for index in range(3)]
        for payload in after_payloads:
            producer.send(payload)

        assert _receive_payloads(consumer_2, len(after_payloads)) == after_payloads
    finally:
        client.close()


def test_non_persist_failover_preserves_order_within_active_epochs(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-failover-order")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="failover-a",
            consumer_type=pulsar.ConsumerType.Failover,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="failover-b",
            consumer_type=pulsar.ConsumerType.Failover,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        before_payloads = [f"before-failover-{index}".encode() for index in range(3)]
        for payload in before_payloads:
            producer.send(payload)

        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(first)
        active = owner
        standby = consumer_2 if active is consumer_1 else consumer_1
        before_received = [
            first.data(),
            *_receive_payloads(active, len(before_payloads) - 1),
        ]
        assert before_received == before_payloads

        active.close()

        after_payloads = [f"after-failover-{index}".encode() for index in range(3)]
        for payload in after_payloads:
            producer.send(payload)

        assert _receive_payloads(standby, len(after_payloads)) == after_payloads
    finally:
        client.close()


def test_non_persist_shared_single_consumer_preserves_send_order(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-shared-order")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-order",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"shared-{index}".encode() for index in range(5)]
        for payload in payloads:
            producer.send(payload)

        assert _receive_payloads(consumer, len(payloads)) == payloads
    finally:
        client.close()


def test_non_persist_key_shared_sticky_preserves_same_key_order(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-sticky-order")
    subscription = unique_name("np-sub")
    sticky_key = _find_key_in_range("sticky-order", 0, 32767)
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="sticky-low",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                sticky_ranges=[(0, 32767)],
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="sticky-high",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                sticky_ranges=[(32768, 65535)],
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"sticky-{index}".encode() for index in range(4)]
        for payload in payloads:
            producer.send(payload, ordering_key=sticky_key)

        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(first)
        received = [first.data(), *_receive_payloads(owner, len(payloads) - 1)]

        assert received == payloads
    finally:
        client.close()


def test_non_persist_key_shared_sticky_dynamic_join_preserves_same_key_order(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-sticky-dynamic-order")
    subscription = unique_name("np-sub")
    sticky_key = _find_key_in_range("sticky-dynamic-order", 0, 32767)
    other_key = _find_key_in_range("sticky-dynamic-other", 32768, 65535)
    client = pulsar.Client(broker_url)
    consumer_2 = None

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="sticky-low",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                sticky_ranges=[(0, 32767)],
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"warmup-low", ordering_key=sticky_key)
        warmup = consumer_1.receive(timeout_millis=5000)
        assert warmup.data() == b"warmup-low"
        consumer_1.acknowledge(warmup)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="sticky-high",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.Sticky,
                sticky_ranges=[(32768, 65535)],
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        wait_for_delivery_ready()

        producer.send(b"other-key", ordering_key=other_key)
        owner, other = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        assert owner.consumer_name() == "sticky-high"
        owner.acknowledge(other)

        payloads = [f"sticky-dynamic-{index}".encode() for index in range(4)]
        for payload in payloads:
            producer.send(payload, ordering_key=sticky_key)

        assert _receive_payloads(consumer_1, len(payloads)) == payloads
    finally:
        close_quietly(consumer_2)
        client.close()


def test_non_persist_key_shared_auto_split_preserves_same_key_order(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-auto-order")
    subscription = unique_name("np-sub")
    sticky_key = _find_key_in_range("auto-order", 32768, 65535)
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="auto-1",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.AutoSplit
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="auto-2",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.AutoSplit
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"auto-{index}".encode() for index in range(4)]
        for payload in payloads:
            producer.send(payload, ordering_key=sticky_key)

        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner.acknowledge(first)
        received = [first.data(), *_receive_payloads(owner, len(payloads) - 1)]

        assert received == payloads
    finally:
        client.close()


def test_non_persist_key_shared_auto_split_dynamic_membership_preserves_same_key_order(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-auto-dynamic-order")
    subscription = unique_name("np-sub")
    low_key = _find_key_in_range("auto-dynamic-low", 0, 32767)
    high_key = _find_key_in_range("auto-dynamic-high", 32768, 65535)
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="auto-1",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.AutoSplit
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"warmup-single", ordering_key=high_key)
        warmup = consumer_1.receive(timeout_millis=5000)
        assert warmup.data() == b"warmup-single"
        consumer_1.acknowledge(warmup)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="auto-2",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.AutoSplit
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        wait_for_delivery_ready()

        join_payloads = [f"join-high-{index}".encode() for index in range(4)]
        for payload in join_payloads:
            producer.send(payload, ordering_key=high_key)

        assert _receive_payloads(consumer_2, len(join_payloads)) == join_payloads

        consumer_2.close()
        wait_for_delivery_ready()

        leave_payloads = [f"leave-high-{index}".encode() for index in range(4)]
        for payload in leave_payloads:
            producer.send(payload, ordering_key=high_key)

        assert _receive_payloads(consumer_1, len(leave_payloads)) == leave_payloads

        low_payloads = [f"leave-low-{index}".encode() for index in range(4)]
        for payload in low_payloads:
            producer.send(payload, ordering_key=low_key)

        assert _receive_payloads(consumer_1, len(low_payloads)) == low_payloads
    finally:
        client.close()
