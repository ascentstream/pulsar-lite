#!/usr/bin/env python3

from __future__ import annotations

import pulsar

from non_persist.support import (
    assert_no_message,
    close_quietly,
    non_persistent_runtime_topic,
    receive_from_any,
    wait_for_delivery_ready,
)


def _murmur3_32(data: bytes, seed: int = 0) -> int:
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    h1 = seed
    length = len(data)
    rounded_end = length & 0xFFFFFFFC

    for offset in range(0, rounded_end, 4):
        k1 = int.from_bytes(data[offset : offset + 4], "little")
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF

        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    k1 = 0
    tail = length & 0x03
    if tail == 3:
        k1 ^= data[rounded_end + 2] << 16
    if tail >= 2:
        k1 ^= data[rounded_end + 1] << 8
    if tail >= 1:
        k1 ^= data[rounded_end]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= length
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return h1


def _sticky_hash(key: str) -> int:
    return _murmur3_32(key.encode("utf-8")) % 65536


def _find_key_in_range(prefix: str, start: int, end: int) -> str:
    for index in range(10000):
        candidate = f"{prefix}-{index}"
        hash_value = _sticky_hash(candidate)
        if start <= hash_value <= end:
            return candidate
    raise AssertionError(f"failed to find sticky key for range {start}-{end}")


def test_non_persist_shared_new_consumer_only_sees_live_traffic_and_shares_load(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-shared-add-consumer")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)
    consumer_2 = None

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"before-join")
        before_join = consumer_1.receive(timeout_millis=5000)
        assert before_join.data() == b"before-join"
        consumer_1.acknowledge(before_join)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        wait_for_delivery_ready()

        post_join_payloads = [f"after-join-{index}".encode() for index in range(6)]
        for payload in post_join_payloads:
            producer.send(payload)

        seen_payloads = set()
        owners = set()
        for _ in post_join_payloads:
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            seen_payloads.add(message.data())
            owners.add(owner.consumer_name())
            owner.acknowledge(message)

        assert seen_payloads == set(post_join_payloads)
        assert owners == {"shared-1", "shared-2"}
    finally:
        close_quietly(consumer_2)
        client.close()


def test_non_persist_shared_survivor_continues_after_other_consumer_closes(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-shared-remove-consumer")
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

        warmup_payloads = [f"warmup-{index}".encode() for index in range(4)]
        for payload in warmup_payloads:
            producer.send(payload)

        warmup_owners = set()
        for _ in warmup_payloads:
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            warmup_owners.add(owner.consumer_name())
            owner.acknowledge(message)

        assert warmup_owners == {"shared-1", "shared-2"}

        consumer_2.close()

        followup_payloads = [f"after-close-{index}".encode() for index in range(4)]
        for payload in followup_payloads:
            producer.send(payload)

        seen = []
        for _ in followup_payloads:
            message = consumer_1.receive(timeout_millis=5000)
            seen.append(message.data())
            consumer_1.acknowledge(message)

        assert seen == followup_payloads
    finally:
        client.close()


def test_non_persist_key_shared_new_consumer_keeps_existing_key_stable_and_routes_new_key(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-dynamic")
    subscription = unique_name("np-sub")
    existing_key = _find_key_in_range("low-key", 0, 32767)
    new_key = _find_key_in_range("high-key", 32768, 65535)
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

        producer.send(b"before-add-existing", ordering_key=existing_key)
        first = consumer_1.receive(timeout_millis=5000)
        assert first.data() == b"before-add-existing"
        consumer_1.acknowledge(first)

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

        producer.send(b"after-add-existing", ordering_key=existing_key)
        producer.send(b"after-add-new", ordering_key=new_key)

        deliveries = {}
        for _ in range(2):
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            deliveries[message.data()] = owner.consumer_name()
            owner.acknowledge(message)

        assert deliveries[b"after-add-existing"] == "sticky-low"
        assert deliveries[b"after-add-new"] == "sticky-high"
    finally:
        close_quietly(consumer_2)
        client.close()


def test_non_persist_key_shared_sticky_survivor_keeps_own_range_and_does_not_take_removed_range(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-sticky-remove")
    subscription = unique_name("np-sub")
    low_key = _find_key_in_range("low-key", 0, 32767)
    high_key = _find_key_in_range("high-key", 32768, 65535)
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

        producer.send(b"warmup-low", ordering_key=low_key)
        producer.send(b"warmup-high", ordering_key=high_key)

        warmup_deliveries = {}
        for _ in range(2):
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            warmup_deliveries[message.data()] = owner.consumer_name()
            owner.acknowledge(message)

        assert warmup_deliveries[b"warmup-low"] == "sticky-low"
        assert warmup_deliveries[b"warmup-high"] == "sticky-high"

        consumer_2.close()
        wait_for_delivery_ready()

        producer.send(b"after-close-low", ordering_key=low_key)
        producer.send(b"after-close-high", ordering_key=high_key)

        low_message = consumer_1.receive(timeout_millis=5000)
        assert low_message.data() == b"after-close-low"
        consumer_1.acknowledge(low_message)
        assert_no_message(consumer_1, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_key_shared_auto_split_new_consumer_shares_live_keys(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-auto-add")
    subscription = unique_name("np-sub")
    low_key = _find_key_in_range("auto-low", 0, 32767)
    high_key = _find_key_in_range("auto-high", 32768, 65535)
    client = pulsar.Client(broker_url)
    consumer_2 = None

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

        producer.send(b"before-add", ordering_key=high_key)
        first = consumer_1.receive(timeout_millis=5000)
        assert first.data() == b"before-add"
        consumer_1.acknowledge(first)

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

        producer.send(b"low-0", ordering_key=low_key)
        producer.send(b"low-1", ordering_key=low_key)
        producer.send(b"high-0", ordering_key=high_key)
        producer.send(b"high-1", ordering_key=high_key)

        deliveries = {}
        for _ in range(4):
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            deliveries[message.data()] = owner.consumer_name()
            owner.acknowledge(message)

        assert deliveries[b"low-0"] == "auto-1"
        assert deliveries[b"low-1"] == "auto-1"
        assert deliveries[b"high-0"] == "auto-2"
        assert deliveries[b"high-1"] == "auto-2"
    finally:
        close_quietly(consumer_2)
        client.close()


def test_non_persist_key_shared_auto_split_survivor_continues_after_other_consumer_closes(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-auto-remove")
    subscription = unique_name("np-sub")
    low_key = _find_key_in_range("auto-low", 0, 32767)
    high_key = _find_key_in_range("auto-high", 32768, 65535)
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

        producer.send(b"warmup-low", ordering_key=low_key)
        producer.send(b"warmup-high", ordering_key=high_key)

        warmup_deliveries = {}
        for _ in range(2):
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            warmup_deliveries[message.data()] = owner.consumer_name()
            owner.acknowledge(message)

        assert warmup_deliveries[b"warmup-low"] == "auto-1"
        assert warmup_deliveries[b"warmup-high"] == "auto-2"

        consumer_2.close()
        wait_for_delivery_ready()

        producer.send(b"after-close-low", ordering_key=low_key)
        producer.send(b"after-close-high", ordering_key=high_key)

        seen = {}
        for _ in range(2):
            message = consumer_1.receive(timeout_millis=5000)
            seen[message.data()] = consumer_1.consumer_name()
            consumer_1.acknowledge(message)

        assert seen == {
            b"after-close-low": "auto-1",
            b"after-close-high": "auto-1",
        }
    finally:
        client.close()
