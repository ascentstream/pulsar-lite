#!/usr/bin/env python3

from __future__ import annotations

import time

import pulsar
import pytest

from non_persist.support import (
    assert_no_message,
    non_persistent_runtime_topic,
    wait_for_delivery_ready,
)
from non_persist.test_non_persist_dynamic_consumers import _find_key_in_range, _murmur3_32


def test_non_persist_shared_flow_full_consumer_stops_receiving_new_messages(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-shared-flow-stop")
    subscription = unique_name("np-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"warmup-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        wait_for_delivery_ready()

        later_payloads = [b"later-1", b"later-2", b"later-3"]
        received_by_consumer_2 = []
        for payload in later_payloads:
            producer.send(payload)
            message = consumer_2.receive(timeout_millis=5000)
            received_by_consumer_2.append(message.data())
            consumer_2.acknowledge(message)

        buffered = consumer_1.receive(timeout_millis=5000)
        assert buffered.data() == b"warmup-0"
        consumer_1.acknowledge(buffered)

        assert received_by_consumer_2 == later_payloads
    finally:
        client.close()


def test_non_persist_shared_flow_consumer_resumes_after_buffer_is_drained(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-shared-flow-resume")
    subscription = unique_name("np-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"warmup-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        wait_for_delivery_ready()

        buffered = consumer_1.receive(timeout_millis=5000)
        assert buffered.data() == b"warmup-0"
        consumer_1.acknowledge(buffered)

        consumer_2.close()
        producer.send(b"resume-1")

        resumed = consumer_1.receive(timeout_millis=5000)
        assert resumed.data() == b"resume-1"
        consumer_1.acknowledge(resumed)
    finally:
        client.close()


def test_non_persist_shared_flow_prefers_consumers_with_available_receive_capacity(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-shared-flow-capacity")
    subscription = unique_name("np-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"idle-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        wait_for_delivery_ready()

        producer.send(b"active-1")

        active_message = consumer_2.receive(timeout_millis=5000)
        assert active_message.data() == b"active-1"
        consumer_2.acknowledge(active_message)

        idle_message = consumer_1.receive(timeout_millis=5000)
        assert idle_message.data() == b"idle-0"
        consumer_1.acknowledge(idle_message)
    finally:
        client.close()


@pytest.mark.skip(
    reason=(
        "The high-level Python client sends FLOW during subscribe() quickly enough that "
        "the non-persistent pre-FLOW drop window is not deterministically observable here."
    )
)
def test_non_persist_shared_flow_not_ready_drops_message_by_current_design(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-shared-flow-not-ready")
    subscription = unique_name("np-sub")

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)

        producer.send(b"dropped-before-flow")
        wait_for_delivery_ready()

        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_exclusive_flow_drops_when_consumer_queue_is_full_and_recovers(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-exclusive-flow")
    subscription = unique_name("np-sub")

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-1",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"warmup-0")
        time.sleep(0.5)
        producer.send(b"dropped-while-full")

        buffered = consumer.receive(timeout_millis=5000)
        assert buffered.data() == b"warmup-0"
        consumer.acknowledge(buffered)
        assert_no_message(consumer, timeout_millis=1500)

        producer.send(b"after-drain")
        recovered = consumer.receive(timeout_millis=5000)
        assert recovered.data() == b"after-drain"
        consumer.acknowledge(recovered)
    finally:
        client.close()


def _failover_active_name(topic: str, consumer_names: list[str]) -> str:
    hash_ring = []
    for consumer_index, consumer_name in enumerate(sorted(consumer_names)):
        for replica in range(100):
            key = f"{consumer_name}{replica}".encode("utf-8")
            hash_ring.append((_murmur3_32(key), consumer_index))
    hash_ring.sort(key=lambda item: item[0])

    topic_hash = _murmur3_32(topic.encode("utf-8"))
    selected_index = next(
        (index for hash_value, index in hash_ring if hash_value >= topic_hash),
        hash_ring[0][1],
    )
    return sorted(consumer_names)[selected_index]


def test_non_persist_failover_flow_does_not_reroute_to_standby_when_active_is_full(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-failover-flow")
    subscription = unique_name("np-sub")

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

        active_name = _failover_active_name(topic, ["failover-a", "failover-b"])
        active = consumer_1 if active_name == "failover-a" else consumer_2
        standby = consumer_2 if active is consumer_1 else consumer_1

        producer.send(b"warmup-0")
        time.sleep(0.5)
        producer.send(b"dropped-while-active-full")

        assert_no_message(standby, timeout_millis=1500)
        warmup = active.receive(timeout_millis=5000)
        assert warmup.data() == b"warmup-0"
        active.acknowledge(warmup)
        assert_no_message(active, timeout_millis=1500)

        producer.send(b"after-active-drain")
        recovered = active.receive(timeout_millis=5000)
        assert recovered.data() == b"after-active-drain"
        active.acknowledge(recovered)
    finally:
        client.close()


def test_non_persist_key_shared_flow_does_not_reroute_key_when_target_consumer_is_full(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-flow")
    subscription = unique_name("np-sub")
    low_key = _find_key_in_range("key-low", 0, 32767)
    high_key = _find_key_in_range("key-high", 32768, 65535)

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
            receiver_queue_size=1,
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
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"warmup-low", ordering_key=low_key)
        time.sleep(0.5)
        producer.send(b"dropped-low", ordering_key=low_key)
        producer.send(b"high-ok", ordering_key=high_key)

        high_message = consumer_2.receive(timeout_millis=5000)
        assert high_message.data() == b"high-ok"
        consumer_2.acknowledge(high_message)

        low_message = consumer_1.receive(timeout_millis=5000)
        assert low_message.data() == b"warmup-low"
        consumer_1.acknowledge(low_message)
        assert_no_message(consumer_1, timeout_millis=1500)

        producer.send(b"after-drain-low", ordering_key=low_key)
        recovered = consumer_1.receive(timeout_millis=5000)
        assert recovered.data() == b"after-drain-low"
        consumer_1.acknowledge(recovered)
    finally:
        client.close()
