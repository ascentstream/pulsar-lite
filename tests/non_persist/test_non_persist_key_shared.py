#!/usr/bin/env python3

from __future__ import annotations

import pulsar
import pytest

from non_persist.support import (
    non_persistent_runtime_topic,
    receive_from_any,
    wait_for_delivery_ready,
)


def test_non_persist_key_shared_sticky_ranges_route_to_expected_consumer(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-sticky")
    subscription = unique_name("np-sub")
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

        producer.send(b"sticky-0", ordering_key="sticky-alpha")
        producer.send(b"sticky-1", ordering_key="sticky-alpha")

        owner_1, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        owner_2, second = receive_from_any([consumer_1, consumer_2], timeout_secs=10)

        assert owner_1.consumer_name() == owner_2.consumer_name()
        owner_1.acknowledge(first)
        owner_2.acknowledge(second)
    finally:
        client.close()


def test_non_persist_key_shared_rejects_incompatible_policy_consumer(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-key-shared-policy")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        _first = client.subscribe(
            topic,
            subscription,
            consumer_name="ks-auto",
            consumer_type=pulsar.ConsumerType.KeyShared,
            key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                key_shared_mode=pulsar.KeySharedMode.AutoSplit
            ),
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        with pytest.raises(Exception):
            client.subscribe(
                topic,
                subscription,
                consumer_name="ks-sticky",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                    key_shared_mode=pulsar.KeySharedMode.Sticky,
                    sticky_ranges=[(0, 65535)],
                ),
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
    finally:
        client.close()
