#!/usr/bin/env python3

from __future__ import annotations

import threading

import pulsar

from non_persist.support import (
    assert_no_message,
    non_persistent_runtime_topic,
    receive_from_any,
    wait_for_delivery_ready,
)


def test_non_persist_multi_producer_single_topic_delivers_all_messages(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-multi-producer")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=20,
        )
        producers = [
            client.create_producer(topic, producer_name=f"producer-{index}", batching_enabled=False)
            for index in range(3)
        ]
        wait_for_delivery_ready()

        producer_payloads = {
            index: [f"p{index}-{message_index}".encode() for message_index in range(4)]
            for index in range(len(producers))
        }
        threads = []
        for index, producer in enumerate(producers):
            thread = threading.Thread(
                target=lambda p=producer, payloads=producer_payloads[index]: [
                    p.send(payload) for payload in payloads
                ]
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        expected = [payload for payloads in producer_payloads.values() for payload in payloads]
        received = []
        for _ in expected:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert set(received) == set(expected)
        assert len(received) == len(expected)
    finally:
        client.close()


def test_non_persist_same_topic_multiple_subscriptions_are_independent(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-multi-sub")
    sub_a = unique_name("np-sub-a")
    sub_b = unique_name("np-sub-b")
    client = pulsar.Client(broker_url)

    try:
        consumer_a = client.subscribe(
            topic,
            sub_a,
            consumer_name="sub-a-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_b = client.subscribe(
            topic,
            sub_b,
            consumer_name="sub-b-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"fanout-0")

        message_a = consumer_a.receive(timeout_millis=5000)
        assert message_a.data() == b"fanout-0"
        consumer_a.acknowledge(message_a)

        message_b = consumer_b.receive(timeout_millis=5000)
        assert message_b.data() == b"fanout-0"
        consumer_b.acknowledge(message_b)
    finally:
        client.close()


def test_non_persist_different_topics_do_not_cross_deliver_messages(broker_url, unique_name):
    topic_a = non_persistent_runtime_topic(unique_name, "np-topic-a")
    topic_b = non_persistent_runtime_topic(unique_name, "np-topic-b")
    client = pulsar.Client(broker_url)

    try:
        consumer_a = client.subscribe(
            topic_a,
            unique_name("np-sub-a"),
            consumer_name="topic-a-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        consumer_b = client.subscribe(
            topic_b,
            unique_name("np-sub-b"),
            consumer_name="topic-b-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer_a = client.create_producer(topic_a, batching_enabled=False)
        producer_b = client.create_producer(topic_b, batching_enabled=False)
        wait_for_delivery_ready()

        producer_a.send(b"only-topic-a")
        message_a = consumer_a.receive(timeout_millis=5000)
        assert message_a.data() == b"only-topic-a"
        consumer_a.acknowledge(message_a)
        assert_no_message(consumer_b, timeout_millis=1000)

        producer_b.send(b"only-topic-b")
        message_b = consumer_b.receive(timeout_millis=5000)
        assert message_b.data() == b"only-topic-b"
        consumer_b.acknowledge(message_b)
        assert_no_message(consumer_a, timeout_millis=1000)
    finally:
        client.close()


def test_non_persist_same_topic_different_subscription_modes_are_independent(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-mode-isolation")
    shared_subscription = unique_name("np-shared-sub")
    exclusive_subscription = unique_name("np-exclusive-sub")
    client = pulsar.Client(broker_url)

    try:
        shared_1 = client.subscribe(
            topic,
            shared_subscription,
            consumer_name="shared-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        shared_2 = client.subscribe(
            topic,
            shared_subscription,
            consumer_name="shared-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        exclusive = client.subscribe(
            topic,
            exclusive_subscription,
            consumer_name="exclusive-1",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"mode-{index}".encode() for index in range(4)]
        for payload in payloads:
            producer.send(payload)

        exclusive_received = []
        for _ in payloads:
            message = exclusive.receive(timeout_millis=5000)
            exclusive_received.append(message.data())
            exclusive.acknowledge(message)
        assert exclusive_received == payloads

        shared_received = set()
        shared_owners = set()
        for _ in payloads:
            owner, message = receive_from_any([shared_1, shared_2], timeout_secs=10)
            shared_received.add(message.data())
            shared_owners.add(owner.consumer_name())
            owner.acknowledge(message)

        assert shared_received == set(payloads)
        assert shared_owners == {"shared-1", "shared-2"}
    finally:
        client.close()
