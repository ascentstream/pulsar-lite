#!/usr/bin/env python3

from __future__ import annotations

import pulsar

from non_persist.support import (
    assert_no_message,
    non_persistent_runtime_topic,
    send_async_and_wait,
    wait_for_delivery_ready,
)


def test_non_persist_late_subscriber_sees_no_backlog(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-late-sub")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        producer = client.create_producer(topic, batching_enabled=False)
        producer.send(b"sent-before-subscribe")

        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="late-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        assert_no_message(consumer, timeout_millis=1500)
    finally:
        client.close()


def test_non_persist_send_async_delivers_and_preserves_message_metadata(broker_url, unique_name):
    topic = non_persistent_runtime_topic(unique_name, "np-async-metadata")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="metadata-consumer",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [b"np-async-0", b"np-async-1"]
        msg_ids = send_async_and_wait(
            producer,
            payloads,
            properties={"origin": "non-persist-test"},
            partition_key="pk-basic",
            ordering_key="ok-basic",
        )
        assert len(msg_ids) == len(payloads)

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message)
            consumer.acknowledge(message)

        assert {msg.data() for msg in received} == set(payloads)
        for message in received:
            assert message.partition_key() == "pk-basic"
            assert message.ordering_key() == "ok-basic"
            assert message.properties().get("origin") == "non-persist-test"
    finally:
        client.close()
