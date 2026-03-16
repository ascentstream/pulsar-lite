#!/usr/bin/env python3
"""Shared integration tests focused on pending ack recovery semantics."""

from __future__ import annotations

import pulsar

from test_support import persistent_topic, receive_from_any


def test_shared_pending_acks_disconnect_triggers_redelivery(broker_url, unique_name):
    topic = persistent_topic(unique_name("shared-redelivery"))
    subscription = unique_name("shared-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        producer.send(b"acked-first")
        owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        assert first.data() == b"acked-first"
        owner.acknowledge(first)

        producer.send(b"redeliver-second")
        owner, second = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
        assert second.data() == b"redeliver-second"

        surviving_consumer = consumer_2 if owner is consumer_1 else consumer_1
        owner.close()

        redelivered = surviving_consumer.receive(timeout_millis=10000)
        assert redelivered.data() == b"redeliver-second"
        surviving_consumer.acknowledge(redelivered)

        try:
            extra = surviving_consumer.receive(timeout_millis=1000)
            assert extra.data() != b"acked-first"
        except pulsar.Timeout:
            pass
    finally:
        client.close()
