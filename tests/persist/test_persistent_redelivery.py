from __future__ import annotations

import time

import pulsar

from persist.support import (
    PersistentBroker,
    assert_no_message,
    persistent_topic,
    pulsar_lite_binary,
)


def _broker(tmp_path, db_path):
    log_path = tmp_path / f"broker-{time.time_ns()}.log"
    return PersistentBroker(pulsar_lite_binary(), db_path, log_path)


def _subscribe(client: pulsar.Client, topic: str, subscription: str, **kwargs):
    options = {
        "consumer_type": pulsar.ConsumerType.Exclusive,
        "initial_position": pulsar.InitialPosition.Earliest,
        "receiver_queue_size": 1,
    }
    options.update(kwargs)
    return client.subscribe(topic, subscription, **options)


def test_persistent_unacked_message_redelivers_after_consumer_close(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-close-redelivery")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(client, topic, subscription)
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"redeliver-after-close")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"redeliver-after-close"
            consumer.close()

            redelivery_consumer = _subscribe(client, topic, subscription)
            redelivered = redelivery_consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"redeliver-after-close"
            redelivery_consumer.acknowledge(redelivered)
        finally:
            client.close()


def test_persistent_unacked_message_redelivers_after_broker_restart(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-restart-redelivery")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(client, topic, subscription)
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"redeliver-after-restart")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"redeliver-after-restart"
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(client, topic, subscription)
            redelivered = consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"redeliver-after-restart"
            consumer.acknowledge(redelivered)
        finally:
            client.close()


def test_persistent_acked_message_does_not_redeliver_after_consumer_close(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-close-no-redelivery")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(client, topic, subscription)
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"acked-before-close")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"acked-before-close"
            consumer.acknowledge(message)
            consumer.close()

            redelivery_consumer = _subscribe(client, topic, subscription)
            assert_no_message(redelivery_consumer, timeout_millis=1000)
        finally:
            client.close()


def test_persistent_shared_negative_ack_redelivers_message(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-negative-ack")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared,
                negative_ack_redelivery_delay_ms=500,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"negative-ack-redelivery")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"negative-ack-redelivery"
            consumer.negative_acknowledge(message)

            time.sleep(1.0)
            redelivered = consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"negative-ack-redelivery"
            assert redelivered.redelivery_count() >= 1
            consumer.acknowledge(redelivered)
        finally:
            client.close()


def test_persistent_shared_ack_timeout_redelivers_message(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-ack-timeout")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared,
                unacked_messages_timeout_ms=10001,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"ack-timeout-redelivery")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"ack-timeout-redelivery"

            time.sleep(11.5)
            redelivered = consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"ack-timeout-redelivery"
            assert redelivered.redelivery_count() >= 1
            consumer.acknowledge(redelivered)
        finally:
            client.close()


def test_persistent_shared_explicit_redelivery_command_redelivers_message(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-explicit-redelivery")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"explicit-redelivery")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"explicit-redelivery"

            consumer.redeliver_unacknowledged_messages()
            time.sleep(0.5)
            redelivered = consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"explicit-redelivery"
            assert redelivered.redelivery_count() >= 1
            consumer.acknowledge(redelivered)
        finally:
            client.close()
