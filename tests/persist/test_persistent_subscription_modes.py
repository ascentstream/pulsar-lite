from __future__ import annotations

import time

import pulsar
import pytest

from persist.support import (
    PersistentBroker,
    assert_no_message,
    persistent_topic,
    pulsar_lite_binary,
    receive_from_any,
)


def _broker(tmp_path, db_path):
    log_path = tmp_path / f"broker-{time.time_ns()}.log"
    return PersistentBroker(pulsar_lite_binary(), db_path, log_path)


def _subscribe(client: pulsar.Client, topic: str, subscription: str, **kwargs):
    options = {
        "initial_position": pulsar.InitialPosition.Earliest,
        "receiver_queue_size": 1,
    }
    options.update(kwargs)
    return client.subscribe(topic, subscription, **options)


def test_persistent_exclusive_rejects_second_consumer(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-exclusive")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            first = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="exclusive-1",
                consumer_type=pulsar.ConsumerType.Exclusive,
            )

            with pytest.raises(Exception):
                _subscribe(
                    client,
                    topic,
                    subscription,
                    consumer_name="exclusive-2",
                    consumer_type=pulsar.ConsumerType.Exclusive,
                )

            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"exclusive-message")
            message = first.receive(timeout_millis=5000)
            assert message.data() == b"exclusive-message"
            first.acknowledge(message)
        finally:
            client.close()


def test_persistent_failover_standby_takes_unacked_backlog_after_active_closes(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-failover")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer_1 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="failover-a",
                consumer_type=pulsar.ConsumerType.Failover,
            )
            consumer_2 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="failover-b",
                consumer_type=pulsar.ConsumerType.Failover,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"failover-unacked")
            owner, message = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            assert message.data() == b"failover-unacked"
            standby = consumer_2 if owner is consumer_1 else consumer_1
            owner.close()

            redelivered = standby.receive(timeout_millis=5000)
            assert redelivered.data() == b"failover-unacked"
            standby.acknowledge(redelivered)
        finally:
            client.close()


def test_persistent_shared_distributes_messages_without_duplicates(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-shared-distribution")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer_1 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="shared-1",
                consumer_type=pulsar.ConsumerType.Shared,
            )
            consumer_2 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="shared-2",
                consumer_type=pulsar.ConsumerType.Shared,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            payloads = [f"shared-{index}".encode() for index in range(4)]
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


def test_persistent_shared_ack_cursor_keeps_only_unacked_message_for_redelivery(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-shared-ack-hole")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="shared-owner",
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=2,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"acked")
            producer.send(b"unacked")
            first = consumer.receive(timeout_millis=5000)
            second = consumer.receive(timeout_millis=5000)
            assert [first.data(), second.data()] == [b"acked", b"unacked"]
            consumer.acknowledge(first)
            consumer.close()

            redelivery_consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="shared-redelivery",
                consumer_type=pulsar.ConsumerType.Shared,
            )
            redelivered = redelivery_consumer.receive(timeout_millis=5000)
            assert redelivered.data() == b"unacked"
            redelivery_consumer.acknowledge(redelivered)
            assert_no_message(redelivery_consumer, timeout_millis=1000)
        finally:
            client.close()


def test_persistent_key_shared_routes_same_key_to_same_consumer(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-key-shared")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer_1 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-1",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            )
            consumer_2 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-2",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"same-key-0", ordering_key="stable-key")
            producer.send(b"same-key-1", ordering_key="stable-key")

            owner_1, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            owner_2, second = receive_from_any([consumer_1, consumer_2], timeout_secs=10)

            assert [first.data(), second.data()] == [b"same-key-0", b"same-key-1"]
            assert owner_1 is owner_2
            owner_1.acknowledge(first)
            owner_2.acknowledge(second)
        finally:
            client.close()


def test_persistent_key_shared_unacked_redelivery_preserves_key_owner(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-key-shared-redelivery")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer_1 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-1",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            )
            consumer_2 = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-2",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(),
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"key-redelivery", ordering_key="stable-key")
            owner, first = receive_from_any([consumer_1, consumer_2], timeout_secs=10)
            assert first.data() == b"key-redelivery"
            survivor = consumer_2 if owner is consumer_1 else consumer_1
            owner.close()

            redelivered = survivor.receive(timeout_millis=5000)
            assert redelivered.data() == b"key-redelivery"
            assert redelivered.redelivery_count() >= 1
            survivor.acknowledge(redelivered)
        finally:
            client.close()
