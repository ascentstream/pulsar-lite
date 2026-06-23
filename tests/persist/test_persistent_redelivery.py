from __future__ import annotations

import time

import pulsar
from non_persist.test_non_persist_dynamic_consumers import _find_key_in_range

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


def test_persistent_unacked_message_redelivers_after_consumer_close(tmp_path, unique_name):
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


def test_persistent_unacked_message_redelivers_after_broker_restart(tmp_path, unique_name):
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


def test_persistent_acked_message_does_not_redeliver_after_consumer_close(tmp_path, unique_name):
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


def test_persistent_shared_ack_hole_redelivery_increments_count(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-shared-ack-hole-redelivery-count")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe(
                client,
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=3,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            for payload in [b"hole-0", b"hole-1", b"hole-2"]:
                producer.send(payload)

            first = consumer.receive(timeout_millis=5000)
            second = consumer.receive(timeout_millis=5000)
            third = consumer.receive(timeout_millis=5000)
            assert [first.data(), second.data(), third.data()] == [
                b"hole-0",
                b"hole-1",
                b"hole-2",
            ]

            consumer.acknowledge(first)
            consumer.acknowledge(third)
            consumer.close()

            replacement = _subscribe(
                client,
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=1,
            )
            redelivered = replacement.receive(timeout_millis=5000)
            assert redelivered.data() == b"hole-1"
            assert redelivered.redelivery_count() >= 1
            replacement.acknowledge(redelivered)
            assert_no_message(replacement, timeout_millis=1000)
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


def test_persistent_shared_explicit_redelivery_command_redelivers_message(tmp_path, unique_name):
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


def test_persistent_key_shared_redelivery_blocks_same_key_but_not_other_key(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-key-shared-redelivery-blocking")
    subscription = unique_name("persist-sub")
    same_key = _find_key_in_range("ks-redelivery-same", 0, 32767)
    other_key = _find_key_in_range("ks-redelivery-other", 32768, 65535)

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            same_owner = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-low",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                    key_shared_mode=pulsar.KeySharedMode.Sticky,
                    sticky_ranges=[(0, 32767)],
                ),
                receiver_queue_size=10,
            )
            other_owner = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-high",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                    key_shared_mode=pulsar.KeySharedMode.Sticky,
                    sticky_ranges=[(32768, 65535)],
                ),
                receiver_queue_size=10,
            )
            producer = client.create_producer(topic, batching_enabled=False)

            producer.send(b"same-redeliver1", ordering_key=same_key)
            first = same_owner.receive(timeout_millis=5000)
            assert first.data() == b"same-redeliver1"
            same_owner.close()

            same_replacement = _subscribe(
                client,
                topic,
                subscription,
                consumer_name="ks-low-replacement",
                consumer_type=pulsar.ConsumerType.KeyShared,
                key_shared_policy=pulsar.ConsumerKeySharedPolicy(
                    key_shared_mode=pulsar.KeySharedMode.Sticky,
                    sticky_ranges=[(0, 32767)],
                ),
                receiver_queue_size=10,
            )
            producer.send(b"same-after-redelivery", ordering_key=same_key)
            producer.send(b"other-continues", ordering_key=other_key)

            redelivered = same_replacement.receive(timeout_millis=5000)
            assert redelivered.data() == b"same-redeliver1"
            assert redelivered.redelivery_count() >= 1

            other = other_owner.receive(timeout_millis=5000)
            assert other.data() == b"other-continues"
            other_owner.acknowledge(other)

            assert_no_message(same_replacement, timeout_millis=1000)
            same_replacement.acknowledge(redelivered)
            time.sleep(0.5)
            producer.send(b"dispatch-trigger", ordering_key=other_key)

            unblocked = same_replacement.receive(timeout_millis=5000)
            assert unblocked.data() == b"same-after-redelivery"
            same_replacement.acknowledge(unblocked)

            trigger = other_owner.receive(timeout_millis=5000)
            assert trigger.data() == b"dispatch-trigger"
            other_owner.acknowledge(trigger)
        finally:
            client.close()
