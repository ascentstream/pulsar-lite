from __future__ import annotations

import time

import pulsar

from persist.support import (
    PersistentBroker,
    assert_no_message,
    persistent_topic,
    pulsar_lite_binary,
    send_async_and_wait,
)


def _broker(tmp_path, db_path):
    log_path = tmp_path / f"broker-{time.time_ns()}.log"
    return PersistentBroker(pulsar_lite_binary(), db_path, log_path)


def _subscribe_exclusive(client: pulsar.Client, topic: str, subscription: str, **kwargs):
    options = {
        "consumer_type": pulsar.ConsumerType.Exclusive,
        "initial_position": pulsar.InitialPosition.Earliest,
        "receiver_queue_size": 1,
    }
    options.update(kwargs)
    return client.subscribe(topic, subscription, **options)


def test_persistent_earliest_late_subscriber_reads_existing_backlog(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-earliest-backlog")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"backlog-0")
            producer.send(b"backlog-1")

            consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Earliest,
            )

            received = []
            for _ in range(2):
                message = consumer.receive(timeout_millis=5000)
                received.append(message.data())
                consumer.acknowledge(message)

            assert received == [b"backlog-0", b"backlog-1"]
        finally:
            client.close()


def test_persistent_latest_late_subscriber_skips_existing_backlog(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-latest-backlog")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"old-0")
            producer.send(b"old-1")

            consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Latest,
            )
            assert_no_message(consumer, timeout_millis=1000)

            producer.send(b"new-0")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"new-0"
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_unacked_message_does_not_repeat_on_additional_flow(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-no-repeat-flow")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            consumer = _subscribe_exclusive(client, topic, subscription)

            producer.send(b"in-flight")
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"in-flight"

            assert_no_message(consumer, timeout_millis=1000)
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_existing_subscription_reuses_saved_cursor(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-saved-cursor")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"acked-before-close")
            producer.send(b"pending-after-close")

            first_consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Earliest,
            )
            first = first_consumer.receive(timeout_millis=5000)
            assert first.data() == b"acked-before-close"
            first_consumer.acknowledge(first)
            first_consumer.close()

            second_consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Latest,
            )
            second = second_consumer.receive(timeout_millis=5000)
            assert second.data() == b"pending-after-close"
            second_consumer.acknowledge(second)
        finally:
            client.close()


def test_persistent_subscriptions_keep_independent_cursors(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-independent-subs")
    subscription_a = unique_name("persist-sub-a")
    subscription_b = unique_name("persist-sub-b")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"fanout-0")

            consumer_a = _subscribe_exclusive(client, topic, subscription_a)
            message_a = consumer_a.receive(timeout_millis=5000)
            assert message_a.data() == b"fanout-0"
            consumer_a.acknowledge(message_a)

            consumer_b = _subscribe_exclusive(client, topic, subscription_b)
            message_b = consumer_b.receive(timeout_millis=5000)
            assert message_b.data() == b"fanout-0"
            consumer_b.acknowledge(message_b)
        finally:
            client.close()


def test_persistent_ack_by_message_id_survives_restart(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-ack-message-id")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"acked-by-id")
            producer.send(b"pending")

            consumer = _subscribe_exclusive(client, topic, subscription)
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"acked-by-id"
            consumer.acknowledge(message.message_id())
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe_exclusive(client, topic, subscription)
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"pending"
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_cumulative_ack_survives_restart(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-cumulative-ack")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"cumulative-0")
            producer.send(b"cumulative-1")
            producer.send(b"pending-2")

            consumer = _subscribe_exclusive(client, topic, subscription)
            first = consumer.receive(timeout_millis=5000)
            second = consumer.receive(timeout_millis=5000)
            assert [first.data(), second.data()] == [b"cumulative-0", b"cumulative-1"]
            consumer.acknowledge_cumulative(second)
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe_exclusive(client, topic, subscription)
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"pending-2"
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_send_async_returns_ids_and_payloads_survive_restart(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-async-send")
    subscription = unique_name("persist-sub")
    payloads = [b"async-0", b"async-1"]

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            message_ids = send_async_and_wait(producer, payloads)
            assert len(message_ids) == len(payloads)
            assert all(message_id is not None for message_id in message_ids)
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = _subscribe_exclusive(client, topic, subscription)
            received = []
            for _ in payloads:
                message = consumer.receive(timeout_millis=5000)
                received.append(message.data())
                consumer.acknowledge(message)

            assert received == payloads
        finally:
            client.close()


def test_persistent_unsubscribe_deletes_subscription_cursor(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-unsubscribe")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url, operation_timeout_seconds=3)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"before-unsubscribe")

            consumer = _subscribe_exclusive(client, topic, subscription)
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"before-unsubscribe"
            consumer.acknowledge(message)
            consumer.unsubscribe()

            producer.send(b"after-unsubscribe")
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url, operation_timeout_seconds=3)
        try:
            consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Earliest,
            )
            received = []
            for _ in range(2):
                message = consumer.receive(timeout_millis=5000)
                received.append(message.data())
                consumer.acknowledge(message)

            assert received == [b"before-unsubscribe", b"after-unsubscribe"]
        finally:
            client.close()


def test_persistent_consumer_seek_to_message_id_redelivers_from_target(
    tmp_path, unique_name
):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-seek-message-id")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url, operation_timeout_seconds=3)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            message_ids = [
                producer.send(b"seek-0"),
                producer.send(b"seek-1"),
                producer.send(b"seek-2"),
            ]

            consumer = _subscribe_exclusive(client, topic, subscription)
            received = []
            for _ in message_ids:
                message = consumer.receive(timeout_millis=5000)
                received.append(message.data())
                consumer.acknowledge(message)

            assert received == [b"seek-0", b"seek-1", b"seek-2"]

            consumer.seek(message_ids[1])
            consumer.close()

            replay_consumer = _subscribe_exclusive(
                client,
                topic,
                subscription,
                initial_position=pulsar.InitialPosition.Latest,
            )
            replayed = []
            for _ in range(2):
                message = replay_consumer.receive(timeout_millis=5000)
                replayed.append(message.data())
                replay_consumer.acknowledge(message)

            assert replayed == [b"seek-1", b"seek-2"]
        finally:
            client.close()


def test_persistent_reader_from_earliest_reads_existing_messages(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-reader-earliest")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url, operation_timeout_seconds=3)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"reader-0")
            producer.send(b"reader-1")

            reader = client.create_reader(
                topic,
                pulsar.MessageId.earliest,
                receiver_queue_size=1,
            )
            received = [
                reader.read_next(timeout_millis=5000).data(),
                reader.read_next(timeout_millis=5000).data(),
            ]

            assert received == [b"reader-0", b"reader-1"]
        finally:
            client.close()
