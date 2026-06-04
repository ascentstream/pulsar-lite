from __future__ import annotations

import time

import pulsar

from persist.support import PersistentBroker, persistent_topic, pulsar_lite_binary


def _broker(tmp_path, db_path):
    log_path = tmp_path / f"broker-{time.time_ns()}.log"
    return PersistentBroker(pulsar_lite_binary(), db_path, log_path)


def test_persistent_backlog_survives_restart(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-backlog")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"before-restart")
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"before-restart"
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_acked_message_does_not_redeliver_after_restart(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-acked")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"acked")
            producer.send(b"pending")

            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"acked"
            consumer.acknowledge(message)
            time.sleep(0.2)
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"pending"
            consumer.acknowledge(message)
        finally:
            client.close()


def test_persistent_unacked_message_redelivers_after_restart(tmp_path, unique_name):
    db_path = tmp_path / "persistent.db"
    topic = persistent_topic(unique_name, "persist-unacked")
    subscription = unique_name("persist-sub")

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
            producer = client.create_producer(topic, batching_enabled=False)
            producer.send(b"redeliver-me")

            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"redeliver-me"
        finally:
            client.close()

    with _broker(tmp_path, db_path) as broker:
        client = pulsar.Client(broker.broker_url)
        try:
            consumer = client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Exclusive,
                initial_position=pulsar.InitialPosition.Earliest,
                receiver_queue_size=1,
            )
            message = consumer.receive(timeout_millis=5000)
            assert message.data() == b"redeliver-me"
            consumer.acknowledge(message)
        finally:
            client.close()
