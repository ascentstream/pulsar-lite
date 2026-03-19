#!/usr/bin/env python3
"""Metadata persistence integration tests against an already running broker."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pulsar
import pytest

from test_support import persistent_topic


REPO_ROOT = Path(__file__).resolve().parents[1]
METADATA_PATH = REPO_ROOT / "rust" / "pulsar-lite.metadata.json"


def _load_metadata_document() -> dict:
    assert METADATA_PATH.exists(), f"Metadata file not found: {METADATA_PATH}"
    return json.loads(METADATA_PATH.read_text(encoding="utf-8"))


def _resource_file_key(document: dict) -> str:
    candidates = [
        key for key in document.keys() if key not in {"version", "partitioned_topics"}
    ]
    assert candidates, f"No metadata resource file key found in document: {document}"
    assert len(candidates) == 1, f"Expected one metadata resource file key, got: {candidates}"
    return candidates[0]


def _persistent_topics(document: dict) -> dict:
    path_key = _resource_file_key(document)
    namespace_node = document[path_key]["public"]["default"]
    if "persistent" in namespace_node:
        return namespace_node["persistent"]
    return namespace_node["domains"]["persistent"]


def _wait_for_partitioned_topic(topic: str, timeout_secs: float = 5.0) -> tuple[dict, int]:
    deadline = time.monotonic() + timeout_secs
    last_document: dict | None = None

    while time.monotonic() < deadline:
        last_document = _load_metadata_document()
        partition_count = (
            last_document.get("partitioned_topics", {}).get(topic, {}).get("partitions")
        )
        if isinstance(partition_count, int) and partition_count > 0:
            return last_document, partition_count
        time.sleep(0.1)

    pytest.skip(
        "Shared broker did not persist partition metadata for this topic; "
        "start pulsar-lite with partitioned topics enabled before running this test."
    )


def _wait_for_metadata(assertion, timeout_secs: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout_secs
    last_document = None
    last_error: AssertionError | None = None

    while time.monotonic() < deadline:
        last_document = _load_metadata_document()
        try:
            assertion(last_document)
            return last_document
        except AssertionError as error:
            last_error = error
            time.sleep(0.1)

    if last_error is not None:
        raise last_error
    raise AssertionError("Timed out waiting for metadata assertion")


def test_metadata_snapshot_persists_topic_and_subscription(broker_url, unique_name):
    topic = persistent_topic(unique_name("metadata-topic"))
    subscription = unique_name("metadata-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
        )
        producer = client.create_producer(topic)
        producer.send(b"metadata-check")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"metadata-check"
        consumer.acknowledge(message)
    finally:
        client.close()

    topic_name = topic.rsplit("/", 1)[-1]

    def _assert(document: dict) -> None:
        assert document["version"] == 2
        persistent_topics = _persistent_topics(document)
        partition_count = document["partitioned_topics"].get(topic, {}).get("partitions", 0)
        exact_topic_node = persistent_topics.get(topic_name)
        if partition_count == 0 and exact_topic_node is not None:
            assert exact_topic_node["subscriptions"][subscription] == {}
            return

        partition_topic_nodes = {
            name: topic_node
            for name, topic_node in persistent_topics.items()
            if name.startswith(f"{topic_name}-partition-")
        }
        assert partition_topic_nodes, f"No metadata topic node found for {topic_name}"
        assert partition_count > 0
        assert any(
            subscription in topic_node["subscriptions"]
            for topic_node in partition_topic_nodes.values()
        )

    document = _wait_for_metadata(_assert)
    assert _resource_file_key(document)


def test_partitioned_topic_metadata_is_recorded_in_shared_metadata_file(broker_url, unique_name):
    topic = persistent_topic(unique_name("metadata-partitioned"))
    subscription = unique_name("metadata-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
        )
        producer = client.create_producer(topic)
        producer.send(b"partitioned-check")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"partitioned-check"
        consumer.acknowledge(message)
    finally:
        client.close()

    topic_name = topic.rsplit("/", 1)[-1]
    document, partition_count = _wait_for_partitioned_topic(topic)
    persistent_topics = _persistent_topics(document)
    assert partition_count > 0
    assert topic_name not in persistent_topics
    assert any(
        name.startswith(f"{topic_name}-partition-") for name in persistent_topics
    )


def test_partition_subscription_is_persisted_under_concrete_partition_topic(broker_url, unique_name):
    topic = persistent_topic(unique_name("metadata-partition-sub"))
    subscription = unique_name("metadata-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
        )
        producer = client.create_producer(topic)
        producer.send(b"partition-sub-check")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"partition-sub-check"
        consumer.acknowledge(message)
    finally:
        client.close()

    document, partition_count = _wait_for_partitioned_topic(topic)
    persistent_topics = _persistent_topics(document)
    assert partition_count > 0
    assert topic.rsplit("/", 1)[-1] not in persistent_topics
    assert any(name.endswith("-partition-0") for name in persistent_topics)
    assert any(
        subscription in topic_node["subscriptions"]
        for topic_name, topic_node in persistent_topics.items()
        if topic_name.startswith(f"{topic.rsplit('/', 1)[-1]}-partition-")
    )


def test_partitioned_topic_persists_all_partitions_and_partition_count(broker_url, unique_name):
    topic = persistent_topic(unique_name("metadata-multi-partition"))
    subscription = unique_name("metadata-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
        )
        producer = client.create_producer(topic)
        producer.send(b"partition-metadata-check")
        message = consumer.receive(timeout_millis=5000)
        assert message.data() == b"partition-metadata-check"
        consumer.acknowledge(message)
    finally:
        client.close()

    topic_name = topic.rsplit("/", 1)[-1]
    document, partition_count = _wait_for_partitioned_topic(topic)
    persistent_topics = _persistent_topics(document)
    expected_partition_topics = {
        f"{topic_name}-partition-{index}" for index in range(partition_count)
    }
    actual_partition_topics = {
        name for name in persistent_topics if name.startswith(f"{topic_name}-partition-")
    }

    assert partition_count > 0
    assert topic_name not in persistent_topics
    assert actual_partition_topics == expected_partition_topics
