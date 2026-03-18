#!/usr/bin/env python3
"""Shared flow-control tests using the official Python client."""

from __future__ import annotations

import time

import pulsar

from test_support import persistent_topic

# 测试 Shared 订阅模式下，receiver_queue_size=1 时的 flow-control 行为
def test_shared_flow_control_with_small_receiver_queue(broker_url, unique_name):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-flow"))
    subscription = unique_name("shared-sub")

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-0",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        payloads = [f"flow-{index}".encode() for index in range(5)]
        for payload in payloads:
            producer.send(payload)

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert received == payloads
    finally:
        client.close()

# 测试 Shared 订阅模式下，receiver_queue_size=1 时，当一个 consumer 满了之后，不应该继续收到新消息
def test_shared_flow_full_consumer_stops_receiving_new_messages(broker_url, unique_name):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-flow-stop"))
    subscription = unique_name("shared-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        # Let consumer_1 fill its only local receive slot and stop polling.
        producer.send(b"warmup-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        later_payloads = [b"later-1", b"later-2", b"later-3"]
        for payload in later_payloads:
            producer.send(payload)

        received_by_consumer_2 = []
        for _ in later_payloads:
            message = consumer_2.receive(timeout_millis=5000)
            received_by_consumer_2.append(message.data())
            consumer_2.acknowledge(message)

        buffered = consumer_1.receive(timeout_millis=5000)
        assert buffered.data() == b"warmup-0"
        consumer_1.acknowledge(buffered)

        assert received_by_consumer_2 == later_payloads
    finally:
        client.close()

# 满掉的 consumer，在自己缓冲被消费掉之后，应该还能恢复接收。
def test_shared_flow_consumer_resumes_after_buffer_is_drained(broker_url, unique_name):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-flow-resume"))
    subscription = unique_name("shared-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        producer.send(b"warmup-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        buffered = consumer_1.receive(timeout_millis=5000)
        assert buffered.data() == b"warmup-0"
        consumer_1.acknowledge(buffered)

        consumer_2.close()
        producer.send(b"resume-3")

        resumed = consumer_1.receive(timeout_millis=5000)
        assert resumed.data() == b"resume-3"
        consumer_1.acknowledge(resumed)
    finally:
        client.close()

# Shared 分发时，应优先发给当前还有可用接收能力的 consumer。
def test_shared_flow_prefers_consumers_with_available_receive_capacity(
    broker_url, unique_name
):
    client = pulsar.Client(broker_url)
    topic = persistent_topic(unique_name("shared-flow-capacity"))
    subscription = unique_name("shared-sub")

    try:
        consumer_1 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-1",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )
        producer = client.create_producer(topic)

        # Fill consumer_1 and keep it idle.
        producer.send(b"idle-0")
        time.sleep(0.5)

        consumer_2 = client.subscribe(
            topic,
            subscription,
            consumer_name="consumer-2",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=1,
        )

        producer.send(b"active-1")
        active_message = consumer_2.receive(timeout_millis=5000)
        assert active_message.data() == b"active-1"
        consumer_2.acknowledge(active_message)

        idle_message = consumer_1.receive(timeout_millis=5000)
        assert idle_message.data() == b"idle-0"
        consumer_1.acknowledge(idle_message)
    finally:
        client.close()
