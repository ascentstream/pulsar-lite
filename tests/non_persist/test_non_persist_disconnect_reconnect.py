#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pulsar

from non_persist.support import (
    assert_no_message,
    non_persistent_runtime_topic,
    wait_for_delivery_ready,
)

CLIENT_PROCESS = Path(__file__).resolve().parents[1] / "pulsar_client_process.py"
READY_SENTINEL = "__PULSAR_LITE_READY__"


def _wait_for_ready_file(path: Path, timeout_secs: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_secs

    while time.monotonic() < deadline:
        if path.exists() and READY_SENTINEL in path.read_text(encoding="utf-8"):
            return
        time.sleep(0.1)

    contents = path.read_text(encoding="utf-8") if path.exists() else ""
    raise AssertionError(
        f"client process did not write ready file within {timeout_secs}s; contents={contents!r}"
    )


def test_non_persist_shared_new_consumer_can_take_over_after_peer_process_is_killed(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-shared-abrupt-exit")
    subscription = unique_name("np-sub")
    ready_dir = Path(tempfile.mkdtemp(prefix="pulsar-lite-np-shared-ready-"))
    ready_file = ready_dir / "ready.txt"
    process = subprocess.Popen(
        [
            sys.executable,
            str(CLIENT_PROCESS),
            "--url",
            broker_url,
            "--topic",
            topic,
            "--subscription",
            subscription,
            "--consumer-name",
            "shared-external",
            "--ready-file",
            str(ready_file),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    try:
        _wait_for_ready_file(ready_file)

        os_killed = False
        try:
            process.kill()
            os_killed = True
        finally:
            if os_killed:
                process.wait(timeout=5)

        time.sleep(0.5)

        client = pulsar.Client(broker_url)
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="shared-local",
            consumer_type=pulsar.ConsumerType.Shared,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        payloads = [f"after-kill-{index}".encode() for index in range(4)]
        for payload in payloads:
            producer.send(payload)

        received = []
        for _ in payloads:
            message = consumer.receive(timeout_millis=5000)
            received.append(message.data())
            consumer.acknowledge(message)

        assert received == payloads
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if "client" in locals():
            client.close()


def test_non_persist_exclusive_reconnect_has_no_backlog_and_receives_new_live_messages(
    broker_url, unique_name
):
    topic = non_persistent_runtime_topic(unique_name, "np-exclusive-reconnect")
    subscription = unique_name("np-sub")
    client = pulsar.Client(broker_url)

    try:
        consumer = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-reconnect",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        producer = client.create_producer(topic, batching_enabled=False)
        wait_for_delivery_ready()

        producer.send(b"before-disconnect")
        first = consumer.receive(timeout_millis=5000)
        assert first.data() == b"before-disconnect"
        consumer.acknowledge(first)

        consumer.close()
        producer.send(b"while-offline")

        reconnected = client.subscribe(
            topic,
            subscription,
            consumer_name="exclusive-reconnect",
            consumer_type=pulsar.ConsumerType.Exclusive,
            initial_position=pulsar.InitialPosition.Earliest,
            receiver_queue_size=10,
        )
        wait_for_delivery_ready()

        assert_no_message(reconnected, timeout_millis=1500)

        producer.send(b"after-reconnect")
        resumed = reconnected.receive(timeout_millis=5000)
        assert resumed.data() == b"after-reconnect"
        reconnected.acknowledge(resumed)
    finally:
        client.close()
