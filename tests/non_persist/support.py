from __future__ import annotations

import threading
import time
from typing import Iterable

import pulsar

from test_support import receive_from_any


def non_persistent_runtime_topic(unique_name, prefix: str) -> str:
    return f"non-persistent://public/default/{unique_name(prefix)}"


def assert_no_message(consumer: pulsar.Consumer, timeout_millis: int = 1000) -> None:
    try:
        message = consumer.receive(timeout_millis=timeout_millis)
        raise AssertionError(f"unexpected message received: {message.data()!r}")
    except pulsar.Timeout:
        return


def close_quietly(*resources: object) -> None:
    for resource in resources:
        if resource is None:
            continue
        try:
            resource.close()
        except Exception:
            pass


def wait_for_delivery_ready(delay_secs: float = 0.3) -> None:
    # Non-persistent delivery drops immediately when broker-side permits are not
    # ready yet. The Python client sends FLOW shortly after subscribe(), so a
    # short settle period avoids false negatives in integration tests.
    time.sleep(delay_secs)


def send_async_and_wait(
    producer: pulsar.Producer,
    payloads: Iterable[bytes],
    **send_kwargs,
) -> list[object]:
    payloads = list(payloads)
    callback_results: list[object] = []
    callback_errors: list[object] = []
    done = threading.Event()

    def callback(result, msg_id):
        if result != pulsar.Result.Ok:
            callback_errors.append(result)
        else:
            callback_results.append(msg_id)
        if len(callback_results) + len(callback_errors) == len(payloads):
            done.set()

    for payload in payloads:
        producer.send_async(payload, callback, **send_kwargs)

    producer.flush()
    assert done.wait(10), "timed out waiting for async send callbacks"
    assert not callback_errors
    return callback_results


__all__ = [
    "assert_no_message",
    "close_quietly",
    "non_persistent_runtime_topic",
    "receive_from_any",
    "send_async_and_wait",
    "wait_for_delivery_ready",
]
