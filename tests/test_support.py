from __future__ import annotations

import time
from pathlib import Path

import pulsar


def persistent_topic(name: str) -> str:
    return f"persistent://public/default/{name}"


def current_log_offset(log_path: Path) -> int:
    if not log_path.exists():
        return 0
    return log_path.stat().st_size


def wait_for_log(log_path: Path, needle: str, start_offset: int, timeout: float) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if log_path.exists():
            with log_path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(start_offset)
                text = fh.read()
            if needle in text:
                return text
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for log line containing: {needle!r}")


def receive_from_any(
    consumers: list[pulsar.Consumer],
    timeout_secs: float = 10.0,
    poll_timeout_millis: int = 250,
):
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        for consumer in consumers:
            try:
                message = consumer.receive(timeout_millis=poll_timeout_millis)
                return consumer, message
            except pulsar.Timeout:
                continue
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for a message on any consumer")
