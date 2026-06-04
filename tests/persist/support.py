from __future__ import annotations

import os
import socket
import subprocess
import threading
import time
from pathlib import Path

import pulsar
import pytest


def persistent_topic(unique_name, prefix: str) -> str:
    return f"persistent://public/default/{unique_name(prefix)}"


def assert_no_message(consumer: pulsar.Consumer, timeout_millis: int = 1000) -> None:
    try:
        message = consumer.receive(timeout_millis=timeout_millis)
        raise AssertionError(f"unexpected message received: {message.data()!r}")
    except pulsar.Timeout:
        return


def send_async_and_wait(
    producer: pulsar.Producer,
    payloads: list[bytes],
    **send_kwargs,
) -> list[object]:
    done = threading.Event()
    callback_results: list[object] = []
    callback_errors: list[object] = []

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


def close_quietly(resource) -> None:
    if resource is None:
        return
    try:
        resource.close()
    except Exception:
        return


def pulsar_lite_binary() -> Path:
    binary = os.environ.get("PULSAR_LITE_BINARY")
    if not binary:
        pytest.skip("PULSAR_LITE_BINARY is required for persistent restart tests")
    path = Path(binary)
    if not path.exists():
        pytest.skip(f"PULSAR_LITE_BINARY does not exist: {path}")
    return path


def reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, timeout_secs: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_secs
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.05)
    raise AssertionError(f"broker did not listen on 127.0.0.1:{port}")


class PersistentBroker:
    def __init__(self, binary: Path, db_path: Path, log_path: Path):
        self.binary = binary
        self.db_path = db_path
        self.log_path = log_path
        self.port = reserve_local_port()
        self.process: subprocess.Popen[str] | None = None
        self._log_file = None

    @property
    def broker_url(self) -> str:
        return f"pulsar://127.0.0.1:{self.port}"

    def start(self) -> "PersistentBroker":
        self._log_file = self.log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen(
            [
                str(self.binary),
                "--addr",
                f"127.0.0.1:{self.port}",
                "--db-path",
                str(self.db_path),
                "--log-level",
                "info",
            ],
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            wait_for_port(self.port)
        except Exception:
            self.stop()
            raise
        return self

    def stop(self) -> None:
        process = self.process
        self.process = None
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None

    def __enter__(self) -> "PersistentBroker":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()
