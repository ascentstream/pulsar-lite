#!/usr/bin/env python3
"""Connection management tests using the high-level client plus broker logs."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from test_support import current_log_offset, persistent_topic, wait_for_log


CLIENT_PROCESS = Path(__file__).with_name("pulsar_client_process.py")
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


def test_keepalive_timeout_is_logged_when_client_stops_responding(
    broker_url,
    broker_log_path,
    broker_timing,
    unique_name,
):
    start_offset = current_log_offset(broker_log_path)
    topic = persistent_topic(unique_name("keepalive-idle"))
    subscription = unique_name("keepalive-sub")
    ready_dir = Path(tempfile.mkdtemp(prefix="pulsar-lite-keepalive-ready-"))
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
            "keepalive-consumer",
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

        os.killpg(process.pid, signal.SIGSTOP)
        timeout_window = (
            broker_timing["keep_alive_interval_secs"]
            + broker_timing["connection_liveness_check_timeout_secs"]
            + 5
        )
        wait_for_log(
            broker_log_path,
            "liveness check timed out",
            start_offset,
            timeout=timeout_window,
        )
        wait_for_log(
            broker_log_path,
            "Closing connection",
            start_offset,
            timeout=timeout_window,
        )
    finally:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
