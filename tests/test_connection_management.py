#!/usr/bin/env python3
"""Connection management tests using the high-level client plus broker logs."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import sys
import time
from pathlib import Path

from test_support import current_log_offset, persistent_topic, wait_for_log


CLIENT_PROCESS = Path(__file__).with_name("pulsar_client_process.py")


def _wait_for_ready(process: subprocess.Popen[str], timeout_secs: float = 15.0) -> None:
    assert process.stdout is not None

    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout_secs
    recent_lines: list[str] = []

    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                break

            timeout = max(0.0, deadline - time.monotonic())
            events = selector.select(timeout)
            if not events:
                break

            for key, _ in events:
                line = key.fileobj.readline()
                if not line:
                    continue

                line = line.strip()
                recent_lines.append(line)
                if line == "READY" :
                    return
    finally:
        selector.close()

    recent_output = "\n".join(recent_lines[-20:])
    raise AssertionError(
        f"client process did not reach READY within {timeout_secs}s; output={recent_output!r}"
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
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )

    try:
        _wait_for_ready(process)

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
