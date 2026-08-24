"""Unit tests for E2E dual-process failure diagnostics."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.perf_cmd import (  # noqa: E402
    explain_exit_code,
    format_e2e_process_failure,
    log_tail,
)


def test_explain_exit_code_sigterm():
    text = explain_exit_code(143)
    assert "143" in text
    assert "SIGTERM" in text


def test_format_e2e_shows_both_rcs_and_log_tails():
    msg = format_e2e_process_failure(
        consumer_rc=143,
        producer_rc=1,
        consumer_out="consumer head\n" + "\n".join(f"c{i}" for i in range(100)),
        producer_out=(
            "Started performance test thread 0\n"
            "Created 1 producers\n"
            "Exception in thread: boom\n"
        ),
        first_failed="producer",
    )
    assert "first_failed=producer" in msg
    assert "consumer_rc=" in msg and "143" in msg
    assert "producer_rc=" in msg and "1" in msg
    assert "Exception in thread: boom" in msg
    assert "--- producer log tail ---" in msg
    assert "--- consumer log tail ---" in msg
    # tail should not be only the log head
    assert "consumer head" not in log_tail("\n".join(f"line{i}" for i in range(80)))


def test_infer_first_failed_when_consumer_sigterm_producer_nonzero():
    msg = format_e2e_process_failure(
        consumer_rc=143,
        producer_rc=1,
        consumer_out="c-tail",
        producer_out="p-tail-error",
        first_failed=None,
    )
    assert "first_failed=producer" in msg
