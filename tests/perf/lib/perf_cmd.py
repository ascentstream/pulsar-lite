from __future__ import annotations

import subprocess
import time
from pathlib import Path

from . import (
    BROKER_BIN,
    CLASSPATH_FILE,
    ENV_BASE,
    JAVA,
    PULSAR_ROOT,
    PULSAR_TESTCLIENT_JAR,
)


def ensure_prereqs(*, require_broker_bin: bool = True) -> None:
    if require_broker_bin and not BROKER_BIN.exists():
        raise FileNotFoundError(f"broker binary missing: {BROKER_BIN}")
    if not PULSAR_TESTCLIENT_JAR.exists():
        raise FileNotFoundError(
            f"pulsar-testclient jar missing: {PULSAR_TESTCLIENT_JAR}\n"
            "Set PULSAR_ROOT=/path/to/pulsar or "
            "PULSAR_TESTCLIENT_JAR=/path/to/pulsar-testclient.jar.\n"
            "If using a Pulsar source checkout, build it with:\n"
            "  mvn -pl pulsar-testclient -am -DskipTests package"
        )
    if not CLASSPATH_FILE.exists():
        if not PULSAR_ROOT.exists():
            raise FileNotFoundError(
                f"pulsar source checkout missing: {PULSAR_ROOT}\n"
                "Set PULSAR_ROOT=/path/to/pulsar, or set "
                "PULSAR_TESTCLIENT_CLASSPATH_FILE to an existing runtime classpath file."
            )
        CLASSPATH_FILE.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [
                "mvn",
                "-pl",
                "pulsar-testclient",
                "dependency:build-classpath",
                "-DincludeScope=runtime",
                f"-Dmdep.outputFile={CLASSPATH_FILE}",
            ],
            cwd=str(PULSAR_ROOT),
            check=True,
        )


def perf_cmd(
    subcommand: str,
    service_url: str,
    extra_args: list[str],
    topic: str,
    histogram_path: Path,
) -> list[str]:
    classpath = (
        f"{PULSAR_TESTCLIENT_JAR}:{CLASSPATH_FILE.read_text(encoding='utf-8').strip()}"
    )
    cmd = [
        str(JAVA),
        "-cp",
        classpath,
        "org.apache.pulsar.testclient.PulsarPerfTestTool",
        str(PULSAR_ROOT / "conf" / "client.conf"),
        subcommand,
        "-u",
        service_url,
    ]
    if subcommand in {"produce", "consume"}:
        cmd.extend(["--histogram-file", str(histogram_path)])
    cmd.extend([*extra_args, topic])
    return cmd


def run_sync(
    cmd: list[str], stdout_path: Path, timeout: float = 300.0
) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=timeout,
        env=ENV_BASE,
    )
    stdout_path.write_text(proc.stdout, encoding="utf-8")
    return proc


def wait_for_log(path: Path, needle: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and needle in path.read_text(
            encoding="utf-8", errors="replace"
        ):
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {needle!r} in {path}")


def explain_exit_code(rc: int | None) -> str:
    """Human-readable process exit status (143 = SIGTERM from harness, etc.)."""
    if rc is None:
        return "still-running"
    if rc == 0:
        return "0 (ok)"
    if rc < 0:
        return f"{rc} (killed by signal {-rc})"
    if rc == 143:
        return (
            "143 (SIGTERM: process was terminated — often by the harness "
            "after the peer failed, or on overall timeout; not a Java business error)"
        )
    if rc == 137:
        return "137 (SIGKILL / likely OOM killer)"
    if rc > 128:
        return f"{rc} (signal {rc - 128})"
    return f"{rc} (non-zero process exit)"


def log_tail(text: str, *, max_lines: int = 50, max_chars: int = 4000) -> str:
    """Tail of a perf log (errors and Aggregated lines are usually at the end)."""
    lines = text.splitlines()
    tail_lines = lines[-max_lines:] if lines else []
    tail = "\n".join(tail_lines)
    if len(tail) > max_chars:
        tail = tail[-max_chars:]
    return tail if tail.strip() else "(log empty)"


def format_e2e_process_failure(
    *,
    consumer_rc: int,
    producer_rc: int,
    consumer_out: str,
    producer_out: str,
    first_failed: str | None = None,
    consumer_label: str = "consumer",
    producer_label: str = "producer",
) -> str:
    """Build an error message that shows both exit codes and log tails."""
    if first_failed is None:
        if producer_rc not in (0, 143) and consumer_rc == 143:
            first_failed = "producer"
        elif consumer_rc not in (0, 143) and producer_rc == 143:
            first_failed = "consumer"
        elif consumer_rc == 143 and producer_rc == 143:
            first_failed = "timeout-or-both-sigterm"
        elif producer_rc != 0:
            first_failed = "producer"
        elif consumer_rc != 0:
            first_failed = "consumer"
        else:
            first_failed = "unknown"

    hang_note = ""
    if first_failed and first_failed.startswith("peer_hang_after_"):
        hang_note = (
            "\n  note: one side finished successfully (rc=0); the peer was still "
            "alive after peer_grace and was SIGTERM'd. Often the peer is stuck "
            "reconnecting after broker drop, or its -time window ends later "
            "because it started later. Check whether broker stayed up."
        )

    parts = [
        "E2E dual-process failure "
        f"(first_failed={first_failed})",
        f"  {consumer_label}_rc={explain_exit_code(consumer_rc)}",
        f"  {producer_label}_rc={explain_exit_code(producer_rc)}",
    ]
    if hang_note:
        parts.append(hang_note)
    parts.extend(
        [
            f"--- {producer_label} log tail ---",
            log_tail(producer_out),
            f"--- {consumer_label} log tail ---",
            log_tail(consumer_out),
        ]
    )
    return "\n".join(parts)


def e2e_success_despite_peer_hang(
    consumer_rc: int,
    producer_rc: int,
    first_failed: str | None,
) -> bool:
    """True when the primary side finished ok and only the peer was grace-killed.

    Used so -time E2E does not hard-fail after a 600s hang when consumer already
    completed successfully and producer is stuck reconnecting.
    """
    if not first_failed or not first_failed.startswith("peer_hang_after_"):
        return False
    if first_failed == "peer_hang_after_consumer_ok":
        return consumer_rc == 0 and producer_rc in (0, 143, -15)
    if first_failed == "peer_hang_after_producer_ok":
        return producer_rc == 0 and consumer_rc in (0, 143, -15)
    return False


def run_consumer_then_feed(
    consumer_cmd: list[str],
    producer_cmd: list[str],
    consumer_log: Path,
    producer_log: Path,
    consumer_timeout: float = 300.0,
    producer_timeout: float = 300.0,
) -> tuple[str, str, int, int, str | None]:
    """Run consumer then feed producer.

    Returns:
        consumer_out, producer_out, consumer_rc, producer_rc, first_failed
        first_failed is \"consumer\" | \"producer\" | \"timeout\" | None (both ok).
    """
    # Line-buffered file captures so interval/error lines show up sooner if the
    # process dies.
    with consumer_log.open("w", encoding="utf-8", buffering=1) as consumer_fh:
        consumer_proc = subprocess.Popen(
            consumer_cmd,
            stdout=consumer_fh,
            stderr=subprocess.STDOUT,
            text=True,
            env=ENV_BASE,
        )

    try:
        wait_for_log(consumer_log, "Start receiving from")
    except Exception:
        _terminate_process(consumer_proc)
        # Ensure FH closed and content readable for the error path.
        raise

    with producer_log.open("w", encoding="utf-8", buffering=1) as producer_fh:
        producer_proc = subprocess.Popen(
            producer_cmd,
            stdout=producer_fh,
            stderr=subprocess.STDOUT,
            text=True,
            env=ENV_BASE,
        )

    consumer_rc, producer_rc, first_failed = _wait_both_or_kill(
        consumer_proc,
        producer_proc,
        consumer_log=consumer_log,
        producer_log=producer_log,
        timeout=max(consumer_timeout, producer_timeout),
    )

    return (
        consumer_log.read_text(encoding="utf-8", errors="replace"),
        producer_log.read_text(encoding="utf-8", errors="replace"),
        consumer_rc,
        producer_rc,
        first_failed,
    )


def _flush_log_path(path: Path) -> None:
    """Best-effort: give the OS/Java a moment, then touch-read the log."""
    time.sleep(0.5)
    try:
        # Force directory entry / page cache visibility for subsequent reads.
        with path.open("rb") as fh:
            fh.seek(0, 2)
    except OSError:
        pass


def _terminate_process(proc: subprocess.Popen) -> int:
    if proc.poll() is not None:
        return proc.returncode if proc.returncode is not None else -1
    proc.terminate()
    try:
        return proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        return proc.wait(timeout=5)


def _wait_both_or_kill(
    consumer_proc: subprocess.Popen,
    producer_proc: subprocess.Popen,
    *,
    consumer_log: Path,
    producer_log: Path,
    timeout: float,
    peer_grace_s: float = 45.0,
) -> tuple[int, int, str | None]:
    """Wait for both processes.

    - Both exit → return their codes.
    - One exits non-zero → SIGTERM the peer immediately (first_failed=that side).
    - One exits zero while the other is still running → give the peer
      ``peer_grace_s`` to finish (covers producer started after consumer, or
      brief drain). If still alive, SIGTERM the peer and tag
      first_failed=\"peer_hang_after_<side>_ok\" so callers can treat metrics
      as usable instead of a mysterious 600s timeout.
    - Overall ``timeout`` still bounds the whole wait.
    """
    deadline = time.monotonic() + timeout
    first_failed: str | None = None
    # When set, the named side already exited 0; peer must finish by this time.
    peer_grace_deadline: float | None = None
    ok_side: str | None = None

    while True:
        consumer_rc = consumer_proc.poll()
        producer_rc = producer_proc.poll()
        now = time.monotonic()

        if consumer_rc is not None and producer_rc is not None:
            return consumer_rc, producer_rc, first_failed

        # One side failed hard: kill peer.
        if consumer_rc is not None and consumer_rc != 0:
            if first_failed is None:
                first_failed = "consumer"
            _flush_log_path(consumer_log)
            peer_rc = _terminate_process(producer_proc)
            _flush_log_path(producer_log)
            return consumer_rc, peer_rc, first_failed

        if producer_rc is not None and producer_rc != 0:
            if first_failed is None:
                first_failed = "producer"
            _flush_log_path(producer_log)
            peer_rc = _terminate_process(consumer_proc)
            _flush_log_path(consumer_log)
            return peer_rc, producer_rc, first_failed

        # One side finished cleanly (rc=0); start/refresh grace for the peer.
        if consumer_rc == 0 and producer_rc is None:
            if ok_side != "consumer":
                ok_side = "consumer"
                peer_grace_deadline = now + peer_grace_s
        elif producer_rc == 0 and consumer_rc is None:
            if ok_side != "producer":
                ok_side = "producer"
                peer_grace_deadline = now + peer_grace_s

        if peer_grace_deadline is not None and now >= peer_grace_deadline:
            # Successful side is done; peer hung (often reconnect after broker drop).
            first_failed = f"peer_hang_after_{ok_side}_ok"
            _flush_log_path(consumer_log)
            _flush_log_path(producer_log)
            if consumer_rc is None:
                consumer_rc = _terminate_process(consumer_proc)
            if producer_rc is None:
                producer_rc = _terminate_process(producer_proc)
            _flush_log_path(consumer_log)
            _flush_log_path(producer_log)
            return consumer_rc, producer_rc, first_failed

        if now >= deadline:
            first_failed = first_failed or "timeout"
            _flush_log_path(consumer_log)
            _flush_log_path(producer_log)
            c_rc = (
                consumer_rc
                if consumer_rc is not None
                else _terminate_process(consumer_proc)
            )
            p_rc = (
                producer_rc
                if producer_rc is not None
                else _terminate_process(producer_proc)
            )
            _flush_log_path(consumer_log)
            _flush_log_path(producer_log)
            return c_rc, p_rc, first_failed

        time.sleep(0.2)
