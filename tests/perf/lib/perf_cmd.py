from __future__ import annotations

import subprocess
import time
from pathlib import Path

from . import BROKER_BIN, CLASSPATH_FILE, ENV_BASE, JAVA, PULSAR_ROOT, PULSAR_TESTCLIENT_JAR


def ensure_prereqs() -> None:
    if not BROKER_BIN.exists():
        raise FileNotFoundError(f'broker binary missing: {BROKER_BIN}')
    if not PULSAR_TESTCLIENT_JAR.exists():
        raise FileNotFoundError(f'pulsar-testclient jar missing: {PULSAR_TESTCLIENT_JAR}')
    if not CLASSPATH_FILE.exists():
        CLASSPATH_FILE.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ['mvn', '-pl', 'pulsar-testclient', 'dependency:build-classpath',
            '-DincludeScope=runtime', f'-Dmdep.outputFile={CLASSPATH_FILE}'],
            cwd=str(PULSAR_ROOT), check=True,
    )


def perf_cmd(subcommand: str, service_url: str, extra_args: list[str], topic: str, histogram_path: Path) -> list[str]:
    classpath = f"{PULSAR_TESTCLIENT_JAR}:{CLASSPATH_FILE.read_text(encoding='utf-8').strip()}"
    return [
        str(JAVA),
        '-cp',
        classpath,
        'org.apache.pulsar.testclient.PulsarPerfTestTool',
        str(PULSAR_ROOT / 'conf' / 'client.conf'),
        subcommand,
        '-u', service_url,
        '--histogram-file', str(histogram_path),
        *extra_args,
        topic,
    ]


def run_sync(cmd: list[str], stdout_path: Path, timeout: float = 300.0) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout, env=ENV_BASE)
    stdout_path.write_text(proc.stdout, encoding='utf-8')
    return proc


def wait_for_log(path: Path, needle: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if path.exists() and needle in path.read_text(encoding='utf-8', errors='replace'):
            return
        time.sleep(0.2)
    raise RuntimeError(f'timed out waiting for {needle!r} in {path}')


def run_consumer_then_feed(consumer_cmd: list[str], producer_cmd: list[str], consumer_log: Path, producer_log: Path, consumer_timeout: float = 300.0) -> tuple[str, str, int, int]:
    with consumer_log.open('w', encoding='utf-8') as consumer_fh:
        consumer_proc = subprocess.Popen(consumer_cmd, stdout=consumer_fh, stderr=subprocess.STDOUT, text=True, env=ENV_BASE)
    wait_for_log(consumer_log, 'Start receiving from')
    producer_proc = run_sync(producer_cmd, producer_log)

    try:
        consumer_rc = consumer_proc.wait(timeout=consumer_timeout)
    except subprocess.TimeoutExpired:
        consumer_proc.terminate()
        try:
            consumer_rc = consumer_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            consumer_proc.kill()
            consumer_rc = consumer_proc.wait(timeout=5)

    return consumer_log.read_text(encoding='utf-8', errors='replace'), producer_proc.stdout, consumer_rc, producer_proc.returncode
