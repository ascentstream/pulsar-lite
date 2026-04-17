# E2E Perf Stress Phase 2 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create an independent stress test script and shared library to find pulsar-lite non-persistent throughput ceiling, with perf record + flamegraph observability.

**Architecture:** Extract shared infrastructure (broker management, output parsing, perf command construction) from `scripts/perf/run_non_persistent_e2e_matrix.py` into `tests/perf/lib/`. Build a new `tests/perf/run_non_persistent_stress.py` that reuses these modules with unlimited-rate, large-payload, long-duration, and high-fanout scenarios. Add `PerfCollector` for automatic perf record capture and flamegraph generation.

**Tech Stack:** Python 3.10, pulsar-perf testclient (Java), Linux perf, inferno (Rust flamegraph tool)

---

### Task 1: Create `tests/perf/lib/` directory and shared constants

**Files:**
- Create: `tests/perf/lib/__init__.py`

**Step 1: Create directory structure**

```bash
mkdir -p tests/perf/lib
```

**Step 2: Create `tests/perf/lib/__init__.py`**

```python
from __future__ import annotations

from pathlib import Path
import os

ROOT = Path(__file__).resolve().parents[2]
PULSAR_ROOT = Path('/home/xtline/code/work/pulsar')
JAVA_HOME = Path('/usr/lib/jvm/java-17-openjdk-amd64')
JAVA = JAVA_HOME / 'bin' / 'java'
BROKER_BIN = ROOT / 'rust' / 'target' / 'release' / 'pulsar-lite'
BASE_CONFIG = ROOT / 'rust' / 'pulsar-lite.toml'
PULSAR_TESTCLIENT_JAR = PULSAR_ROOT / 'pulsar-testclient' / 'target' / 'pulsar-testclient.jar'
CLASSPATH_FILE = Path('/tmp/pulsar-testclient.classpath')

ENV_BASE = os.environ.copy()
ENV_BASE['JAVA_HOME'] = str(JAVA_HOME)
ENV_BASE['PATH'] = f"{JAVA_HOME / 'bin'}:{ENV_BASE.get('PATH', '')}"
```

**Step 3: Verify import works**

Run: `python3 -c "from tests.perf.lib import ROOT; print(ROOT)"`
Expected: prints the project root path

**Step 4: Commit**

```bash
git add tests/perf/lib/__init__.py
git commit -m "perf(stress): create shared lib with constants"
```

---

### Task 2: Extract `broker.py` — BrokerProcess, BrokerSampler, BrokerConfig

**Files:**
- Create: `tests/perf/lib/broker.py`

**Step 1: Create `tests/perf/lib/broker.py`**

Extract these classes from `scripts/perf/run_non_persistent_e2e_matrix.py` verbatim:
- `BrokerConfig` (lines 39-43)
- `BrokerSampler` (lines 196-233)
- `BrokerProcess` (lines 236-302)

The module should import `ROOT`, `BASE_CONFIG`, `BROKER_BIN` from `tests.perf.lib`.

```python
from __future__ import annotations

import dataclasses
import os
import re
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from . import BASE_CONFIG, BROKER_BIN


@dataclasses.dataclass
class BrokerConfig:
    name: str
    port: int
    default_partitions: int


class BrokerSampler(threading.Thread):
    def __init__(self, pid: int, interval: float = 0.5):
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = interval
        self.samples: list[dict[str, float]] = []
        self._stop_event = threading.Event()
        self._last_total = None
        self._last_time = None
        self._clk_tck = os.sysconf(os.sysconf_names['SC_CLK_TCK'])

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                with open(f'/proc/{self.pid}/stat', 'r', encoding='utf-8') as fh:
                    stat_fields = fh.read().split()
                with open(f'/proc/{self.pid}/status', 'r', encoding='utf-8') as fh:
                    status_text = fh.read()
            except FileNotFoundError:
                break

            total_ticks = float(stat_fields[13]) + float(stat_fields[14])
            now = time.time()
            cpu_pct = 0.0
            if self._last_total is not None and self._last_time is not None:
                delta_ticks = total_ticks - self._last_total
                delta_time = max(now - self._last_time, 1e-6)
                cpu_pct = (delta_ticks / self._clk_tck) / delta_time * 100.0
            self._last_total = total_ticks
            self._last_time = now

            rss_match = re.search(r'^VmRSS:\s+(\d+)\s+kB$', status_text, re.MULTILINE)
            rss_mb = (float(rss_match.group(1)) / 1024.0) if rss_match else 0.0
            self.samples.append({'cpu_pct': cpu_pct, 'rss_mb': rss_mb})
            time.sleep(self.interval)

    def write_csv(self, csv_path: Path) -> None:
        """Write CPU/RSS time series to CSV file."""
        import csv
        with csv_path.open('w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['elapsed_sec', 'cpu_pct', 'rss_mb'])
            start = self.samples[0]['_time'] if self.samples and '_time' in self.samples[0] else None
            for i, sample in enumerate(self.samples):
                elapsed = i * self.interval if start is None else sample.get('_time', i * self.interval) - start
                writer.writerow([f'{elapsed:.1f}', f'{sample["cpu_pct"]:.3f}', f'{sample["rss_mb"]:.3f}'])


class BrokerProcess:
    def __init__(self, config: BrokerConfig):
        self.config = config
        self.proc: subprocess.Popen[str] | None = None
        self.workdir: Path | None = None
        self.log_path: Path | None = None
        self.sampler: BrokerSampler | None = None

    def start(self) -> None:
        temp_dir = Path(tempfile.mkdtemp(prefix=f'pulsar-lite-{self.config.name}-', dir='/tmp'))
        config_text = BASE_CONFIG.read_text(encoding='utf-8')
        config_text = re.sub(r'^addr\s*=\s*".*"$', f'addr = "127.0.0.1:{self.config.port}"', config_text, flags=re.MULTILINE)
        config_text = re.sub(r'^db_path\s*=\s*".*"$', f'db_path = "{temp_dir / "pulsar-lite.db"}"', config_text, flags=re.MULTILINE)
        config_text = re.sub(r'^default_partitions\s*=\s*\d+$', f'default_partitions = {self.config.default_partitions}', config_text, flags=re.MULTILINE)
        (temp_dir / 'pulsar-lite.toml').write_text(config_text, encoding='utf-8')
        self.log_path = temp_dir / 'broker.log'
        log_file = self.log_path.open('w', encoding='utf-8')
        self.proc = subprocess.Popen(
            [str(BROKER_BIN)],
            cwd=temp_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.workdir = temp_dir
        self._wait_for_port()
        self.sampler = BrokerSampler(self.proc.pid)
        self.sampler.start()

    def _wait_for_port(self, timeout: float = 15.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(f'broker {self.config.name} exited early: {self.log_path.read_text(encoding="utf-8", errors="replace") if self.log_path and self.log_path.exists() else "no log"}')
            try:
                with socket.create_connection(('127.0.0.1', self.config.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(f'broker {self.config.name} did not bind port {self.config.port}')

    def stop(self) -> dict[str, float]:
        metrics = self.metrics()
        if self.sampler:
            self.sampler.stop()
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self.sampler:
            self.sampler.join(timeout=2)
        return metrics

    def metrics(self) -> dict[str, float]:
        samples = self.sampler.samples if self.sampler else []
        if not samples:
            return {'broker_avg_cpu_pct': 0.0, 'broker_peak_cpu_pct': 0.0, 'broker_peak_rss_mb': 0.0}
        cpu_values = [sample['cpu_pct'] for sample in samples[1:]] or [0.0]
        rss_values = [sample['rss_mb'] for sample in samples]
        return {
            'broker_avg_cpu_pct': round(sum(cpu_values) / len(cpu_values), 3),
            'broker_peak_cpu_pct': round(max(cpu_values), 3),
            'broker_peak_rss_mb': round(max(rss_values), 3),
        }
```

**Step 2: Verify import**

Run: `python3 -c "from tests.perf.lib.broker import BrokerProcess, BrokerSampler, BrokerConfig; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add tests/perf/lib/broker.py
git commit -m "perf(stress): extract broker management to shared lib"
```

---

### Task 3: Extract `parsing.py` — output parsers

**Files:**
- Create: `tests/perf/lib/parsing.py`

**Step 1: Create `tests/perf/lib/parsing.py`**

Extract from `scripts/perf/run_non_persistent_e2e_matrix.py`:
- `parse_producer_output` (lines 330-345)
- `parse_consumer_output` (lines 348-365)

```python
from __future__ import annotations

import re
from typing import Any


def parse_producer_output(text: str) -> dict[str, Any]:
    throughput = re.search(r'Aggregated throughput stats ---\s+(\d+) records sent ---\s+([\d.]+) msg/s ---\s+([\d.]+) Mbit/s', text)
    latency = re.search(r'Aggregated latency stats --- Latency: mean:\s+([\d.]+) ms - med:\s+([\d.]+) - 95pct:\s+([\d.]+) - 99pct:\s+([\d.]+) - 99\.9pct:\s+([\d.]+) - 99\.99pct:\s+([\d.]+) - 99\.999pct:\s+([\d.]+) - Max:\s+([\d.]+)', text)
    if not throughput or not latency:
        raise RuntimeError(f'failed to parse producer output:\n{text}')
    return {
        'records': int(throughput.group(1)),
        'throughput_msg_s': float(throughput.group(2)),
        'throughput_mbit_s': float(throughput.group(3)),
        'latency_mean_ms': float(latency.group(1)),
        'latency_p50_ms': float(latency.group(2)),
        'latency_p95_ms': float(latency.group(3)),
        'latency_p99_ms': float(latency.group(4)),
        'latency_p999_ms': float(latency.group(5)),
        'latency_max_ms': float(latency.group(8)),
    }


def parse_consumer_output(text: str) -> dict[str, Any]:
    throughput = re.search(r'Aggregated throughput stats ---\s+(\d+) records received ---\s+([\d.]+) msg/s ---\s+([\d.]+) Mbit/s --- AckRate: ([\d.]+)\s+msg/s --- ack failed (\d+) msg', text)
    latency = re.search(r'Aggregated latency stats --- Latency: mean:\s+([\d.]+) ms - med:\s+([\d.]+) - 95pct:\s+([\d.]+) - 99pct:\s+([\d.]+) - 99\.9pct:\s+([\d.]+) - 99\.99pct:\s+([\d.]+) - 99\.999pct:\s+([\d.]+) - Max:\s+([\d.]+)', text)
    if not throughput or not latency:
        raise RuntimeError(f'failed to parse consumer output:\n{text}')
    return {
        'records': int(throughput.group(1)),
        'throughput_msg_s': float(throughput.group(2)),
        'throughput_mbit_s': float(throughput.group(3)),
        'ack_rate_msg_s': float(throughput.group(4)),
        'ack_failed': int(throughput.group(5)),
        'latency_mean_ms': float(latency.group(1)),
        'latency_p50_ms': float(latency.group(2)),
        'latency_p95_ms': float(latency.group(3)),
        'latency_p99_ms': float(latency.group(4)),
        'latency_p999_ms': float(latency.group(5)),
        'latency_max_ms': float(latency.group(8)),
    }
```

**Step 2: Verify import**

Run: `python3 -c "from tests.perf.lib.parsing import parse_producer_output, parse_consumer_output; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add tests/perf/lib/parsing.py
git commit -m "perf(stress): extract output parsers to shared lib"
```

---

### Task 4: Extract `perf_cmd.py` — command construction + helpers

**Files:**
- Create: `tests/perf/lib/perf_cmd.py`

**Step 1: Create `tests/perf/lib/perf_cmd.py`**

Extract from `scripts/perf/run_non_persistent_e2e_matrix.py`:
- `perf_cmd` (lines 314-327)
- `run_sync` (lines 368-371)
- `wait_for_log` (lines 374-380)
- `run_consumer_then_feed` (lines 383-399)
- `ensure_prereqs` (lines 305-311)

```python
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from . import BROKER_BIN, PULSAR_TESTCLIENT_JAR, CLASSPATH_FILE, JAVA, PULSAR_ROOT, ENV_BASE


def ensure_prereqs() -> None:
    if not BROKER_BIN.exists():
        raise FileNotFoundError(f'broker binary missing: {BROKER_BIN}')
    if not PULSAR_TESTCLIENT_JAR.exists():
        raise FileNotFoundError(f'pulsar-testclient jar missing: {PULSAR_TESTCLIENT_JAR}')
    if not CLASSPATH_FILE.exists():
        raise FileNotFoundError(f'classpath file missing: {CLASSPATH_FILE}')


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
    producer_proc = run_sync(producer_cmd, producer_log, timeout=consumer_timeout)

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
```

Note: `run_sync` and `run_consumer_then_feed` timeout increased from 180s to 300s to accommodate sustained scenarios.

**Step 2: Verify import**

Run: `python3 -c "from tests.perf.lib.perf_cmd import perf_cmd, ensure_prereqs; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add tests/perf/lib/perf_cmd.py
git commit -m "perf(stress): extract perf command helpers to shared lib"
```

---

### Task 5: Create `observability.py` — PerfCollector

**Files:**
- Create: `tests/perf/lib/observability.py`

**Step 1: Create `tests/perf/lib/observability.py`**

```python
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path


class PerfCollector:
    """Manages perf record capture for a broker process during a scenario run."""

    def __init__(self, pid: int, duration: int, perf_data_path: Path):
        self.pid = pid
        self.duration = duration
        self.perf_data_path = perf_data_path
        self._proc: subprocess.Popen | None = None

    def start(self) -> None:
        """Start perf record in the background."""
        if not shutil.which('perf'):
            return
        self._proc = subprocess.Popen(
            [
                'perf', 'record',
                '-F', '99',
                '-g',
                '-p', str(self.pid),
                '-o', str(self.perf_data_path),
                '--', 'sleep', str(self.duration),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def stop(self) -> None:
        """Wait for perf record to finish."""
        if self._proc and self._proc.poll() is None:
            self._proc.wait(timeout=30)

    @staticmethod
    def generate_flamegraph(perf_data_path: Path, svg_output_path: Path) -> bool:
        """Generate flamegraph SVG from perf data using inferno-flamegraph.

        Returns True if successful, False if tool not available.
        """
        inferno = shutil.which('inferno-flamegraph')
        if not inferno:
            return False

        script_proc = subprocess.Popen(
            ['perf', 'script', '-i', str(perf_data_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        with svg_output_path.open('w', encoding='utf-8') as svg_fh:
            flamegraph_proc = subprocess.Popen(
                [inferno],
                stdin=script_proc.stdout,
                stdout=svg_fh,
                stderr=subprocess.DEVNULL,
            )
            script_proc.stdout.close()
            flamegraph_proc.wait(timeout=60)
            script_proc.wait(timeout=10)

        return svg_output_path.exists() and svg_output_path.stat().st_size > 0
```

**Step 2: Verify import**

Run: `python3 -c "from tests.perf.lib.observability import PerfCollector; print('OK')"`
Expected: `OK`

**Step 3: Commit**

```bash
git add tests/perf/lib/observability.py
git commit -m "perf(stress): add PerfCollector for perf record + flamegraph"
```

---

### Task 6: Install inferno dependency

**Step 1: Install inferno**

Run: `cargo install inferno`
Expected: installs `inferno-flamegraph` binary to `~/.cargo/bin/`

**Step 2: Verify**

Run: `which inferno-flamegraph`
Expected: path to binary (e.g., `/home/xtline/.cargo/bin/inferno-flamegraph`)

---

### Task 7: Create `run_non_persistent_stress.py` — stress test script

**Files:**
- Create: `tests/perf/run_non_persistent_stress.py`

**Step 1: Create the stress test script**

This is the main deliverable. It imports from `lib/`, defines 9 stress scenarios, runs them with perf collection, and generates flamegraphs.

```python
#!/usr/bin/env python3
from __future__ import annotations

import dataclasses
import json
import time
from pathlib import Path
from typing import Any

from lib.broker import BrokerConfig, BrokerProcess, BrokerSampler
from lib.parsing import parse_producer_output, parse_consumer_output
from lib.perf_cmd import ensure_prereqs, perf_cmd, run_sync, run_consumer_then_feed
from lib.observability import PerfCollector
from lib import ENV_BASE

ROOT = Path(__file__).resolve().parent.parent.parent
RESULTS_PATH = ROOT / 'docs' / 'perf' / 'data' / 'non_persistent_stress_results.json'
ARTIFACTS_DIR = ROOT / 'docs' / 'perf' / 'data' / 'non_persistent_stress_logs'


@dataclasses.dataclass
class StressScenario:
    name: str
    kind: str  # produce | consume_e2e
    broker: str
    description: str
    producer_args: list[str]
    consumer_args: list[str] | None = None
    feed_producer_args: list[str] | None = None
    estimated_duration: int = 60  # seconds, used for perf record sleep


BROKERS = {
    'nonpartitioned': BrokerConfig('nonpartitioned', 6651, 0),
    'nonpersistent_partitioned': BrokerConfig('nonpersistent_partitioned', 6652, 4),
}

STRESS_SCENARIOS: list[StressScenario] = [
    # --- Producer stress ---
    StressScenario(
        name='stress_producer_max_rate',
        kind='produce',
        broker='nonpartitioned',
        description='单 producer 不限速吞吐 ceiling',
        producer_args=['-r', '0', '-m', '500000', '-s', '1024'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_producer_max_rate_multi_producer',
        kind='produce',
        broker='nonpartitioned',
        description='4 producers 不限速并发吞吐 ceiling',
        producer_args=['-r', '0', '-m', '500000', '-s', '1024', '-n', '4'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_producer_large_payload',
        kind='produce',
        broker='nonpartitioned',
        description='100KiB payload 带宽瓶颈',
        producer_args=['-r', '0', '-s', '102400', '-m', '100000'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_producer_sustained',
        kind='produce',
        broker='nonpartitioned',
        description='5 分钟持续发送稳定性',
        producer_args=['-time', '300', '-r', '0', '-s', '1024'],
        estimated_duration=300,
    ),
    # --- Consumer / E2E stress ---
    StressScenario(
        name='stress_consume_shared_max_rate',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Shared 单 consumer 不限速吞吐 ceiling',
        producer_args=[],
        consumer_args=['-m', '500000', '-q', '10000', '-st', 'Shared'],
        feed_producer_args=['-r', '0', '-m', '500000', '-s', '1024'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_consume_shared_high_fanout',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Shared 16 consumers 不限速高 fanout',
        producer_args=[],
        consumer_args=['-m', '500000', '-q', '10000', '-st', 'Shared', '-n', '16'],
        feed_producer_args=['-r', '0', '-m', '500000', '-s', '1024'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_consume_multi_subscription_fanout',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='8 subscriptions 不限速高 fanout',
        producer_args=[],
        consumer_args=['-m', '2000000', '-q', '10000', '-st', 'Shared', '-ns', '8'],
        feed_producer_args=['-r', '0', '-m', '500000', '-s', '1024'],
        estimated_duration=60,
    ),
    StressScenario(
        name='stress_consume_sustained',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='5 分钟持续消费稳定性',
        producer_args=[],
        consumer_args=['-time', '300', '-q', '10000', '-st', 'Shared'],
        feed_producer_args=['-time', '300', '-r', '0', '-s', '1024'],
        estimated_duration=300,
    ),
    StressScenario(
        name='stress_consume_partitioned_max_rate',
        kind='consume_e2e',
        broker='nonpersistent_partitioned',
        description='Partitioned 4 partitions Shared 4 consumers 不限速',
        producer_args=[],
        consumer_args=['-m', '500000', '-q', '10000', '-st', 'Shared', '-n', '4'],
        feed_producer_args=['-r', '0', '-m', '500000', '-s', '1024'],
        estimated_duration=60,
    ),
]


def scenario_topic(run_id: str, scenario: StressScenario) -> str:
    return f'non-persistent://public/default/{run_id}-{scenario.name}'


def main() -> int:
    ensure_prereqs()
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime('%Y%m%d-%H%M%S')
    run_dir = ARTIFACTS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, Any] = {
        'run_id': run_id,
        'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'scenarios': [],
    }

    broker_metrics_by_name: dict[str, dict[str, float]] = {}
    perf_collectors: list[PerfCollector] = []
    perf_data_files: list[tuple[Path, Path]] = []  # (perf.data, target.svg)

    for broker_name, broker_cfg in BROKERS.items():
        broker = BrokerProcess(broker_cfg)
        print(f'==> starting broker {broker_name} on {broker_cfg.port} (default_partitions={broker_cfg.default_partitions})', flush=True)
        broker.start()
        service_url = f'pulsar://127.0.0.1:{broker_cfg.port}'
        try:
            for scenario in [s for s in STRESS_SCENARIOS if s.broker == broker_name]:
                topic = scenario_topic(run_id, scenario)
                print(f'==> running {scenario.name} [{scenario.description}] on {service_url}', flush=True)
                start_time = time.time()
                scenario_dir = run_dir / scenario.name
                scenario_dir.mkdir(parents=True, exist_ok=True)
                histogram = scenario_dir / f'{scenario.name}.hdr'

                # Start perf record
                perf_data_path = scenario_dir / 'perf.data'
                perf_collector = PerfCollector(broker.proc.pid, scenario.estimated_duration, perf_data_path)
                perf_collector.start()
                perf_collectors.append(perf_collector)

                result_entry: dict[str, Any] = {
                    'name': scenario.name,
                    'kind': scenario.kind,
                    'broker_profile': broker_name,
                    'service_url': service_url,
                    'description': scenario.description,
                    'topic': topic,
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(start_time)),
                    'status': 'ok',
                }
                try:
                    if scenario.kind == 'produce':
                        cmd = perf_cmd('produce', service_url, scenario.producer_args, topic, histogram)
                        producer_log = scenario_dir / 'producer.log'
                        proc = run_sync(cmd, producer_log, timeout=scenario.estimated_duration + 120)
                        if proc.returncode != 0:
                            raise RuntimeError(proc.stdout)
                        result_entry['metrics'] = parse_producer_output(proc.stdout)
                    elif scenario.kind == 'consume_e2e':
                        consumer_log = scenario_dir / 'consumer.log'
                        producer_log = scenario_dir / 'feed_producer.log'
                        consumer_cmd = perf_cmd('consume', service_url, scenario.consumer_args or [], topic, histogram)
                        producer_cmd = perf_cmd('produce', service_url, scenario.feed_producer_args or [], topic, scenario_dir / 'feed_producer.hdr')
                        consumer_timeout = scenario.estimated_duration + 120
                        consumer_text, producer_text, consumer_rc, producer_rc = run_consumer_then_feed(
                            consumer_cmd, producer_cmd, consumer_log, producer_log, consumer_timeout=consumer_timeout,
                        )
                        if producer_rc != 0:
                            raise RuntimeError(f'producer failed:\n{producer_text}')
                        if consumer_rc != 0:
                            raise RuntimeError(f'consumer failed:\n{consumer_text}')
                        result_entry['producer_metrics'] = parse_producer_output(producer_text)
                        result_entry['metrics'] = parse_consumer_output(consumer_text)
                    else:
                        raise ValueError(f'unknown scenario kind: {scenario.kind}')
                except Exception as exc:
                    result_entry['status'] = 'failed'
                    result_entry['error'] = str(exc)
                finally:
                    # Stop perf record
                    perf_collector.stop()

                    result_entry['duration_secs'] = round(time.time() - start_time, 3)
                    current_metrics = broker.metrics()
                    result_entry.update(current_metrics)

                    # Write broker timeseries CSV
                    if broker.sampler:
                        csv_path = scenario_dir / 'broker_timeseries.csv'
                        broker.sampler.write_csv(csv_path)
                        result_entry['broker_timeseries_file'] = csv_path.name

                    # Record perf data file paths for flamegraph generation
                    if perf_data_path.exists():
                        svg_path = scenario_dir / 'flamegraph.svg'
                        perf_data_files.append((perf_data_path, svg_path))
                        result_entry['perf_data_file'] = perf_data_path.name
                        result_entry['flamegraph_file'] = svg_path.name

                    results['scenarios'].append(result_entry)
        finally:
            broker_metrics_by_name[broker_name] = broker.stop()
            print(f'==> stopped broker {broker_name}', flush=True)

    # Generate flamegraphs
    print('==> generating flamegraphs...', flush=True)
    for perf_data_path, svg_path in perf_data_files:
        if perf_data_path.exists():
            PerfCollector.generate_flamegraph(perf_data_path, svg_path)

    results['broker_stop_metrics'] = broker_metrics_by_name
    RESULTS_PATH.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'==> results saved to {RESULTS_PATH}', flush=True)

    failed = [s for s in results['scenarios'] if s['status'] != 'ok']
    if failed:
        print(json.dumps(failed, ensure_ascii=False, indent=2), file=__import__('sys').stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
```

**Step 2: Verify syntax**

Run: `python3 -c "import ast; ast.parse(open('tests/perf/run_non_persistent_stress.py').read()); print('syntax OK')"`
Expected: `syntax OK`

**Step 3: Commit**

```bash
git add tests/perf/run_non_persistent_stress.py
git commit -m "perf(stress): add non-persistent stress test script"
```

---

### Task 8: Update coverage script to use shared lib

**Files:**
- Modify: `tests/perf/run_non_persistent_e2e_matrix.py` (migrated from `scripts/perf/`)

**Step 1: Migrate the coverage script**

```bash
cp scripts/perf/run_non_persistent_e2e_matrix.py tests/perf/run_non_persistent_e2e_matrix.py
```

**Step 2: Update imports in the migrated script**

Replace the inline `BrokerSampler`, `BrokerProcess`, `BrokerConfig`, `parse_producer_output`, `parse_consumer_output`, `perf_cmd`, `run_sync`, `wait_for_log`, `run_consumer_then_feed`, `ensure_prereqs` with imports from `lib/`:

```python
# At top of file, after existing imports, add:
from lib.broker import BrokerConfig, BrokerSampler, BrokerProcess
from lib.parsing import parse_producer_output, parse_consumer_output
from lib.perf_cmd import ensure_prereqs, perf_cmd, run_sync, wait_for_log, run_consumer_then_feed
from lib import ROOT, PULSAR_ROOT, JAVA_HOME, JAVA, BROKER_BIN, BASE_CONFIG, PULSAR_TESTCLIENT_JAR, CLASSPATH_FILE, ENV_BASE
```

Then remove all the duplicated class/function definitions that are now in `lib/`.

**Step 3: Verify coverage script still works**

Run: `python3 -c "import ast; ast.parse(open('tests/perf/run_non_persistent_e2e_matrix.py').read()); print('syntax OK')"`
Expected: `syntax OK`

**Step 4: Commit**

```bash
git add tests/perf/run_non_persistent_e2e_matrix.py
git rm scripts/perf/run_non_persistent_e2e_matrix.py
git commit -m "perf: migrate coverage script to tests/perf/, use shared lib"
```

---

### Task 9: Smoke test the stress script

**Step 1: Verify prerequisites**

```bash
ls rust/target/release/pulsar-lite
ls /home/xtline/code/work/pulsar/pulsar-testclient/target/pulsar-testclient.jar
ls /tmp/pulsar-testclient.classpath
```

All three should exist.

**Step 2: Run stress script with a quick scenario to verify the plumbing works**

Add a temporary fast scenario to the script (or run just one scenario manually) to verify:
- Broker starts and stops correctly
- pulsar-perf produce/consume commands execute
- perf.data is captured
- JSON results are written
- Flamegraph SVG is generated

```bash
cd tests/perf
python3 run_non_persistent_stress.py
```

**Step 3: Verify output artifacts**

```bash
ls docs/perf/data/non_persistent_stress_results.json
ls docs/perf/data/non_persistent_stress_logs/*/
```

Expected: JSON results file + per-scenario directories with logs, perf.data, flamegraph.svg, broker_timeseries.csv.

**Step 4: Commit any fixes**

If any fixes were needed during smoke testing, commit them:

```bash
git add -u tests/perf/
git commit -m "perf(stress): fix issues found during smoke test"
```

---

### Task 10: Update CLAUDE.md with new perf commands

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add stress test commands to CLAUDE.md**

In the "常用命令" section, after existing perf references, add:

```markdown
# Perf stress tests (E2E throughput ceiling)
python3 tests/perf/run_non_persistent_stress.py

# Perf coverage baseline
python3 tests/perf/run_non_persistent_e2e_matrix.py
```

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add stress and coverage perf commands to CLAUDE.md"
```
