from __future__ import annotations

import csv
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
        with csv_path.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.DictWriter(fh, fieldnames=['cpu_pct', 'rss_mb'])
            writer.writeheader()
            writer.writerows(self.samples)


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
