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
import shutil
import sys

from . import BASE_CONFIG, BROKER_BIN


@dataclasses.dataclass
class BrokerConfig:
    name: str
    port: int
    default_partitions: int


class BrokerSampler(threading.Thread):
    def __init__(self, pid: int, interval: float = 0.5, cgroup_dir: str | None = None):
        super().__init__(daemon=True)
        self.pid = pid
        self.interval = interval
        self.cgroup_dir = cgroup_dir
        self.samples: list[dict[str, float]] = []
        self._stop_event = threading.Event()
        self._last_total = None
        self._last_time = None
        self._clk_tck = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

    def stop(self) -> None:
        self._stop_event.set()

    def reset(self) -> None:
        """Drop samples and restart the CPU delta baseline. Call before each
        scenario so metrics() reflects only that scenario's window."""
        self.samples.clear()
        self._last_total = None
        self._last_time = None

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                with open(f"/proc/{self.pid}/stat", "r", encoding="utf-8") as fh:
                    stat_fields = fh.read().split()
                with open(f"/proc/{self.pid}/status", "r", encoding="utf-8") as fh:
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

            rss_match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", status_text, re.MULTILINE)
            rss_mb = (float(rss_match.group(1)) / 1024.0) if rss_match else 0.0
            sample: dict[str, float] = {"cpu_pct": cpu_pct, "rss_mb": rss_mb}
            if self.cgroup_dir:
                try:
                    with open(
                        f"{self.cgroup_dir}/memory.stat", "r", encoding="utf-8"
                    ) as fh:
                        mem_stat = fh.read()
                    for line in mem_stat.splitlines():
                        key, _, value = line.partition(" ")
                        if key == "anon":
                            sample["anon_mb"] = int(value) / 1048576.0
                        elif key == "file":
                            sample["file_mb"] = int(value) / 1048576.0
                except (OSError, ValueError):
                    pass
            self.samples.append(sample)
            time.sleep(self.interval)

    def write_csv(self, csv_path: Path) -> None:
        fieldnames = ["cpu_pct", "rss_mb"]
        if any("anon_mb" in sample for sample in self.samples):
            fieldnames += ["anon_mb", "file_mb"]
        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.samples)


def _config_text(config: BrokerConfig, db_path: str) -> str:
    config_text = BASE_CONFIG.read_text(encoding="utf-8")
    config_text = re.sub(
        r'^addr\s*=\s*".*"$',
        f'addr = "127.0.0.1:{config.port}"',
        config_text,
        flags=re.MULTILINE,
    )
    config_text = re.sub(
        r'^db_path\s*=\s*".*"$',
        f'db_path = "{db_path}"',
        config_text,
        flags=re.MULTILINE,
    )
    config_text = re.sub(
        r"^default_partitions\s*=\s*\d+$",
        f"default_partitions = {config.default_partitions}",
        config_text,
        flags=re.MULTILINE,
    )
    return config_text


class BrokerProcess:
    def __init__(
        self,
        config: BrokerConfig,
        cgroup_memory: str | None = None,
        cgroup_cpus: str | None = None,
    ):
        """Local broker process.

        cgroup_memory: MemoryMax for systemd-run --user --scope (e.g. "4294967296"
            or "4G"); MemorySwapMax is pinned to 0 to match docker --memory-swap.
            Requires user-scope cgroup delegation (memory controller).
        cgroup_cpus: CPU affinity via taskset -c (e.g. "0-3"), equivalent to
            docker --cpuset-cpus (same sched_setaffinity mechanism).
        """
        self.config = config
        self.cgroup_memory = cgroup_memory
        self.cgroup_cpus = cgroup_cpus
        self.proc: subprocess.Popen[str] | None = None
        self.broker_pid: int | None = None
        self.workdir: Path | None = None
        self.log_path: Path | None = None
        self.sampler: BrokerSampler | None = None

    def _broker_cmd(self) -> list[str]:
        cmd: list[str] = []
        if self.cgroup_memory:
            cmd += [
                "systemd-run",
                "--user",
                "--scope",
                "-p",
                f"MemoryMax={self.cgroup_memory}",
                "-p",
                "MemorySwapMax=0",
            ]
        if self.cgroup_cpus:
            cmd += ["taskset", "-c", self.cgroup_cpus]
        cmd.append(str(BROKER_BIN))
        return cmd

    def start(self) -> None:
        temp_dir = Path(
            tempfile.mkdtemp(prefix=f"pulsar-lite-{self.config.name}-", dir="/tmp")
        )
        (temp_dir / "pulsar-lite.toml").write_text(
            _config_text(self.config, str(temp_dir / "pulsar-lite.db")),
            encoding="utf-8",
        )
        self.log_path = temp_dir / "broker.log"
        log_file = self.log_path.open("w", encoding="utf-8")
        self.proc = subprocess.Popen(
            self._broker_cmd(),
            cwd=temp_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "RUST_BACKTRACE": "1"},
        )
        self.workdir = temp_dir
        self._wait_for_port()
        self.broker_pid = self.proc.pid
        self.sampler = BrokerSampler(self.broker_pid)
        self.sampler.start()

    def _wait_for_port(self, timeout: float = 15.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(
                    f'broker {self.config.name} exited early: {self.log_path.read_text(encoding="utf-8", errors="replace") if self.log_path and self.log_path.exists() else "no log"}'
                )
            try:
                with socket.create_connection(
                    ("127.0.0.1", self.config.port), timeout=0.5
                ):
                    return
            except OSError:
                time.sleep(0.2)
        raise RuntimeError(
            f"broker {self.config.name} did not bind port {self.config.port}"
        )

    def stop(self, cleanup: bool = False) -> dict[str, float]:
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
        self.broker_pid = None
        if cleanup and self.workdir:
            shutil.rmtree(self.workdir, ignore_errors=True)
            self.workdir = None
            self.log_path = None
        return metrics

    def restart(self, preserve_storage: bool = False) -> None:
        """Stop and start broker.

        Args:
            preserve_storage: If True, reuse existing workdir and DB.
                            If False (default), create fresh workdir and DB.
        """
        if preserve_storage:
            # Keep existing workdir and DB, only stop/start process
            self.stop()
            if not self.workdir or not self.log_path:
                raise RuntimeError("Cannot preserve storage: workdir not initialized")

            # Reopen log file for appending
            log_file = self.log_path.open("a", encoding="utf-8")
            self.proc = subprocess.Popen(
                self._broker_cmd(),
                cwd=self.workdir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                env={**os.environ, "RUST_BACKTRACE": "1"},
            )
            self._wait_for_port()
            self.broker_pid = self.proc.pid
            self.sampler = BrokerSampler(self.broker_pid)
            self.sampler.start()
        else:
            # Fresh storage: drop the previous /tmp workdir so disk does not accumulate.
            self.stop(cleanup=True)
            self.start()

    def metrics(self) -> dict[str, float]:
        samples = self.sampler.samples if self.sampler else []
        if not samples:
            return {
                "broker_avg_cpu_pct": 0.0,
                "broker_peak_cpu_pct": 0.0,
                "broker_peak_rss_mb": 0.0,
                "broker_peak_anon_mb": 0.0,
                "broker_peak_file_mb": 0.0,
            }
        cpu_values = [sample["cpu_pct"] for sample in samples[1:]] or [0.0]
        rss_values = [sample["rss_mb"] for sample in samples]
        anon_values = [sample.get("anon_mb", 0.0) for sample in samples]
        file_values = [sample.get("file_mb", 0.0) for sample in samples]
        return {
            "broker_avg_cpu_pct": round(sum(cpu_values) / len(cpu_values), 3),
            "broker_peak_cpu_pct": round(max(cpu_values), 3),
            "broker_peak_rss_mb": round(max(rss_values), 3),
            "broker_peak_anon_mb": round(max(anon_values), 3),
            "broker_peak_file_mb": round(max(file_values), 3),
        }


class ExternalBrokerProcess(BrokerProcess):
    """Adapter for an already-running external broker (e.g. Apache Pulsar
    standalone started manually with cgroup limits).

    No lifecycle management: the harness only builds perf commands against
    ``broker.config.port``. Scenarios that restart the broker
    (restart_replay, redelivery_unacked) are not supported.

    If ``unit`` is given (systemd unit name, e.g. ``pulsar-standalone``), the
    broker PID is resolved via ``systemctl show <unit> -p MainPID`` and CPU /
    RSS / cgroup anon+file metrics are sampled like the local backend.
    """

    def __init__(self, config: BrokerConfig, unit: str | None = None):
        super().__init__(config)
        self.log_path = None
        self.unit = unit
        self.sampler = None

    def _resolve_pid(self) -> int | None:
        try:
            out = subprocess.run(
                ["systemctl", "show", self.unit, "-p", "MainPID", "--value"],
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()
        except (OSError, subprocess.TimeoutExpired):
            return None
        if not out.isdigit():
            return None
        pid = int(out)
        if not os.path.exists(f"/proc/{pid}"):
            return None
        return pid

    def _resolve_cgroup_dir(self, pid: int) -> str | None:
        try:
            with open(f"/proc/{pid}/cgroup", "r", encoding="utf-8") as fh:
                rel = fh.read().strip().split(":")[-1]
            path = f"/sys/fs/cgroup{rel}"
            if os.path.isfile(f"{path}/memory.stat"):
                return path
        except OSError:
            pass
        return None

    def start(self) -> None:
        # External broker must already be listening.
        self.broker_pid = None
        self._wait_for_port()
        if not self.unit:
            return
        pid = self._resolve_pid()
        if pid is None:
            print(
                f"  [warn] systemd unit '{self.unit}' not found; "
                "broker CPU/memory metrics disabled",
                file=sys.stderr,
            )
            return
        self.broker_pid = pid
        self.sampler = BrokerSampler(pid, cgroup_dir=self._resolve_cgroup_dir(pid))
        self.sampler.start()

    def stop(self, cleanup: bool = False) -> dict[str, float]:
        metrics = self.metrics()
        if self.sampler:
            self.sampler.stop()
        return metrics

    def restart(self, preserve_storage: bool = False) -> None:
        raise NotImplementedError(
            "external broker backend cannot restart the broker; "
            "use scenarios that do not restart "
            "(produce / consume_e2e / backlog_drain)"
        )


class DockerBrokerProcess(BrokerProcess):
    def __init__(
        self, config: BrokerConfig, image_tag: str, cpuset_cpus: str, memory: str
    ):
        super().__init__(config)
        self.image_tag = image_tag
        self.cpuset_cpus = cpuset_cpus
        self.memory = memory
        self.container_name: str | None = None

    def start(self) -> None:
        temp_dir = Path(
            tempfile.mkdtemp(prefix=f"pulsar-lite-{self.config.name}-", dir="/tmp")
        )
        (temp_dir / "pulsar-lite.toml").write_text(
            _config_text(self.config, "/work/pulsar-lite.db"), encoding="utf-8"
        )
        self.log_path = temp_dir / "broker.log"
        self.container_name = f"pulsar-lite-perf-{temp_dir.name}"
        log_file = self.log_path.open("w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                self.container_name,
                "--network",
                "host",
                "--cpuset-cpus",
                self.cpuset_cpus,
                "--memory",
                self.memory,
                "--memory-swap",
                self.memory,
                "-v",
                f"{temp_dir}:/work",
                "-w",
                "/work",
                "-e",
                "RUST_BACKTRACE=1",
                self.image_tag,
                "/app/pulsar-lite",
            ],
            cwd=temp_dir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ, "RUST_BACKTRACE": "1"},
        )
        self.workdir = temp_dir
        self._wait_for_port()
        self.broker_pid = self._container_pid()
        self.sampler = BrokerSampler(self.broker_pid)
        self.sampler.start()

    def _container_pid(self) -> int:
        if not self.container_name:
            raise RuntimeError("docker container name is not set")
        proc = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Pid}}", self.container_name],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        pid_text = proc.stdout.strip()
        if not pid_text or pid_text == "0":
            raise RuntimeError(
                f"docker container {self.container_name} has no host pid"
            )
        return int(pid_text)

    def stop(self,cleanup: bool = False) -> dict[str, float]:
        metrics = self.metrics()
        if self.sampler:
            self.sampler.stop()
        if self.container_name:
            subprocess.run(
                ["docker", "stop", self.container_name],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.terminate()
                try:
                    self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.proc.kill()
                    self.proc.wait(timeout=5)
        if self.sampler:
            self.sampler.join(timeout=2)
        self.broker_pid = None
        self.container_name = None
        if cleanup and self.workdir:
            # The files written by the Docker container as root cannot be deleted by an ordinary user.
            # Use a temporary Alpine container with root privileges to clean up the mounted directory.
            subprocess.run(
                [
                    "docker", "run", "--rm", "-v", f"{self.workdir}:/work", "alpine:latest", "sh", "-c", "rm -rf /work/* /work/.[!.]* 2>/dev/null || true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            shutil.rmtree(self.workdir, ignore_errors=True)
            self.workdir = None
            self.log_path = None
        return metrics

    def restart(self, preserve_storage: bool = False) -> None:
        """Stop and start broker.

        Args:
            preserve_storage: If True, reuse existing workdir and DB.
                            If False (default), create fresh workdir and DB.
        """
        if preserve_storage:
            # Keep existing workdir and DB, only stop/start process
            self.stop()
            if not self.workdir or not self.log_path:
                raise RuntimeError("Cannot preserve storage: workdir not initialized")

            # Generate new container name
            self.container_name = f"pulsar-lite-perf-{self.workdir.name}"

            # Reopen log file for appending
            log_file = self.log_path.open("a", encoding="utf-8")
            self.proc = subprocess.Popen(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    self.container_name,
                    "--network",
                    "host",
                    "--cpuset-cpus",
                    self.cpuset_cpus,
                    "--memory",
                    self.memory,
                    "--memory-swap",
                    self.memory,
                    "-v",
                    f"{self.workdir}:/work",
                    "-w",
                    "/work",
                    "-e",
                    "RUST_BACKTRACE=1",
                    self.image_tag,
                    "/app/pulsar-lite",
                ],
                cwd=self.workdir,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                env={**os.environ, "RUST_BACKTRACE": "1"},
            )
            self._wait_for_port()
            self.broker_pid = self._container_pid()
            self.sampler = BrokerSampler(self.broker_pid)
            self.sampler.start()
        else:
            # Fresh storage: drop the previous /tmp workdir so disk does not accumulate.
            self.stop(cleanup=True)
            self.start()
