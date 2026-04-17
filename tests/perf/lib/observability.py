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
        if self._proc and self._proc.poll() is None:
            self._proc.wait(timeout=30)

    @staticmethod
    def generate_flamegraph(perf_data_path: Path, svg_output_path: Path) -> bool:
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
