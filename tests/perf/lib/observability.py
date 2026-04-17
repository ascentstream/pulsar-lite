from __future__ import annotations

import shutil
import signal
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
            # Send SIGINT to perf record so it flushes data and exits
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.terminate()
                self._proc.wait(timeout=5)

    @staticmethod
    def generate_flamegraph(perf_data_path: Path, svg_output_path: Path) -> bool:
        collapse = shutil.which('inferno-collapse-perf')
        flamegraph = shutil.which('inferno-flamegraph')
        if not collapse or not flamegraph:
            return False

        # Step 1: perf script -> inferno-collapse-perf (folded format)
        script_proc = subprocess.Popen(
            ['perf', 'script', '-i', str(perf_data_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        collapse_proc = subprocess.Popen(
            [collapse],
            stdin=script_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        script_proc.stdout.close()

        # Step 2: folded format -> inferno-flamegraph (SVG)
        with svg_output_path.open('w', encoding='utf-8') as svg_fh:
            fg_proc = subprocess.Popen(
                [flamegraph],
                stdin=collapse_proc.stdout,
                stdout=svg_fh,
                stderr=subprocess.DEVNULL,
            )
            collapse_proc.stdout.close()
            fg_proc.wait(timeout=60)

        collapse_proc.wait(timeout=10)
        script_proc.wait(timeout=10)

        return svg_output_path.exists() and svg_output_path.stat().st_size > 0
