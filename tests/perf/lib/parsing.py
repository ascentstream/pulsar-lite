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
