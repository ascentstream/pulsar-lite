from __future__ import annotations

import re
import statistics
from typing import Any

# pulsar-perf prints one interval line about every 10s (Thread.sleep(10000)).
# First window is often cold (connect / warmup / empty feed); drop it when we
# have 2+ samples and take median of the rest as steady-state thr.

_AGG_PRODUCER_THR = re.compile(
    r"Aggregated throughput stats ---\s+(\d+) records sent ---\s+([\d.]+) msg/s ---\s+([\d.]+) Mbit/s"
)
_AGG_CONSUMER_THR = re.compile(
    r"Aggregated throughput stats ---\s+(\d+) records received ---\s+([\d.]+) msg/s ---\s+([\d.]+) Mbit/s --- AckRate: ([\d.]+)\s+msg/s --- ack failed (\d+) msg"
)
_AGG_LATENCY = re.compile(
    r"Aggregated latency stats --- Latency: mean:\s+([\d.]+) ms - med:\s+([\d.]+) - 95pct:\s+([\d.]+) - 99pct:\s+([\d.]+) - 99\.9pct:\s+([\d.]+) - 99\.99pct:\s+([\d.]+) - 99\.999pct:\s+([\d.]+) - Max:\s+([\d.]+)"
)

# Interval lines (per ~10s window). cumulative total is first number; rate is window thr.
_INTERVAL_PRODUCER = re.compile(
    r"Throughput produced:\s+(\d+) msg ---\s+([\d.]+)\s+msg/s ---\s+([\d.]+)\s+Mbit/s"
    r".*?Latency: mean:\s+([\d.]+)\s+ms - med:\s+([\d.]+) - 95pct:\s+([\d.]+) - 99pct:\s+([\d.]+)"
    r".*?Max:\s+([\d.]+)",
    re.S,
)
_INTERVAL_CONSUMER = re.compile(
    r"Throughput received:\s+(\d+) msg ---\s+([\d.]+)\s+msg/s ---\s+([\d.]+)\s+Mbit/s"
    r".*?Latency: mean:\s+([\d.]+)\s+ms - med:\s+([\d.]+) - 95pct:\s+([\d.]+) - 99pct:\s+([\d.]+)"
    r".*?Max:\s+([\d.]+)",
    re.S,
)


def _median(values: list[float]) -> float:
    return float(statistics.median(values))


def _steady_slices(intervals: list[dict[str, float]]) -> list[dict[str, float]]:
    """Drop the first interval when 2+ windows exist (cold start / empty feed)."""
    if len(intervals) >= 2:
        return intervals[1:]
    return intervals


def _from_intervals(intervals: list[dict[str, float]]) -> dict[str, Any] | None:
    if not intervals:
        return None
    steady = _steady_slices(intervals)
    thr = [row["throughput_msg_s"] for row in steady]
    mbit = [row["throughput_mbit_s"] for row in steady]
    lat_mean = [row["latency_mean_ms"] for row in steady]
    lat_p50 = [row["latency_p50_ms"] for row in steady]
    lat_p95 = [row["latency_p95_ms"] for row in steady]
    lat_p99 = [row["latency_p99_ms"] for row in steady]
    lat_max = [row["latency_max_ms"] for row in steady]
    return {
        "throughput_msg_s": _median(thr),
        "throughput_mbit_s": _median(mbit),
        "latency_mean_ms": _median(lat_mean),
        "latency_p50_ms": _median(lat_p50),
        "latency_p95_ms": _median(lat_p95),
        "latency_p99_ms": _median(lat_p99),
        "latency_max_ms": max(lat_max),
        "interval_count": len(intervals),
        "steady_interval_count": len(steady),
        "interval_throughput_msg_s_median": _median(thr),
        "interval_throughput_msg_s_min": min(thr),
        "interval_throughput_msg_s_max": max(thr),
        "records_cumulative_last": int(intervals[-1]["records_cumulative"]),
        "partial": len(steady) < 2,
    }


def _parse_producer_intervals(text: str) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for m in _INTERVAL_PRODUCER.finditer(text):
        rows.append(
            {
                "records_cumulative": float(m.group(1)),
                "throughput_msg_s": float(m.group(2)),
                "throughput_mbit_s": float(m.group(3)),
                "latency_mean_ms": float(m.group(4)),
                "latency_p50_ms": float(m.group(5)),
                "latency_p95_ms": float(m.group(6)),
                "latency_p99_ms": float(m.group(7)),
                "latency_max_ms": float(m.group(8)),
            }
        )
    return rows


def _parse_consumer_intervals(text: str) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for m in _INTERVAL_CONSUMER.finditer(text):
        rows.append(
            {
                "records_cumulative": float(m.group(1)),
                "throughput_msg_s": float(m.group(2)),
                "throughput_mbit_s": float(m.group(3)),
                "latency_mean_ms": float(m.group(4)),
                "latency_p50_ms": float(m.group(5)),
                "latency_p95_ms": float(m.group(6)),
                "latency_p99_ms": float(m.group(7)),
                "latency_max_ms": float(m.group(8)),
            }
        )
    return rows


def parse_producer_output(text: str) -> dict[str, Any]:
    """Parse pulsar-perf producer log.

    Prefer ~10s interval windows (drop first when 2+ exist, median of rest) for
    throughput/latency. Fall back to Aggregated wall-clock stats when no
    interval lines exist (short -m runs).
    """
    intervals = _parse_producer_intervals(text)
    steady = _from_intervals(intervals)

    agg_thr = _AGG_PRODUCER_THR.search(text)
    agg_lat = _AGG_LATENCY.search(text)

    if steady is not None:
        records = int(steady["records_cumulative_last"])
        if agg_thr:
            records = int(agg_thr.group(1))
        result = {
            "records": records,
            "throughput_msg_s": steady["throughput_msg_s"],
            "throughput_mbit_s": steady["throughput_mbit_s"],
            "latency_mean_ms": steady["latency_mean_ms"],
            "latency_p50_ms": steady["latency_p50_ms"],
            "latency_p95_ms": steady["latency_p95_ms"],
            "latency_p99_ms": steady["latency_p99_ms"],
            "latency_p999_ms": (
                float(agg_lat.group(5)) if agg_lat else None
            ),
            "latency_max_ms": steady["latency_max_ms"],
            "partial": steady["partial"],
            "metric_source": "interval_median",
            "interval_count": steady["interval_count"],
            "steady_interval_count": steady["steady_interval_count"],
            "interval_throughput_msg_s_median": steady[
                "interval_throughput_msg_s_median"
            ],
            "interval_throughput_msg_s_min": steady["interval_throughput_msg_s_min"],
            "interval_throughput_msg_s_max": steady["interval_throughput_msg_s_max"],
        }
        if agg_thr:
            result["aggregated_throughput_msg_s"] = float(agg_thr.group(2))
            result["aggregated_throughput_mbit_s"] = float(agg_thr.group(3))
        if agg_lat:
            result["aggregated_latency_mean_ms"] = float(agg_lat.group(1))
            result["aggregated_latency_p99_ms"] = float(agg_lat.group(4))
            result["aggregated_latency_max_ms"] = float(agg_lat.group(8))
        return result

    if not agg_thr or not agg_lat:
        raise RuntimeError(f"failed to parse producer output:\n{text}")

    return {
        "records": int(agg_thr.group(1)),
        "throughput_msg_s": float(agg_thr.group(2)),
        "throughput_mbit_s": float(agg_thr.group(3)),
        "latency_mean_ms": float(agg_lat.group(1)),
        "latency_p50_ms": float(agg_lat.group(2)),
        "latency_p95_ms": float(agg_lat.group(3)),
        "latency_p99_ms": float(agg_lat.group(4)),
        "latency_p999_ms": float(agg_lat.group(5)),
        "latency_max_ms": float(agg_lat.group(8)),
        "partial": False,
        "metric_source": "aggregated",
        "interval_count": 0,
        "steady_interval_count": 0,
    }


def parse_consumer_output(text: str) -> dict[str, Any]:
    """Parse pulsar-perf consumer log.

    Same interval-median preference as producer. Ack fields still come from
    Aggregated when present (interval lines do not include ack rate).
    """
    intervals = _parse_consumer_intervals(text)
    steady = _from_intervals(intervals)

    agg_thr = _AGG_CONSUMER_THR.search(text)
    agg_lat = _AGG_LATENCY.search(text)

    if steady is not None:
        records = int(steady["records_cumulative_last"])
        if agg_thr:
            records = int(agg_thr.group(1))
        result = {
            "records": records,
            "throughput_msg_s": steady["throughput_msg_s"],
            "throughput_mbit_s": steady["throughput_mbit_s"],
            "ack_rate_msg_s": float(agg_thr.group(4)) if agg_thr else None,
            "ack_failed": int(agg_thr.group(5)) if agg_thr else None,
            "latency_mean_ms": steady["latency_mean_ms"],
            "latency_p50_ms": steady["latency_p50_ms"],
            "latency_p95_ms": steady["latency_p95_ms"],
            "latency_p99_ms": steady["latency_p99_ms"],
            "latency_p999_ms": float(agg_lat.group(5)) if agg_lat else None,
            "latency_max_ms": steady["latency_max_ms"],
            "partial": steady["partial"],
            "metric_source": "interval_median",
            "interval_count": steady["interval_count"],
            "steady_interval_count": steady["steady_interval_count"],
            "interval_throughput_msg_s_median": steady[
                "interval_throughput_msg_s_median"
            ],
            "interval_throughput_msg_s_min": steady["interval_throughput_msg_s_min"],
            "interval_throughput_msg_s_max": steady["interval_throughput_msg_s_max"],
        }
        if agg_thr:
            result["aggregated_throughput_msg_s"] = float(agg_thr.group(2))
            result["aggregated_throughput_mbit_s"] = float(agg_thr.group(3))
        if agg_lat:
            result["aggregated_latency_mean_ms"] = float(agg_lat.group(1))
            result["aggregated_latency_p99_ms"] = float(agg_lat.group(4))
            result["aggregated_latency_max_ms"] = float(agg_lat.group(8))
        return result

    if not agg_thr or not agg_lat:
        raise RuntimeError(f"failed to parse consumer output:\n{text}")

    return {
        "records": int(agg_thr.group(1)),
        "throughput_msg_s": float(agg_thr.group(2)),
        "throughput_mbit_s": float(agg_thr.group(3)),
        "ack_rate_msg_s": float(agg_thr.group(4)),
        "ack_failed": int(agg_thr.group(5)),
        "latency_mean_ms": float(agg_lat.group(1)),
        "latency_p50_ms": float(agg_lat.group(2)),
        "latency_p95_ms": float(agg_lat.group(3)),
        "latency_p99_ms": float(agg_lat.group(4)),
        "latency_p999_ms": float(agg_lat.group(5)),
        "latency_max_ms": float(agg_lat.group(8)),
        "partial": False,
        "metric_source": "aggregated",
        "interval_count": 0,
        "steady_interval_count": 0,
    }
