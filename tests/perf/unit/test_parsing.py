"""Unit tests for pulsar-perf log parsing (interval-median thr)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lib.parsing import parse_consumer_output, parse_producer_output  # noqa: E402


PRODUCER_LOG = """
2026-07-30T18:00:00,000 - INFO  - [main:PerformanceProducer@392] - Throughput produced:  100000 msg ---  10000.0 msg/s ---     78.1 Mbit/s  --- failure      0.0 msg/s --- Latency: mean:  10.000 ms - med:   9.000 - 95pct:  20.000 - 99pct:  30.000 - 99.9pct:  40.000 - 99.99pct:  50.000 - Max:  60.000
2026-07-30T18:00:10,000 - INFO  - [main:PerformanceProducer@392] - Throughput produced:  300000 msg ---  20000.0 msg/s ---    156.2 Mbit/s  --- failure      0.0 msg/s --- Latency: mean:   5.000 ms - med:   4.000 - 95pct:  10.000 - 99pct:  15.000 - 99.9pct:  20.000 - 99.99pct:  25.000 - Max:  30.000
2026-07-30T18:00:20,000 - INFO  - [main:PerformanceProducer@392] - Throughput produced:  500000 msg ---  22000.0 msg/s ---    171.9 Mbit/s  --- failure      0.0 msg/s --- Latency: mean:   6.000 ms - med:   5.000 - 95pct:  11.000 - 99pct:  16.000 - 99.9pct:  21.000 - 99.99pct:  26.000 - Max:  35.000
2026-07-30T18:00:30,000 - INFO  - [main:PerformanceProducer@392] - Throughput produced:  700000 msg ---  18000.0 msg/s ---    140.6 Mbit/s  --- failure      0.0 msg/s --- Latency: mean:   7.000 ms - med:   6.000 - 95pct:  12.000 - 99pct:  17.000 - 99.9pct:  22.000 - 99.99pct:  27.000 - Max:  40.000
2026-07-30T18:00:40,000 - INFO  - [perf-client-shutdown:PerformanceProducer@774] - Aggregated throughput stats --- 700000 records sent --- 11666.667 msg/s --- 91.146 Mbit/s 
2026-07-30T18:00:40,000 - INFO  - [perf-client-shutdown:PerformanceProducer@784] - Aggregated latency stats --- Latency: mean:   8.000 ms - med:   7.000 - 95pct:  13.000 - 99pct:  18.000 - 99.9pct:  23.000 - 99.99pct:  28.000 - 99.999pct:  33.000 - Max:  45.000
"""

CONSUMER_LOG = """
2026-07-30T18:00:00,000 - INFO  - [main:PerformanceConsumer@484] - Throughput received:  50000 msg ---   5000.000  msg/s --- 39.062 Mbit/s  --- Latency: mean: 100.000 ms - med: 90 - 95pct: 200 - 99pct: 300 - 99.9pct: 400 - 99.99pct: 500 - Max: 600
2026-07-30T18:00:10,000 - INFO  - [main:PerformanceConsumer@484] - Throughput received: 250000 msg ---  20000.000  msg/s --- 156.250 Mbit/s  --- Latency: mean: 50.000 ms - med: 40 - 95pct: 80 - 99pct: 100 - 99.9pct: 120 - 99.99pct: 140 - Max: 160
2026-07-30T18:00:20,000 - INFO  - [main:PerformanceConsumer@484] - Throughput received: 470000 msg ---  22000.000  msg/s --- 171.875 Mbit/s  --- Latency: mean: 55.000 ms - med: 45 - 95pct: 85 - 99pct: 105 - 99.9pct: 125 - 99.99pct: 145 - Max: 165
2026-07-30T18:00:30,000 - INFO  - [main:PerformanceConsumer@484] - Throughput received: 650000 msg ---  18000.000  msg/s --- 140.625 Mbit/s  --- Latency: mean: 60.000 ms - med: 50 - 95pct: 90 - 99pct: 110 - 99.9pct: 130 - 99.99pct: 150 - Max: 170
2026-07-30T18:00:40,000 - INFO  - [perf-client-shutdown:PerformanceConsumer@562] - Aggregated throughput stats --- 650000 records received --- 10833.333 msg/s --- 84.635 Mbit/s --- AckRate: 10833.0  msg/s --- ack failed 0 msg
2026-07-30T18:00:40,000 - INFO  - [perf-client-shutdown:PerformanceConsumer@575] - Aggregated latency stats --- Latency: mean: 70.000 ms - med: 60 - 95pct: 100 - 99pct: 120 - 99.9pct: 140 - 99.99pct: 160 - 99.999pct: 180 - Max: 200
"""

PRODUCER_AGG_ONLY = """
2026-07-30T18:00:10,000 - INFO  - [perf-client-shutdown:PerformanceProducer@774] - Aggregated throughput stats --- 10000 records sent --- 5000.000 msg/s --- 39.062 Mbit/s 
2026-07-30T18:00:10,000 - INFO  - [perf-client-shutdown:PerformanceProducer@784] - Aggregated latency stats --- Latency: mean:   1.500 ms - med:   1.000 - 95pct:   2.000 - 99pct:   3.000 - 99.9pct:   4.000 - 99.99pct:   5.000 - 99.999pct:   6.000 - Max:   7.000
"""


def test_producer_prefers_interval_median_dropping_first_window():
    # windows: 10k, 20k, 22k, 18k → drop first → median(20,22,18)=20k
    result = parse_producer_output(PRODUCER_LOG)
    assert result["metric_source"] == "interval_median"
    assert result["records"] == 700000
    assert result["throughput_msg_s"] == pytest.approx(20000.0)
    assert result["interval_count"] == 4
    assert result["steady_interval_count"] == 3
    assert result["partial"] is False
    assert result["aggregated_throughput_msg_s"] == pytest.approx(11666.667)
    assert result["latency_p99_ms"] == pytest.approx(16.0)  # median of 15,16,17
    assert result["latency_max_ms"] == pytest.approx(40.0)  # max of steady windows


def test_consumer_prefers_interval_median_dropping_first_window():
    result = parse_consumer_output(CONSUMER_LOG)
    assert result["metric_source"] == "interval_median"
    assert result["records"] == 650000
    assert result["throughput_msg_s"] == pytest.approx(20000.0)
    assert result["ack_failed"] == 0
    assert result["ack_rate_msg_s"] == pytest.approx(10833.0)
    assert result["steady_interval_count"] == 3
    assert result["partial"] is False


def test_producer_falls_back_to_aggregated_when_no_intervals():
    result = parse_producer_output(PRODUCER_AGG_ONLY)
    assert result["metric_source"] == "aggregated"
    assert result["records"] == 10000
    assert result["throughput_msg_s"] == pytest.approx(5000.0)
    assert result["interval_count"] == 0
    assert result["partial"] is False


def test_single_interval_marked_partial():
    single = """
2026-07-30T18:00:10,000 - INFO  - [main:PerformanceProducer@392] - Throughput produced:  100000 msg ---  15000.0 msg/s ---    117.2 Mbit/s  --- failure      0.0 msg/s --- Latency: mean:   2.000 ms - med:   1.500 - 95pct:   3.000 - 99pct:   4.000 - 99.9pct:   5.000 - 99.99pct:   6.000 - Max:   7.000
2026-07-30T18:00:15,000 - INFO  - [perf-client-shutdown:PerformanceProducer@774] - Aggregated throughput stats --- 100000 records sent --- 6666.667 msg/s --- 52.083 Mbit/s 
2026-07-30T18:00:15,000 - INFO  - [perf-client-shutdown:PerformanceProducer@784] - Aggregated latency stats --- Latency: mean:   2.000 ms - med:   1.500 - 95pct:   3.000 - 99pct:   4.000 - 99.9pct:   5.000 - 99.99pct:   6.000 - 99.999pct:   7.000 - Max:   7.000
"""
    result = parse_producer_output(single)
    assert result["metric_source"] == "interval_median"
    assert result["throughput_msg_s"] == pytest.approx(15000.0)
    assert result["partial"] is True
    assert result["steady_interval_count"] == 1
