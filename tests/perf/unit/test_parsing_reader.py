from __future__ import annotations

import pytest

from ..lib.parsing import parse_reader_output


def test_parse_reader_aggregated_stats():
    """Parse complete reader output with aggregated stats."""
    output = """
2026-07-06T10:00:00,000 [main] INFO  org.apache.pulsar.testclient.PerformanceReader - Start reading from persistent://public/default/test-topic
2026-07-06T10:00:05,000 [pulsar-reader-reader-0] INFO  o.a.p.c.impl.ConsumerStatsRecorderImpl - Read throughput: 1000 msg --- 2000.5 msg/s --- 15.625 Mbit/s --- Latency: mean: 5.123 ms - med: 4.5 - 95pct: 8.9 - 99pct: 12.3 - 99.9pct: 15.8 - 99.99pct: 18.2 - 99.999pct: 19.5 - Max: 20.1
Aggregated throughput stats --- 5000 records received --- 1000.25 msg/s --- 7.815 Mbit/s
Aggregated latency stats --- Latency: mean: 5.234 ms - med: 4.6 - 95pct: 9.1 - 99pct: 12.5 - 99.9pct: 16.0 - 99.99pct: 18.4 - 99.999pct: 19.7 - Max: 21.3
"""
    result = parse_reader_output(output)
    assert result["records_received"] == 5000
    assert result["msg_rate"] == 1000.25
    assert result["throughput_mbit"] == 7.815
    assert result["latency_mean_ms"] == 5.234
    assert result["latency_p50_ms"] == 4.6
    assert result["latency_p95_ms"] == 9.1
    assert result["latency_p99_ms"] == 12.5
    assert result["latency_p999_ms"] == 16.0
    assert result["latency_max_ms"] == 21.3
    assert result["partial"] is False


def test_parse_reader_partial_output():
    """Parse reader output with only interval stats (no aggregated)."""
    output = """
2026-07-06T10:00:05,000 [pulsar-reader-reader-0] INFO  o.a.p.c.impl.ConsumerStatsRecorderImpl - Read throughput: 1000 msg --- 2000.5 msg/s --- 15.625 Mbit/s --- Latency: mean: 5.123 ms - med: 4.5 - 95pct: 8.9 - 99pct: 12.3 - 99.9pct: 15.8 - 99.99pct: 18.2 - 99.999pct: 19.5 - Max: 20.1
"""
    result = parse_reader_output(output)
    assert result["records_received"] == 1000
    assert result["msg_rate"] == 2000.5
    assert result["throughput_mbit"] == 15.625
    assert result["latency_mean_ms"] == 5.123
    assert result["latency_p50_ms"] is None
    assert result["latency_p99_ms"] == 12.3
    assert result["latency_max_ms"] == 20.1
    assert result["partial"] is True


def test_parse_reader_no_match_raises():
    """Parse should raise on unparseable output."""
    with pytest.raises(RuntimeError, match="failed to parse reader output"):
        parse_reader_output("garbage output with no stats")
