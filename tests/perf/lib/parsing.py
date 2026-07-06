        if not interval_matches:
            raise RuntimeError(f"failed to parse reader output:\n{text}")

        # Use last interval
        (
            records,
            msg_rate,
            throughput_mbit,
            latency_mean_ms,
            latency_p99_ms,
            latency_max_ms,
        ) = interval_matches[-1]

        return {
            "records_received": int(records),
            "msg_rate": float(msg_rate),
            "throughput_mbit": float(throughput_mbit),
            "latency_mean_ms": float(latency_mean_ms),
            "latency_p50_ms": None,
            "latency_p95_ms": None,
            "latency_p99_ms": float(latency_p99_ms),
            "latency_p999_ms": None,
            "latency_max_ms": float(latency_max_ms),
            "partial": True,
        }

    # Aggregated stats found - complete run
    return {
        "records_received": int(throughput.group(1)),
        "msg_rate": float(throughput.group(2)),
        "throughput_mbit": float(throughput.group(3)),
        "latency_mean_ms": float(latency.group(1)),
        "latency_p50_ms": float(latency.group(2)),
        "latency_p95_ms": float(latency.group(3)),
        "latency_p99_ms": float(latency.group(4)),
        "latency_p999_ms": float(latency.group(5)),
        "latency_max_ms": float(latency.group(8)),
        "partial": False,
    }
