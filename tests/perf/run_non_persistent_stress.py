#!/usr/bin/env python3
"""Non-persistent stress test scenarios for pulsar-lite.

Runs 9 stress scenarios (4 producer, 5 consumer/e2e) against broker profiles,
capturing perf record data and broker CPU/RSS timeseries for each scenario,
then batch-generates flamegraphs and writes aggregated JSON results.

Usage:
    python3 tests/perf/run_non_persistent_stress.py
    python3 tests/perf/run_non_persistent_stress.py stress_consume_multi_subscription_fanout
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# --- lib imports (run from repo root or with sys.path adjusted) ---
sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import ROOT
from lib.broker import BrokerConfig, BrokerProcess
from lib.observability import PerfCollector
from lib.parsing import parse_consumer_output, parse_producer_output
from lib.perf_cmd import (
    ensure_prereqs,
    perf_cmd,
    run_consumer_then_feed,
    run_sync,
)

# ---------------------------------------------------------------------------
# Data definitions
# ---------------------------------------------------------------------------

DATA_DIR = ROOT / "docs" / "perf" / "data"
RESULTS_PATH = DATA_DIR / "non_persistent_stress_results.json"
LOGS_ROOT = DATA_DIR / "non_persistent_stress_logs"

BROKERS: dict[str, BrokerConfig] = {
    "nonpartitioned": BrokerConfig("nonpartitioned", 6651, 0),
    "nonpersistent_partitioned": BrokerConfig("nonpersistent_partitioned", 6652, 4),
}


@dataclasses.dataclass
class StressScenario:
    name: str
    kind: str  # 'produce' | 'consume_e2e'
    broker: str
    description: str
    producer_args: list[str]
    consumer_args: list[str] | None = None
    feed_producer_args: list[str] | None = None
    estimated_duration: int = 60


STRESS_SCENARIOS: list[StressScenario] = [
    # --- Producer stress ---
    StressScenario(
        name="stress_producer_max_rate",
        kind="produce",
        broker="nonpartitioned",
        description="单 producer 1M msg/s offered load 吞吐 ceiling",
        producer_args=["-time", "60", "-r", "999999", "-s", "1024"],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_producer_max_rate_multi_producer",
        kind="produce",
        broker="nonpartitioned",
        description="4 producers 1M msg/s aggregate offered load 并发吞吐 ceiling",
        producer_args=["-time", "60", "-r", "999999", "-s", "1024", "-n", "4", "-threads", "4", "-c", "4"],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_producer_large_payload",
        kind="produce",
        broker="nonpartitioned",
        description="100KiB payload 1M msg/s offered load 带宽瓶颈",
        producer_args=["-time", "60", "-r", "999999", "-s", "102400"],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_producer_sustained",
        kind="produce",
        broker="nonpartitioned",
        description="5 分钟持续发送稳定性",
        producer_args=["-time", "300", "-r", "999999", "-s", "1024"],
        estimated_duration=300,
    ),
    # --- Consumer / E2E stress ---
    StressScenario(
        name="stress_consume_shared_max_rate",
        kind="consume_e2e",
        broker="nonpartitioned",
        description="Shared 单 consumer 1M msg/s offered load 吞吐 ceiling",
        producer_args=[],
        consumer_args=["-time", "60", "-q", "10000", "-st", "Shared"],
        feed_producer_args=["-time", "60", "-r", "999999", "-s", "1024"],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_consume_shared_high_fanout",
        kind="consume_e2e",
        broker="nonpartitioned",
        description="Shared 16 consumers 1M msg/s offered load 高 fanout",
        producer_args=[],
        consumer_args=["-time", "60", "-q", "10000", "-st", "Shared", "-n", "16", "-c", "4"],
        feed_producer_args=["-time", "60", "-r", "999999", "-s", "1024", "-c", "4"],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_consume_multi_subscription_fanout",
        kind="consume_e2e",
        broker="nonpartitioned",
        description="8 subscriptions 1M msg/s offered load 高 fanout",
        producer_args=[],
        consumer_args=["-time", "60", "-q", "10000", "-st", "Shared", "-ns", "8", "-c", "4"],
        feed_producer_args=[
            "-time", "60",
            "-r", "999999",
            "-s", "1024",
            "-c", "4",
            "--memory-limit", "268435456",
            "--max-outstanding", "4096",
            "--max-outstanding-across-partitions", "16384",
        ],
        estimated_duration=60,
    ),
    StressScenario(
        name="stress_consume_sustained",
        kind="consume_e2e",
        broker="nonpartitioned",
        description="5 分钟持续消费稳定性",
        producer_args=[],
        consumer_args=["-time", "300", "-q", "10000", "-st", "Shared", "-c", "4"],
        feed_producer_args=["-time", "300", "-r", "999999", "-s", "1024", "-c", "4"],
        estimated_duration=300,
    ),
    StressScenario(
        name="stress_consume_partitioned_max_rate",
        kind="consume_e2e",
        broker="nonpersistent_partitioned",
        description="Partitioned 4 partitions Shared 4 consumers 1M msg/s offered load",
        producer_args=[],
        consumer_args=["-time", "60", "-q", "10000", "-st", "Shared", "-n", "4", "-c", "4"],
        feed_producer_args=["-time", "60", "-r", "999999", "-s", "1024", "-c", "4"],
        estimated_duration=60,
    ),
]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run non-persistent stress scenarios for pulsar-lite."
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Optional scenario names to run. Defaults to all scenarios.",
    )
    return parser


def select_scenarios(requested_names: list[str]) -> list[StressScenario]:
    if not requested_names:
        return STRESS_SCENARIOS

    by_name = {scenario.name: scenario for scenario in STRESS_SCENARIOS}
    missing = [name for name in requested_names if name not in by_name]
    if missing:
        parser = build_arg_parser()
        parser.error(
            "unknown scenario(s): "
            + ", ".join(missing)
            + ". Available: "
            + ", ".join(sorted(by_name))
        )
    return [by_name[name] for name in requested_names]

# ---------------------------------------------------------------------------
# Runner helpers
# ---------------------------------------------------------------------------


def _topic_for(run_id: str, scenario: StressScenario) -> str:
    return f"non-persistent://public/default/{run_id}-{scenario.name}"


def _service_url_for(scenario: StressScenario) -> str:
    cfg = BROKERS[scenario.broker]
    return f"pulsar://127.0.0.1:{cfg.port}"


def _run_produce_scenario(
    scenario: StressScenario,
    run_id: str,
    scenario_dir: Path,
    broker_proc: BrokerProcess,
    perf_collector: PerfCollector | None,
) -> dict:
    """Execute a single producer-only stress scenario."""
    topic = _topic_for(run_id, scenario)
    service_url = _service_url_for(scenario)
    timeout = scenario.estimated_duration + 120

    histogram_path = scenario_dir / "producer_histogram.log"
    stdout_path = scenario_dir / "producer_stdout.log"

    cmd = perf_cmd("produce", service_url, scenario.producer_args, topic, histogram_path)

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    proc = run_sync(cmd, stdout_path, timeout=timeout)
    duration_secs = round(time.monotonic() - t0, 3)

    # Stop perf before parsing
    if perf_collector is not None:
        perf_collector.stop()

    broker_metrics = broker_proc.metrics()

    status = "ok" if proc.returncode == 0 else f"exit:{proc.returncode}"
    result: dict = {
        "name": scenario.name,
        "kind": scenario.kind,
        "broker_profile": scenario.broker,
        "service_url": service_url,
        "description": scenario.description,
        "topic": topic,
        "started_at": started_at,
        "status": status,
        "duration_secs": duration_secs,
        **broker_metrics,
    }

    # Parse metrics
    try:
        result["metrics"] = parse_producer_output(proc.stdout)
    except RuntimeError as exc:
        print(f"  WARNING: parse failed: {exc}", file=sys.stderr)
        result["metrics"] = None
        result["status"] = "parse_error"

    return result


def _run_consume_e2e_scenario(
    scenario: StressScenario,
    run_id: str,
    scenario_dir: Path,
    broker_proc: BrokerProcess,
    perf_collector: PerfCollector | None,
) -> dict:
    """Execute a single consumer e2e stress scenario (feed-producer + consumer)."""
    topic = _topic_for(run_id, scenario)
    service_url = _service_url_for(scenario)
    timeout = scenario.estimated_duration + 120

    consumer_args = scenario.consumer_args or []
    feed_args = scenario.feed_producer_args or []

    consumer_histogram = scenario_dir / "consumer_histogram.log"
    producer_histogram = scenario_dir / "feed_producer_histogram.log"
    consumer_log = scenario_dir / "consumer_stdout.log"
    producer_log = scenario_dir / "feed_producer_stdout.log"

    consumer_cmd = perf_cmd("consume", service_url, consumer_args, topic, consumer_histogram)
    producer_cmd = perf_cmd("produce", service_url, feed_args, topic, producer_histogram)

    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.monotonic()
    consumer_out, producer_out, consumer_rc, producer_rc = run_consumer_then_feed(
        consumer_cmd, producer_cmd, consumer_log, producer_log, consumer_timeout=timeout, producer_timeout=timeout,
    )
    duration_secs = round(time.monotonic() - t0, 3)

    # Stop perf before parsing
    if perf_collector is not None:
        perf_collector.stop()

    broker_metrics = broker_proc.metrics()

    status = "ok" if consumer_rc == 0 and producer_rc == 0 else (
        f"consumer_exit:{consumer_rc},producer_exit:{producer_rc}"
    )
    result: dict = {
        "name": scenario.name,
        "kind": scenario.kind,
        "broker_profile": scenario.broker,
        "service_url": service_url,
        "description": scenario.description,
        "topic": topic,
        "started_at": started_at,
        "status": status,
        "duration_secs": duration_secs,
        **broker_metrics,
    }

    # Parse producer (feed) metrics
    try:
        result["producer_metrics"] = parse_producer_output(producer_out)
    except RuntimeError as exc:
        print(f"  WARNING: feed-producer parse failed: {exc}", file=sys.stderr)
        result["producer_metrics"] = None
        if result["status"] == "ok":
            result["status"] = "producer_parse_error"

    # Parse consumer metrics
    try:
        result["metrics"] = parse_consumer_output(consumer_out)
    except RuntimeError as exc:
        print(f"  WARNING: consumer parse failed: {exc}", file=sys.stderr)
        result["metrics"] = None
        if result["status"] == "ok":
            result["status"] = "consumer_parse_error"

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    print("=== Non-persistent stress test ===", file=sys.stderr)
    args = build_arg_parser().parse_args(argv)
    scenarios = select_scenarios(args.scenarios)
    ensure_prereqs()

    run_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_base = LOGS_ROOT / run_id
    log_base.mkdir(parents=True, exist_ok=True)
    print(f"Run ID: {run_id}", file=sys.stderr)
    print(f"Artifacts: {log_base}", file=sys.stderr)

    # Start brokers
    broker_procs: dict[str, BrokerProcess] = {}
    for name, cfg in BROKERS.items():
        print(f"Starting broker [{name}] on port {cfg.port} ...", file=sys.stderr)
        bp = BrokerProcess(cfg)
        bp.start()
        broker_procs[name] = bp
        print(f"  Broker [{name}] PID={bp.proc.pid}", file=sys.stderr)

    scenario_results: list[dict] = []

    try:
        for idx, scenario in enumerate(scenarios, 1):
            label = f"[{idx}/{len(scenarios)}] {scenario.name}"
            print(f"\n--- {label} ---", file=sys.stderr)
            print(f"  {scenario.description}", file=sys.stderr)

            broker_proc = broker_procs[scenario.broker]
            scenario_dir = log_base / scenario.name
            scenario_dir.mkdir(parents=True, exist_ok=True)

            # Restart broker between scenarios to clear residual topics/subscriptions
            print(f"  restarting broker [{scenario.broker}] ...", file=sys.stderr)
            broker_proc.restart()

            # Start perf recording (must be after restart to capture the new PID)
            perf_data_path = scenario_dir / "perf.data"
            perf_collector: PerfCollector | None = None
            if broker_proc.proc:
                perf_collector = PerfCollector(
                    pid=broker_proc.proc.pid,
                    duration=scenario.estimated_duration + 30,
                    perf_data_path=perf_data_path,
                )
                perf_collector.start()
                print(f"  perf record started -> {perf_data_path}", file=sys.stderr)

            # Run the scenario
            try:
                if scenario.kind == "produce":
                    result = _run_produce_scenario(
                        scenario, run_id, scenario_dir, broker_proc, perf_collector,
                    )
                elif scenario.kind == "consume_e2e":
                    result = _run_consume_e2e_scenario(
                        scenario, run_id, scenario_dir, broker_proc, perf_collector,
                    )
                else:
                    print(f"  UNKNOWN kind: {scenario.kind}, skipping", file=sys.stderr)
                    continue
            except Exception as exc:
                print(f"  ERROR: {exc}", file=sys.stderr)
                if perf_collector is not None:
                    perf_collector.stop()
                result = {
                    "name": scenario.name,
                    "kind": scenario.kind,
                    "broker_profile": scenario.broker,
                    "service_url": _service_url_for(scenario),
                    "description": scenario.description,
                    "topic": _topic_for(run_id, scenario),
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "status": f"error:{exc}",
                    "duration_secs": 0,
                    "broker_avg_cpu_pct": 0.0,
                    "broker_peak_cpu_pct": 0.0,
                    "broker_peak_rss_mb": 0.0,
                }

            # Write broker timeseries CSV
            timeseries_path = scenario_dir / "broker_timeseries.csv"
            if broker_proc.sampler:
                broker_proc.sampler.write_csv(timeseries_path)
                print(f"  broker timeseries -> {timeseries_path}", file=sys.stderr)

            # Save broker log (contains dispatch metrics and other diagnostics)
            if broker_proc.log_path and broker_proc.log_path.exists():
                import shutil
                broker_log_dest = scenario_dir / "broker.log"
                shutil.copy2(broker_proc.log_path, broker_log_dest)
                print(f"  broker log -> {broker_log_dest}", file=sys.stderr)

            # Record artifact paths in result
            result["broker_timeseries_file"] = str(timeseries_path.relative_to(ROOT))
            if perf_data_path.exists():
                result["perf_data_file"] = str(perf_data_path.relative_to(ROOT))
                svg_path = scenario_dir / "flamegraph.svg"
                ok = PerfCollector.generate_flamegraph(perf_data_path, svg_path)
                if ok:
                    result["flamegraph_file"] = str(svg_path.relative_to(ROOT))
                    print(f"  flamegraph -> {svg_path}", file=sys.stderr)
                else:
                    result["flamegraph_file"] = None
                    print(f"  flamegraph skipped for {perf_data_path.name}", file=sys.stderr)
            else:
                result["perf_data_file"] = None
                result["flamegraph_file"] = None
                print("  perf data not captured", file=sys.stderr)

            scenario_results.append(result)
            print(f"  status={result['status']}  duration={result['duration_secs']}s", file=sys.stderr)

    finally:
        # Stop all brokers
        print("\nStopping brokers ...", file=sys.stderr)
        broker_stop_metrics: dict[str, dict] = {}
        for name, bp in broker_procs.items():
            metrics = bp.stop()
            broker_stop_metrics[name] = metrics
            print(f"  Broker [{name}] stopped: avg_cpu={metrics['broker_avg_cpu_pct']}%", file=sys.stderr)

    # Write results JSON
    output = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenarios": scenario_results,
        "broker_stop_metrics": broker_stop_metrics,
    }
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nResults written to {RESULTS_PATH}", file=sys.stderr)
    print("=== Done ===", file=sys.stderr)


if __name__ == "__main__":
    main()
