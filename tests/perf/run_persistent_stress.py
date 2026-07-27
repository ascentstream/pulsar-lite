#!/usr/bin/env python3
"""Persistent topic stress tests.

Performance-focused long-running tests covering producer stress,
consumer/E2E stress, and persistent-specific backlog/restart scenarios.
"""

from __future__ import annotations

import dataclasses
import json
import sys
import time
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import ROOT
from lib.broker import BrokerConfig, BrokerProcess, DockerBrokerProcess
from lib.docker_image import build_broker_image
from lib.observability import PerfCollector
from lib.parsing import parse_consumer_output, parse_producer_output
from lib.perf_cmd import ensure_prereqs, perf_cmd, run_consumer_then_feed, run_sync

RESULTS_PATH = ROOT / "docs" / "perf" / "data" / "persistent_stress_results.json"
ARTIFACTS_DIR = ROOT / "docs" / "perf" / "data" / "persistent_stress_logs"


@dataclasses.dataclass
class Scenario:
    name: str
    kind: str  # produce | consume_e2e | backlog_drain | restart_replay | redelivery_unacked
    broker: str
    description: str
    producer_args: list[str] | None = None
    consumer_args: list[str] | None = None
    reader_args: list[str] | None = None
    feed_producer_args: list[str] | None = None
    restart_preserve: bool = False


BROKERS = {
    "persistent_stress": BrokerConfig("persistent_stress", 6671, 0),
    "persistent_stress_partitioned": BrokerConfig(
        "persistent_stress_partitioned", 6672, 4
    ),
}


SCENARIOS: list[Scenario] = [
    # Producer stress (4)
    Scenario(
        name="stress_persistent_producer_max_rate",
        kind="produce",
        broker="persistent_stress",
        description="单 producer 全速发送 500k 条",
        producer_args=["-m", "500000", "-s", "1024", "-r", "999999", "-o", "1000"],
    ),
    Scenario(
        name="stress_persistent_producer_multi_producer",
        kind="produce",
        broker="persistent_stress",
        description="4 producers 并发全速发送 100k 条",
        producer_args=[
            "-m",
            "100000",
            "-s",
            "1024",
            "-r",
            "999999",
            "-n",
            "4",
            "-threads",
            "4",
            "-c",
            "4",
            "-o",
            "1000",
        ],
    ),
    Scenario(
        name="stress_persistent_producer_large_payload",
        kind="produce",
        broker="persistent_stress",
        description="100KiB payload 发送 10k 条",
        # 10k * 100KiB ≈ 1GiB payload; 200k would be ~19GiB
        producer_args=["-m", "10000", "-s", "102400", "-r", "500"],
    ),
    Scenario(
        name="stress_persistent_producer_sustained",
        kind="produce",
        broker="persistent_stress",
        description="持续限速发送 500k 条 @ 10k msg/s (~50s)",
        producer_args=["-m", "500000", "-s", "1024", "-r", "10000"],
    ),
    # Consumer/E2E stress (4)
    Scenario(
        name="stress_persistent_consume_shared_max_rate",
        kind="consume_e2e",
        broker="persistent_stress",
        description="Shared 全速消费 200k 条",
        consumer_args=["-m", "200000", "-q", "1000", "-st", "Shared"],
        feed_producer_args=["-m", "200000", "-s", "1024", "-r", "999999"],
    ),
    Scenario(
        name="stress_persistent_consume_shared_high_fanout",
        kind="consume_e2e",
        broker="persistent_stress",
        description="16 consumers 高扇出消费 200k 条",
        consumer_args=["-m", "200000", "-q", "1000", "-st", "Shared", "-n", "16"],
        feed_producer_args=["-m", "200000", "-s", "1024", "-r", "999999"],
    ),
    Scenario(
        name="stress_persistent_consume_multi_subscription_fanout",
        kind="consume_e2e",
        broker="persistent_stress",
        description="8 subscriptions 扇出：生产 100k / 消费 800k 条",
        # each subscription receives a full copy; consumer -m = produce * ns
        consumer_args=["-m", "800000", "-q", "1000", "-st", "Shared", "-ns", "8"],
        feed_producer_args=["-m", "100000", "-s", "1024", "-r", "999999"],
    ),
    Scenario(
        name="stress_persistent_consume_partitioned_max_rate",
        kind="consume_e2e",
        broker="persistent_stress_partitioned",
        description="4 partitions + 4 consumers 消费 200k 条",
        consumer_args=["-m", "200000", "-q", "1000", "-st", "Shared", "-n", "4"],
        feed_producer_args=["-m", "200000", "-s", "1024", "-r", "999999"],
    ),
    # Persistent-specific stress (3)
    Scenario(
        name="stress_persistent_backlog_drain",
        kind="backlog_drain",
        broker="persistent_stress",
        description="大 backlog drain：生产/消费 200k 条",
        producer_args=["-m", "200000", "-r", "999999", "-s", "1024", "-db", "-o", "1000"],
        consumer_args=["-m", "200000", "-q", "1000", "-st", "Shared", "-sp", "Earliest"],
    ),
    Scenario(
        name="stress_persistent_restart_replay",
        kind="restart_replay",
        broker="persistent_stress",
        description="重启后 backlog replay：生产/消费 200k 条",
        producer_args=["-m", "200000", "-r", "999999", "-s", "1024", "-db", "-o", "1000"],
        consumer_args=["-m", "200000", "-q", "1000", "-st", "Shared", "-sp", "Earliest"],
        restart_preserve=True,
    ),
    Scenario(
        name="stress_persistent_redelivery_unacked",
        kind="redelivery_unacked",
        broker="persistent_stress",
        description="未 ack redelivery 成本：生产 30k / 仅 ack 10k",
        producer_args=["-m", "30000", "-r", "999999", "-s", "1024", "-db", "-o", "1000"],
        consumer_args=["-m", "10000", "-q", "1000", "-st", "Shared"],  # Only ack 1/3
        feed_producer_args=["-m", "30000", "-r", "999999", "-s", "1024", "-db", "-o", "1000"],
        restart_preserve=True,
    ),
]


def run_produce_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Run produce-only stress scenario."""
    topic = f"persistent://public/default/stress-{uuid.uuid4().hex[:8]}"
    producer_log = run_dir / "producer.log"
    histogram_file = run_dir / "histogram.hdr"

    cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.producer_args,
        topic,
        histogram_file,
    )

    start = time.time()
    result_proc = run_sync(
        cmd, producer_log, timeout=600.0
    )  # 10 min timeout for stress
    duration = time.time() - start

    if result_proc.returncode != 0:
        raise RuntimeError(
            f"producer failed with rc={result_proc.returncode}: {producer_log.read_text()[:500]}"
        )

    producer_result = parse_producer_output(producer_log.read_text(encoding="utf-8"))
    broker_metrics = broker.metrics()

    return {
        "producer": producer_result,
        "broker": broker_metrics,
        "duration_s": round(duration, 2),
    }


def run_consume_e2e_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Run consumer + producer E2E stress scenario."""
    topic = f"persistent://public/default/stress-{uuid.uuid4().hex[:8]}"
    consumer_log = run_dir / "consumer.log"
    producer_log = run_dir / "producer.log"
    histogram_file = run_dir / "histogram.hdr"

    consumer_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.consumer_args,
        topic,
        histogram_file,
    )

    producer_cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.feed_producer_args,
        topic,
        run_dir / "producer_histogram.hdr",
    )

    start = time.time()
    consumer_out, producer_out, consumer_rc, producer_rc = run_consumer_then_feed(
        consumer_cmd,
        producer_cmd,
        consumer_log,
        producer_log,
        consumer_timeout=600.0,
        producer_timeout=600.0,
    )
    duration = time.time() - start

    if consumer_rc != 0:
        raise RuntimeError(
            f"consumer failed with rc={consumer_rc}: {consumer_out[:500]}"
        )
    if producer_rc != 0:
        raise RuntimeError(
            f"producer failed with rc={producer_rc}: {producer_out[:500]}"
        )

    consumer_result = parse_consumer_output(consumer_out)
    producer_result = parse_producer_output(producer_out)
    broker_metrics = broker.metrics()

    return {
        "consumer": consumer_result,
        "producer": producer_result,
        "broker": broker_metrics,
        "duration_s": round(duration, 2),
    }


def run_backlog_drain_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Produce large backlog then drain with consumer."""
    topic = f"persistent://public/default/stress-{uuid.uuid4().hex[:8]}"

    # Step 1: Produce backlog fast
    producer_log = run_dir / "producer.log"
    producer_histogram = run_dir / "producer_histogram.hdr"
    producer_cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.producer_args,
        topic,
        producer_histogram,
    )

    print("  Producing backlog...")
    producer_proc = run_sync(producer_cmd, producer_log, timeout=600.0)
    if producer_proc.returncode != 0:
        raise RuntimeError(f"producer failed: {producer_log.read_text()[:500]}")

    producer_result = parse_producer_output(producer_log.read_text(encoding="utf-8"))
    print(f"  Backlog: {producer_result['records']} messages")

    # Step 2: Drain with consumer from Earliest
    consumer_log = run_dir / "consumer.log"
    consumer_histogram = run_dir / "histogram.hdr"
    consumer_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.consumer_args,
        topic,
        consumer_histogram,
    )

    print("  Draining backlog...")
    start = time.time()
    consumer_proc = run_sync(consumer_cmd, consumer_log, timeout=600.0)
    drain_duration = time.time() - start

    if consumer_proc.returncode != 0:
        raise RuntimeError(f"consumer failed: {consumer_log.read_text()[:500]}")

    consumer_result = parse_consumer_output(consumer_log.read_text(encoding="utf-8"))
    broker_metrics = broker.metrics()

    return {
        "producer": producer_result,
        "consumer": consumer_result,
        "broker": broker_metrics,
        "drain_duration_s": round(drain_duration, 2),
        "drain_throughput_msg_s": round(consumer_result["records"] / drain_duration, 2),
    }


def run_restart_replay_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Produce backlog → restart → consumer replay from Earliest."""
    topic = f"persistent://public/default/stress-{uuid.uuid4().hex[:8]}"

    # Step 1: Produce backlog
    producer_log = run_dir / "producer_pre.log"
    producer_histogram = run_dir / "producer_histogram.hdr"
    producer_cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.producer_args,
        topic,
        producer_histogram,
    )

    print("  Producing backlog...")
    producer_proc = run_sync(producer_cmd, producer_log, timeout=600.0)
    if producer_proc.returncode != 0:
        raise RuntimeError(f"producer failed: {producer_log.read_text()[:500]}")

    producer_result = parse_producer_output(producer_log.read_text(encoding="utf-8"))
    print(f"  Backlog: {producer_result['records']} messages")

    # Step 2: Restart with storage preservation
    print(f"  Restarting broker (preserve_storage={scenario.restart_preserve})...")
    broker.restart(preserve_storage=scenario.restart_preserve)
    time.sleep(2)

    # Step 3: Consumer replay from Earliest
    consumer_log = run_dir / "consumer_post.log"
    consumer_histogram = run_dir / "histogram.hdr"
    consumer_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.consumer_args,
        topic,
        consumer_histogram,
    )

    print("  Consumer replaying from Earliest...")
    start = time.time()
    consumer_proc = run_sync(consumer_cmd, consumer_log, timeout=600.0)
    replay_duration = time.time() - start

    if consumer_proc.returncode != 0:
        raise RuntimeError(f"consumer failed: {consumer_log.read_text()[:500]}")

    consumer_result = parse_consumer_output(consumer_log.read_text(encoding="utf-8"))
    broker_metrics = broker.metrics()

    return {
        "producer": producer_result,
        "consumer": consumer_result,
        "broker": broker_metrics,
        "replay_duration_s": round(replay_duration, 2),
        "replay_throughput_msg_s": round(
            consumer_result["records"] / replay_duration, 2
        ),
    }


def run_redelivery_unacked_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Produce → partial consume (no full ack) → restart → consume remaining."""
    topic = f"persistent://public/default/stress-{uuid.uuid4().hex[:8]}"

    # Step 1: Consumer partially consumes (will ack some)
    consumer1_log = run_dir / "consumer_partial.log"
    consumer1_histogram = run_dir / "consumer_partial_histogram.hdr"
    producer_log = run_dir / "producer.log"
    producer_histogram = run_dir / "producer_histogram.hdr"

    consumer_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.consumer_args,
        topic,
        consumer1_histogram,
    )

    producer_cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.feed_producer_args,
        topic,
        producer_histogram,
    )

    print("  Partial consume...")
    consumer_out, producer_out, consumer_rc, producer_rc = run_consumer_then_feed(
        consumer_cmd,
        producer_cmd,
        consumer1_log,
        producer_log,
        consumer_timeout=600.0,
        producer_timeout=600.0,
    )

    if consumer_rc != 0:
        raise RuntimeError(f"consumer1 failed: {consumer_out[:500]}")
    if producer_rc != 0:
        raise RuntimeError(f"producer failed: {producer_out[:500]}")

    producer_result = parse_producer_output(producer_out)
    consumer1_result = parse_consumer_output(consumer_out)

    print(
        f"  Produced: {producer_result['records']}, Consumed: {consumer1_result['records']}"
    )
    unacked = producer_result["records"] - consumer1_result["records"]
    print(f"  Unacked: {unacked}")

    # Step 2: Restart
    print(f"  Restarting broker (preserve_storage={scenario.restart_preserve})...")
    broker.restart(preserve_storage=scenario.restart_preserve)
    time.sleep(2)

    # Step 3: Consume remaining (should get unacked messages)
    consumer2_log = run_dir / "consumer_redelivery.log"
    consumer2_histogram = run_dir / "histogram.hdr"
    consumer2_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        [
            "-m",
            str(unacked * 2),
            "-q",
            "1000",
            "-st",
            "Shared",
            "-time",
            "60",
        ],  # Allow time for redelivery
        topic,
        consumer2_histogram,
    )

    print("  Consuming redelivered...")
    start = time.time()
    consumer2_proc = run_sync(consumer2_cmd, consumer2_log, timeout=600.0)
    redelivery_duration = time.time() - start

    if consumer2_proc.returncode != 0:
        # Redelivery might timeout if no unacked - that's ok
        print("  (consumer2 exited non-zero, checking output)")

    consumer2_result = parse_consumer_output(consumer2_log.read_text(encoding="utf-8"))
    broker_metrics = broker.metrics()

    return {
        "producer": producer_result,
        "consumer_partial": consumer1_result,
        "consumer_redelivery": consumer2_result,
        "broker": broker_metrics,
        "unacked_expected": unacked,
        "redelivered_actual": consumer2_result["records"],
        "redelivery_duration_s": round(redelivery_duration, 2),
    }


def run_scenario(
    scenario: Scenario, broker: BrokerProcess, run_dir: Path
) -> dict[str, Any]:
    """Dispatch to appropriate scenario runner."""
    if scenario.kind == "produce":
        return run_produce_scenario(scenario, broker, run_dir)
    elif scenario.kind == "consume_e2e":
        return run_consume_e2e_scenario(scenario, broker, run_dir)
    elif scenario.kind == "backlog_drain":
        return run_backlog_drain_scenario(scenario, broker, run_dir)
    elif scenario.kind == "restart_replay":
        return run_restart_replay_scenario(scenario, broker, run_dir)
    elif scenario.kind == "redelivery_unacked":
        return run_redelivery_unacked_scenario(scenario, broker, run_dir)
    else:
        raise ValueError(f"Unknown scenario kind: {scenario.kind}")


def main(argv: list[str]) -> int:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run persistent stress scenarios for pulsar-lite"
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Scenario names to run. If empty, run all scenarios",
    )
    parser.add_argument(
        "--broker-backend",
        choices=["local", "docker"],
        default="local",
        help="Broker launch backend. local uses rust/target/release/pulsar-lite; "
        "docker builds and runs a constrained broker container.",
    )
    parser.add_argument(
        "--docker-cpuset",
        default="0-3",
        help="CPU set passed to docker run --cpuset-cpus when --broker-backend=docker.",
    )
    parser.add_argument(
        "--docker-memory",
        default="4g",
        help="Memory limit passed to docker run --memory when --broker-backend=docker.",
    )
    parser.add_argument(
        "--skip-docker-build",
        action="store_true",
        help="Reuse an existing Docker image instead of rebuilding it before the run.",
    )
    args = parser.parse_args(argv[1:])
    ensure_prereqs(require_broker_bin=args.broker_backend == "local")

    # Parse scenario filter from argv
    filter_names = set(args.scenarios)

    # Docker image setup
    docker_image_metadata: dict[str, Any] = {}
    if args.broker_backend == "docker":
        print("Building Docker image for persistent broker...", file=sys.stderr)
        docker_image_metadata = build_broker_image(
            skip_docker_build=args.skip_docker_build,
        )
        print(
            f"Docker image: {docker_image_metadata['docker_image_tag']}",
            file=sys.stderr,
        )
        if docker_image_metadata.get("docker_build_performed"):
            print(
                f"  Build reason: {docker_image_metadata['docker_build_reason']}",
                file=sys.stderr,
            )

    run_id = time.strftime("%Y%m%d-%H%M%S")
    run_artifacts = ARTIFACTS_DIR / run_id
    run_artifacts.mkdir(parents=True, exist_ok=True)

    results = {
        "run_id": run_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scenarios": [],
    }

    # Group scenarios by broker
    scenarios_by_broker: dict[str, list[Scenario]] = {}
    for scenario in SCENARIOS:
        if filter_names and scenario.name not in filter_names:
            continue
        scenarios_by_broker.setdefault(scenario.broker, []).append(scenario)

    passed = 0
    failed = 0

    for broker_name, broker_scenarios in scenarios_by_broker.items():
        print(f"\n=== Broker: {broker_name} ===")
        broker_config = BROKERS[broker_name]

        # Create broker instance based on backend
        if args.broker_backend == "docker":
            broker = DockerBrokerProcess(
                broker_config,
                image_tag=docker_image_metadata["docker_image_tag"],
                cpuset_cpus=args.docker_cpuset,
                memory=args.docker_memory,
            )
        else:
            broker = BrokerProcess(broker_config)
        broker.start()

        try:
            
            for scenario in broker_scenarios:
                print(f"\n[{scenario.name}] {scenario.description}")
                scenario_dir = run_artifacts / scenario.name
                scenario_dir.mkdir(parents=True, exist_ok=True)
                # Start perf recording (must be after restart to capture the new PID)
                perf_data_path = scenario_dir / "perf.data"
                perf_collector: PerfCollector | None = None
                if broker.broker_pid:
                    perf_collector = PerfCollector(
                        pid=broker.broker_pid,
                        duration=300,
                        perf_data_path=perf_data_path,
                    )
                    perf_collector.start_persist()
                try:
                    result = run_scenario(scenario, broker, scenario_dir)

                    # Save broker log and timeseries
                    if broker.log_path:
                        (scenario_dir / "broker.log").write_text(
                            broker.log_path.read_text(
                                encoding="utf-8", errors="replace"
                            )
                        )
                    if broker.sampler:
                        broker.sampler.write_csv(scenario_dir / "broker_timeseries.csv")

                    # Record artifact paths in result
                    perf_collector.stop()
                    if perf_data_path.exists():
                        svg_path = scenario_dir / "flamegraph.svg"
                        ok = PerfCollector.generate_flamegraph(perf_data_path, svg_path)
                        if ok:
                            result["flamegraph_file"] = str(svg_path.relative_to(ROOT))
                            print(f"  flamegraph -> {svg_path}", file=sys.stderr)
                        else:
                            result["flamegraph_file"] = None
                            print(
                                f"  flamegraph skipped for {perf_data_path.name}",
                                file=sys.stderr,
                            )
                    else:
                        result["perf_data_file"] = None
                        result["flamegraph_file"] = None
                        print("  perf data not captured", file=sys.stderr)
                    
                    results["scenarios"].append(
                        {
                            "name": scenario.name,
                            "kind": scenario.kind,
                            "broker": broker_name,
                            "description": scenario.description,
                            "result": result,
                            "status": "pass",
                        }
                    )
                    passed += 1
                    print(f"  ✓ PASS")

                except Exception as e:
                    print(f"  ERROR: {e}", file=sys.stderr)
                    if perf_collector is not None:
                        perf_collector.stop()
                    results["scenarios"].append(
                        {
                            "name": scenario.name,
                            "kind": scenario.kind,
                            "broker": broker_name,
                            "description": scenario.description,
                            "error": str(e),
                            "status": "fail",
                        }
                    )
                    failed += 1
                    print(f"  ✗ FAIL: {e}")

        finally:
            broker.stop(cleanup=True)

    results["summary"] = {
        "total": passed + failed,
        "passed": passed,
        "failed": failed,
    }

    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_PATH.write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"\n=== Summary ===")
    print(f"Total: {passed + failed}, Passed: {passed}, Failed: {failed}")
    print(f"Results: {RESULTS_PATH}")
    print(f"Artifacts: {run_artifacts}")

    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
