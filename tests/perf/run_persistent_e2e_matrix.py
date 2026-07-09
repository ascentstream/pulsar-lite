#!/usr/bin/env python3
"""Persistent topic E2E functional coverage matrix.

Small test matrix covering producer, consumer, and restart scenarios
with persistent:// topics. Focuses on functional completeness, not
high-load stress testing.
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
from lib.parsing import parse_consumer_output, parse_producer_output
from lib.perf_cmd import ensure_prereqs, perf_cmd, run_consumer_then_feed, run_sync

RESULTS_PATH = ROOT / "docs" / "perf" / "data" / "persistent_e2e_matrix_results.json"
ARTIFACTS_DIR = ROOT / "docs" / "perf" / "data" / "persistent_e2e_matrix_logs"

PULSE_PRODUCER_RATE = 2000
PULSE_CONSUMER_RATE = 2000
BASE_MSGS = 5000


@dataclasses.dataclass
class Scenario:
    name: str
    kind: str  # produce | consume_e2e | restart_smoke
    broker: str
    description: str
    producer_args: list[str] | None = None
    consumer_args: list[str] | None = None
    reader_args: list[str] | None = None  # Keep for future use
    feed_producer_args: list[str] | None = None
    restart_preserve: bool = False  # For restart scenarios


BROKERS = {
    "persistent_nonpartitioned": BrokerConfig("persistent_nonpartitioned", 6661, 0),
    "persistent_partitioned": BrokerConfig("persistent_partitioned", 6662, 4),
}


SCENARIOS: list[Scenario] = [
    # Producer scenarios (9)
    Scenario(
        name="producer_single_baseline",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="单 producer 基线 (persistent topic)",
        producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE), "-s", "256"],
    ),
    Scenario(
        name="producer_multi_producer",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="4 producers",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE * 2),
            "-s", "256", "-n", "4",
        ],
    ),
    Scenario(
        name="producer_multi_thread",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="4 线程 producer",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE * 2),
            "-s", "256", "-threads", "4",
        ],
    ),
    Scenario(
        name="producer_disable_batching",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="关闭 batching",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE),
            "-s", "256", "-db",
        ],
    ),
    Scenario(
        name="producer_lz4_compression",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="LZ4 压缩",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE),
            "-s", "256", "-z", "LZ4",
        ],
    ),
    Scenario(
        name="producer_large_payload",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="10KiB payload",
        producer_args=[
            "-m", str(BASE_MSGS // 2), "-r", str(PULSE_PRODUCER_RATE // 4),
            "-s", "10240",
        ],
    ),
    Scenario(
        name="producer_multi_topic",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="4 topics",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE * 2),
            "-s", "256", "-t", "4",
        ],
    ),
    Scenario(
        name="producer_persistent_partitioned_topic",
        kind="produce",
        broker="persistent_partitioned",
        description="4-partition topic",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE * 2),
            "-s", "256",
        ],
    ),
    Scenario(
        name="producer_message_key_random",
        kind="produce",
        broker="persistent_nonpartitioned",
        description="消息带随机 key",
        producer_args=[
            "-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE),
            "-s", "256", "-mk", "random",
        ],
    ),
    
    # Consumer scenarios (11)
    Scenario(
        name="consume_shared_baseline",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="Shared 订阅基线",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_shared_multi_consumer",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="4 consumers, Shared",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared", "-n", "4"],
        feed_producer_args=["-m", str(BASE_MSGS * 2), "-r", str(PULSE_CONSUMER_RATE * 2), "-s", "256"],
    ),
    Scenario(
        name="consume_multi_subscription",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="3 subscriptions 同时消费",
        consumer_args=["-m", str(BASE_MSGS * 3), "-q", "1000", "-st", "Shared", "-ns", "3"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE * 2), "-s", "256"],
    ),
    Scenario(
        name="consume_exclusive",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="Exclusive 订阅",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Exclusive"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_failover",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="Failover 订阅",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Failover", "-n", "2"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_key_shared",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="Key_Shared 订阅",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Key_Shared", "-n", "2"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256", "-mk", "random"],
    ),
    Scenario(
        name="consume_small_receiver_queue",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="小 receiver queue (10)",
        consumer_args=["-m", str(BASE_MSGS), "-q", "10", "-st", "Shared"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_ack_delay_zero",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="Ack 延迟为 0",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared", "--acks-delay-millis", "0"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_subscription_position_earliest",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="从 Earliest 开始消费",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared", "-sp", "Earliest"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_subscription_position_latest",
        kind="consume_e2e",
        broker="persistent_nonpartitioned",
        description="从 Latest 开始消费",
        consumer_args=["-m", "100", "-q", "1000", "-st", "Shared", "-sp", "Latest", "-time", "2"],
        feed_producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_CONSUMER_RATE), "-s", "256"],
    ),
    Scenario(
        name="consume_persistent_partitioned_shared",
        kind="consume_e2e",
        broker="persistent_partitioned",
        description="分区 topic, Shared",
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared", "-n", "2"],
        feed_producer_args=["-m", str(BASE_MSGS * 2), "-r", str(PULSE_CONSUMER_RATE * 2), "-s", "256"],
    ),

    # Restart scenarios (2)
    Scenario(
        name="restart_backlog_replay",
        kind="restart_smoke",
        broker="persistent_nonpartitioned",
        description="produce → restart → consume Earliest",
        producer_args=["-m", str(BASE_MSGS), "-r", str(PULSE_PRODUCER_RATE * 4), "-s", "256", "-db"],
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared", "-sp", "Earliest"],
        restart_preserve=True,
    ),
    Scenario(
        name="restart_cursor_recovery",
        kind="restart_smoke",
        broker="persistent_nonpartitioned",
        description="consume partial ack → restart → consume remaining",
        producer_args=["-m", str(BASE_MSGS * 2), "-r", str(PULSE_PRODUCER_RATE * 4), "-s", "256", "-db"],
        consumer_args=["-m", str(BASE_MSGS), "-q", "1000", "-st", "Shared"],
        feed_producer_args=["-m", str(BASE_MSGS * 2), "-r", str(PULSE_CONSUMER_RATE * 2), "-s", "256"],
        restart_preserve=True,
    ),
]


def run_produce_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Run produce-only scenario."""
    topic = f"persistent://public/default/test-{uuid.uuid4().hex[:8]}"
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
    result_proc = run_sync(cmd, producer_log, timeout=120.0)
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
    """Run consumer + producer E2E scenario."""
    topic = f"persistent://public/default/test-{uuid.uuid4().hex[:8]}"
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
        consumer_cmd, producer_cmd, consumer_log, producer_log,
        consumer_timeout=120.0, producer_timeout=120.0,
    )
    duration = time.time() - start
    
    if consumer_rc != 0:
        raise RuntimeError(f"consumer failed with rc={consumer_rc}: {consumer_out[:500]}")
    if producer_rc != 0:
        raise RuntimeError(f"producer failed with rc={producer_rc}: {producer_out[:500]}")
    
    consumer_result = parse_consumer_output(consumer_out)
    producer_result = parse_producer_output(producer_out)
    broker_metrics = broker.metrics()
    
    return {
        "consumer": consumer_result,
        "producer": producer_result,
        "broker": broker_metrics,
        "duration_s": round(duration, 2),
    }


def run_restart_smoke_scenario(
    scenario: Scenario,
    broker: BrokerProcess,
    run_dir: Path,
) -> dict[str, Any]:
    """Run restart scenario: produce → restart → consume."""
    topic = f"persistent://public/default/test-{uuid.uuid4().hex[:8]}"
    
    # Step 1: Produce messages
    producer_log = run_dir / "producer_pre.log"
    producer_histogram = run_dir / "producer_histogram.hdr"
    producer_cmd = perf_cmd(
        "produce",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.producer_args,
        topic,
        producer_histogram,
    )
    
    producer_proc = run_sync(producer_cmd, producer_log, timeout=120.0)
    if producer_proc.returncode != 0:
        raise RuntimeError(f"producer failed: {producer_log.read_text()[:500]}")
    
    producer_result = parse_producer_output(producer_log.read_text(encoding="utf-8"))
    expected_records = producer_result["records"]
    
    # Step 2: Restart broker with storage preservation
    print(f"  Restarting broker (preserve_storage={scenario.restart_preserve})...")
    broker.restart(preserve_storage=scenario.restart_preserve)
    time.sleep(2)
    
    # Step 3: Consume messages
    consumer_log = run_dir / "consumer_post.log"
    consumer_histogram = run_dir / "histogram.hdr"
    consumer_cmd = perf_cmd(
        "consume",
        f"pulsar://127.0.0.1:{broker.config.port}",
        scenario.consumer_args,
        topic,
        consumer_histogram,
    )
    
    start = time.time()
    consumer_proc = run_sync(consumer_cmd, consumer_log, timeout=120.0)
    duration = time.time() - start
    
    if consumer_proc.returncode != 0:
        raise RuntimeError(f"consumer failed: {consumer_log.read_text()[:500]}")
    
    consumer_result = parse_consumer_output(consumer_log.read_text(encoding="utf-8"))
    actual_records = consumer_result["records"]
    
    broker_metrics = broker.metrics()
    
    return {
        "producer": producer_result,
        "consumer": consumer_result,
        "broker": broker_metrics,
        "verification": {
            "expected_records": expected_records,
            "actual_records": actual_records,
            "match": actual_records >= expected_records * 0.9,  # Allow 10% tolerance
        },
        "duration_s": round(duration, 2),
    }


def run_scenario(scenario: Scenario, broker: BrokerProcess, run_dir: Path) -> dict[str, Any]:
    """Dispatch to appropriate scenario runner."""
    if scenario.kind == "produce":
        return run_produce_scenario(scenario, broker, run_dir)
    elif scenario.kind == "consume_e2e":
        return run_consume_e2e_scenario(scenario, broker, run_dir)
    elif scenario.kind == "restart_smoke":
        return run_restart_smoke_scenario(scenario, broker, run_dir)
    else:
        raise ValueError(f"Unknown scenario kind: {scenario.kind}")


def main(argv: list[str]) -> int:
    """Main entry point."""
    ensure_prereqs()

    # Parse arguments
    import argparse
    parser = argparse.ArgumentParser(
        description="Run persistent e2e matrix scenarios for pulsar-lite"
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        help="Scenario names to run. If empty, run all scenarios."
    )
    parser.add_argument(
        "--broker-backend",
        choices=["local","docker"],
        default="local",
        help="Broker launch backend. local user rust/target/release/pulsar-lite; "
        "docker builds and runs a constrained broker container.",
    )
    parser.add_argument(
        "--docker-cpuset",
        default="0-3",
        help="Cpu set passed to docker run --cpuset-cpus when --broker-backend=docker.",
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
    filter_names = set(args.scenarios)

    # Docker image setup
    docker_image_metadata: dict[str, Any] = {}
    if args.broker_backend == "docker":
        print("Building Docker image for persistent broker...", file=sys.stderr)
        docker_image_metadata = build_broker_image(
            skip_docker_build=args.skip_docker_build,
        )
        print(f"Docker image: {docker_image_metadata['docker_image_tag']}", file=sys.stderr)
        if docker_image_metadata.get("docker_build_performed"):
            print(f"  Build reason: {docker_image_metadata['docker_build_reason']}", file=sys.stderr)

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
                
                try:
                    result = run_scenario(scenario, broker, scenario_dir)
                    
                    # Save broker log and timeseries
                    if broker.log_path:
                        (scenario_dir / "broker.log").write_text(
                            broker.log_path.read_text(encoding="utf-8", errors="replace")
                        )
                    if broker.sampler:
                        broker.sampler.write_csv(scenario_dir / "broker_timeseries.csv")
                    
                    results["scenarios"].append({
                        "name": scenario.name,
                        "kind": scenario.kind,
                        "broker": broker_name,
                        "description": scenario.description,
                        "result": result,
                        "status": "pass",
                    })
                    passed += 1
                    print(f"  ✓ PASS")
                
                except Exception as e:
                    results["scenarios"].append({
                        "name": scenario.name,
                        "kind": scenario.kind,
                        "broker": broker_name,
                        "description": scenario.description,
                        "error": str(e),
                        "status": "fail",
                    })
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
    RESULTS_PATH.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    
    print(f"\n=== Summary ===")
    print(f"Total: {passed + failed}, Passed: {passed}, Failed: {failed}")
    print(f"Results: {RESULTS_PATH}")
    print(f"Artifacts: {run_artifacts}")
    
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
