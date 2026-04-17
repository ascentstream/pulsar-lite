#!/usr/bin/env python3
from __future__ import annotations

import dataclasses
import json
import sys
import time
from pathlib import Path
from typing import Any

from lib import ROOT, PULSAR_ROOT, JAVA_HOME, JAVA, BROKER_BIN, BASE_CONFIG, PULSAR_TESTCLIENT_JAR, CLASSPATH_FILE, ENV_BASE
from lib.broker import BrokerConfig, BrokerSampler, BrokerProcess
from lib.parsing import parse_producer_output, parse_consumer_output
from lib.perf_cmd import ensure_prereqs, perf_cmd, run_sync, wait_for_log, run_consumer_then_feed

RESULTS_PATH = ROOT / 'docs' / 'perf' / 'data' / 'non_persistent_e2e_matrix_results.json'
ARTIFACTS_DIR = ROOT / 'docs' / 'perf' / 'data' / 'non_persistent_e2e_matrix_logs'

PULSE_PRODUCER_RATE = 2000
PULSE_CONSUMER_RATE = 2000
BASE_MSGS = 5000


@dataclasses.dataclass
class Scenario:
    name: str
    kind: str  # produce | consume_e2e
    broker: str
    description: str
    producer_args: list[str]
    consumer_args: list[str] | None = None
    feed_producer_args: list[str] | None = None


BROKERS = {
    'nonpartitioned': BrokerConfig('nonpartitioned', 6651, 0),
    'nonpersistent_partitioned': BrokerConfig('nonpersistent_partitioned', 6652, 4),
}

SCENARIOS: list[Scenario] = [
    Scenario(
        name='producer_single_baseline',
        kind='produce',
        broker='nonpartitioned',
        description='单 producer / 单线程 / 速率控制基线',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE), '-s', '256'],
    ),
    Scenario(
        name='producer_multi_producer',
        kind='produce',
        broker='nonpartitioned',
        description='多 producer（4）',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE * 2), '-s', '256', '-n', '4'],
    ),
    Scenario(
        name='producer_multi_thread',
        kind='produce',
        broker='nonpartitioned',
        description='多线程 producer（4 线程）',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE * 2), '-s', '256', '-threads', '4'],
    ),
    Scenario(
        name='producer_disable_batching',
        kind='produce',
        broker='nonpartitioned',
        description='关闭 batching',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE), '-s', '256', '-db'],
    ),
    Scenario(
        name='producer_lz4_compression',
        kind='produce',
        broker='nonpartitioned',
        description='LZ4 compression',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE), '-s', '1024', '-z', 'LZ4'],
    ),
    Scenario(
        name='producer_multi_topic',
        kind='produce',
        broker='nonpartitioned',
        description='4 topics fanout',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE * 2), '-s', '256', '-t', '4'],
    ),
    Scenario(
        name='producer_non_persistent_partitioned_topic',
        kind='produce',
        broker='nonpersistent_partitioned',
        description='non-persistent 4 partitions auto topic',
        producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_PRODUCER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_shared_baseline',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Shared 单 consumer 基线',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Shared'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_shared_multi_consumer',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Shared 4 consumers',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Shared', '-n', '4'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE * 2), '-s', '256'],
    ),
    Scenario(
        name='consume_multi_subscription',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='4 subscriptions',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS * 4), '-q', '1000', '-st', 'Shared', '-ns', '4'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_exclusive',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Exclusive 单 consumer',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Exclusive'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_failover',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Failover 双 consumer',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Failover', '-n', '2'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_key_shared',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='Key_Shared 双 consumer',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Key_Shared', '-n', '2'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256', '-mk', 'random'],
    ),
    Scenario(
        name='consume_small_receiver_queue',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='receiver queue = 1',
        producer_args=[],
        consumer_args=['-time', '30', '-q', '1', '-st', 'Shared'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', '500', '-s', '256'],
    ),
    Scenario(
        name='consume_ack_delay_zero',
        kind='consume_e2e',
        broker='nonpartitioned',
        description='ack delay = 0ms',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Shared', '--acks-delay-millis', '0'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE), '-s', '256'],
    ),
    Scenario(
        name='consume_non_persistent_partitioned_shared',
        kind='consume_e2e',
        broker='nonpersistent_partitioned',
        description='non-persistent partitioned topic + Shared 4 consumers',
        producer_args=[],
        consumer_args=['-m', str(BASE_MSGS), '-q', '1000', '-st', 'Shared', '-n', '4'],
        feed_producer_args=['-m', str(BASE_MSGS), '-r', str(PULSE_CONSUMER_RATE * 2), '-s', '256'],
    ),
]


def scenario_topic(run_id: str, scenario: Scenario) -> str:
    return f'non-persistent://public/default/{run_id}-{scenario.name}'


def main() -> int:
    ensure_prereqs()
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = time.strftime('%Y%m%d-%H%M%S')
    results: dict[str, Any] = {'run_id': run_id, 'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'scenarios': []}

    broker_metrics_by_name: dict[str, dict[str, float]] = {}
    for broker_name, broker_cfg in BROKERS.items():
        broker = BrokerProcess(broker_cfg)
        print(f'==> starting broker {broker_name} on {broker_cfg.port} (default_partitions={broker_cfg.default_partitions})', flush=True)
        broker.start()
        service_url = f'pulsar://127.0.0.1:{broker_cfg.port}'
        try:
            for scenario in [s for s in SCENARIOS if s.broker == broker_name]:
                topic = scenario_topic(run_id, scenario)
                print(f'==> running {scenario.name} [{scenario.description}] on {service_url}', flush=True)
                start_time = time.time()
                scenario_dir = ARTIFACTS_DIR / scenario.name
                scenario_dir.mkdir(parents=True, exist_ok=True)
                histogram = scenario_dir / f'{scenario.name}.hdr'
                result_entry: dict[str, Any] = {
                    'name': scenario.name,
                    'kind': scenario.kind,
                    'broker_profile': broker_name,
                    'service_url': service_url,
                    'description': scenario.description,
                    'topic': topic,
                    'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(start_time)),
                    'status': 'ok',
                }
                try:
                    if scenario.kind == 'produce':
                        cmd = perf_cmd('produce', service_url, scenario.producer_args, topic, histogram)
                        producer_log = scenario_dir / 'producer.log'
                        proc = run_sync(cmd, producer_log)
                        if proc.returncode != 0:
                            raise RuntimeError(proc.stdout)
                        result_entry['metrics'] = parse_producer_output(proc.stdout)
                    elif scenario.kind == 'consume_e2e':
                        consumer_log = scenario_dir / 'consumer.log'
                        producer_log = scenario_dir / 'feed_producer.log'
                        consumer_cmd = perf_cmd('consume', service_url, scenario.consumer_args or [], topic, histogram)
                        producer_cmd = perf_cmd('produce', service_url, scenario.feed_producer_args or [], topic, scenario_dir / 'feed_producer.hdr')
                        consumer_text, producer_text, consumer_rc, producer_rc = run_consumer_then_feed(consumer_cmd, producer_cmd, consumer_log, producer_log)
                        if producer_rc != 0:
                            raise RuntimeError(f'producer failed:\n{producer_text}')
                        if consumer_rc != 0:
                            raise RuntimeError(f'consumer failed:\n{consumer_text}')
                        result_entry['producer_metrics'] = parse_producer_output(producer_text)
                        result_entry['metrics'] = parse_consumer_output(consumer_text)
                    else:
                        raise ValueError(f'unknown scenario kind: {scenario.kind}')
                except Exception as exc:  # noqa: BLE001
                    result_entry['status'] = 'failed'
                    result_entry['error'] = str(exc)
                finally:
                    result_entry['duration_secs'] = round(time.time() - start_time, 3)
                    current_metrics = broker.metrics()
                    result_entry.update(current_metrics)
                    results['scenarios'].append(result_entry)
        finally:
            broker_metrics_by_name[broker_name] = broker.stop()
            print(f'==> stopped broker {broker_name}', flush=True)

    results['broker_stop_metrics'] = broker_metrics_by_name
    RESULTS_PATH.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')

    failed = [scenario for scenario in results['scenarios'] if scenario['status'] != 'ok']
    if failed:
        print(json.dumps(failed, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
