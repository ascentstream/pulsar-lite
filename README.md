# Pulsar Lite

[![CI](https://github.com/ascentstream/pulsar-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/ascentstream/pulsar-lite/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

English | [简体中文](README.zh-CN.md)

Pulsar Lite is a lightweight local broker that implements the core Apache Pulsar
binary protocol for development, integration testing, and small local prototypes.

It is designed for fast local feedback, not as a production replacement for an
Apache Pulsar cluster. Use Apache Pulsar for production workloads that require
multi-broker scheduling, replication, capacity management, tenant isolation, or
operational SLAs.

## Why Pulsar Lite

Many applications only need a local broker to validate the messaging path:
producers, consumers, subscriptions, flow control, failover, and key-based
routing. Setting up a full Pulsar deployment can be more expensive than the
test or prototype itself.

Pulsar Lite keeps the client-facing API close to Pulsar while reducing the
local setup cost:

- Run a single local broker process.
- Connect with the official Pulsar Python client.
- Use Pulsar topic names such as `persistent://...` and `non-persistent://...`.
- Exercise Shared, Failover, Exclusive, and KeyShared subscription behavior.
- Use RocksDB-backed persistent storage when built with `rocksdb-storage`.

## Current Capabilities

| Area | Status |
| --- | --- |
| Binary protocol | Core commands are implemented: Connect, Lookup, PartitionMetadata, Producer, Send, Subscribe, Flow, Ack, Close, Ping/Pong, redelivery paths. |
| Official client compatibility | The official Pulsar Python client can connect to `pulsar://localhost:6650`. |
| Topic names | Supports `persistent://...` and `non-persistent://...` topic URIs. |
| Non-persistent topics | Dispatch-or-drop runtime semantics with coverage for flow control, disconnect/reconnect, ordering, dynamic consumers, and KeyShared routing. |
| Persistent topics | RocksDB-backed managed-ledger style storage is available behind the `rocksdb-storage` feature. |
| Subscription modes | Shared, Failover, Exclusive, and KeyShared are covered by Rust and Python integration tests. |
| Partitioned topics | Default partition metadata and partition topic routing are supported for local testing. |
| Python package | Provides a small helper SDK that can start and manage a local broker process. |

## Requirements

- Rust stable with `rustfmt` and `clippy`.
- Python 3.10 or newer for the tested development workflow.
- `protobuf-compiler` / `protoc`.
- RocksDB build dependencies for `rocksdb-storage`.

On Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y protobuf-compiler clang libclang-dev
```

On macOS:

```bash
brew install protobuf llvm
```

## Quick Start

Build the broker with persistent storage support:

```bash
cd rust
cargo build --release --features rocksdb-storage
```

Install the Python package in editable mode:

```bash
cd ../python
pip install -e ".[dev]"
```

Start the local broker:

```bash
../rust/pulsar-lite.sh start
```

Connect with the official Pulsar client:

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
topic = "non-persistent://public/default/events"

consumer = client.subscribe(topic, "demo-sub", consumer_type=pulsar.ConsumerType.Shared)
producer = client.create_producer(topic)

producer.send(b"event-1")
message = consumer.receive(timeout_millis=5000)
consumer.acknowledge(message)

producer.close()
consumer.close()
client.close()
```

Stop the broker:

```bash
../rust/pulsar-lite.sh stop
```

## Embedded Python Usage

The Python helper can start a local broker for short-lived tests or examples:

```python
import pulsar
from pulsar_lite import PulsarClient

topic = "non-persistent://public/default/quick-start"

with PulsarClient("./demo.db") as client:
    consumer = client.subscribe(topic, "quick-start-sub", consumer_type=pulsar.ConsumerType.Shared)
    producer = client.create_producer(topic)

    producer.send(b"hello from pulsar lite")
    message = consumer.receive(timeout_millis=5000)
    consumer.acknowledge(message)
```

## Topic and Subscription Behavior

Pulsar Lite accepts standard Pulsar topic names:

```text
persistent://public/default/my-topic
non-persistent://public/default/my-topic
```

Use `non-persistent://...` for live event dispatch where slow or disconnected
consumers should not create a durable backlog. Use `persistent://...` when a
test requires stored entries, cursor replay, acknowledgements across restart,
or redelivery behavior. Persistent behavior requires a broker binary built with
`--features rocksdb-storage`.

Supported subscription modes:

| Mode | Summary |
| --- | --- |
| Exclusive | One active consumer; additional consumers are rejected. |
| Failover | One active consumer with standby takeover. |
| Shared | Messages are distributed across available consumers. |
| KeyShared | Messages with the same key are routed to the same consumer. |

## Development Commands

```bash
make build        # Build the Rust broker with rocksdb-storage
make install      # Install the Python package in editable mode
make test         # Run Rust and Python tests
make test-rust    # Run Rust tests with rocksdb-storage
make test-python  # Run Python integration tests with a local broker
make fmt          # Format Rust and Python code
make lint         # Run Rust clippy and Python ruff checks
```

The Python integration suite expects a broker binary with RocksDB support:

```bash
cd rust
cargo build --release --features rocksdb-storage
cd ../python
PULSAR_LITE_BINARY=../rust/target/release/pulsar-lite pytest ../tests/ -q
```

## Repository Layout

```text
pulsar-lite/
├── rust/                  # Rust broker implementation
│   ├── src/broker/        # Broker service, connection handling, dispatchers
│   ├── src/protocol/      # Pulsar binary protocol codec and commands
│   ├── src/storage/       # Metadata, resources, managed ledger, RocksDB storage
│   └── proto/             # Pulsar protobuf definitions
├── python/                # Python helper package and broker process manager
├── tests/                 # Python integration and behavior tests
├── examples/              # Small Python usage examples
└── docs/                  # Protocol, design, comparison, test, and perf notes
```

## Documentation

- [Documentation index](docs/README.md)
- [Contributing guide](docs/CONTRIBUTING.md)
- [Pulsar binary protocol notes](docs/PULSAR_BINARY_PROTOCOL.md)
- [Changelog](docs/CHANGELOG.md)

## Project Boundaries

Pulsar Lite intentionally does not provide:

- Multi-broker coordination or load balancing.
- Cross-cluster replication.
- Production-grade authorization or tenant governance.
- BookKeeper compatibility.
- Production durability or availability guarantees.

The project is useful for local development and compatibility testing, but
production deployments should use Apache Pulsar.

## License

Pulsar Lite is licensed under the [Apache License 2.0](LICENSE).
