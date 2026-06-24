# Pulsar Lite

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ascentstream/pulsar-lite/blob/main/LICENSE)

Pulsar Lite is a lightweight local broker that implements the core Apache Pulsar
binary protocol. This package ships a small Python helper that can start and
manage a local broker process, so you can use Pulsar topic names and the
official Pulsar Python client with a single local file path.

It is designed for fast local feedback, not as a production replacement for an
Apache Pulsar cluster. Use Apache Pulsar for production workloads that require
multi-broker scheduling, replication, capacity management, tenant isolation, or
operational SLAs.

## Why use the Python helper

Many applications only need a local broker to validate the messaging path:
producers, consumers, subscriptions, flow control, failover, and key-based
routing. Setting up a full Pulsar deployment can be more expensive than the
test or prototype itself.

`pulsar-lite` keeps the client-facing API close to Pulsar while removing the
local setup cost:

- Start a local broker by passing a file path, no manual process management.
- Connect with the official `pulsar-client` (installed automatically).
- Use Pulsar topic names such as `persistent://...` and `non-persistent://...`.
- Exercise Shared, Failover, Exclusive, and KeyShared subscription behavior.

## Installation

```bash
pip install pulsar-lite
```

The package declares `pulsar-client>=3.0.0` as a runtime dependency, so the
official Pulsar Python client is installed automatically.

Platform wheels include a prebuilt broker binary. If no wheel matches your
platform, see the [build from source](#building-the-broker-from-source) section.

## Quick start: embedded mode

Pass a local file path and the helper starts a broker process for you:

```python
import pulsar
from pulsar_lite import PulsarClient

topic = "non-persistent://public/default/quick-start"

with PulsarClient("./demo.db") as client:
    consumer = client.subscribe(
        topic,
        "quick-start-sub",
        consumer_type=pulsar.ConsumerType.Shared,
    )
    producer = client.create_producer(topic)

    producer.send(b"hello from pulsar lite")
    message = consumer.receive(timeout_millis=5000)
    print(message.data().decode("utf-8"))
    consumer.acknowledge(message)
```

When the `PulsarClient` context exits, the embedded broker process is stopped
automatically (reference counted, so multiple clients on the same path share
one broker).

## Quick start: remote mode

If you already have a broker running (locally or remotely), pass a `pulsar://`
URI instead and the helper acts as a thin wrapper over `pulsar.Client`:

```python
import pulsar
from pulsar_lite import PulsarClient

with PulsarClient("pulsar://localhost:6650") as client:
    producer = client.create_producer("non-persistent://public/default/events")
    producer.send(b"event-1")
```

You can also skip the helper entirely and use the official client directly,
since the broker speaks the standard Pulsar binary protocol:

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
```

## Explicit broker lifecycle

For cases where you want to start the broker and then connect with the official
client (or multiple clients), use `start_broker`:

```python
import pulsar
from pulsar_lite import start_broker

with start_broker("./demo.db") as broker:
    client = pulsar.Client(broker.url)
    # ... use the official client against broker.url ...
    client.close()
```

## API

### `PulsarClient(uri, **kwargs)`

- `uri`:
  - A local file path (e.g. `"./demo.db"`) starts an embedded broker.
  - A `pulsar://` or `pulsar+ssl://` URI connects to an existing broker.
- `**kwargs` are forwarded to `pulsar.Client`.
- Any attribute access not defined on `PulsarClient` itself is forwarded to the
  underlying `pulsar.Client`, so the standard Pulsar API (`create_producer`,
  `subscribe`, `get_topic_partitions`, ...) is fully available.
- Supports `with` statements and auto-closes the embedded broker on exit.

Properties:

| Property | Description |
| --- | --- |
| `is_embedded` | `True` when the helper started a local broker. |
| `db_path` | The absolute local file path (embedded mode only). |
| `pulsar_url` | The `pulsar://localhost:<port>` URL the client connects to. |

### `start_broker(db_path) -> BrokerHandle`

Starts (or reuses) an embedded broker for the given path and returns a
`BrokerHandle` with `.url`, `.port`, and a `.stop()` method. Use it as a
context manager for automatic cleanup.

## Topic names and subscription modes

Pulsar Lite accepts standard Pulsar topic URIs:

```text
persistent://public/default/my-topic
non-persistent://public/default/my-topic
```

Use `non-persistent://...` for live event dispatch where slow or disconnected
consumers should not create a durable backlog. Use `persistent://...` when a
test requires stored entries, cursor replay, or acknowledgements across restart
(requires a broker binary built with RocksDB storage support).

Supported subscription modes:

| Mode | Summary |
| --- | --- |
| Exclusive | One active consumer; additional consumers are rejected. |
| Failover | One active consumer with standby takeover. |
| Shared | Messages are distributed across available consumers. |
| KeyShared | Messages with the same key are routed to the same consumer. |

## Binary discovery

The helper looks for the bundled broker binary in this order:

1. The `pulsar_lite/bin/` directory shipped inside the wheel.
2. The `PULSAR_LITE_BINARY` environment variable.
3. The Rust release output at `rust/target/release/pulsar-lite` (development mode).
4. The system `PATH`.
5. Common install locations such as `/usr/local/bin/pulsar-lite`.

If you build the broker yourself, point `PULSAR_LITE_BINARY` at the resulting
binary instead of reinstalling the package.

## Building the broker from source

If no prebuilt wheel matches your platform, build the broker from source and
let the helper discover it via `PULSAR_LITE_BINARY`:

```bash
git clone https://github.com/ascentstream/pulsar-lite.git
cd pulsar-lite/rust
cargo build --release                 # core build
cargo build --release --features rocksdb-storage   # with persistent storage
```

Then install the Python package and point it at your binary:

```bash
cd ../python
pip install -e .
export PULSAR_LITE_BINARY=$(pwd)/../rust/target/release/pulsar-lite
```

## Links

- Source: <https://github.com/ascentstream/pulsar-lite>
- Issues: <https://github.com/ascentstream/pulsar-lite/issues>
- Full documentation: <https://github.com/ascentstream/pulsar-lite#readme>

## Project boundaries

Pulsar Lite intentionally does not provide:

- Multi-broker coordination or load balancing.
- Cross-cluster replication.
- Production-grade authorization or tenant governance.
- BookKeeper compatibility.
- Production durability or availability guarantees.

The project is useful for local development and compatibility testing, but
production deployments should use Apache Pulsar.

## License

Pulsar Lite is licensed under the [Apache License 2.0](https://github.com/ascentstream/pulsar-lite/blob/main/LICENSE).
