# Pulsar Lite

**本地嵌入式 Apache Pulsar 协议兼容 Broker，用于开发、测试、Agent 原型和轻量 demo。**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> 当前定位：Pulsar Lite 面向本地开发和测试验证，不是生产级 Apache Pulsar 集群替代品。生产环境请使用完整 Apache Pulsar 集群。

## 为什么需要 Pulsar Lite

很多应用只想快速验证一段消息链路：生产者写入、消费者订阅、Shared 分摊、Failover 接管、KeyShared 保序。真正耗时的往往不是 API，而是先准备一套可用的消息队列环境。

Pulsar Lite 的目标是把这段前置成本降下来：

- 本地文件路径即可启动嵌入式 Broker。
- 官方 Pulsar 客户端可以直接连接。
- 本地开发和生产集群使用同一套 Pulsar API。
- 测试可以使用独立本地路径，减少 topic / subscription 状态污染。
- Agent 或脚本可以直接创建可用消息环境，继续完成原型验证。

![Pulsar Lite 快速连接流程](docs/assets/readme/quick-start-connection-flow.png)

## 当前能力

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| Pulsar 二进制协议 | 已支持核心命令 | Connect、Lookup、PartitionMetadata、Producer、Send、Subscribe、Flow、Ack、Close、Ping/Pong |
| 官方 Python 客户端 | 可直连 | `pulsar.Client("pulsar://localhost:6650")` |
| Pulsar Lite Python SDK | 可用 | `PulsarClient("./demo.db")` 自动启动本地 Broker |
| Topic 命名 | 兼容 Pulsar URI | 支持 `persistent://...` 与 `non-persistent://...` 命名 |
| 分区 Topic | 已支持 | `default_partitions > 0` 时自动使用分区 topic |
| 订阅模式 | 已覆盖主要模式 | Shared、Failover、Exclusive，non-persistent 路径覆盖 KeyShared |
| Non-persistent 实时语义 | 覆盖较完整 | 动态 consumer、顺序、flow control、断连重连、KeyShared 策略等测试已沉淀 |
| Metadata / resources | 本地路径持久化 | tenant、namespace、topic、subscription 等资源语义逐步拆分中 |
| 消息存储 | 当前为内存运行时 | managed-ledger 风格骨架已存在，生产级持久化仍在演进 |

## 快速开始

### 1. 构建 Broker

```bash
make build
```

等价于：

```bash
cd rust && cargo build --release
```

生成的 Broker 二进制位于：

```text
rust/target/release/pulsar-lite
```

### 2. 安装 Python SDK

```bash
cd python
pip install -e .
```

Python SDK 依赖官方 `pulsar-client>=3.0.0`。

### 3. 嵌入式模式

适合测试、demo、Notebook 和 Agent 原型。传入本地路径后，SDK 会自动启动本地 Broker。

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

    msg = consumer.receive(timeout_millis=5000)
    print(msg.data().decode("utf-8"))
    consumer.acknowledge(msg)

    producer.close()
    consumer.close()
```

### 4. 官方客户端直连

先启动本地 Broker：

```bash
rust/pulsar-lite.sh start
```

默认监听：

```text
pulsar://localhost:6650
```

然后使用官方 Pulsar 客户端连接：

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")
topic = "non-persistent://public/default/events"

consumer = client.subscribe(
    topic,
    "demo-sub",
    consumer_type=pulsar.ConsumerType.Shared,
)
producer = client.create_producer(topic)

producer.send(b"event-1")
msg = consumer.receive(timeout_millis=5000)
consumer.acknowledge(msg)

producer.close()
consumer.close()
client.close()
```

## 两种连接姿势

| 写法 | 含义 | 适用场景 |
| --- | --- | --- |
| `PulsarClient("./demo.db")` | 自动启动并连接嵌入式 Broker | 测试、demo、Notebook、Agent 原型 |
| `PulsarClient("pulsar://localhost:6650")` | 通过 SDK 连接已有 Broker | 本地服务或远程 Pulsar |
| `pulsar.Client("pulsar://localhost:6650")` | 官方客户端直连 | 协议兼容验证 |

## Topic 和订阅模式

Pulsar Lite 兼容 Pulsar Topic URI：

```text
persistent://public/default/my-topic
non-persistent://public/default/my-topic
```

如果要验证实时事件、任务触发、在线 consumer 分发，优先使用 `non-persistent://...`。如果要验证和生产 Pulsar 更接近的 Topic 命名和客户端调用路径，可以使用 `persistent://...`。需要注意，当前消息数据仍是内存运行时，不应把 Pulsar Lite 当成生产级持久化存储。

订阅模式选择：

![Pulsar Lite 订阅模式](docs/assets/readme/subscription-modes.png)

| 目标 | 推荐模式 | 说明 |
| --- | --- | --- |
| 单消费者独占处理 | `Exclusive` | 第二个 consumer 会被拒绝 |
| 主备切换 | `Failover` | active 消费，standby 等待接管 |
| 多消费者分摊任务 | `Shared` | 适合并发 worker |
| 同 key 保序 | `KeyShared` | non-persistent 路径覆盖 Sticky / AutoSplit 相关语义 |

## 配置

默认配置文件：

```text
rust/pulsar-lite.toml
```

常用配置：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `addr` | `0.0.0.0:6650` | Broker 监听地址 |
| `db_path` | `./pulsar-lite.db` | 本地 metadata / resource 路径 |
| `default_partitions` | `0` | 新 topic 默认分区数，`0` 表示非分区 |
| `log_level` | `info` | 日志级别 |
| `keep_alive_interval_secs` | `30` | 心跳间隔 |
| `max_connections` | `0` | 最大连接数，`0` 表示不限制 |
| `max_connections_per_ip` | `0` | 单 IP 最大连接数，`0` 表示不限制 |
| `max_message_size_bytes` | `5242880` | 单条消息大小上限 |

## 架构概览

![Pulsar Lite 架构](docs/assets/readme/pulsar-lite-architecture.png)

## 主要目录

```text
pulsar-lite/
├── rust/                    # Rust Broker
│   ├── src/broker/          # 连接、服务、dispatcher、non-persistent runtime
│   ├── src/protocol/        # Pulsar 二进制协议编解码
│   ├── src/storage/         # metadata/resources/managed-ledger 相关模块
│   └── proto/PulsarApi.proto
├── python/                  # Python SDK 与进程管理
├── examples/                # 基础示例
├── tests/                   # Python 集成测试、non-persistent 语义测试、perf 脚本
└── docs/                    # 设计、差异、协议、性能和测试覆盖文档
```

## 开发命令

```bash
make build        # 构建 Rust Broker
make install      # 安装 Python SDK（开发模式）
make test         # 运行 Rust + Python 测试
make test-rust    # 运行 Rust 测试
make test-python  # 运行 Python 集成测试
make fmt          # 格式化 Rust / Python
make lint         # cargo clippy + ruff
```


## 与 Apache Pulsar 的关系

| 维度 | Pulsar Lite | Apache Pulsar |
| --- | --- | --- |
| 定位 | 本地开发、测试、demo、Agent 原型 | 生产级云原生消息平台 |
| 部署 | 单进程本地 Broker / 嵌入式启动 | 多组件分布式集群 |
| 客户端 | 官方 Pulsar 客户端 | 官方 Pulsar 客户端 |
| 协议 | 实现核心 Pulsar 二进制协议命令 | 完整协议与生态 |
| 存储 | 当前消息状态为内存运行时 | BookKeeper / Managed Ledger |
| 运维 | 本地进程和文件路径 | 集群容量、高可用、复制、治理 |

## 当前边界

- 不提供生产集群级别的多 Broker 调度、跨节点复制和容量治理。
- 当前消息数据仍是内存运行时；managed-ledger 风格持久化仍在演进。
- non-persistent 路径已沉淀较完整语义覆盖，但 redelivery、negative ack、ack timeout 等行为仍有明确边界。
- 如果业务依赖生产级消息保留、跨节点高可用、多租户治理或 SLA，应直接使用 Apache Pulsar。

## 许可证

Apache License 2.0
