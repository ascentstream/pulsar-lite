# Pulsar Lite

**嵌入式轻量级消息队列，完全兼容 Apache Pulsar 协议**

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## 项目简介

Pulsar Lite 是 Apache Pulsar 的嵌入式轻量级实现，借鉴 [Milvus Lite](https://github.com/milvus-io/milvus-lite) 的设计理念，提供**零部署、即开即用**的消息队列体验。

### 🎯 核心特性

- ✅ **标准 Pulsar 协议**: 完全兼容 Pulsar 二进制协议，可直接使用官方客户端
- ✅ **零部署成本**: 本地文件即可运行，无需独立集群
- ✅ **API 100% 兼容**: 与 Pulsar Standalone/Distributed 使用相同 API
- ✅ **Milvus Lite 体验**: 本地文件路径自动启动，远程 URI 直接连接
- ✅ **消息持久化**: 基于 RocksDB 的可靠存储
- ✅ **模块化架构**: 参考 Apache Pulsar 的清晰代码结构

### 🚀 适用场景

- **AI 应用原型开发**: 快速验证想法，无需搭建 Pulsar 集群
- **单元测试和集成测试**: 每个测试用例独立的消息队列实例
- **Jupyter Notebook / Google Colab**: 在笔记本中直接使用消息队列
- **边缘设备**: 资源受限环境下的消息处理
- **本地开发环境**: 开发时使用，生产环境无缝切换到 Pulsar 集群

**不适合**:
- 大规模生产环境（请使用 [Apache Pulsar](https://pulsar.apache.org/)）

## 快速开始

### 方式1：使用官方 Pulsar 客户端（推荐）

```python
import pulsar

# 直接连接到 Pulsar Lite（就像连接到普通 Pulsar 服务器）
client = pulsar.Client("pulsar://localhost:6650")

# 创建生产者并发送消息
producer = client.create_producer("persistent://public/default/my-topic")
producer.send(b"Hello Pulsar Lite!")

print("消息发送成功！")
client.close()
```

### 方式2：使用 Pulsar Lite SDK（Milvus Lite 风格）

```python
from pulsar_lite import PulsarClient

# 嵌入式模式 - 指定本地文件，自动启动服务器
client = PulsarClient("./my_queue.db")

# 后续使用完全兼容 Pulsar API
producer = client.create_producer("my-topic")
producer.send(b"Hello Pulsar Lite!")

# 关闭时自动停止服务器
client.close()
```

### 生产环境无缝切换

```python
# 开发环境 - 使用 Pulsar Lite
client = PulsarClient("./dev.db")

# 生产环境 - 连接到 Pulsar 集群（只需改 URI）
client = PulsarClient("pulsar://prod-cluster:6650")

# API 完全相同，无需修改其他代码
```

## 安装

### 1. 构建 Rust Broker

```bash
make build
# 或
cd rust && cargo build --release
```

二进制文件位置: `rust/target/release/pulsar-lite`

### 2. 启动服务器

```bash
# 使用脚本启动
rust/pulsar-lite.sh start

# 默认监听 pulsar://localhost:6650
```

### 3. 安装 Python SDK（可选）

如果想使用 Milvus Lite 风格的嵌入式体验：

```bash
cd python
pip install -e .
```

## 项目结构

```
pulsar-lite/
├── CLAUDE.md                     # Claude Code 开发指南
├── Makefile                      # 构建命令
├── README.md                     # 项目说明
│
├── rust/                         # Rust Broker (核心实现)
│   ├── Cargo.toml               # Rust 依赖配置
│   ├── build.rs                 # Protobuf 代码生成
│   ├── pulsar-lite.sh           # 启动脚本
│   ├── pulsar-lite.toml         # 配置文件
│   ├── proto/
│   │   └── PulsarApi.proto      # Pulsar 官方协议定义
│   ├── tests/                   # Rust 单元测试
│   └── src/
│       ├── main.rs              # 程序入口
│       ├── lib.rs               # 库导出
│       ├── config.rs            # 配置解析
│       ├── error.rs             # 错误类型定义
│       │
│       ├── broker/              # Broker 核心模块
│       │   ├── mod.rs
│       │   ├── broker_service.rs    # Broker 主服务 (连接管理、消息路由)
│       │   │
│       │   ├── dispatcher/          # 消息分发器
│       │   │   ├── mod.rs
│       │   │   ├── traits.rs        # Dispatcher trait 定义
│       │   │   ├── enums.rs         # 订阅类型枚举
│       │   │   ├── shared.rs        # Shared 订阅分发器 (Round-Robin)
│       │   │   ├── failover.rs      # Failover 订阅分发器 (主备)
│       │   │   └── exclusive.rs     # Exclusive 订阅分发器 (独占)
│       │   │
│       │   ├── handler/             # 协议命令处理器
│       │   │   ├── mod.rs
│       │   │   ├── connection_handler.rs   # 连接处理 (Connect/Connected)
│       │   │   ├── lookup_handler.rs       # Topic 查找 (Lookup)
│       │   │   ├── producer_handler.rs     # 生产者命令 (Producer/Send)
│       │   │   └── consumer_handler.rs     # 消费者命令 (Subscribe/Flow/Ack)
│       │   │
│       │   ├── service/             # 服务层
│       │   │   ├── mod.rs
│       │   │   ├── server_cnx.rs        # 服务器连接上下文
│       │   │   ├── producer.rs           # 生产者管理
│       │   │   ├── consumer.rs            # 消费者管理
│       │   │   └── topic/                # Topic 管理
│       │   │       ├── mod.rs
│       │   │       ├── topic.rs            # Topic 实现
│       │   │       ├── partitioned_topic.rs # 分区 Topic
│       │   │       └── subscription.rs     # 订阅管理
│       │   │
│       │   └── stats/              # 统计指标
│       │       ├── mod.rs
│       │       └── metrics.rs          # Metrics 收集
│       │
│       ├── protocol/               # 协议层
│       │   ├── mod.rs
│       │   ├── command.rs          # 命令类型定义
│       │   └── codec.rs            # 二进制协议编解码
│       │
│       └── storage/                # 存储层
│           └── mod.rs              # 内存存储 (MVP版本)
│
├── python/                       # Python SDK
│   ├── pyproject.toml            # Python 包配置
│   ├── setup.py                  # 安装脚本
│   ├── example_usage.py          # 使用示例
│   └── src/pulsar_lite/
│       ├── __init__.py           # 包导出
│       ├── client.py             # 主客户端 (代理到 pulsar.Client)
│       ├── process_manager.py    # Broker 进程管理器
│       ├── binary_finder.py      # 二进制文件查找
│       └── lib/                  # 预编译二进制
│
├── tests/                        # 集成测试
│   ├── test_binary_protocol.py   # 二进制协议测试
│   ├── test_shared_simple.py     # Shared 订阅基础测试
│   └── test_shared_dispatcher.py # Shared 分发器详细测试
│
├── examples/                     # 使用示例
│   └── basic_usage.py            # 基础用法示例
│
└── docs/                         # 项目文档
    ├── PROJECT_OVERVIEW.md       # 项目概览
    ├── CONTRIBUTING.md           # 贡献指南
    ├── CHANGELOG.md              # 变更日志
    ├── PULSAR_BINARY_PROTOCOL.md # 协议实现文档
    │
    ├── design/                   # 设计文档
    │   └── consumer/
    │       └── shared.md         # Shared 消费者设计
    │
    └── difference/               # 与原生 Pulsar 差异分析
        ├── shared_subscription_comparison.md    # Shared 订阅对比
        ├── failover_subscription_comparison.md  # Failover 订阅对比
        └── exclusive_subscription_comparison.md # Exclusive 订阅对比
```

### 核心模块说明

| 模块 | 路径 | 职责 |
|------|------|------|
| **Broker Service** | `rust/src/broker/broker_service.rs` | TCP 连接管理、命令路由 |
| **Dispatcher** | `rust/src/broker/dispatcher/` | 消息分发策略 (Shared/Failover/Exclusive) |
| **Handler** | `rust/src/broker/handler/` | 协议命令处理 |
| **Service** | `rust/src/broker/service/` | 业务实体管理 (Producer/Consumer/Topic) |
| **Protocol** | `rust/src/protocol/` | Pulsar 二进制协议编解码 |
| **Storage** | `rust/src/storage/` | 消息持久化存储 |

## 功能支持

### ✅ 已实现

| 功能 | 状态 | 说明 |
|------|------|------|
| **生产者** | ✅ | 完整支持 |
| 消息发送 | ✅ | 同步发送，支持消息回执 |
| 消息持久化 | ✅ | 内存存储（MVP版本） |
| Connect 协议 | ✅ | 标准握手 |
| Lookup 协议 | ✅ | Topic 查找 |
| CloseProducer | ✅ | 优雅关闭 |
| **消费者** | ✅ | 完整支持所有订阅模式 |
| **消息推送** | ✅ | Flow 控制，permit-based 流控 |
| **消息确认** | ✅ | Ack 命令，支持可选 request_id |
| **订阅模式** | ✅ | Shared, Failover, Exclusive 完整支持 |
| **CloseConsumer** | ✅ | 优雅关闭消费者 |
| **Ping/Pong** | ✅ | 心跳检测 |
| **独占访问控制** | ✅ | Exclusive 模式强制单消费者 |
| **主备切换** | ✅ | Failover 模式主消费者优先 |
| **Metrics 收集** | ✅ | Broker 性能指标统计 |
| **分区 Topic** | ✅ | PartitionedTopic 支持多分区 |
| **消息 ID partition** | ✅ | MessageId 包含 partition 字段 |
| **独立 ledger_id** | ✅ | 每个 Topic 独立的 ledger_id |

### ⏳ 开发中

| 功能 | 状态 | 说明 |
|------|------|------|
| Key_Shared 订阅 | ⏳ | 按键共享订阅模式 |

## 使用示例

### 基础生产者和消费者（Shared 模式）

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")

# 创建生产者并发送消息
producer = client.create_producer("persistent://public/default/test-topic")
for i in range(10):
    msg_id = producer.send(f"Message {i}".encode('utf-8'))
    print(f"Sent message {i}: {msg_id}")
producer.close()

# 创建消费者（Shared 模式）
consumer = client.subscribe(
    "persistent://public/default/test-topic",
    "my-subscription",
    consumer_type=pulsar.ConsumerType.Shared
)

# 消费消息
for i in range(10):
    msg = consumer.receive(timeout_millis=5000)
    print(f"Received: {msg.data().decode('utf-8')}")
    consumer.acknowledge(msg)

consumer.close()
client.close()
```

### 多消费者共享订阅

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")

# 创建两个消费者共享同一个订阅
consumer1 = client.subscribe(
    "persistent://public/default/test-topic",
    "shared-subscription",
    consumer_type=pulsar.ConsumerType.Shared,
    consumer_name="consumer-1"
)

consumer2 = client.subscribe(
    "persistent://public/default/test-topic",
    "shared-subscription",
    consumer_type=pulsar.ConsumerType.Shared,
    consumer_name="consumer-2"
)

# 消息会自动分配给两个消费者（round-robin）
# consumer1 和 consumer2 会各处理一部分消息

consumer1.close()
consumer2.close()
client.close()
```

### Failover 订阅模式（主备切换）

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")

# 创建主消费者
primary_consumer = client.subscribe(
    "persistent://public/default/test-topic",
    "failover-subscription",
    consumer_type=pulsar.ConsumerType.Failover,
    consumer_name="primary-consumer"
)

# 创建备用消费者
standby_consumer = client.subscribe(
    "persistent://public/default/test-topic",
    "failover-subscription",
    consumer_type=pulsar.ConsumerType.Failover,
    consumer_name="standby-consumer"
)

# 主消费者接收所有消息
# 如果主消费者失败，备用消费者自动接管
msg = primary_consumer.receive(timeout_millis=5000)
print(f"Primary received: {msg.data().decode('utf-8')}")
primary_consumer.acknowledge(msg)

primary_consumer.close()
standby_consumer.close()
client.close()
```

### Exclusive 订阅模式（独占访问）

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")

# 创建独占消费者
exclusive_consumer = client.subscribe(
    "persistent://public/default/test-topic",
    "exclusive-subscription",
    consumer_type=pulsar.ConsumerType.Exclusive,
    consumer_name="exclusive-consumer"
)

# 尝试创建第二个消费者会失败
try:
    second_consumer = client.subscribe(
        "persistent://public/default/test-topic",
        "exclusive-subscription",
        consumer_type=pulsar.ConsumerType.Exclusive,
        consumer_name="second-consumer"
    )
    print("Error: Second consumer should not be created!")
except Exception as e:
    print(f"Expected error: {e}")  # 独占订阅已存在

# 独占消费者接收所有消息，保证顺序
msg = exclusive_consumer.receive(timeout_millis=5000)
print(f"Exclusive received: {msg.data().decode('utf-8')}")
exclusive_consumer.acknowledge(msg)

exclusive_consumer.close()
client.close()
```

### 使用 with 语句

```python
from pulsar_lite import PulsarClient

# 自动资源管理
with PulsarClient("./demo.db") as client:
    producer = client.create_producer("my-topic")
    producer.send(b"Auto cleanup!")
    # 退出 with 块时自动关闭客户端和停止服务器
```

## 开发指南

### 环境要求

- Rust 1.70+
- Python 3.8+
- protobuf (protoc)

### 常用命令

```bash
# 构建
make build              # 构建 Rust broker

# 测试
make test              # 所有测试
make test-rust         # Rust 单元测试
make test-python       # Python 集成测试

# 开发
rust/pulsar-lite.sh start                          # 启动服务器

# 代码质量
make fmt              # 格式化代码
make lint             # 代码检查
```

## 架构设计

### 整体架构

```
┌─────────────────────────────────────────┐
│          用户应用代码                     │
│                                         │
│  方式1: import pulsar                   │
│  方式2: from pulsar_lite import Client  │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│      pulsar-client (官方 Python SDK)    │
│  • 标准 Pulsar 二进制协议                │
│  • 完整的 Pulsar API                    │
└─────────────┬───────────────────────────┘
              │
              │ Pulsar 二进制协议 (TCP)
              ▼
┌─────────────────────────────────────────┐
│       Pulsar Lite Broker (Rust)         │
│  ├── broker_service.rs  (连接管理)      │
│  ├── dispatcher/        (消息分发)      │
│  ├── handler/           (命令处理)      │
│  ├── service/           (业务实体)      │
│  ├── protocol/codec.rs  (协议编解码)    │
│  └── storage/           (消息存储)      │
└─────────────┬───────────────────────────┘
              │
              ▼
         ./pulsar-lite.db (本地文件)
```

### 消息流转

```
Producer                    Broker                      Consumer
   │                          │                            │
   │ ─────── Send ─────────> │                            │
   │                         │ ──── store message ────>   │
   │ <───── Receipt ──────── │                            │
   │                          │                            │
   │                          │ <──── Flow(permits) ───── │
   │                          │                            │
   │                          │ ───── Message ─────────> │
   │                          │ <───── Ack ───────────── │
   │                          │                            │
```

## 性能特点

- **零拷贝**: 使用 Pulsar 原生二进制协议，无需协议转换
- **异步 IO**: 基于 tokio 的高性能异步运行时
- **嵌入式存储**: 内存存储 (MVP)，后续支持 RocksDB
- **轻量级**: 单二进制文件，无外部依赖

## 技术栈

### Rust
- **tokio**: 异步运行时
- **prost**: Protobuf 编解码
- **tokio-util**: 编解码器工具

### Python
- **pulsar-client**: 官方 Pulsar Python SDK (>=3.0.0)
- **setuptools**: 包管理

## 与 Apache Pulsar 对比

| 特性 | Pulsar Lite | Apache Pulsar |
|------|-------------|---------------|
| 部署方式 | 单机嵌入式 | 分布式集群 |
| 协议支持 | 标准 Pulsar 协议 | 完整 Pulsar 协议 |
| 客户端 | 官方客户端 | 官方客户端 |
| 持久化 | 内存/RocksDB | BookKeeper |
| 消息顺序 | 单分区 | 多分区 |
| 适用场景 | 开发/测试/边缘 | 生产环境 |
| 运维成本 | 零 | 需要专业团队 |

### 已知差异

详细的差异分析请查看 `docs/difference/` 目录：

- [Shared 订阅差异](docs/difference/shared_subscription_comparison.md)
- [Failover 订阅差异](docs/difference/failover_subscription_comparison.md)
- [Exclusive 订阅差异](docs/difference/exclusive_subscription_comparison.md)

## 路线图

### ✅ 阶段一：核心功能（已完成）
- [x] Pulsar 二进制协议支持
- [x] 模块化 Broker 架构
- [x] 生产者功能
- [x] Python SDK (Milvus Lite 风格)

### ✅ 阶段二：消费者功能（已完成）
- [x] 消费者订阅（Subscribe 命令）
- [x] 消息推送（Flow 控制）
- [x] 消息确认（Ack 命令）
- [x] Shared 订阅模式
- [x] CloseConsumer 命令
- [x] Ping/Pong 心跳检测

### ✅ 阶段三：订阅模式完善（已完成）
- [x] Failover 订阅模式
- [x] Exclusive 订阅模式
- [x] Broker Metrics 收集
- [x] 代码重构优化

### ⏳ 阶段四：完善与发布（进行中）
- [ ] Key_Shared 订阅模式
- [ ] 消息重投递机制
- [ ] 持久化存储（RocksDB）
- [ ] 性能优化
- [ ] 文档完善
- [ ] PyPI 发布

## 参考项目

- [Apache Pulsar](https://github.com/apache/pulsar) - 云原生分布式消息系统
- [Milvus Lite](https://github.com/milvus-io/milvus-lite) - 嵌入式向量数据库
- [Pulsar Protocol](https://pulsar.apache.org/docs/developing-binary-protocol/) - Pulsar 二进制协议规范

## 贡献

欢迎贡献代码！请查看 [贡献指南](docs/CONTRIBUTING.md) 了解详情。

特别欢迎：
- 消费者功能实现
- 性能优化
- 测试用例
- 文档改进

## 许可证

Apache License 2.0

## 联系方式

- GitHub Issues: https://github.com/your-org/pulsar-lite/issues
- 项目文档: [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md)

---

**Pulsar Lite - 轻量级嵌入式消息队列，让消息处理更简单！** 🚀
