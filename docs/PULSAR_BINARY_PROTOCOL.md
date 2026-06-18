# Pulsar 二进制协议实现文档

本文档描述 Pulsar Lite 中实现的 Apache Pulsar 二进制协议。

## 概述

Pulsar Lite 完全兼容 Apache Pulsar 的标准二进制协议，允许使用官方 Pulsar 客户端直接连接。

## 协议规范

### 帧格式

Pulsar 二进制协议使用自定义的帧格式：

```
[4 bytes - Total Size]
[4 bytes - Command Size]
[Command Size bytes - Protobuf Command]
[Optional: 4 bytes - Metadata Size]
[Optional: Metadata Size bytes - Protobuf Metadata]
[Optional: Payload]
```

**字段说明**:
- **Total Size**: 整个帧的大小（不包括这4字节）
- **Command Size**: Protobuf 命令的大小
- **Command**: BaseCommand 的 Protobuf 序列化数据
- **Metadata Size**: 消息元数据的大小（仅消息相关命令）
- **Metadata**: MessageMetadata 的 Protobuf 序列化数据
- **Payload**: 消息体（仅消息相关命令）

### 编解码器实现

Pulsar Lite 使用 `tokio_util::codec` 实现异步编解码：

**位置**: `rust/src/protocol/codec.rs`

```rust
pub struct PulsarFrameCodec {
    decode_state: DecodeState,
}

enum DecodeState {
    Head,       // 等待帧头
    Data(usize), // 等待数据
}
```

## 支持的命令

### 已实现的命令

| 命令 | 类型值 | 方向 | 说明 | 状态 |
|------|--------|------|------|------|
| Connect | 2 | C→S | 客户端连接 | ✅ |
| Connected | 3 | S→C | 连接成功响应 | ✅ |
| PartitionMetadata | 8 | C→S | 查询分区元数据 | ✅ |
| PartitionMetadataResponse | 9 | S→C | 分区元数据响应 | ✅ |
| Lookup | 10 | C→S | 查询 Topic 位置 | ✅ |
| LookupResponse | 11 | S→C | Topic 位置响应 | ✅ |
| Producer | 12 | C→S | 创建生产者 | ✅ |
| ProducerSuccess | 13 | S→C | 生产者创建成功 | ✅ |
| Send | 14 | C→S | 发送消息 | ✅ |
| SendReceipt | 15 | S→C | 消息接收确认 | ✅ |
| CloseProducer | 18 | C→S | 关闭生产者 | ✅ |
| Success | 7 | S→C | 通用成功响应 | ✅ |
| Ping | 36 | C→S | 心跳检测 | ✅ |
| Pong | 37 | S→C | 心跳响应 | ✅ |
| **Subscribe** | 16 | C→S | 创建消费者（Shared 模式） | ✅ |
| **Flow** | 22 | C→S | 请求消息流（permit-based） | ✅ |
| **Message** | 23 | S→C | 推送消息 | ✅ |
| **Ack** | 24 | C→S | 确认消息 | ✅ |
| **AckResponse** | 25 | S→C | 确认响应（可选） | ✅ |
| **CloseConsumer** | 19 | C→S | 关闭消费者 | ✅ |
| **RedeliverUnacknowledgedMessages** | 26 | C→S | 重新投递未确认消息（Shared/KeyShared persistent） | ✅ |

### 订阅模式（通过 Subscribe 命令选择）

| 模式 | 说明 | Persistent | Non-Persistent |
|------|------|------------|----------------|
| Shared | 多消费者 round-robin | ✅ | ✅ |
| Exclusive | 单消费者独占 | ✅ | ✅ |
| Failover | 主备切换（priority + rewind） | ✅ | ✅ |
| Key_Shared | 按 ordering_key sticky 路由 | ✅ | ✅ |

> Persistent 订阅使用 `initialize_or_open_cursor` + dispatcher `read_position` + hole-aware `read_from`；negative ack / ack timeout 由官方客户端发送 `RedeliverUnacknowledgedMessages` 触发，broker 不实现独立 ack-timeout 定时器。

## 交互流程

### 1. 连接建立

```
Client                              Server (Pulsar Lite)
  |                                     |
  |-------- Connect --------------->   |  # 协议版本、客户端信息
  |                                     |
  |   <------ Connected -------------  |  # 服务器版本
  |                                     |
```

### 2. 生产者流程

```
Client                              Server
  |                                     |
  |-- PartitionMetadata (topic) --->   |  # 查询是否分区 Topic
  |                                     |
  |   <-- PartitionMetadataResponse -- |  # partitions=0 (非分区)
  |                                     |
  |-- Lookup (topic) -------------->   |  # 查询 Topic 位置
  |                                     |
  |   <-- LookupResponse ------------  |  # broker URL
  |                                     |
  |-- Producer (topic, name) ------->  |  # 创建生产者
  |                                     |
  |   <-- ProducerSuccess -----------  |  # producer_name, producer_id
  |                                     |
  |-- Send (producer_id, seq, msg) ->  |  # 发送消息
  |                                     |
  |   <-- SendReceipt ---------------  |  # message_id (ledger, entry)
  |                                     |
  |-- CloseProducer (producer_id) -->  |  # 关闭生产者
  |                                     |
  |   <-- Success -------------------  |  # 关闭成功
  |                                     |
```

### 3. 消费者流程

```
Client                              Server
  |                                     |
  |-- Subscribe (topic, sub, type) --> |  # Shared / Exclusive / Failover / KeyShared
  |                                     |
  |   <-- Success -------------------  |  # consumer_id
  |                                     |
  |-- Flow (consumer_id, permits) ---> |  # 请求消息（permits=1000）
  |                                     |
  |   <-- Message -------------------  |  # 推送消息（最多 20 条/批次）
  |   <-- Message -------------------  |  # dispatcherMaxRoundRobinBatchSize
  |   <-- Message -------------------  |
  |                                     |
  |-- Ack (consumer_id, msg_id) -----> |  # 确认消息
  |                                     |
  |   <-- AckResponse (optional) ----  |  # 仅当 request_id 存在时响应
  |                                     |
  |-- Redeliver (consumer_id) -------> |  # 可选：nack / ack_timeout / 显式 redeliver
  |                                     |
  |-- CloseConsumer (consumer_id) ---> |  # 关闭消费者
  |                                     |
  |   <-- Success -------------------  |  # 关闭成功
  |                                     |
```

**订阅模式特性**:
- **Shared / KeyShared**: 多消费者；KeyShared 按 `ordering_key` sticky 路由
- **Exclusive / Failover**: SingleActive；Failover 按 priority + name 选 active，active 关闭时 rewind
- Persistent 读路径：`redelivery-first` → `read_from(read_position)` → pending/ack 过滤
- 使用 `dispatcherMaxRoundRobinBatchSize = 20`（与 Apache Pulsar 一致）

## 实现细节

### 命令处理架构

Pulsar Lite 采用模块化的命令处理架构：

```
broker/
├── mod.rs           # 模块导出
├── service.rs       # 核心服务和连接管理
└── handler.rs       # 命令处理器
    ├── handle_connect()
    ├── handle_partition_metadata()
    ├── handle_lookup()
    ├── handle_producer()
    ├── handle_send()
    ├── handle_close_producer()
    ├── handle_subscribe()
    ├── handle_flow()
    ├── handle_ack()
    ├── handle_redeliver_unacknowledged_messages()
    └── handle_close_consumer()
```

### ServerCommand 枚举

用于统一构建服务端响应：

```rust
pub enum ServerCommand {
    Connected { server_version: String },
    PartitionMetadataResponse { request_id: u64, partitions: i32 },
    LookupResponse { request_id: u64, broker_service_url: String },
    ProducerSuccess { request_id: u64, producer_name: String, producer_id: u64 },
    SendReceipt { producer_id: u64, sequence_id: u64, ledger_id: u64, entry_id: u64, partition: i32 },
    Success { request_id: u64 },
    Message { consumer_id: u64, ledger_id: u64, entry_id: u64, partition: i32, payload: Vec<u8> },
    AckResponse { consumer_id: u64, request_id: u64 },
    Pong,
}
```

### 消息 ID 格式

Pulsar 使用 (ledger_id, entry_id, partition, batch_index) 标识消息：

```rust
pub struct MessageId {
    pub ledger: u64,      // Ledger ID（每个 Topic 独立分配）
    pub entry: u64,       // Entry ID（同一 Topic 内自增）
    pub partition: i32,   // 分区 ID（-1 表示非分区 topic，0+ 表示分区 ID）
}
```

在 Pulsar Lite 中：
- **ledger_id**: 每个 Topic 独立分配，由 Storage 层维护 `topic_ledger_ids` 映射
- **entry_id**: 同一 Topic 内自增序号（0, 1, 2, ...）
- **partition**: 从 Topic 名称自动解析（如 `topic-partition-0` → partition=0），非分区 topic 为 -1

## 使用示例

### 生产者示例

```python
import pulsar

# 连接到 Pulsar Lite（就像连接到标准 Pulsar）
client = pulsar.Client("pulsar://localhost:6650")

# 创建生产者
producer = client.create_producer("persistent://public/default/my-topic")

# 发送消息
msg_id = producer.send(b"Hello Pulsar Lite!")
print(f"Message ID: {msg_id}")

# 清理
producer.close()
client.close()
```

### 消费者示例（Shared 模式）

```python
import pulsar

client = pulsar.Client("pulsar://localhost:6650")

# 创建消费者（Shared 模式）
consumer = client.subscribe(
    "persistent://public/default/my-topic",
    "my-subscription",
    consumer_type=pulsar.ConsumerType.Shared
)

# 接收消息
while True:
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

# 创建两个消费者共享同一订阅
consumer1 = client.subscribe(
    "persistent://public/default/my-topic",
    "shared-subscription",
    consumer_type=pulsar.ConsumerType.Shared,
    consumer_name="consumer-1"
)

consumer2 = client.subscribe(
    "persistent://public/default/my-topic",
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

### 使用 Pulsar Lite SDK（嵌入式模式）

```python
from pulsar_lite import PulsarClient

# 嵌入式模式（自动启动服务器）
with PulsarClient("./demo.db") as client:
    producer = client.create_producer("my-topic")
    producer.send(b"Hello!")
    # 自动清理
```

## 协议兼容性

### 客户端兼容性

Pulsar Lite 兼容以下官方客户端：
- ✅ Python (pulsar-client >= 3.0.0)
- ✅ Java (pulsar-client-original)
- ✅ C++ (pulsar-client-cpp)
- ✅ Go (pulsar-client-go)

### 协议版本

- **支持的协议版本**: 21 (Pulsar 3.0+)
- **协议定义**: `proto/PulsarApi.proto` (官方)

## 调试

### 启用日志

```bash
# 使用脚本启动（推荐）
RUST_LOG=debug rust/pulsar-lite.sh start
```

### 查看协议交互

```bash
# 启动服务器并记录日志
RUST_LOG=debug rust/pulsar-lite.sh start 2>&1 | tee pulsar.log

# 查看命令处理
grep "Handling.*command" pulsar.log

# 查看消息存储
grep "Stored message" pulsar.log
```

## 参考资源

- [Apache Pulsar Binary Protocol](https://pulsar.apache.org/docs/developing-binary-protocol/)
- [PulsarApi.proto](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto)
- [Pulsar Client Implementation](https://github.com/apache/pulsar/tree/master/pulsar-client)

---

**Pulsar Lite - 完全兼容标准 Pulsar 协议！** 🚀
