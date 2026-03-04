# Pulsar Lite 项目概览

## 最新进展

**✅ 分区 Topic 支持完成！**

Pulsar Lite 已完成分区 Topic 的完整支持：

### 分区 Topic 支持（2026-03-04）
- **PartitionedTopic** - 分区 Topic 管理
  - 自动创建分区 Topic 实例
  - Round-robin 消息路由到各分区
  - 分区元数据查询支持

- **MessageId 增强**
  - 添加 `partition` 字段，支持分区消息 ID
  - 从 Topic 名称自动解析分区 ID（如 `topic-partition-0` → partition=0）
  - 非分区 Topic 使用 partition=-1

- **独立 ledger_id**
  - 每个 Topic 拥有独立的 ledger_id
  - Storage 层维护 `topic_ledger_ids` 映射
  - 自动为新 Topic 分配唯一的 ledger_id

### Dispatcher 架构（2026-03-02）
- **Dispatcher Trait** - 统一的消息分发器接口
  - 使用 `impl Future` 明确 `Send` bound
  - 零编译警告，生产级代码质量
  - 统一接口定义：`get_type()`, `add_consumer()`, `remove_consumer()`, `dispatch_messages()`

- **DispatcherEnum** - 零开销抽象
  - 使用 enum 代替 `Box<dyn Dispatcher>`
  - 静态分发，无 vtable 查找
  - 编译时类型安全

- **Subscription 管理** - 完全对齐 Apache Pulsar
  - Subscription 持有 DispatcherEnum
  - 懒加载创建 Dispatcher（首次添加消费者）
  - 自动根据订阅类型选择正确的 Dispatcher
  - 所有消费者管理委托给 Dispatcher

### 订阅模式支持
完整支持 Apache Pulsar 的三种主要订阅模式：
- **Shared** - 共享订阅（round-robin 消息分配）
- **Failover** - 主备切换（主消费者优先，备用待命）
- **Exclusive** - 独占访问（单消费者保证）

**所有 31 个单元测试通过，零编译警告！**

## 项目定位

Pulsar Lite 是一个嵌入式轻量级消息队列，借鉴 [Milvus Lite](https://github.com/milvus-io/milvus-lite) 的设计理念：

- **零部署**: 本地文件即可运行，无需独立集群
- **协议兼容**: 100% 兼容 Apache Pulsar 二进制协议
- **API 一致**: 与 Pulsar Standalone/Distributed 使用相同 API
- **无缝切换**: 开发环境使用本地文件，生产环境连接 Pulsar 集群

## 核心架构

### 整体架构

```
┌─────────────────────────────────────────┐
│          用户应用代码                     │
│                                         │
│  方式1: import pulsar (官方客户端)       │
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
│  ├── broker/service.rs  (连接管理)       │
│  ├── broker/handler.rs  (命令处理)       │
│  ├── protocol/codec.rs  (协议编解码)      │
│  └── storage/           (存储层)         │
└─────────────┬───────────────────────────┘
              │
              ▼
         本地文件 (./pulsar-lite.db)
```

### 模块设计

**Rust Broker** (`rust/src/`):
- `main.rs` - 服务入口，TCP 服务器监听 6650 端口
- `broker/service/` - 核心服务和连接生命周期管理
  - `server_cnx.rs` - 连接处理（Apache Pulsar ServerCnx 风格）
  - `producer.rs` - Producer 实体（Apache Pulsar 风格）
  - `consumer.rs` - Consumer 实体（Apache Pulsar 风格）
  - `topic/` - Topic 管理
    - `topic.rs` - Topic 实体和订阅管理
    - `subscription.rs` - Subscription 持有 DispatcherEnum
- `broker/handler/` - 协议命令处理器
  - `producer_handler.rs` - 生产者命令
  - `consumer_handler.rs` - 消费者命令（通过 Subscription 调用 Dispatcher）
- `broker/dispatcher/` - 消息分发器（Apache Pulsar 风格）
  - `dispatcher_trait.rs` - Dispatcher trait 统一接口
  - `shared.rs` - Shared 订阅分发（round-robin）
  - `failover.rs` - Failover 订阅分发（主备切换）
  - `exclusive.rs` - Exclusive 订阅分发（单消费者）
- `broker/stats/` - 统计和监控
  - `metrics.rs` - Broker 性能指标收集
- `protocol/` - 协议实现
  - `codec.rs` - 帧编解码器（状态机实现）
  - `command.rs` - 命令定义和序列化
- `storage/mod.rs` - 消息存储抽象层
- `error.rs` - 自定义错误类型
- `traits.rs` - 核心接口抽象

**Python SDK** (`python/src/pulsar_lite/`):
- `client.py` - 智能客户端，自动检测嵌入式/远程模式
- `process_manager.py` - 单例进程管理器，带引用计数
- `binary_finder.py` - 跨平台二进制文件定位器

### 双模式设计

**嵌入式模式** (Milvus Lite 风格):
```python
from pulsar_lite import PulsarClient

# 指定本地文件，自动启动服务器
client = PulsarClient("./my_queue.db")
producer = client.create_producer("my-topic")
producer.send(b"Hello")
```

**远程模式** (标准 Pulsar):
```python
import pulsar

# 连接到独立服务器
client = pulsar.Client("pulsar://localhost:6650")
producer = client.create_producer("my-topic")
producer.send(b"Hello")
```

两种模式使用相同的 API，仅在初始化时指定不同的 URI。

### Dispatcher 架构（Apache Pulsar 风格）

```
┌─────────────────────────────────────────────┐
│              handle_flow()                   │
│         (Consumer Flow 命令处理)              │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│          Subscription (订阅管理)              │
│  • 持有 DispatcherEnum (懒加载)               │
│  • 根据 sub_type 创建对应 Dispatcher          │
│  • 委托消费者管理给 Dispatcher                │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│        DispatcherEnum (零开销抽象)            │
│  ┌─────────────────────────────────────┐    │
│  │ enum DispatcherEnum {               │    │
│  │   Exclusive(ExclusiveDispatcher),   │    │
│  │   Shared(SharedDispatcher),         │    │
│  │   Failover(FailoverDispatcher),     │    │
│  │ }                                   │    │
│  └─────────────────────────────────────┘    │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│      Dispatcher Trait 实现 (实例方法)         │
│  • get_type() -> SubscriptionType           │
│  • add_consumer(Arc<Consumer>)              │
│  • remove_consumer(id) -> Arc<Consumer>     │
│  • dispatch_messages(...) -> Future         │
└──────────────────┬──────────────────────────┘
                   │
      ┌────────────┼────────────┐
      │            │            │
      ▼            ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│Exclusive │ │  Shared  │ │ Failover │
│Dispatcher│ │Dispatcher│ │Dispatcher│
└──────────┘ └──────────┘ └──────────┘
     │            │            │
     │            │            │
  单消费者     多消费者      主备切换
  Option<>   HashMap<>    Vec<>
```

**架构优势**：
- ✅ 零运行时开销（静态分发，无 vtable 查找）
- ✅ 编译时类型安全
- ✅ 完全对齐 Apache Pulsar 设计
- ✅ 易于扩展（添加新订阅类型）

### 协议实现

Pulsar Lite 实现了 Apache Pulsar 的二进制协议：

**帧格式**:
```
[4 字节 - 总大小]
[4 字节 - 命令大小]
[命令大小字节 - Protobuf BaseCommand]
[可选: 4字节元数据大小 + Protobuf MessageMetadata]
[可选: 消息载荷]
```

**已实现的命令**:
- ✅ Connect / Connected - 客户端握手
- ✅ PartitionMetadata / Response - Topic 分区查询
- ✅ Lookup / Response - Topic 位置查询
- ✅ Producer / ProducerSuccess - 创建生产者
- ✅ Send / SendReceipt - 发送消息
- ✅ CloseProducer / Success - 关闭生产者
- ✅ Ping / Pong - 心跳检测
- ✅ Subscribe / Success - 创建消费者（Shared 模式）
- ✅ Flow - 请求消息流（permit-based 流控）
- ✅ Message - 推送消息
- ✅ Ack / AckResponse - 消息确认（支持可选 request_id）
- ✅ CloseConsumer / Success - 关闭消费者

**待实现的命令**:
- ⏳ Exclusive 订阅模式 - 独占订阅
- ⏳ Failover 订阅模式 - 故障转移订阅
- ⏳ Key_Shared 订阅模式 - 按键共享订阅

详见 `docs/PULSAR_BINARY_PROTOCOL.md`

### 存储设计

当前使用内存存储 (MVP)，计划集成 RocksDB：

```
RocksDB 实例
├── CF_meta (元数据列族)
│   └── topic:<name> → Topic 元数据
│
├── CF_data (数据列族)
│   └── <topic>:<ledger>:<entry> → 消息内容
│
└── CF_cursor (游标列族)
    └── <topic>:<subscription> → 消费进度
```

消息 ID 格式: `(ledger_id, entry_id)` - 遵循 Pulsar 约定

## 技术栈

### Rust Broker
- **tokio** - 异步运行时
- **prost** - Protobuf 编解码
- **tokio-util** - 帧编解码器工具
- **rocksdb** - 嵌入式存储（计划中）
- **clap** - 命令行参数解析

### Python SDK
- **pulsar-client** - 官方 Pulsar Python SDK (>=3.0.0)
- **setuptools** - 包管理

## 项目进度

### ✅ 阶段一：核心功能（已完成）
- [x] 实现 Pulsar 二进制协议编解码
- [x] 构建模块化 Broker 架构
- [x] 集成官方 Pulsar 客户端
- [x] 实现生产者完整功能
- [x] Python SDK (Milvus Lite 风格)
- [x] 代码重构和优化

### ✅ 阶段二：消费者功能（已完成）
- [x] 实现消费者订阅（Subscribe 命令）
- [x] 实现消息推送（Flow 控制，permit-based 流控）
- [x] 实现消息确认（Ack 命令，支持可选 request_id）
- [x] 支持 Shared 订阅模式（与 Apache Pulsar 完全一致）
- [x] CloseConsumer 命令（优雅关闭）
- [x] Ping/Pong 心跳检测
- [x] 消息分配追踪（避免重复消费）
- [x] Round-robin 批处理（dispatcherMaxRoundRobinBatchSize = 20）
- [x] 消费者测试

### ⏳ 阶段三：完善订阅模式（进行中）
- [ ] Exclusive 订阅模式
- [ ] Failover 订阅模式
- [ ] Key_Shared 订阅模式
- [ ] 多分区支持

### 📋 阶段四：完善与发布
- [ ] 持久化存储（RocksDB）
- [ ] 性能优化
- [ ] 压力测试
- [ ] 文档完善
- [ ] PyPI 发布

## 使用场景

### 适合
- ✅ AI 应用原型开发
- ✅ 单元测试和集成测试
- ✅ Jupyter Notebook / Google Colab
- ✅ 边缘设备消息处理
- ✅ 本地开发环境

### 不适合
- ❌ 大规模生产环境（请使用 [Apache Pulsar](https://pulsar.apache.org/)）
- ❌ 需要多副本和高可用
- ❌ 跨数据中心复制

## 与 Apache Pulsar 对比

| 特性 | Pulsar Lite | Apache Pulsar |
|------|-------------|---------------|
| 部署方式 | 单机嵌入式 | 分布式集群 |
| 协议支持 | 标准 Pulsar 协议 | 完整 Pulsar 协议 |
| 客户端兼容 | 官方客户端 | 官方客户端 |
| 持久化 | 本地文件 | BookKeeper |
| 消息顺序 | 单分区 | 多分区 |
| 高可用 | 无 | 多副本 |
| 适用场景 | 开发/测试/边缘 | 生产环境 |
| 运维成本 | 零 | 需要专业团队 |

## 版本历史

| 版本 | 协议 | 客户端 | 代码量 | 状态 |
|------|------|--------|--------|------|
| v0.1 (旧) | 自定义 gRPC | 自定义 Python | ~1000 行 | 已废弃 |
| v0.2 (新) | 标准 Pulsar 二进制 | 官方 Pulsar 客户端 | ~600 行 | 当前版本 |

新版本优势：
- 原生性能（无需协议转换）
- 完全兼容所有 Pulsar 工具
- 代码更简洁易维护

## 参考资源

- **项目文档**:
  - `../README.md` - 项目介绍和快速开始
  - `CONTRIBUTING.md` - 贡献指南
  - `../CLAUDE.md` - 开发指南
  - `PULSAR_BINARY_PROTOCOL.md` - 协议实现细节

- **外部资源**:
  - [Apache Pulsar Binary Protocol](https://pulsar.apache.org/docs/developing-binary-protocol/)
  - [PulsarApi.proto](https://github.com/apache/pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto)
  - [Milvus Lite](https://github.com/milvus-io/milvus-lite) - 设计灵感来源

## 已知限制

1. **订阅模式**: 目前仅支持 Shared 订阅模式，其他模式（Exclusive、Failover、Key_Shared）开发中
2. **持久化**: 当前使用内存存储，重启后消息丢失（将集成 RocksDB）
3. **单实例**: 不支持集群模式
4. **分区**: 不支持多分区 Topic

## 贡献

欢迎贡献代码！详见 `CONTRIBUTING.md`

特别欢迎：
- 其他订阅模式实现（Exclusive、Failover、Key_Shared）
- 性能优化
- 测试用例
- 文档改进

## 许可证

Apache License 2.0

---

**Pulsar Lite - 轻量级嵌入式消息队列，完全兼容 Pulsar 生态！**
