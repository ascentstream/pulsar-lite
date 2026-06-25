# Changelog

All notable changes to Pulsar Lite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added - 2026-03-04

#### 分区 Topic 支持
- **PartitionedTopic 完整实现** - 支持多分区 Topic
  - Topic 名称自动解析分区 ID（如 `topic-partition-0` → partition=0）
  - 非分区 Topic 使用 partition=-1
  - 消息通过 round-robin 路由到各分区
  - 每个 Topic 独立的 ledger_id 分配

- **MessageId 增强**
  - 添加 `partition` 字段到 `MessageId` 结构体
  - `ServerCommand::SendReceipt` 包含 partition 信息
  - `ServerCommand::Message` 包含 partition 信息
  - 完整的 partition 字段编解码支持

- **Storage 层优化**
  - 新增 `topic_ledger_ids: HashMap<String, u64>` 维护每个 Topic 的 ledger_id
  - 新增 `next_ledger_id: u64` 全局计数器分配新 ledger_id
  - 每个 Topic 首次写入时自动分配独立的 ledger_id
  - 移除全局共享的 ledger_id

#### 代码改进
- 从 `Topic::new` 中移除 ledger_id 参数（改由 Storage 管理）
- 优化日志级别（debug/info 合理分配）
- 清理调试日志

#### 技术细节
```rust
// MessageId 结构体
pub struct MessageId {
    pub ledger: u64,      // 每个 Topic 独立分配
    pub entry: u64,       // 同一 Topic 内自增
    pub partition: i32,   // -1=非分区, 0+=分区ID
}

// Storage 层管理
pub struct Storage {
    topics: HashMap<String, Vec<(MessageId, Vec<u8>)>>,
    topic_ledger_ids: HashMap<String, u64>,  // 每个 Topic 的 ledger_id
    next_ledger_id: u64,                      // 全局计数器
}

// Topic 名称解析
"persistent://public/default/topic-partition-0" → partition=0
"persistent://public/default/topic-partition-1" → partition=1
"persistent://public/default/topic" → partition=-1
```

### Added - 2026-03-02

#### Dispatcher 架构优化
- **Dispatcher Trait 统一接口** - 完整实现消息分发器抽象层
  - 新增 `broker/dispatcher/dispatcher_trait.rs`
  - 使用 `impl Future` 明确 `Send` bound，避免 `async fn in trait` 警告
  - 统一接口：`get_type()`, `add_consumer()`, `remove_consumer()`, `dispatch_messages()` 等
  - 所有 Dispatcher 实现（Exclusive/Shared/Failover）遵循统一接口

- **Subscription 持有 DispatcherEnum** - 对齐 Apache Pulsar 架构设计
  - 使用 `enum DispatcherEnum` 代替 `Box<dyn Dispatcher>` 实现零开销抽象
  - 懒加载创建 Dispatcher（首次添加消费者时）
  - 根据 `sub_type` 自动创建对应的 Dispatcher
  - 所有消费者管理完全委托给 Dispatcher

- **代码质量提升**
  - 消除所有编译警告（未使用变量、async fn in trait）
  - 添加 `get_active_consumers()` 方法支持 Failover 主消费者查询
  - 优化错误类型转换，使用 `.map_err()` 统一错误处理
  - 代码质量达到生产级别标准

#### 架构改进详情
```rust
// DispatcherEnum 设计（零开销抽象）
enum DispatcherEnum {
    Exclusive(ExclusiveDispatcher),
    Shared(SharedDispatcher),
    Failover(FailoverDispatcher),
}

// Subscription 管理 Dispatcher
pub struct Subscription {
    pub name: String,
    pub topic: String,
    pub sub_type: SubscriptionType,
    dispatcher: Option<DispatcherEnum>,  // 懒加载
}

// 简化的调用链
handle_flow()
  → subscription.dispatch_messages()
    → dispatcher.dispatch_messages()  // 自动选择正确的 Dispatcher
```

#### 技术优势
- ✅ 完全对齐 Apache Pulsar 设计模式
- ✅ 零运行时开销（静态分发，无 vtable 查找）
- ✅ 编译时类型安全保证
- ✅ 职责分明，易于维护和扩展
- ✅ 所有 31 个单元测试通过
- ✅ 零编译警告

### Changed - 2026-03-02
- 所有 Dispatcher 从静态方法改为实例方法
- Dispatcher 内部管理消费者（Exclusive: `Option<Arc<Consumer>>`, Shared: `HashMap<u64, Arc<Consumer>>`, Failover: `Vec<Arc<Consumer>>`）
- `handle_flow` 调用简化，通过 Subscription 自动选择 Dispatcher
- `ServerCnx` 泛型参数添加 `Send` bound 支持
- `sub_type` 字段改为 public，支持外部读取

### Added - 2026-03-01

#### 订阅模式完善
- **Failover 订阅模式** - 完整实现主备切换机制
  - 新增 `broker/dispatcher/failover.rs`
  - 主消费者接收所有消息
  - 备用消费者待命，主消费者失败时自动接管
  - 完整测试用例验证（tests/test_consumer.py:test_failover_subscription）

- **Exclusive 订阅模式** - 完整实现独占访问控制
  - 新增 `broker/dispatcher/exclusive.rs`
  - 新增 `SubscriptionType` 枚举（Exclusive, Shared, Failover, KeyShared）
  - 独占访问控制：拒绝第二个消费者订阅
  - 消费者关闭后重连支持
  - 完整测试用例验证（tests/test_consumer.py:test_exclusive_subscription, test_exclusive_after_close）

#### Broker Metrics 收集
- 新增 `broker/stats/metrics.rs`
  - 原子计数器跟踪连接、生产者、消费者数量
  - 消息发布/传递统计
  - 字节吞吐量统计
  - 性能指标计算（消息速率）
  - 错误计数

#### 代码优化重构
- **模块拆分**
  - 从 `protocol/codec.rs` 拆分 `protocol/command.rs`
  - 命令定义独立模块，提高代码可维护性

- **Trait 和接口抽象**
  - 新增 `traits.rs` 定义核心接口
    - `CommandHandler` trait - 命令处理器接口
    - `Dispatcher` trait - 消息分发器接口
    - `StorageBackend` trait - 存储后端接口
  - 添加 `async-trait` 依赖支持异步 trait

- **错误处理改进**
  - 新增 `error.rs` 自定义错误类型
  - 定义 `Error` 枚举，包含具体错误场景
  - 实现 `From` trait 支持错误转换
  - 定义 `Result<T>` 类型别名

#### 测试用例
- 新增 Failover 订阅模式测试
- 新增 Exclusive 订阅模式测试（包括独占访问控制验证）
- 新增消费者重连测试
- 测试覆盖率：Shared, Failover, Exclusive 三种订阅模式

### Changed
- 扩展 `ConsumerInfo` 结构体，新增 `sub_type` 字段
- 更新 `handle_subscribe` 函数，添加订阅类型检查和 Exclusive 访问控制
- 优化项目文档（README.md, PROJECT_OVERVIEW.md）

### Technical Details

#### Exclusive 订阅实现细节
```rust
// SubscriptionType 枚举定义
pub enum SubscriptionType {
    Exclusive = 0,
    Shared = 1,
    Failover = 2,
    KeyShared = 3,
}

// Exclusive 访问控制逻辑
if sub_type == SubscriptionType::Exclusive {
    let has_active_consumer = consumers.values().any(|c| {
        c.topic == subscribe_cmd.topic &&
        c.subscription == subscribe_cmd.subscription &&
        c.sub_type == SubscriptionType::Exclusive
    });

    if has_active_consumer {
        // 拒绝创建新消费者，返回 Error 响应
        return Err("Exclusive subscription already has active consumer".into());
    }
}
```

#### 测试结果
- ✅ Shared 订阅模式：100% 通过（5/5 消息，10/10 消息多消费者）
- ✅ Failover 订阅模式：100% 通过（主消费者 10/10，备用 0/10）
- ✅ Exclusive 订阅拒绝：成功拒绝第二个消费者
- ✅ Exclusive 重连：100% 通过（消费者关闭后新消费者可订阅）

## [0.1.0] - 2026-02-28

### Added
- Pulsar 二进制协议支持
- 模块化 Broker 架构
- 生产者功能
- Python SDK（嵌入式设计，自动管理进程）
- 消费者订阅（Subscribe 命令）
- 消息推送（Flow 控制，permit-based 流控）
- 消息确认（Ack 命令）
- Shared 订阅模式
- CloseConsumer 命令
- Ping/Pong 心跳检测
- 消息分配追踪（避免重复消费）
- Round-robin 批处理（dispatcherMaxRoundRobinBatchSize = 20）

[Unreleased]: https://github.com/ascentstream/pulsar-lite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ascentstream/pulsar-lite/releases/tag/v0.1.0
