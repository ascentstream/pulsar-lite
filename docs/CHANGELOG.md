# Changelog

All notable changes to Pulsar Lite will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-26

### Highlights

v0.1.0 之后最大的一次升级：broker 从纯内存存储演进为 **RocksDB managed-ledger 持久化存储引擎**（entrylog + cursor 持久化 + 重启恢复），补齐持久化订阅语义（KeyShared、redelivery、seek、cumulative ack），新增 **Prometheus 指标 + Grafana 看板**可观测性栈，并落地 **write-queue 异步批量写入**性能栈（批量 RocksDB 写、entrylog 批量刷盘、send-receipt 批量回执、fan-out worker、ack range 合并、jemalloc）。工作区拆分为 proto / storage / metrics 多 crate。

### Added

#### 持久化存储引擎（RocksDB managed ledger + entrylog）
- Pulsar 风格 RocksDB managed-ledger key 布局，protobuf 编码的 ledger 元数据，全局 ledger ID 分配
- entrylog 文件存储消息 payload，entry 位置索引与读取校验
- cursor 持久化：订阅时 cursor 初始化、按 position 的读取 API、重启后 cursor 位置归一化
- managed-ledger entry metadata 持久化与恢复
- RocksDB 调优：managed ledger 缓存、有界点读替代全 ledger 扫描、内存上限、LAC 经 ArcSwap 发布
- 存储层拆为 workspace crates：pulsar-lite-storage（core / metadata / resources / managed-ledger / managed-ledger-rocksdb）

#### 持久化订阅语义
- broker 重启后恢复订阅：从 read position 分发、ack 状态与 redelivery 恢复
- KeyShared 订阅：sticky key hash 路由、hash blocking、redelivery 期间保持 key ownership
- RedeliveryController（Shared/KeyShared）：redeliver unacknowledged 命令、redelivery 队列在 ack 后剪枝
- seek：按 message id（cursor 重置 + dispatcher 状态清理）、按 publish time（顺序扫描 + RocksDB 二分）
- Failover 持久化恢复：active consumer 确定性选择、single-active rewind
- cumulative ack、reader 起始/最后 message id、unsubscribe cursor 重置修复与测试覆盖

#### 可观测性
- 新增 pulsar-lite-metrics crate：storage 侧 Prometheus 指标家族
- broker 各层接线 Prometheus 指标，`GET /metrics` 暴露
- Grafana dashboard + docker compose 栈（Prometheus/Grafana，宿主 7070 端口）；perf broker 专属可抓取 metrics 端口，支持 perf 运行实时观测
- batch 感知的消息计数（修正 batched-send 计数与聚合锯齿）；storage 锁忙时 gauge 保持上次值

#### 性能（write-queue 栈）
- managed-ledger 单写者 append 队列；append 批量合并为 RocksDB 批量写；等待 IO 期间释放 topic/storage 锁
- entrylog 专用 writer 线程批量刷盘；异步 write-queue + send pipeline + entrylog batch flush
- 持久化 enqueue 热路径去锁；dispatch 与 send-receipt 解耦；push completions；持久化 send receipt 批量 flush
- 非 persistent 发布：有序 batched fan-out worker
- ack 合并成 range；TCP_NODELAY；jemalloc 分配器（缓解 glibc arena RSS 滞留）
- 背压：pending publish 字节上限 + 连接写状态配置；unacked 闸门限制 shared dispatcher 内存；permit 计数修复；flow permits 在 subscription 锁下同步应用
- dispatcher 按剩余 permit 批量 drain；write-queue 拆分独立 crate 并增加 backpressure

#### perf 测试套件
- persistent stress / e2e matrix 脚本；pulsar-perf 读结果解析；broker restart 保留存储参数
- 外部 broker backend（直连 standalone，无生命周期管理）+ systemd-run/taskset 本地资源限制；10GB solo-consumer backlog drain 场景

### Changed
- 协议层拆出 pulsar-lite-proto crate，broker 迁移并移除主 crate 中重复的 protocol/storage 模块
- persistent topic / subscription runtime 提取重构；seek helpers 移入 storage core
- CI/CD：工作流合并为统一 CD 流水线，构建提速 40-50%；SHA256SUMS 不随 dist 上传 PyPI
- 依赖升级：tokio 1.52、rocksdb 0.24、clap 4.6、log 0.4.33、bytes 1.12、uuid 1.23、serde_json 1.0.150 及 GitHub Actions v6/v7

### Fixed
- LAC 读可见性：write-queue 批量写入路由进共享 ledger 缓存
- 共享订阅重启后 redelivery 恢复；shared redelivery 队列在 ack 后剪枝
- persistent 订阅 unsubscribe / seek by message id / reader start ids / last message id
- 发版构建修复：CD 构建 broker 补上 `--features rocksdb-storage`（此前 wheel 内 broker 会静默退化为内存存储）；Linux broker 改在 manylinux_2_28 容器内构建并静态链接 libstdc++/libgcc（rocksdb 引入的 glibc 2.38 / GLIBCXX 依赖使 v0.1.0 的 manylinux_2_17 标签不再成立），wheel 标签为 manylinux_2_28_x86_64
- 跨平台编译：prometheus process collector 与 jemalloc 仅在 Linux 启用（前者是 linux-only API，后者无法在 windows-msvc 构建），macOS/Windows wheel 构建首次打通

### 早期条目（2026-03，首次随本版发布）

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

[Unreleased]: https://github.com/ascentstream/pulsar-lite/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ascentstream/pulsar-lite/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/ascentstream/pulsar-lite/releases/tag/v0.1.0
