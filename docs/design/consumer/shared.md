# Shared 订阅模式实现详解

本文档详细描述了 Shared 订阅模式从收到客户端命令到最终消息收发的完整流程。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Client (Python/Java)                           │
│                     pulsar.Client.subscribe(sub_type=Shared)                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      │ TCP Connection + Pulsar Protocol
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              BrokerService                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                            Topic                                      │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                      Subscription                               │  │  │
│  │  │  ┌────────────────────────────────────────────────────────────┐ │  │  │
│  │  │  │                   SharedDispatcher                         │ │  │  │
│  │  │  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │ │  │  │
│  │  │  │  │  Consumer1  │ │  Consumer2  │ │  Consumer3  │           │ │  │  │
│  │  │  │  │  (permits)  │ │  (permits)  │ │  (permits)  │           │ │  │  │
│  │  │  │  │   [queue]   │ │   [queue]   │ │   [queue]   │           │ │  │  │
│  │  │  │  └─────────────┘ └─────────────┘ └─────────────┘           │ │  │  │
│  │  │  └────────────────────────────────────────────────────────────┘ │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                      │                                      │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                            Storage                                    │  │
│  │   topics[topic_name] -> Vec<(MessageId, payload)>                     │  │
│  │   cursors[topic:subscription] -> entry_id                             │  │
│  │   message_assignments[topic:subscription:entry] -> consumer_id        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. SharedDispatcher

**文件位置：** `rust/src/broker/dispatcher/shared.rs`

负责 Shared 模式的消息分发，实现 Round-Robin 算法。

```rust
pub struct SharedDispatcher {
    /// 所有消费者
    consumers: HashMap<u64, Arc<Consumer>>,

    /// Round-Robin 索引（原子操作）
    round_robin_index: AtomicUsize,

    /// 所有消费者的可用 permits 总数
    total_available_permits: AtomicU32,

    /// 防止重入分发
    dispatch_in_progress: AtomicBool,
}
```

**关键特性：**
- 遵循 Apache Pulsar 的 `DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE = 20`
- 使用原子操作保证线程安全
- Round-Robin 算法确保消息均匀分配

### 2. Consumer

**文件位置：** `rust/src/broker/service/consumer.rs`

代表一个消费者连接。

```rust
pub struct Consumer {
    pub consumer_id: u64,
    pub consumer_name: String,
    pub subscription: Arc<RwLock<Subscription>>,
    pub connection_id: String,
    stats: Arc<RwLock<ConsumerStats>>,
    pending_messages: Arc<RwLock<VecDeque<PendingMessage>>>,
}
```

**关键特性：**
- 每个 Consumer 维护自己的 permits（流量控制）
- 持有待发送消息队列 `pending_messages`
- 直接持有 Subscription 引用（Apache Pulsar 风格）

### 3. Subscription

**文件位置：** `rust/src/broker/service/topic/subscription.rs`

订阅管理，持有 Dispatcher。

```rust
pub struct Subscription {
    pub name: String,
    pub topic: String,
    pub sub_type: SubscriptionType,
    dispatcher: Option<DispatcherEnum>,  // 懒加载
}
```

### 4. Storage

**文件位置：** `rust/src/storage/mod.rs`

存储层，管理消息和分配状态。

```rust
pub struct Storage {
    topics: HashMap<String, Vec<(MessageId, Vec<u8>)>>,
    cursors: HashMap<String, u64>,  // topic:subscription -> entry_id
    message_assignments: HashMap<String, u64>,  // topic:subscription:entry -> consumer_id
    ledger_id: u64,
}
```

---

## 完整流程

### Phase 1: Subscribe 订阅流程

```
Client                     Handler                    Subscription           SharedDispatcher
   │                          │                           │                       │
   │──── Subscribe ──────────>│                           │                       │
   │     topic="my-topic"     │                           │                       │
   │     subscription="sub1"  │                           │                       │
   │     sub_type=Shared      │                           │                       │
   │                          │                           │                       │
   │                          │── get_or_create_topic ───>│                       │
   │                          │                           │                       │
   │                          │── get_or_create_subscription ──────>│             │
   │                          │                           │                       │
   │                          │                           │── reuse_or_create_dispatcher ─>│
   │                          │                           │                       │
   │                          │                           │                       │── new SharedDispatcher()
   │                          │                           │                       │
   │                          │── new Consumer(subscription) ────────────────────────────>│
   │                          │                           │                       │
   │                          │── add_consumer(consumer) ────────────>│             │
   │                          │                           │                       │── consumers.insert(id, consumer)
   │                          │                           │                       │
   │<─── Success ─────────────│                           │                       │
   │     request_id=X         │                           │                       │
```

**代码入口：** `rust/src/broker/handler/consumer_handler.rs`

```rust
// handle_subscribe() 函数

// 1. 获取或创建 Topic
let topic = manager.get_or_create_topic(&subscribe_cmd.topic).await;

// 2. 获取或创建 Subscription（会创建 SharedDispatcher）
let subscription_arc = topic_guard
    .get_or_create_subscription(&subscribe_cmd.subscription, sub_type)
    .await?;

// 3. 创建 Consumer（持有 Subscription 引用）
let consumer = Arc::new(Consumer::new(
    consumer_id,
    consumer_name,
    subscription_arc.clone(),
    connection_id,
));

// 4. 将 Consumer 添加到 Subscription（进而添加到 SharedDispatcher）
sub_guard.add_consumer(consumer.clone())?;
```

---

### Phase 2: Flow 命令触发消息分发

```
Client                     Handler                    Consumer              Subscription         SharedDispatcher
   │                          │                           │                       │                     │
   │──── Flow ───────────────>│                           │                       │                     │
   │     consumer_id=1        │                           │                       │                     │
   │     permits=100          │                           │                       │                     │
   │                          │                           │                       │                     │
   │                          │── flow_message(100) ─────>│                       │                     │
   │                          │                           │── add_permits(100)    │                     │
   │                          │                           │                       │                     │
   │                          │── dispatch_messages() ────────────────────────────>│                     │
   │                          │                           │                       │── dispatch_if_permits_available() ─>│
   │                          │                           │                       │                     │
   │                          │                           │                       │                     │── dispatch_messages_batch()
   │                          │                           │                       │                     │    │
   │                          │                           │                       │                     │    ├── for _ in 0..max_batch:
   │                          │                           │                       │                     │    │     │
   │                          │                           │                       │                     │    │     ├── get_next_available_consumer() [Round-Robin]
   │                          │                           │                       │                     │    │     │
   │                          │                           │                       │                     │    │     ├── consumer.use_permit()
   │                          │                           │                       │                     │    │     │
   │                          │                           │                       │                     │    │     ├── storage.get_next_unassigned_message()
   │                          │                           │                       │                     │    │     │
   │                          │                           │<── enqueue_message() ─────────────────────────────┘     │
   │                          │                           │                       │                     │
   │<─── Message ────────────────────────────────────────│                       │                     │
   │     consumer_id=1        │                           │                       │                     │
   │     ledger_id=0          │                           │                       │                     │
   │     entry_id=0           │                           │                       │                     │
   │     payload=...          │                           │                       │                     │
```

**代码入口：** `rust/src/broker/handler/consumer_handler.rs`

```rust
// handle_flow() 函数

// 1. 增加 Consumer 的 permits
consumer.flow_message(flow_cmd.message_permits).await;

// 2. 通过 Subscription 触发消息分发
let subscription = consumer.get_subscription();
let sub_guard = subscription.read().await;
sub_guard.dispatch_messages(framed, consumer_id, storage).await?;
```

---

### Phase 3: Round-Robin 消息分发算法

**代码位置：** `rust/src/broker/dispatcher/shared.rs`

`SharedDispatcher::dispatch_messages_batch()` 核心逻辑：

```rust
async fn dispatch_messages_batch(
    &self,
    storage: SharedStorage,
    topic: String,
    subscription: String,
    broker_service: SharedBrokerService,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // 1. 检查是否有可用的 permits
    let total_permits = self.total_available_permits.load(Ordering::Relaxed);
    if total_permits == 0 {
        return Ok(());
    }

    // 2. 计算批次大小（最多 20 条消息）
    let max_batch = std::cmp::min(total_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);

    // 3. 循环分发
    for _ in 0..max_batch {
        // 3.1 Round-Robin 选择下一个有 permits 的 Consumer
        let consumer = match self.get_next_available_consumer().await {
            Some(c) => c,
            None => break,
        };

        // 3.2 消耗一个 permit
        if !consumer.use_permit().await {
            break;
        }
        self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

        // 3.3 从 Storage 获取下一条未分配的消息
        let message_opt = {
            let mut guard = storage.lock().await;
            guard.get_next_unassigned_message(&topic, &subscription, consumer_id)?
        };

        if let Some((message_id, payload)) = message_opt {
            // 3.4 将消息入队到 Consumer 的 pending_messages
            consumer.enqueue_message(message_id.clone(), payload.clone()).await;

            // 3.5 记录统计
            consumer.record_message_dispatched(payload.len()).await;
        } else {
            // 没有更多消息，恢复 permit
            consumer.add_permits(1).await;
            self.total_available_permits.fetch_add(1, Ordering::Relaxed);
            break;
        }
    }

    Ok(())
}
```

**Round-Robin 选择算法：**

```rust
async fn get_next_available_consumer(&self) -> Option<Arc<Consumer>> {
    if self.consumers.is_empty() {
        return None;
    }

    let consumers: Vec<_> = self.consumers.values().cloned().collect();
    let consumer_count = consumers.len();

    // 尝试每个 Consumer（Round-Robin）
    for _ in 0..consumer_count {
        // 原子递增索引并取模
        let index = self.round_robin_index
            .fetch_add(1, Ordering::Relaxed) % consumer_count;

        let consumer = consumers[index].clone();

        // 检查是否有 permits
        if consumer.get_available_permits().await > 0 {
            return Some(consumer);
        }
    }

    None  // 所有 Consumer 都没有 permits
}
```

---

### Phase 4: 消息确认流程

```
Client                     Handler                    Consumer              Storage
   │                          │                           │                     │
   │──── Ack ────────────────>│                           │                     │
   │     consumer_id=1        │                           │                     │
   │     ledger_id=0          │                           │                     │
   │     entry_id=0           │                           │                     │
   │                          │                           │                     │
   │                          │── ack_message() ─────────>│                     │
   │                          │                           │── stats.acked++     │
   │                          │                           │                     │
   │                          │── storage.ack_message() ───────────────────────>│
   │                          │                           │                     │
   │                          │                           │                     │── cursors[topic:sub] = entry_id
   │                          │                           │                     │── message_assignments.remove(key)
   │                          │                           │                     │
   │<─── AckResponse ─────────│                           │                     │
   │     consumer_id=1        │                           │                     │
   │     request_id=X         │                           │                     │
```

**代码入口：** `rust/src/broker/handler/consumer_handler.rs`

```rust
// handle_ack() 函数

// 1. 记录 Consumer 统计
consumer.ack_message(msg_id.clone()).await;

// 2. 更新 Storage 游标
let mut guard = storage.lock().await;
guard.ack_message(&topic_name, &sub_name, msg_id)?;

// 3. 发送 AckResponse（如果有 request_id）
if let Some(request_id) = ack_cmd.request_id {
    framed.send(ServerCommand::AckResponse { consumer_id, request_id }).await?;
}
```

---

## Storage 层详解

**文件位置：** `rust/src/storage/mod.rs`

### 消息分配机制

Storage 通过 `message_assignments` 跟踪消息分配状态，避免重复投递：

```rust
pub fn get_next_unassigned_message(
    &mut self,
    topic: &str,
    subscription: &str,
    consumer_id: u64,
) -> Result<Option<(MessageId, Vec<u8>)>> {
    let cursor_key = format!("{}:{}", topic, subscription);
    let current_entry = self.cursors.get(&cursor_key).copied().unwrap_or(u64::MAX);

    if let Some(messages) = self.topics.get(topic) {
        // 遍历所有消息
        for (message_id, data) in messages.iter() {
            // 跳过已确认的消息
            if current_entry != u64::MAX && message_id.entry <= current_entry {
                continue;
            }

            // 检查是否已分配
            let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);

            if !self.message_assignments.contains_key(&assignment_key) {
                // 标记为已分配
                self.message_assignments.insert(assignment_key, consumer_id);
                return Ok(Some((message_id.clone(), data.clone())));
            }
        }
    }

    Ok(None)
}
```

### 消息确认机制

```rust
pub fn ack_message(
    &mut self,
    topic: &str,
    subscription: &str,
    message_id: MessageId
) -> Result<()> {
    // 1. 更新游标
    let cursor_key = format!("{}:{}", topic, subscription);
    self.cursors.insert(cursor_key, message_id.entry);

    // 2. 清除分配状态
    let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
    self.message_assignments.remove(&assignment_key);

    Ok(())
}
```

---

## 流量控制（Flow Control）

### Permits 机制

1. **Client 端：** 通过 `Flow` 命令声明可接收的消息数量
2. **Consumer 端：** 维护 `available_permits` 计数器
3. **Dispatcher 端：** 维护 `total_available_permits` 总数

```rust
// Consumer 添加 permits
pub async fn add_permits(&self, permits: u32) {
    let mut stats = self.stats.write().await;
    stats.available_permits += permits;
}

// Consumer 消耗 permit
pub async fn use_permit(&self) -> bool {
    let mut stats = self.stats.write().await;
    if stats.available_permits > 0 {
        stats.available_permits -= 1;
        true
    } else {
        false
    }
}
```

---

## 消息队列（Pending Messages）

每个 Consumer 维护一个消息队列，解耦分发和发送：

```rust
pub struct PendingMessage {
    pub message_id: MessageId,
    pub payload: Vec<u8>,
}

// 入队（Dispatcher 调用）
pub async fn enqueue_message(&self, message_id: MessageId, payload: Vec<u8>) {
    let mut queue = self.pending_messages.write().await;
    queue.push_back(PendingMessage { message_id, payload });
}

// 出队（连接处理循环调用）
pub async fn dequeue_message(&self) -> Option<PendingMessage> {
    let mut queue = self.pending_messages.write().await;
    queue.pop_front()
}
```

---

## 关键设计决策

### 1. 为什么用 Round-Robin？

- **公平性：** 确保所有 Consumer 机会均等
- **简单高效：** O(1) 时间复杂度选择下一个 Consumer
- **与 Apache Pulsar 一致：** 遵循官方实现

### 2. 为什么需要 message_assignments？

- **防止重复投递：** 同一消息只能分配给一个 Consumer
- **支持 Ack 追踪：** 知道哪条消息分配给了哪个 Consumer
- **故障恢复：** Consumer 断开时可重新分配

### 3. 为什么用原子操作？

```rust
round_robin_index: AtomicUsize,
total_available_permits: AtomicU32,
dispatch_in_progress: AtomicBool,
```

- **线程安全：** 多个连接可能同时触发分发
- **无锁设计：** 避免锁竞争，提高性能
- **防止重入：** `dispatch_in_progress` 防止并发分发

### 4. 消息 ID 设计

```rust
pub struct MessageId {
    pub ledger: u64,  // 类似 BookKeeper 的 ledger ID
    pub entry: u64,   // 在 ledger 中的偏移量
}
```

- **兼容 Pulsar 协议：** 客户端可直接识别
- **单调递增：** `entry` 严格递增（0, 1, 2, ...）
- **全局唯一：** `(ledger, entry)` 组合唯一

---

## 消息推送机制（Push 模式）

### 实现状态：✅ 已实现

Pulsar Lite 采用 **Push 模式**，与 Apache Pulsar 一致。当 Producer 发送消息时，Broker 会立即将消息推送给有可用 permits 的 Consumer。

### 实现方式

**代码位置：**
- `rust/src/broker/handler/producer_handler.rs` - `handle_send()` 函数
- `rust/src/broker/service/topic/topic.rs` - `dispatch_to_subscriptions()` 方法

**流程：**

```
Producer                   Handler                    Topic                   Subscription            Consumer
    │                          │                         │                        │                      │
    │──── Send ───────────────>│                         │                        │                      │
    │                          │── publish_message() ───>│                        │                      │
    │                          │                         │── 存储消息 ───────────>│                      │
    │                          │                         │                        │                      │
    │                          │<── message_id ──────────│                        │                      │
    │                          │                         │                        │                      │
    │                          │── dispatch_to_subscriptions() ──────────────────>│                      │
    │                          │                         │                        │                      │
    │                          │                         │                        │── dispatch_if_permits_available()
    │                          │                         │                        │                      │
    │                          │                         │                        │── enqueue_message() │
    │                          │                         │                        │                      │
    │                          │                         │                        │──────── Message ───>│
    │                          │                         │                        │                      │
    │<─── SendReceipt ─────────│                         │                        │                      │
```

**关键代码：**

```rust
// producer_handler.rs - handle_send()

// 1. 发布消息到存储
let message_id = producer.publish_message(&frame.payload).await?;

// 2. Push 模式：立即触发分发
{
    let topic = producer.get_topic();
    let topic_guard = topic.read().await;
    topic_guard.dispatch_to_subscriptions(topic_manager.clone()).await;
}

// 3. 发送 SendReceipt
framed.send(ServerCommand::SendReceipt { ... }).await?;
```

### 注意事项

1. **Permits 必须可用：** 只有 Consumer 有可用 permits 时才会收到消息
2. **客户端需发送 Flow：** Pulsar 客户端在订阅后会自动发送 Flow 命令
3. **实时推送：** 消息存储后立即推送，无需等待 Consumer 轮询

---

## 与 Apache Pulsar 的差异

| 特性 | Apache Pulsar | Pulsar Lite |
|------|---------------|-------------|
| 存储后端 | BookKeeper | 内存 HashMap |
| 消息持久化 | 是 | 否（重启丢失） |
| Batch Message | 支持 | 支持（最多 20 条） |
| Message Batching | 支持 | 简化版 |
| Push 模式 | ✅ 支持 | ✅ 已实现 |
| Redelivery | 支持 | 待实现 |
| Dead Letter Queue | 支持 | 待实现 |

---

## 测试验证

参考测试文件：
- `tests/test_shared_dispatcher.py` - 端到端测试
- `tests/test_partitioned_shared.py` - 分区 + Shared 测试

---

## 后续优化

1. **消息重投递：** 未 Ack 的消息超时后重新分配
2. **Consumer 优先级：** 支持优先级调度
3. **加权 Round-Robin：** 根据 Consumer 处理能力分配
4. **消息压缩：** 减少网络传输
5. **持久化存储：** 集成 RocksDB
