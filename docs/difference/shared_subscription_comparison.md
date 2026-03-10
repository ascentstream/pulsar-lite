# Pulsar Lite Shared 消费模式 vs 原生 Pulsar 全面对比分析

> 分析日期: 2026-03-10
> 对比版本: Pulsar Lite (当前实现) vs Apache Pulsar (官方实现)

---

## 一、架构对比

### 1.1 核心组件映射

| 组件 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **Dispatcher** | `SharedDispatcher` | `PersistentDispatcherMultipleConsumers` |
| **消费者选择** | `get_next_available_consumer()` | `getNextConsumer()` (AbstractDispatcherMultipleConsumers) |
| **消息分配器** | 无（直接在 storage 中跟踪） | `SharedConsumerAssignor` |
| **重投递控制** | 无 | `MessageRedeliveryController` |
| **游标管理** | 简单 HashMap | ManagedCursor + BookKeeper |
| **连接管理** | 无状态，无超时检测 | 完整的连接生命周期管理 |
| **事务支持** | 无 | 完整事务支持 |
| **消息元数据** | 仅存储 payload | 完整的属性和元数据支持 |

---

## 二、Round-Robin 算法差异

### 2.1 Pulsar Lite 实现

```rust
// rust/src/broker/dispatcher/shared.rs:47-75
async fn get_next_available_consumer(&self) -> Option<Arc<Consumer>> {
    let consumers: Vec<_> = self.consumers.values().cloned().collect();
    let consumer_count = consumers.len();

    for _ in 0..consumer_count {
        let index = self.round_robin_index.fetch_add(1, Ordering::Relaxed) % consumer_count;
        let consumer = consumers[index].clone();

        if consumer.get_available_permits().await > 0 {
            return Some(consumer);
        }
    }
    None
}
```

**特点**：
- ✅ 简单的原子 Round-Robin 索引
- ❌ **无优先级支持**
- ✅ 支持流控 (permits)

### 2.2 原生 Pulsar 实现

```java
// AbstractDispatcherMultipleConsumers.java:115-146
public Consumer getNextConsumer() {
    // 1. 消费者按优先级排序存储 (priority 0 = 最高)
    // 2. 先检查更高优先级的消费者
    if (currentRoundRobinConsumerPriority != 0) {
        int higherPriorityConsumerIndex = getConsumerFromHigherPriority(currentRoundRobinConsumerPriority);
        if (higherPriorityConsumerIndex != -1) {
            return consumerList.get(higherPriorityConsumerIndex);
        }
    }

    // 3. 同优先级内 Round-Robin
    // 4. 无可用时才降级到低优先级
    int availableConsumerIndex = getNextConsumerFromSameOrLowerLevel(currentConsumerRoundRobinIndex);
    return consumerList.get(availableConsumerIndex);
}
```

**特点**：
- ✅ **支持消费者优先级** (priorityLevel)
- ✅ 同优先级内 Round-Robin
- ✅ 优先级阶梯式降级
- ✅ CopyOnWriteArrayList 线程安全

### 2.3 关键差异总结

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **优先级调度** | ❌ 不支持 | ✅ 完整支持 (0=最高) |
| **算法复杂度** | O(n) 简单轮询 | O(n) 带优先级 |
| **消费者排序** | 无序 HashMap | 按优先级排序的 List |
| **索引管理** | 原子 usize | volatile int + 同步块 |

---

## 三、消息分发流程差异

### 3.1 Pulsar Lite 流程

```
Flow 命令 → consumer_flow() → dispatch_messages_batch()
                                    ↓
                         循环 max(permits, 20) 次
                                    ↓
                         get_next_available_consumer()
                                    ↓
                         storage.get_next_unassigned_message()
                                    ↓
                         consumer.enqueue_message()
```

**特点**：
- 每次最多分发 20 条消息 (`DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE`)
- 消息分配状态存储在 `message_assignments` HashMap
- **无 Replay 机制**

### 3.2 原生 Pulsar 流程

```
Flow 命令 → consumerFlow() → totalAvailablePermits += N
                                  ↓
                          readMoreEntriesAsync()
                                  ↓
              cursor.asyncReadEntriesWithSkipOrWait()
                                  ↓
                      readEntriesComplete() 回调
                                  ↓
                      trySendMessagesToConsumers()
                                  ↓
              ┌────────────────────────────────────┐
              │  1. 过滤 delayed/aborted 消息      │
              │  2. 处理 chunked messages         │
              │  3. SharedConsumerAssignor 分配   │
              │  4. 更新 permits 和 redelivery    │
              └────────────────────────────────────┘
```

**特点**：
- 异步读取 + 回调驱动
- 支持 **Replay** (重投递未确认消息)
- 支持 **Delayed Delivery** (延迟消息)
- 支持 **Chunked Messages** (大消息分块)
- 精细化批量大小计算

### 3.3 关键差异

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **读取模式** | 同步从内存读取 | 异步从 BookKeeper 读取 |
| **Replay 机制** | ❌ 无 | ✅ MessageRedeliveryController |
| **延迟消息** | ❌ 无 | ✅ DelayedDeliveryTracker |
| **大消息分块** | ❌ 无 | ✅ SharedConsumerAssignor UUID 跟踪 |
| **批量计算** | 固定 20 条 | 动态计算 |

---

## 四、消息确认机制差异

### 4.1 Pulsar Lite 确认逻辑

```rust
// rust/src/storage/mod.rs:146-158
pub fn ack_message(&mut self, topic: &str, subscription: &str, message_id: MessageId) -> Result<()> {
    // 1. 更新游标到当前消息
    let cursor_key = format!("{}:{}", topic, subscription);
    self.cursors.insert(cursor_key, message_id.entry);

    // 2. 清除消息分配状态
    let assignment_key = format!("{}:{}:{}", topic, subscription, message_id.entry);
    self.message_assignments.remove(&assignment_key);
}
```

**问题**：
- ⚠️ **仅更新到当前消息**，不支持累计确认
- ⚠️ **无 pending acks 跟踪**
- ⚠️ **无 unacked messages 计数**

### 4.2 原生 Pulsar 确认逻辑

```java
// PersistentDispatcherMultipleConsumers.java
// 1. 消费者持有 pendingAcks 映射
// 2. Ack 时：
//    - 从 pendingAcks 移除
//    - 更新 cursor 的 markDeletePosition
//    - 减少 totalUnackedMessages
// 3. 支持累计确认 (cumulative) 和单独确认
// 4. Consumer 断开时，pendingAcks 消息被 replay
```

**特点**：
- ✅ **Pending Acks 跟踪** - 每个 Consumer 维护
- ✅ **Unacked 消息限制** - 防止消息积压
- ✅ **Consumer 断开重投递** - 自动 replay pending 消息
- ✅ **累积确认支持**

---

## 五、重投递机制差异

### 5.1 Pulsar Lite

**❌ 完全缺失重投递机制**

当 Consumer 断开或消息未确认时：
- 消息分配状态保留在 `message_assignments`
- 不会自动重投递
- 需要 Consumer 主动重新订阅

### 5.2 原生 Pulsar

```java
// MessageRedeliveryController.java
public class MessageRedeliveryController {
    // 待重投递消息集合
    ConcurrentBitmapSortedLongPairSet messagesToRedeliver;

    // 有序投递阻塞的 hash (Key_Shared 模式)
    ConcurrentLongLongPairHashMap hashesToBeBlocked;

    public void add(long ledgerId, long entryId, long stickyKeyHash) {
        messagesToRedeliver.add(ledgerId, entryId);
        if (!allowOutOfOrderDelivery) {
            hashesToBeBlocked.put(ledgerId, entryId, stickyKeyHash, 0);
        }
    }
}
```

**特点**：
- ✅ **自动重投递** - 未确认消息自动回到 replay 队列
- ✅ **有序投递保证** - Key_Shared 模式的 hash 阻塞
- ✅ **Consumer 移除时处理** - pendingAcks 转入 replay

```java
// Consumer 移除时的重投递
public synchronized void removeConsumer(Consumer consumer) {
    consumer.getPendingAcks().forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> {
        boolean addedToReplay = addMessageToReplay(ledgerId, entryId, stickyKeyHash);
    });
}
```

---

## 六、流控机制差异

### 6.1 Pulsar Lite

```rust
// SharedDispatcher
total_available_permits: AtomicU32,  // 全局 permits

// consumer_flow 是同步函数
fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
    // permit 更新通过异步任务执行
    let consumer = self.consumers.get(&consumer_id).cloned();
    if let Some(consumer) = consumer {
        consumer.add_permits(additional_permits);  // 内部使用异步任务
    }
    self.total_available_permits.fetch_add(additional_permits, Ordering::Relaxed);
}
```

**特点**：
- ✅ 简单的全局 permits 计数
- ⚠️ `consumer_flow` 是同步函数，但 permit 更新通过异步任务执行
- ❌ **无 per-consumer unacked 限制**
- ❌ **无 backpressure 机制**

### 6.2 原生 Pulsar

```java
// 多层流控
volatile int totalAvailablePermits = 0;       // 全局 permits
volatile int totalUnackedMessages = 0;        // 未确认消息数
int maxUnackedMessages;                       // 最大未确认限制

// 批量大小计算考虑多个因素
static int getMaxEntriesInThisBatch(
    int remainingMessages,
    int maxUnackedMessages,      // 最大未确认限制
    int unackedMessages,         // 当前未确认数
    int avgBatchSizePerMsg,      // 平均批量大小
    int availablePermits,        // 可用 permits
    int dispatcherMaxRoundRobinBatchSize) {

    int maxMessagesInThisBatch = Math.min(remainingMessages, availablePermits);
    if (maxUnackedMessages > 0) {
        int maxAdditionalUnackedMessages = Math.max(maxUnackedMessages - unackedMessages, 0);
        maxMessagesInThisBatch = Math.min(maxMessagesInThisBatch, maxAdditionalUnackedMessages);
    }
    // ...
}
```

**特点**：
- ✅ **多维度流控** - permits + unacked
- ✅ **精确流控** - 考虑批量消息
- ✅ **Dispatcher 阻塞** - unacked 超限暂停分发

---

## 七、连接管理差异

### 7.1 Pulsar Lite

**❌ 缺少完整的连接管理**

```rust
// rust/src/broker/service/consumer.rs
pub struct Consumer {
    consumer_id: u64,
    consumer_name: String,
    available_permits: AtomicU32,
    // 缺少:
    // - connected: AtomicBool
    // - last_active: Instant
    // - heartbeat tracking
}
```

**问题**：
- ❌ **无连接状态跟踪** - 不知道 Consumer 是否存活
- ❌ **无超时断开检测** - Consumer 崩溃后不会自动清理
- ❌ **无心跳机制** - 无法检测僵尸连接
- ❌ **无连接恢复** - 断开后需要重新订阅

### 7.2 原生 Pulsar

```java
// ServerCnx.java - 连接管理
public class ServerCnx extends PulsarHandler {
    // 连接状态
    State state = State.Start;
    ChannelHandlerContext ctx;
    volatile boolean isActive = true;

    // 心跳检测
    long lastActive = System.currentTimeMillis();
    int maxConsecutiveFailedHeartbeats = 3;

    // 超时处理
    void checkConnectionTimeout() {
        if (System.currentTimeMillis() - lastActive > connectionTimeoutMs) {
            closeConnection();
        }
    }

    // Consumer 移除时清理
    void closeConsumer(Consumer consumer) {
        consumer.close();
        removeConsumerFromDispatcher(consumer);
        replayPendingMessages(consumer);
    }
}
```

**特点**：
- ✅ **完整的连接状态机** - Start, Connected, Closing, Closed
- ✅ **心跳检测** - 定期发送 Ping/Pong
- ✅ **超时断开** - 自动清理无效连接
- ✅ **优雅关闭** - Consumer 移除时 replay pending 消息

### 7.3 关键差异

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **连接状态跟踪** | ❌ 无 | ✅ State 状态机 |
| **心跳检测** | ❌ 无 | ✅ Ping/Pong + 失败计数 |
| **超时断开** | ❌ 无 | ✅ 可配置超时 |
| **优雅关闭** | ❌ 直接移除 | ✅ replay pending 消息 |
| **连接恢复** | ❌ 需重新订阅 | ✅ 支持 reconnect |

---

## 八、事务支持差异

### 8.1 Pulsar Lite

**❌ 完全不支持事务**

当前实现没有任何事务相关的代码：
- 无事务协调器 (Transaction Coordinator)
- 无事务日志
- 无事务性消息发送
- 无事务性确认

### 8.2 原生 Pulsar

```java
// TransactionMetadataStoreService.java
public class TransactionMetadataStoreService {
    // 事务状态管理
    enum TxnStatus { OPEN, COMMITTING, COMMITTED, ABORTING, ABORTED }

    // 事务协调器
    Map<Long, TransactionMetadata> transactions;

    // 事务性发送
    void addPublishToTxn(long txnId, long ledgerId, long entryId) {
        transactions.get(txnId).addEntry(ledgerId, entryId);
    }

    // 事务性确认
    void addAckToTxn(long txnId, String topic, String subscription, List<Position> positions) {
        transactions.get(txnId).addAckPositions(positions);
    }

    // 提交/回滚
    void commitTxn(long txnId) { /* ... */ }
    void abortTxn(long txnId) { /* ... */ }
}
```

**特点**：
- ✅ **原子性发送** - 多条消息原子写入
- ✅ **原子性确认** - 批量确认的原子性保证
- ✅ **跨 Topic 事务** - 一个事务可涉及多个 Topic
- ✅ **2PC 支持** - 两阶段提交协议

### 8.3 关键差异

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **事务协调器** | ❌ 无 | ✅ TransactionCoordinator |
| **事务性发送** | ❌ 无 | ✅ 原子写入多条消息 |
| **事务性确认** | ❌ 无 | ✅ 批量确认原子性 |
| **跨 Topic 事务** | ❌ 无 | ✅ 支持 |
| **2PC 协议** | ❌ 无 | ✅ 完整支持 |

---

## 九、消息元数据差异

### 9.1 Pulsar Lite

```rust
// rust/src/storage/mod.rs
pub struct Message {
    pub payload: Vec<u8>,
    // 缺少:
    // - properties: HashMap<String, String>
    // - event_time: Option<i64>
    // - key: Option<String>
    // - ordering_key: Option<Vec<u8>>
    // - sequence_id: Option<i64>
}
```

**问题**：
- ❌ **无消息属性** - 不能携带自定义 key-value
- ❌ **无 event_time** - 无法按事件时间处理
- ❌ **无 ordering_key** - 无法保证有序性
- ❌ **无 sequence_id** - 无法检测消息丢失/重复

### 9.2 原生 Pulsar

```java
// MessageMetadata.proto
message MessageMetadata {
    required string producer_name = 1;
    required uint64 sequence_id = 2;
    optional uint64 event_time = 4;
    repeated KeyValue properties = 5;
    optional string partition_key = 6;
    optional bytes ordering_key = 16;
    // ... 更多字段
}

// 消息处理
public class MessageImpl {
    private MessageMetadata metadata;
    private ByteBuf payload;

    public String getProperty(String key) {
        return metadata.getProperties().get(key);
    }

    public long getEventTime() {
        return metadata.getEventTime();
    }

    public String getOrderingKey() {
        return metadata.getOrderingKey();
    }
}
```

**特点**：
- ✅ **完整的元数据** - properties, event_time, key 等
- ✅ **自定义属性** - 用户可添加任意 key-value
- ✅ **事件时间语义** - 支持按 event_time 处理
- ✅ **有序性保证** - ordering_key 支持严格有序

### 9.3 关键差异

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **消息属性** | ❌ 无 | ✅ HashMap<String, String> |
| **event_time** | ❌ 无 | ✅ 支持 |
| **ordering_key** | ❌ 无 | ✅ 支持严格有序 |
| **sequence_id** | ❌ 无 | ✅ 检测丢失/重复 |
| **partition_key** | ❌ 无 | ✅ 分区路由 |
| **压缩支持** | ❌ 无 | ✅ LZ4/Zlib/Zstd |

---

## 十、缺失功能清单

### 10.1 Pulsar Lite 当前缺失的关键功能

| 功能 | 优先级 | 影响范围 |
|------|--------|----------|
| **Pending Acks 跟踪** | 高 | 消息可能丢失 |
| **自动重投递** | 高 | Consumer 故障时消息无法恢复 |
| **Unacked 消息限制** | 高 | 慢消费者可能拖垮系统 |
| **Dispatcher 阻塞机制** | 高 | 背压缺失 |
| **连接状态管理** | 高 | 僵尸连接无法检测 |
| **心跳/超时检测** | 高 | Consumer 崩溃后无法清理 |
| **消息元数据** | 中 | 无法携带属性和有序性保证 |
| **消费者优先级** | 中 | 负载分配不均衡 |
| **累积确认** | 中 | 性能优化 |
| **延迟消息** | 中 | 功能缺失 |
| **事务支持** | 低 | 不支持原子性操作 |
| **Chunked Messages** | 低 | 大消息支持 |
| **消息压缩** | 低 | 带宽优化 |

---

## 十一、架构改进建议

### 11.1 短期改进（MVP 完善）

```rust
// 1. 添加 Pending Acks 跟踪
pub struct Consumer {
    pending_acks: HashMap<(u64, u64), PendingAckInfo>,  // (ledger, entry) -> info
    unacked_messages: AtomicU32,
    max_unacked_messages: u32,
}

// 2. 添加重投递队列
pub struct SharedDispatcher {
    messages_to_redeliver: ConcurrentBitmapSet,
    redelivery_backoff: Backoff,
}

// 3. Consumer 移除时处理
impl SharedDispatcher {
    fn remove_consumer(&mut self, consumer_id: u64) {
        if let Some(consumer) = self.consumers.remove(&consumer_id) {
            // 将 pending_acks 转入 redelivery 队列
            for (ledger, entry) in consumer.pending_acks.keys() {
                self.messages_to_redeliver.insert(ledger, entry);
            }
            self.notify_redelivery_message_added();
        }
    }
}
```

### 11.2 中期改进（功能对齐）

```rust
// 4. 消费者优先级支持
pub struct Consumer {
    priority_level: i32,  // 0 = 最高
}

impl SharedDispatcher {
    fn get_next_consumer(&self) -> Option<Arc<Consumer>> {
        // 按优先级分组
        // 同优先级内 Round-Robin
        // 无可用时降级
    }
}

// 5. Unacked 限制和阻塞
pub struct SharedDispatcher {
    total_unacked_messages: AtomicU32,
    max_unacked_on_subscription: u32,
    blocked_on_unacked: AtomicBool,
}
```

---

## 十二、总结

### 12.1 相似之处

| 方面 | 说明 |
|------|------|
| Round-Robin 基本算法 | 都使用轮询方式选择消费者 |
| Flow 控制 | 都基于 permits 机制 |
| 批量限制 | 都有 `dispatcherMaxRoundRobinBatchSize` |
| 消息分配跟踪 | 都跟踪消息分配状态 |

### 12.2 主要差异

| 差异点 | Pulsar Lite | 原生 Pulsar |
|--------|-------------|-------------|
| **可靠性** | 缺失重投递，消息可能丢失 | 完整的 ack/replay 机制 |
| **优先级** | 不支持 | 完整支持 |
| **背压** | 无 | 多层流控 |
| **存储** | 内存 HashMap | BookKeeper + ManagedCursor |
| **异步** | 简单同步 | 完整异步回调链 |
| **连接管理** | 无状态，无超时检测 | 完整连接生命周期 |
| **事务支持** | 无 | 完整事务协调器 |
| **消息元数据** | 仅 payload | 完整属性和元数据 |

### 12.3 Pulsar Lite 定位

作为一个 **轻量级嵌入式消息队列**，Pulsar Lite 当前的 Shared 模式实现：
- ✅ 适合开发测试场景
- ✅ 基本的 Round-Robin 分发可用
- ⚠️ 不适合生产环境（缺失关键可靠性机制）
- ⚠️ 需要补充 pending acks 和重投递机制才能用于正式场景

---

## 十三、参考代码位置

### Pulsar Lite 关键文件

| 文件 | 说明 |
|------|------|
| `rust/src/broker/dispatcher/shared.rs` | Shared Dispatcher 实现 |
| `rust/src/broker/service/consumer.rs` | Consumer 定义 |
| `rust/src/storage/mod.rs` | 消息存储和确认 |
| `rust/src/broker/service/topic/subscription.rs` | 订阅管理 |
| `rust/src/protocol/codec.rs` | 协议编解码 |

### 原生 Pulsar 关键文件

| 文件 | 说明 |
|------|------|
| `pulsar-broker/.../PersistentDispatcherMultipleConsumers.java` | Shared Dispatcher 主实现 |
| `pulsar-broker/.../AbstractDispatcherMultipleConsumers.java` | 基类，消费者选择算法 |
| `pulsar-broker/.../SharedConsumerAssignor.java` | 消息分配器 |
| `pulsar-broker/.../MessageRedeliveryController.java` | 重投递控制 |
| `pulsar-broker/.../ServerCnx.java` | 连接管理 |
| `pulsar-broker/.../TransactionMetadataStoreService.java` | 事务支持 |
| `pulsar-common/.../MessageMetadata.proto` | 消息元数据定义 |
