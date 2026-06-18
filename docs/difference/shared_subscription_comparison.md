# Pulsar Lite Shared 消费模式 vs 原生 Pulsar 全面对比分析

> 分析日期: 2026-03-10（2026-06 更新：persistent Shared 读路径与 redelivery 已对齐 Phase 0–4）
> 对比版本: Pulsar Lite (当前实现) vs Apache Pulsar (官方实现)

---

## 一、架构对比

### 1.1 核心组件映射

| 组件 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **Dispatcher** | `SharedDispatcher` | `PersistentDispatcherMultipleConsumers` |
| **消费者选择** | `get_next_available_consumer()` | `getNextConsumer()` (AbstractDispatcherMultipleConsumers) |
| **消息分配器** | 无（直接在 storage 中跟踪） | `SharedConsumerAssignor` |
| **重投递控制** | `BTreeMap` 简化队列（persistent） | `MessageRedeliveryController` |
| **游标管理** | ManagedCursor（RocksDB / memory） | ManagedCursor + BookKeeper |
| **连接管理** | 可治理版状态机 + keep-alive + 连接限流 | 完整的连接生命周期管理 |
| **事务支持** | 无 | 完整事务支持 |
| **消息元数据** | 仅存储 payload | 完整的属性和元数据支持 |

---

## 二、Round-Robin 算法差异

### 2.1 Pulsar Lite 实现

```rust
// rust/src/broker/dispatcher/shared.rs
async fn get_next_available_consumer(&self) -> Option<Arc<Consumer>> {
    let consumers: Vec<_> = self.consumers.values().cloned().collect();
    let mut best_priority: Option<i32> = None;
    let mut eligible_indices = Vec::new();

    for (index, consumer) in consumers.iter().enumerate() {
        let permits = consumer.get_available_permits().await;
        if permits == 0 {
            continue;
        }

        let priority = consumer.get_priority_level();
        match best_priority {
            Some(current_best) if priority > current_best => {}
            Some(current_best) if priority == current_best => eligible_indices.push(index),
            _ => {
                best_priority = Some(priority);
                eligible_indices.clear();
                eligible_indices.push(index);
            }
        }
    }

    let eligible_count = eligible_indices.len();
    let start = self.round_robin_index.fetch_add(1, Ordering::Relaxed) % eligible_count;

    for offset in 0..eligible_count {
        let vector_index = eligible_indices[(start + offset) % eligible_count];
        let consumer = consumers[vector_index].clone();
        if consumer.get_available_permits().await > 0 {
            return Some(consumer);
        }
    }
    None
}
```

**特点**：
- ✅ **已支持消费者优先级**
- ✅ 先选择数值更小的 `priority_level`（`0 = 最高优先级`）
- ✅ 同优先级内继续保持 Round-Robin
- ✅ 高优先级组无 permit 时才降级到低优先级组
- ✅ 支持流控 (permits)

同时，订阅命令路径已经补上 `priority_level` 透传：

```rust
// rust/src/broker/handler/consumer_handler.rs
let priority_level = subscribe_cmd.priority_level.unwrap_or(0);

let consumer = Arc::new(Consumer::new(
    consumer_id,
    consumer_name.clone(),
    subscription_arc.clone(),
    connection_id,
    message_tx,
    priority_level,
));
```

```rust
// rust/src/broker/service/consumer.rs
pub struct Consumer {
    // ...
    /// Lower value means higher priority, consistent with native Pulsar.
    priority_level: i32,
}
```

这意味着 Shared 模式现在已经不是“无序 HashMap + 简单轮询”，而是具备了与原生 Pulsar 更接近的优先级选择语义。

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
| **优先级调度** | ✅ 已支持 (0=最高) | ✅ 完整支持 (0=最高) |
| **算法复杂度** | O(n) 优先级筛选 + 同组轮询 | O(n) 带优先级 |
| **消费者排序** | 运行时按优先级筛选 | 按优先级排序的 List |
| **索引管理** | 原子 usize | volatile int + 同步块 |

**当前仍与原生存在的差异**：
- `pulsar-lite` 目前是在运行时遍历 `HashMap` 后筛选优先级组，还没有原生 Pulsar 那种长期按优先级组织的 consumer 列表
- 当前优先级调度只覆盖 Shared 路径，没有扩散到其他订阅模式
- 尚未实现和优先级联动的更复杂 blocked/unacked 调度策略

---

## 三、消息分发流程差异

### 3.1 Pulsar Lite 流程

```
Flow 命令 → consumer_flow() → dispatch_messages_batch()
                                    ↓
                         优先 pop messages_to_redeliver
                                    ↓
                         next_unacked_candidate(read_position)
                                    ↓
                         storage.read_from + is_acknowledged 过滤
                                    ↓
                         consumer.enqueue_message()
```

**特点**：
- 每次最多分发 20 条消息 (`DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE`)
- Persistent：`read_position` 由 dispatcher 持有，避免重复 dispatch
- **Persistent 已支持 replay**（断连 recovery、显式 Redeliver、nack/ack_timeout 经客户端 Redeliver 命令）
- Non-persistent：仍走简化内存路径

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
| **Replay 机制** | ✅ persistent 简化版（BTreeMap + redelivery-first） | ✅ MessageRedeliveryController |
| **延迟消息** | ❌ 无 | ✅ DelayedDeliveryTracker |
| **大消息分块** | ❌ 无 | ✅ SharedConsumerAssignor UUID 跟踪 |
| **批量计算** | 固定 20 条 | 动态计算 |

---

## 四、消息确认机制差异

### 4.1 Pulsar Lite 确认逻辑

```rust
// rust/src/broker/service/consumer.rs
pub struct PendingAck {
    pub dispatched_at: Instant,
    pub redelivery_count: u32,
}

pub async fn track_message_dispatched(&self, message_id: &MessageId, redelivery_count: u32) {
    let mut pending = self.pending_acks.write().await;
    pending.insert(
        message_id.clone(),
        PendingAck {
            dispatched_at: Instant::now(),
            redelivery_count,
        },
    );
}

pub async fn remove_pending_ack(&self, message_id: &MessageId) -> bool {
    self.pending_acks.write().await.remove(message_id).is_some()
}
```

**当前状态**：
- ✅ **已支持 Pending Acks 跟踪** - Shared 模式下每个 Consumer 维护自己的 `pending_acks`
- ✅ **已支持 ack 归属校验** - 只有持有该消息的 Consumer 才会移除 pending ack 并落 storage ack
- ✅ **已支持断开恢复所需的 pending drain** - Consumer 移除时可一次性取出全部待确认消息
- ✅ **已对齐原生 Pulsar 的“先写 pending ack，再投递消息”时序** - Shared `send_message()` 现在会先记录 `pending_acks`，再把消息送入 consumer channel；如果 channel 发送失败，会回滚刚写入的 pending ack
- ✅ **已修正 Shared ack ownership 的协议边界问题** - `handle_ack()` 现在会先把非分区 ack 的 `partition` 归一化为 `-1`，再继续按完整 `MessageId` 精确匹配 owner，并执行 `remove_pending_ack()` 与 `storage.ack_message_shared(...)`
- ✅ **已补上 close/recovery 前的 pending ack 关闭语义** - Consumer 进入 remove/recovery 前会先关闭 pending ack 写入入口，避免 close 过程中继续混入新的 pending ack，行为上更接近原生 Pulsar `PendingAcksMap.forEachAndClose()`

**与原生 Pulsar 的剩余差异**：
- ℹ️ **累计确认 (cumulative ack) 不属于 Shared 模式目标** - 原生 Pulsar 中 Shared / Key_Shared 明确走 `individual ack`，累计确认只适用于 Exclusive / Failover
- ⚠️ **无 unacked messages 计数/限流** - 还没有官方那套 per-consumer / per-subscription unacked 控制
- ⚠️ **Pending Ack 元数据较少** - 当前只记录时间和重投递次数，未覆盖 batchSize、stickyKeyHash 等更完整信息
- ⚠️ **storage 的 Shared assignment / ack 粒度仍偏粗** - 当前 `message_assignments`、`get_assignment_owner()`、`is_acknowledged_shared()` 等路径主要按 `entry` 建模，而原生 Pulsar 的 pending ack / replay 跟踪至少精确到 `ledgerId + entryId`

### 4.2 原生 Pulsar 确认逻辑

```java
// Subscription.java
static boolean isCumulativeAckMode(SubType subType) {
    return SubType.Exclusive.equals(subType) || SubType.Failover.equals(subType);
}

static boolean isIndividualAckMode(SubType subType) {
    return SubType.Shared.equals(subType) || SubType.Key_Shared.equals(subType);
}
```

```java
// Consumer.java
if (ack.getAckType() == AckType.Cumulative) {
    if (Subscription.isIndividualAckMode(subType)) {
        log.warn("[{}] [{}] Received cumulative ack on shared subscription, ignoring",
                subscription, consumerId);
        return CompletableFuture.completedFuture(null);
    }
    subscription.acknowledgeMessage(positionsAcked, AckType.Cumulative, properties);
}
```

```java
// PersistentSubscription.java
if (ackType == AckType.Cumulative) {
    cursor.asyncMarkDelete(position, mergeCursorProperties(properties),
            markDeleteCallback, previousMarkDeletePosition);
} else {
    cursor.asyncDelete(positions, deleteCallback, previousMarkDeletePosition);
}
```

**特点**：
- ✅ **Pending Acks 跟踪** - 每个 Consumer 维护
- ✅ **Unacked 消息限制** - 防止消息积压
- ✅ **Consumer 断开重投递** - 自动 replay pending 消息
- ✅ **Exclusive / Failover 支持累积确认**
- ✅ **Shared / Key_Shared 明确走单条确认**

**原生 Pulsar 在 Shared ack / recovery 上更关键的时序点**：

```java
// Consumer.java
// 先写 pending ack，再真正发送给 consumer，避免 disconnect race
pendingAcks.put(ledgerId, entryId, batchSize, stickyKeyHash);
ctx.write(message);
```

```java
// Consumer.java
// ack 时按 ledgerId + entryId 移除 pending ack
private void removePendingAcks(Position position) {
    pendingAcks.remove(position.getLedgerId(), position.getEntryId());
}
```

```java
// PersistentDispatcherMultipleConsumers.java
// consumer remove 时关闭 pending ack map，并把仍未确认的消息转入 replay
consumer.getPendingAcks().forEachAndClose((ledgerId, entryId, batchSize, stickyKeyHash) -> {
    addMessageToReplay(ledgerId, entryId, stickyKeyHash);
});
```

**这次按原生时序对齐后解决的问题**：
- 修复前，`pulsar-lite` Shared 路径里存在“先发消息，后写 pending ack”的窗口，ack / close / recovery 并发时容易产生竞态
- 协议层解析层原先把“未显式携带 partition 的非分区 ack”错误地补成了 `0`，而 broker/storage 内部非分区消息一直使用 `-1`，导致按完整 `MessageId` 查 owner 时会误判“无 ownership”
- 上述两个问题叠加时，会表现为“消息已经 ack，但 owner close 后仍被当作 pending ack 放回 replay”

**本次修复涉及的关键代码**：
- [consumer.rs](/home/xtline/code/work/pulsar-lite/rust/src/broker/service/consumer.rs)
- [consumer_handler.rs](/home/xtline/code/work/pulsar-lite/rust/src/broker/handler/consumer_handler.rs)
- [shared.rs](/home/xtline/code/work/pulsar-lite/rust/src/broker/dispatcher/shared.rs)

**为什么这里不把 cumulative ack 作为 Shared 目标**：
- 原生 Pulsar 在 `Subscription.isCumulativeAckMode(...)` 中只把 `Exclusive` 和 `Failover` 视为累计确认模式
- 原生 Pulsar 在 `Subscription.isIndividualAckMode(...)` 中把 `Shared` 和 `Key_Shared` 归为单条确认模式
- `Consumer` 在收到 `AckType.Cumulative` 且订阅模式为 Shared 时，会直接记录 warning 并忽略，不会推进 subscription/cursor 状态
- 因此从“与原生 Pulsar Shared 语义对齐”的角度看，`pulsar-lite` 不需要把 cumulative ack 当成 Shared 的缺失能力去补齐

---

## 五、重投递机制差异

### 5.1 Pulsar Lite

**✅ 已支持基础重投递机制**

当前行为：
- ✅ **Consumer 断开自动重投递** - 移除 Consumer 时会 drain `pending_acks`，释放 assignment，并放入 replay 队列
- ✅ **优先 replay 再分发新消息** - dispatcher 先消费 `messages_to_redeliver`，再取新消息
- ✅ **发送失败自动回队** - 分发失败的消息会重新加入 redelivery queue
- ✅ **已 ack 消息跳过 replay** - replay 前会检查 storage 中是否已确认，避免重复恢复
- ✅ **close/recovery 与 ack 已对齐到更安全的 Shared 时序** - Consumer remove 前先关闭 pending ack 跟踪入口，再 drain 并回收真正仍未确认的消息

**与原生 Pulsar 的剩余差异**：
- ⚠️ **重投递控制器仍是简化版** - 当前主要是 `BTreeMap<MessageId, redelivery_count>`，还不是官方 `MessageRedeliveryController`
- ⚠️ **KeyShared hash 阻塞/有序重投递** - KeyShared persistent 已有 sticky 路由，但 `allow_out_of_order_delivery=false` 时尚无原生 `hashesToBeBlocked` 语义
- ✅ **Persistent 显式 redeliver** - `CommandRedeliverUnacknowledgedMessages` 已注册；nack / ack_timeout 由官方 Python client 发 Redeliver 触发（`tests/persist/test_persistent_redelivery.py`）
- ⚠️ **Non-persistent Shared** - 仍不支持客户端 redeliver / nack 驱动重投（见 `tests/non_persist/`）

**本次修复前后的行为差异**：
- 修复前：Shared ack 在 owner 查找失败时会直接跳过 storage ack；随后 consumer close 会把同一条消息当作 pending ack 放回 redelivery queue
- 修复后：Shared ack 会先回查真实 tracked `MessageId`，正确清理 owner 的 pending ack，并把真实 `MessageId` 落到 storage；consumer close 时如果消息已被 ack，不会再进入 replay
- 端到端验证上，`tests/test_shared_integration.py` 中新增的“已 ack 消息在 owner close 后不重投递”和“recovery 不重复回放已 ack 消息”场景已经通过

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
    
}
```

**特点**：
- ✅ 简单的全局 permits 计数
- ⚠️ `consumer_flow` 是同步函数
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

**⚠️ 已具备基础连接管理，但仍不完整**

```rust
// rust/src/broker/service/server_cnx.rs
pub struct ServerCnx<T> {
    state: State,                  // Start / Connecting / Connected / Failed / Closing / Closed
    handshake_completed: bool,
    last_activity: Instant,
    waiting_for_pong: bool,
    remote_protocol_version: i32,
    connection_check_in_progress: Option<...>,
    keep_alive_interval: Duration,
    handshake_timeout: Duration,
}
```

**当前能力**：
- ✅ **可治理版连接状态机** - `Start / Connecting / Connected / Failed / Closing / Closed`
- ✅ **握手超时关闭** - `Connect` 未在超时内完成时关闭连接
- ✅ **broker 侧 Ping/Pong keep-alive** - 仅对支持 `Ping/Pong` 的协议版本启用主动 keep-alive
- ✅ **单次连接活性检查** - 通过一次性 `Ping` + timeout 判断连接是否仍存活
- ✅ **连接数限制** - 支持全局连接数和每地址连接数限制
- ✅ **超时关闭接入 cleanup** - 连接超时后继续走现有 Shared recovery

**当前缺口**：
- ⚠️ **无 consumer 级活动跟踪** - 仍没有更细粒度的 consumer liveness
- ✅ **Persistent Shared ack timeout / negative ack** - 由客户端发 `RedeliverUnacknowledgedMessages`，broker handler 入队 redelivery（非 broker 定时器）
- ⚠️ **Non-persistent** - 连接存活时的 nack/timeout 仍不触发 redelivery
- ⚠️ **无连接可写性驱动的节流/暂停读取** - 尚未对齐官方的 channel writability / auto-read 协调
- ⚠️ **显式 liveness check 仍偏内部化** - 已能发起一次性探测，但还没有官方那样更完整的异步结果语义与上层复用
- ⚠️ **连接关闭生命周期仍较简化** - 已能统一 cleanup，但还没有官方那么完整的统计、回调、任务取消与上下文回收
- ⚠️ **无复杂 backoff/reconnect 协调** - 仅有 broker 侧最小超时关闭

### 7.2 原生 Pulsar

```java
// PulsarHandler.java
private final long keepAliveIntervalSeconds;
private boolean waitingForPingResponse = false;
private ScheduledFuture<?> keepAliveTask;

@Override
protected void messageReceived(BaseCommand cmd) {
    waitingForPingResponse = false;
}

@Override
public void channelActive(ChannelHandlerContext ctx) {
    this.keepAliveTask = ctx.executor().scheduleAtFixedRate(
        this::handleKeepAliveTimeout,
        keepAliveIntervalSeconds,
        keepAliveIntervalSeconds,
        TimeUnit.SECONDS
    );
}

private void handleKeepAliveTimeout() {
    if (!isHandshakeCompleted()) {
        ctx.close();
    } else if (waitingForPingResponse && ctx.channel().config().isAutoRead()) {
        ctx.close();
    } else if (getRemoteEndpointProtocolVersion() >= ProtocolVersion.v1.getValue()) {
        waitingForPingResponse = true;
        sendPing();
    }
}
```

```java
// ServerCnx.java
enum State { Start, Connected, Failed, Connecting }

private void completeConnect(int clientProtoVersion, String clientVersion) {
    writeAndFlush(Commands.newConnected(...));
    state = State.Connected;
    setRemoteEndpointProtocolVersion(clientProtoVersion);
}

@Override
protected boolean isHandshakeCompleted() {
    return state == State.Connected;
}
```

**特点**：
- ✅ **连接状态机** - `Start / Connecting / Connected / Failed`
- ✅ **握手超时关闭** - `isHandshakeCompleted()` 未完成时由 keep-alive task 主动关闭连接
- ✅ **Ping/Pong keep-alive** - `PulsarHandler` 周期性发送 `Ping`，收到任意合法命令后清除等待状态
- ✅ **连接关闭清理** - `ServerCnx.channelInactive()` 中统一关闭 producer / consumer 并清理连接上下文
- ✅ **显式连接活性探测** - `checkConnectionLiveness()` 提供一次性 Ping + future 结果
- ✅ **连接可写性治理** - 不可写时可暂停接收请求，恢复后重新开启

### 7.3 关键差异

| 特性 | Pulsar Lite | 原生 Pulsar |
|------|-------------|-------------|
| **连接状态跟踪** | ✅ `Start/Connecting/Connected/Failed` + close reason | ✅ State 状态机 |
| **握手完成判定** | ✅ `handshake_completed` | ✅ `isHandshakeCompleted()` |
| **心跳检测** | ✅ broker 侧 Ping/Pong + 协议版本判定 | ✅ `PulsarHandler` 周期性 Ping/Pong |
| **超时断开** | ✅ 握手超时 + keep-alive/liveness timeout | ✅ 握手超时 + keep-alive timeout |
| **显式活性检查** | ✅ 支持一次性 Ping + timeout | ✅ `checkConnectionLiveness()` 返回 future 结果 |
| **连接准入治理** | ✅ 全局/每地址连接数限制 | ✅ `ConnectionController` 等更完整治理 |
| **连接背压治理** | ⚠️ 暂无 channel writability / auto-read 协调 | ✅ 不可写时暂停接收请求 |
| **连接关闭清理** | ✅ cleanup 接入 Shared recovery | ✅ `channelInactive()` 统一清理 producer/consumer |
| **关闭后配套回收** | ⚠️ 以基础 cleanup 为主 | ✅ 统计、回调、任务取消、pending check 完成 |
| **连接恢复能力** | ❌ 需重新订阅 | ✅ broker 具备完整连接生命周期管理，客户端可重建连接 |

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
// rust/src/broker/handler/producer_handler.rs
let message_id = producer.publish_message(&frame.payload).await?;
```

```rust
// rust/src/protocol/codec.rs
let metadata_bytes = if metadata.is_empty() {
    MessageMetadata {
        sequence_id: entry_id,
        ..Default::default()
    }
    .encode_to_vec()
} else {
    metadata.to_vec()
};
```

**当前状态（2026-06）**：
- ✅ **Persistent metadata 已持久化** - entrylog/storage 保存 `MessageMetadata`（含 `ordering_key` 等）；dispatch 下发时从 storage 读出
- ✅ **协议层 metadata / compression 透传** - codec 与 `ServerCommand::Message` 支持携带 metadata
- ⚠️ **压缩** - broker 不解压/不重压缩，按 Pulsar 协议交由客户端处理
- ⚠️ **partition_key / event_time / sequence_id 全链路** - 部分字段可存可发，尚未对齐原生全部语义

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
| **消息属性** | ✅ persistent 可存可发（简化） | ✅ HashMap<String, String> |
| **event_time** | ⚠️ 部分支持 | ✅ 支持 |
| **ordering_key** | ✅ persistent KeyShared 路由 + 下发 | ✅ 支持严格有序 |
| **sequence_id** | ⚠️ 部分支持 | ✅ 检测丢失/重复 |
| **partition_key** | ⚠️ 尚未作为本轮重点补齐 | ✅ 分区路由 |
| **压缩支持** | ⚠️ 协议层基础透传（不解压、不持久化） | ✅ LZ4/Zlib/Zstd |

**当前仍与原生存在的差异**：
- `pulsar-lite` 这轮保留的是协议层/下发层接口，没有把 `MessageMetadata` 贯通到 storage 与 replay 路径
- 压缩目前只是**协议层透传能力保留**，不是 broker 侧编解码；broker 不会主动解压缩或重压缩
- `partition_key`、更完整的 schema/encryption/batch 相关字段还未纳入本轮范围

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
| **可靠性** | persistent：ack + redelivery + read_position；non-persistent 仍简化 | 完整的 ack/replay 机制 |
| **优先级** | ✅ Shared 已支持 | 完整支持 |
| **背压** | 无 | 多层流控 |
| **存储** | RocksDB ManagedCursor / memory | BookKeeper + ManagedCursor |
| **异步** | 简单同步 | 完整异步回调链 |
| **连接管理** | 可治理版状态机 + keep-alive + 连接限流 | 完整连接生命周期 |
| **事务支持** | 无 | 完整事务协调器 |
| **消息元数据** | persistent 已持久化基础 metadata | 完整属性和元数据 |

### 12.3 Pulsar Lite 定位

作为一个 **轻量级嵌入式消息队列**，Pulsar Lite 的 Shared 模式：
- ✅ Persistent 主链路（cursor、ack hole、redelivery）已通过 `tests/persist/`
- ✅ 基本的 Round-Robin + 优先级分发可用
- ⚠️ 与原生仍有差距：正式 RedeliveryController、unacked 限流、KeyShared hash 阻塞
- ⚠️ Non-persistent Shared 的 redelivery 语义仍不完整

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
