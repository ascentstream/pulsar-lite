# Failover Dispatcher 实现对比分析

> 分析日期: 2026-03-09（2026-06 更新：persistent Failover SingleActive rewind / priority 已落地）
> 对比版本: Pulsar Lite (当前实现) vs Apache Pulsar (官方实现)

---

## 1. 核心逻辑对比

### 1.1 Pulsar Lite 实现

**文件**: `rust/src/broker/dispatcher/failover.rs`

```rust
pub struct FailoverDispatcher {
    consumers: Vec<Arc<Consumer>>,
    active_consumer_id: Option<u64>,
    total_available_permits: AtomicU32,
    read_position: RwLock<Option<ManagedLedgerPosition>>,
}

// 消息分发：仅 active consumer；读路径 read_from(read_position)
// remove_consumer_with_recovery：active 关闭时 drain pending → rewind_read_position → promote
```

**分析（2026-06）**:
- ✅ 按 priority + consumer name 排序并选择 active
- ✅ Active Consumer 变更通知（`notify_active_consumer_change`）
- ✅ **Persistent Cursor Rewind**（`single_active::rewind_read_position`）
- ✅ `read_position` 由 dispatcher 持有，配合 hole-aware 读
- ❌ **无 Failover 延迟机制**（默认 1000ms）
- ❌ **无 Pending Read 取消**（异步 read 链简化）
- ❌ **无一致性哈希 / 分区 topic 主消费者策略**

### 1.2 原生 Pulsar 实现

**文件**:
- `AbstractDispatcherSingleActiveConsumer.java` (核心逻辑)
- `PersistentDispatcherSingleActiveConsumer.java` (持久化实现)

```java
// 消费者选择算法
protected boolean pickAndScheduleActiveConsumer() {
    checkArgument(!consumers.isEmpty());
    AtomicBoolean hasPriorityConsumer = new AtomicBoolean(false);

    // 1. 按优先级排序，同优先级按名称排序
    consumers.sort((c1, c2) -> {
        int priority = c1.getPriorityLevel() - c2.getPriorityLevel();
        if (priority != 0) {
            hasPriorityConsumer.set(true);
            return priority;
        }
        return c1.consumerName().compareTo(c2.consumerName());
    });

    // 2. 找出最高优先级的消费者数量
    int consumersSize = consumers.size();
    if (hasPriorityConsumer.get()) {
        int highestPriorityLevel = consumers.get(0).getPriorityLevel();
        for (int i = 0; i < consumers.size(); i++) {
            if (highestPriorityLevel != consumers.get(i).getPriorityLevel()) {
                consumersSize = i;
                break;
            }
        }
    }

    // 3. 选择 Active Consumer
    // - 分区 topic: partitionIndex % consumersSize
    // - 非分区 topic (或启用一致性哈希): 从哈希环选择
    int index = partitionIndex >= 0 && !serviceConfig.isActiveConsumerFailoverConsistentHashing()
            ? partitionIndex % consumersSize
            : peekConsumerIndexFromHashRing(makeHashRing(consumersSize));

    Consumer selectedConsumer = consumers.get(index);

    // 4. 如果 Active Consumer 变更，触发切换逻辑
    if (selectedConsumer == activeConsumer) {
        return false;  // 未变化
    } else {
        activeConsumer = selectedConsumer;
        scheduleReadOnActiveConsumer();
        return true;
    }
}

// Failover 延迟切换 (关键!)
protected void scheduleReadOnActiveConsumer() {
    cancelPendingRead();

    if (havePendingRead) {
        return;
    }

    // Failover 延迟: 防止消息重复
    if (subscriptionType != SubType.Failover ||
        serviceConfig.getActiveConsumerFailoverDelayTimeMillis() <= 0) {
        // 无延迟，立即切换
        Consumer activeConsumer = getActiveConsumer();
        cursor.rewind(activeConsumer != null && activeConsumer.readCompacted());
        notifyActiveConsumerChanged(activeConsumer);
        readMoreEntries(activeConsumer);
        return;
    }

    // 延迟 1000ms 后执行 rewind 和读取
    if (readOnActiveConsumerTask != null) {
        return;
    }

    readOnActiveConsumerTask = executor.schedule(() -> {
        Consumer activeConsumer = getActiveConsumer();
        cursor.rewind(activeConsumer != null && activeConsumer.readCompacted());
        notifyActiveConsumerChanged(activeConsumer);
        readMoreEntries(activeConsumer);
        readOnActiveConsumerTask = null;
    }, serviceConfig.getActiveConsumerFailoverDelayTimeMillis(), TimeUnit.MILLISECONDS);
}

// 通知所有消费者状态变更
protected void notifyActiveConsumerChanged(Consumer activeConsumer) {
    if (null != activeConsumer && subscriptionType == SubType.Failover) {
        consumers.forEach(consumer ->
            consumer.notifyActiveConsumerChange(activeConsumer));
    }
}

// 消费者移除时处理
public synchronized void removeConsumer(Consumer consumer) {
    log.info("Removing consumer {}", consumer);
    consumers.remove(consumer);

    if (consumers.isEmpty()) {
        activeConsumer = null;
    }

    if (closeFuture == null && !consumers.isEmpty()) {
        // 有消费者剩余，重新选择 Active Consumer
        pickAndScheduleActiveConsumer();
        return;
    }

    cancelPendingRead();
}

// 重投递未确认消息
public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
    if (consumer != getActiveConsumer()) {
        log.info("Ignoring reDeliverUnAcknowledgedMessages: Only the active consumer can call resend");
        return;
    }

    if (readOnActiveConsumerTask != null) {
        log.info("Ignoring reDeliverUnAcknowledgedMessages: consumer is waiting for cursor to be rewinded");
        return;
    }

    cursor.cancelPendingReadRequest();
    havePendingRead = false;
    cursor.rewind(consumer.readCompacted());
    readMoreEntries(consumer);
}
```

**分析**:
- ✅ 完整的优先级支持 (priorityLevel + consumerName)
- ✅ 分区 topic 支持 (partitionIndex % consumersSize)
- ✅ 可选一致性哈希 (100 虚拟节点)
- ✅ **Failover 延迟** (默认 1000ms) 防止消息重复
- ✅ **Cursor Rewind** 确保从 unacked 位置重新消费
- ✅ **消费者通知机制** 告知谁成为 Active
- ✅ **Pending Read 取消** 防止旧消费者读取

---

## 2. 差异矩阵

| 特性 | Pulsar Lite | 原生 Pulsar | 影响等级 |
|------|-------------|-------------|----------|
| **基本分发** | ✅ Active Consumer | ✅ Active Consumer | 低 |
| **消费者优先级** | ✅ priority + name | ✅ priorityLevel 排序 | 低 |
| **分区 Topic 支持** | ❌ 简单列表 | ✅ partitionIndex % consumersSize | **高** |
| **一致性哈希** | ❌ 不支持 | ✅ 100 虚拟节点 (可选) | 中 |
| **Failover 延迟** | ❌ 立即切换 | ✅ 1000ms 延迟 (可配置) | **高** |
| **Cursor Rewind** | ✅ persistent（SingleActive） | ✅ 从 unacked 重读 | 低 |
| **Active 通知** | ✅ notifyActiveConsumerChange | ✅ notifyActiveConsumerChange | 低 |
| **Pending Read 取消** | ❌ 无 | ✅ cancelPendingRead | **高** |
| **消费者断开处理** | ✅ rewind + promote（persistent） | ✅ 完整切换逻辑 | 中 |
| **Topic 转移检测** | ❌ 无 | ✅ isTransferring 检查 | 中 |
| **Dispatch Rate Limiter** | ❌ 无 | ✅ 流量限制 | 低 |
| **Compacted Topic 支持** | ❌ 无 | ✅ readCompacted | 低 |
| **Redeliver 命令** | ❌ SingleActive 忽略（靠 close rewind） | ✅ redeliverUnacknowledgedMessages | 中 |
| **Consumer Epoch** | ❌ 无 | ✅ 防止旧请求干扰 | 中 |

---

## 3. 缺失功能

### 3.1 高优先级 (影响消息正确性)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **Failover 延迟机制** | 高 | 消息可能重复 | 切换时需要等待 pending 消息处理完成 |
| **Pending Read 取消** | 高 | 消息状态混乱 | 需要取消旧消费者的 pending read |
| **分区 Topic 主消费者分配** | 中 | 主消费者分布不均 | 需要根据 partitionIndex 分配 |
| **一致性哈希选择** | 中 | 切换频繁 | 分区 topic 的稳定主消费者选择 |
| **Consumer Epoch** | 中 | 旧请求干扰 | 防止过期请求影响当前状态 |

**已实现（2026-06，persistent）**：消费者优先级、Active 通知、Cursor Rewind、standby 接管 unacked backlog（`tests/persist/test_persistent_subscription_modes.py`）。

### 3.2 中优先级 (影响功能完整性)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **分区 Topic 支持** | 中 | 主消费者分布不均 | 需要根据 partitionIndex 分配 |
| **一致性哈希选择** | 中 | 切换频繁 | 分区 topic 的稳定主消费者选择 |
| **Consumer Epoch** | 中 | 旧请求干扰 | 防止过期请求影响当前状态 |
| **Exclusive/Failover 显式 Redeliver** | 中 | nack 行为与原生不一致 | SingleActive 当前忽略 Redeliver 命令 |

### 3.3 低优先级 (可选优化)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **Dispatch Rate Limiter** | 低 | 无流量控制 | 限制消息分发速率 |
| **Compacted Topic 支持** | 低 | 压缩 topic 功能缺失 | 支持压缩 topic 读取 |
| **Topic 转移检测** | 低 | 转移时消息可能丢失 | 检测 topic 是否在转移中 |

---

## 4. 改进建议

### 4.1 必须实现 (高优先级)

#### 建议 1: 添加 Failover 延迟机制

**原因**: 防止主消费者切换时消息重复投递

**原生实现**: `PersistentDispatcherSingleActiveConsumer.java:98-141`

**实现思路**:

```rust
pub struct FailoverDispatcher {
    consumers: Vec<Arc<Consumer>>,
    total_available_permits: AtomicU32,

    // 新增字段
    /// Failover 延迟时间 (毫秒)，默认 1000ms
    failover_delay_ms: u64,
    /// 延迟任务句柄
    failover_task: Option<tokio::task::JoinHandle<()>>,
    /// 是否有 pending read
    have_pending_read: AtomicBool,
}

impl FailoverDispatcher {
    /// 当 Active Consumer 变化时调用
    fn on_active_consumer_change(&mut self, storage: SharedStorage, topic: String, subscription: String) {
        // 1. 取消之前的延迟任务
        if let Some(task) = self.failover_task.take() {
            task.abort();
        }

        // 2. 取消 pending read
        self.cancel_pending_read();

        // 3. 如果已经有 pending read，等待完成
        if self.have_pending_read.load(Ordering::Relaxed) {
            return;
        }

        // 4. 启动延迟任务
        let delay = self.failover_delay_ms;
        let have_pending_read = self.have_pending_read.clone();

        self.failover_task = Some(tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

            // 5. 执行 rewind 和重新读取
            // rewind_cursor(storage, topic, subscription).await;
            // read_more_entries(...).await;
        }));
    }

    fn cancel_pending_read(&self) -> bool {
        self.have_pending_read.swap(false, Ordering::SeqCst)
    }
}
```

#### 建议 2: 实现 Cursor Rewind

**原因**: 主消费者切换后需要从 unacked 位置重新消费

**原生实现**: `PersistentDispatcherSingleActiveConsumer.java:113,133,295`

**实现思路**:

```rust
// 在 storage/mod.rs 中添加
impl Storage {
    /// Rewind cursor to last acknowledged position
    ///
    /// 当 Failover 切换时，需要将游标回退到最后确认的位置，
    /// 以便新的 Active Consumer 能够重新消费未确认的消息。
    pub fn rewind_cursor(&mut self, topic: &str, subscription: &str) -> Result<()> {
        let cursor_key = format!("{}:{}", topic, subscription);

        // 获取当前游标位置
        let current_cursor = self.cursors.get(&cursor_key).copied().unwrap_or(u64::MAX);

        if current_cursor == u64::MAX {
            // 游标尚未定位，无需 rewind
            return Ok(());
        }

        // 清除该订阅的所有消息分配状态
        // 这样消息可以被重新分配给新的 Active Consumer
        let prefix = format!("{}:{}:", topic, subscription);
        self.message_assignments.retain(|key, _| !key.starts_with(&prefix));

        info!("Cursor rewound for {}:{}", topic, subscription);
        Ok(())
    }

    /// 获取未确认的消息 (用于重投递)
    pub fn get_unacknowledged_messages(
        &self,
        topic: &str,
        subscription: &str,
    ) -> Vec<(MessageId, Vec<u8>)> {
        let cursor_key = format!("{}:{}", topic, subscription);
        let current_cursor = self.cursors.get(&cursor_key).copied().unwrap_or(u64::MAX);

        if current_cursor == u64::MAX {
            return Vec::new();
        }

        // 返回游标之后的所有消息
        if let Some(messages) = self.topics.get(topic) {
            messages.iter()
                .filter(|(msg_id, _)| msg_id.entry > current_cursor)
                .map(|(msg_id, data)| (msg_id.clone(), data.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }
}

// 在 FailoverDispatcher 中使用
impl FailoverDispatcher {
    fn on_active_consumer_change(&mut self, storage: SharedStorage, topic: String, subscription: String) {
        // ...

        self.failover_task = Some(tokio::spawn(async move {
            tokio::time::sleep(delay).await;

            // Rewind cursor
            {
                let mut guard = storage.lock().await;
                guard.rewind_cursor(&topic, &subscription).unwrap();
            }

            // 触发重新读取
            // ...
        }));
    }
}
```

#### 建议 3: 添加消费者优先级支持

**原因**: 需要控制哪个消费者成为主消费者

**原生实现**: `AbstractDispatcherSingleActiveConsumer.java:110-130`

**实现思路**:

```rust
// 在 consumer.rs 中添加优先级字段
pub struct Consumer {
    pub consumer_id: u64,
    pub consumer_name: String,
    pub priority_level: i32,  // 新增: 0 = 最高优先级
    // ...
}

impl FailoverDispatcher {
    /// 按优先级和名称排序消费者
    fn sort_consumers(&mut self) {
        self.consumers.sort_by(|a, b| {
            // 先按优先级排序 (小值优先)
            match a.priority_level.cmp(&b.priority_level) {
                std::cmp::Ordering::Equal => {
                    // 同优先级按名称排序
                    a.consumer_name.cmp(&b.consumer_name)
                }
                other => other,
            }
        });
    }

    /// 选择 Active Consumer
    fn select_active_consumer(&self, partition_index: i32) -> Option<Arc<Consumer>> {
        if self.consumers.is_empty() {
            return None;
        }

        // 找出最高优先级的消费者数量
        let highest_priority = self.consumers.first()?.priority_level;
        let high_priority_count = self.consumers.iter()
            .take_while(|c| c.priority_level == highest_priority)
            .count();

        // 根据分区索引选择
        let index = if partition_index >= 0 {
            (partition_index as usize) % high_priority_count
        } else {
            0
        };

        Some(self.consumers[index].clone())
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumers.iter().any(|c| c.consumer_id == consumer.consumer_id) {
            return Err(format!("Consumer {} already exists", consumer.consumer_id));
        }

        self.consumers.push(consumer);
        self.sort_consumers();

        // 检查 Active Consumer 是否变化
        self.check_active_consumer_change();

        Ok(())
    }
}
```

#### 建议 4: 实现 Pending Read 取消

**原因**: 主消费者切换时需要取消未完成的读取

**原生实现**: `PersistentDispatcherSingleActiveConsumer.java:150-154,293`

**实现思路**:

```rust
pub struct FailoverDispatcher {
    // ...
    have_pending_read: AtomicBool,
    pending_read_cancelled: AtomicBool,
}

impl FailoverDispatcher {
    fn cancel_pending_read(&self) -> bool {
        self.pending_read_cancelled.store(true, Ordering::SeqCst);
        self.have_pending_read.swap(false, Ordering::SeqCst)
    }

    async fn dispatch_messages(&self, ...) -> Result<...> {
        // 检查是否已被取消
        if self.pending_read_cancelled.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.have_pending_read.store(true, Ordering::SeqCst);

        // ... 分发逻辑

        self.have_pending_read.store(false, Ordering::SeqCst);
        self.pending_read_cancelled.store(false, Ordering::SeqCst);

        Ok(())
    }
}
```

#### 建议 5: 实现重投递未确认消息

**原因**: 允许 Active Consumer 请求重新投递未确认的消息

**原生实现**: `PersistentDispatcherSingleActiveConsumer.java:268-306`

**实现思路**:

```rust
impl FailoverDispatcher {
    /// 重投递未确认的消息
    ///
    /// 只有 Active Consumer 可以调用此方法
    pub fn redeliver_unacknowledged_messages(
        &mut self,
        consumer: &Arc<Consumer>,
        consumer_epoch: u64,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) {
        // 1. 检查是否是 Active Consumer
        let active = self.consumers.first();
        if active.is_none() || active.unwrap().consumer_id != consumer.consumer_id {
            log::warn!(
                "Ignoring redeliverUnacknowledgedMessages: Only active consumer can call"
            );
            return;
        }

        // 2. 检查是否在等待 failover 延迟
        if self.failover_task.is_some() {
            log::warn!(
                "Ignoring redeliverUnacknowledgedMessages: Waiting for cursor rewind"
            );
            return;
        }

        // 3. 取消 pending read
        self.cancel_pending_read();

        // 4. Rewind cursor
        tokio::spawn(async move {
            let mut guard = storage.lock().await;
            guard.rewind_cursor(&topic, &subscription).unwrap();
        });

        // 5. 触发重新读取
        // self.read_more_entries(...);
    }
}
```

### 4.2 建议实现 (中优先级)

#### 建议 6: 分区 Topic 支持

**原生实现**: `AbstractDispatcherSingleActiveConsumer.java:131-133`

```rust
pub struct FailoverDispatcher {
    // ...
    partition_index: i32,  // -1 表示非分区
}

impl FailoverDispatcher {
    fn select_active_consumer(&self) -> Option<Arc<Consumer>> {
        if self.consumers.is_empty() {
            return None;
        }

        let high_priority_count = self.get_high_priority_count();

        let index = if self.partition_index >= 0 {
            (self.partition_index as usize) % high_priority_count
        } else {
            0
        };

        Some(self.consumers[index].clone())
    }
}
```

#### 建议 7: Active Consumer 通知

**原生实现**: `AbstractDispatcherSingleActiveConsumer.java:91-96`

```rust
impl FailoverDispatcher {
    fn notify_active_consumer_changed(&self, active: Option<Arc<Consumer>>) {
        for consumer in &self.consumers {
            consumer.notify_active_consumer_change(active.clone());
        }
    }
}

// 在 consumer.rs 中添加
impl Consumer {
    pub fn notify_active_consumer_change(&self, active: Option<Arc<Consumer>>) {
        let is_active = active
            .map(|a| a.consumer_id == self.consumer_id)
            .unwrap_or(false);

        log::info!(
            "Consumer {} active state changed: {}",
            self.consumer_id,
            is_active
        );

        // 可以在这里触发客户端回调
    }
}
```

### 4.3 可选优化 (低优先级)

#### 建议 8: 一致性哈希选择

**原生实现**: `AbstractDispatcherSingleActiveConsumer.java:148-164`

```rust
use std::collections::BTreeMap;

struct HashRing {
    ring: BTreeMap<u32, usize>,
    virtual_nodes: usize,
}

impl HashRing {
    fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    fn add(&mut self, index: usize, name: &str) {
        for i in 0..self.virtual_nodes {
            let key = format!("{}{}", name, i);
            let hash = murmur3_hash(&key);
            self.ring.insert(hash, index);
        }
    }

    fn get(&self, topic_hash: u32) -> Option<usize> {
        if self.ring.is_empty() {
            return None;
        }

        if let Some((_, &index)) = self.ring.range(topic_hash..).next() {
            Some(index)
        } else {
            self.ring.values().next().copied()
        }
    }
}
```

---

## 5. Failover 检查清单

### 已实现功能
- [x] 主消费者消息分发
- [x] 消费者添加/移除
- [x] Flow 控制 (permits)
- [x] 批量大小限制
- [x] 消费者优先级排序（persistent）
- [x] Active Consumer 通知
- [x] Cursor Rewind（persistent SingleActive）
- [x] Standby 接管 active 未 ack backlog

### 缺失功能 (高优先级)
- [ ] **Failover 延迟切换** (1000ms)
- [ ] **Pending Read 取消**

### 缺失功能 (中优先级)
- [ ] **分区 Topic 主消费者分配**
- [ ] **一致性哈希选择**
- [ ] **Consumer Epoch**
- [ ] **Exclusive/Failover 显式 Redeliver 命令**（当前忽略，依赖 close + rewind）

### 缺失功能 (低优先级)
- [ ] Dispatch Rate Limiter
- [ ] Compacted Topic 支持
- [ ] Topic 转移检测

---

## 6. 参考文件

### Pulsar Lite
- `rust/src/broker/dispatcher/failover.rs` - Failover Dispatcher 实现
- `rust/src/broker/dispatcher/mod.rs` - Dispatcher trait 定义
- `rust/src/storage/mod.rs` - 存储层
- `rust/src/broker/service/consumer.rs` - Consumer 定义

### 原生 Pulsar
- `pulsar-broker/.../AbstractDispatcherSingleActiveConsumer.java` - 核心逻辑
- `pulsar-broker/.../PersistentDispatcherSingleActiveConsumer.java` - 持久化实现
- `pulsar-broker-common/.../ServiceConfiguration.java` - 配置项

---

## 7. 关键配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `activeConsumerFailoverDelayTimeMillis` | 1000 | Failover 延迟时间 |
| `activeConsumerFailoverConsistentHashing` | false | 是否启用一致性哈希 |
| `dispatcherMaxReadBatchSize` | 100 | 最大读取批量大小 |

---

## 8. 总结

### 8.1 核心差异

Pulsar Lite 的 Failover 实现是**面向 embedded 的 SingleActive MVP**，persistent 已具备 rewind 与 priority，仍缺少原生 Failover 的延迟切换与异步 read 取消：

| 最关键的剩余差距 | 影响 |
|-------------|------|
| **Failover 延迟** | 切换瞬间可能重复投递 |
| **Pending Read 取消** | 复杂并发下状态可能混乱 |
| **分区 / 一致性哈希选主** | 多分区场景与原生不一致 |

### 8.2 Pulsar Lite 定位

作为一个 **轻量级嵌入式消息队列**，Pulsar Lite 的 Failover 模式：
- ✅ Persistent 基本主备切换 + rewind 可用（`tests/persist/`）
- ⚠️ 无 Failover 延迟，生产高并发切换需谨慎
- ⚠️ 不支持分区 topic 专用选主策略
- ⚠️ 需要补充 Failover 延迟和 Cursor Rewind 才能用于正式场景

### 8.3 建议实施顺序

1. **第一阶段**: Failover 延迟 + Cursor Rewind (保证消息不丢失/不重复)
2. **第二阶段**: 消费者优先级 + Active 通知 (增强可控性)
3. **第三阶段**: 分区 Topic 支持 + 一致性哈希 (完善功能)
4. **第四阶段**: Dispatch Rate Limiter 等优化 (性能调优)
