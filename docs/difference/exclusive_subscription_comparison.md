# Exclusive 订阅实现对比分析

> 分析日期: 2026-03-09
> 对比版本: Pulsar Lite (当前实现) vs Apache Pulsar (官方实现)

---

## 1. 核心逻辑对比

### 1.1 Pulsar Lite 实现

**文件**: `rust/src/broker/dispatcher/exclusive.rs`

```rust
/// Exclusive mode dispatcher
pub struct ExclusiveDispatcher {
    /// The single consumer for this exclusive subscription
    consumer: Option<Arc<Consumer>>,

    /// Total available permits (for the single consumer)
    total_available_permits: AtomicU32,
}

impl Dispatcher for ExclusiveDispatcher {
    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        // 简单检查：如果已有消费者，拒绝新消费者
        if self.consumer.is_some() {
            return Err("Exclusive subscription already has a consumer".to_string());
        }
        self.consumer = Some(consumer);
        Ok(())
    }

    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(ref consumer) = self.consumer {
            if consumer.consumer_id == consumer_id {
                return self.consumer.take();
            }
        }
        None
    }

    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32) {
        if let Some(ref consumer) = self.consumer {
            if consumer.consumer_id == consumer_id {
                self.total_available_permits.fetch_add(additional_permits, Ordering::Relaxed);
            }
        }
    }

    async fn dispatch_messages(&self, ...) {
        if let Some(consumer) = &self.consumer {
            let available_permits = self.total_available_permits.load(Ordering::Relaxed);
            if available_permits == 0 {
                return Ok(());
            }

            let max_messages = std::cmp::min(available_permits, DISPATCHER_MAX_ROUND_ROBIN_BATCH_SIZE);

            for _ in 0..max_messages {
                if !consumer.use_permit().await {
                    break;
                }
                // ... 获取和分发消息
            }
        }
    }
}
```

**分析**:
- ✅ 基本的独占消费者检查
- ✅ Flow 控制支持
- ✅ 批量消息分发
- ❌ **无连接活性检查**
- ❌ **无 ConsumerBusyException 细化**
- ❌ **无 Key Hash Range 过滤**
- ❌ **无消费者断开重试机制**

### 1.2 原生 Pulsar 实现

**文件**: `AbstractDispatcherSingleActiveConsumer.java`

原生 Pulsar 的 Exclusive 订阅与 Failover 订阅共用同一个 Dispatcher (`PersistentDispatcherSingleActiveConsumer`)，但在添加消费者时有特殊逻辑：

```java
// 添加消费者时的 Exclusive 特殊处理
private synchronized CompletableFuture<Void> internalAddConsumer(Consumer consumer, int retryCount) {
    if (IS_CLOSED_UPDATER.get(this) == TRUE) {
        consumer.disconnect();
        return CompletableFuture.completedFuture(null);
    }

    // ========== Exclusive 订阅的特殊逻辑 ==========
    if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
        Consumer actConsumer = getActiveConsumer();
        if (actConsumer != null) {
            final var callerThread = Thread.currentThread();

            // 1. 检查现有消费者的连接是否存活
            return actConsumer.cnx().checkConnectionLiveness().thenCompose(actConsumerStillAlive -> {
                if (actConsumerStillAlive.isEmpty() || actConsumerStillAlive.get()) {
                    // 1a. 连接存活：拒绝新消费者
                    return FutureUtil.failedFuture(
                        new ConsumerBusyException("Exclusive consumer is already connected"));
                } else if (retryCount >= MAX_RETRY_COUNT_FOR_ADD_CONSUMER_RACE) {
                    // 1b. 重试次数超限：拒绝新消费者
                    log.warn("[{}] The active consumer's connection is still inactive after all retries",
                            getName(), actConsumer, consumer);
                    return FutureUtil.failedFuture(
                        new ConsumerBusyException("Exclusive consumer is already connected after "
                            + MAX_RETRY_COUNT_FOR_ADD_CONSUMER_RACE + " attempts"));
                } else {
                    // 1c. 连接不活跃：等待后重试
                    if (Thread.currentThread().equals(callerThread)) {
                        // 处理 ServerCnx#channelInactive 中的竞态条件
                        log.warn("[{}] race condition happened that cnx of the active consumer ({}) "
                                + "is inactive but it's not removed, retrying", getName(), actConsumer);
                        final var future = new CompletableFuture<Void>();
                        CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
                                .execute(() -> future.complete(null));
                        return future.thenCompose(__ -> internalAddConsumer(consumer, retryCount + 1));
                    } else {
                        return internalAddConsumer(consumer, retryCount + 1);
                    }
                }
            });
        } else {
            return FutureUtil.failedFuture(new ConsumerBusyException(
                "Active consumer is in a strange state. Active consumer is null, but there are "
                + consumers.size() + " registered."));
        }
    }
    // ========== Exclusive 特殊逻辑结束 ==========

    // ========== Key Hash Range 过滤 (Exclusive 特有) ==========
    if (subscriptionType == SubType.Exclusive
            && consumer.getKeySharedMeta() != null
            && consumer.getKeySharedMeta().getHashRangesList() != null
            && consumer.getKeySharedMeta().getHashRangesList().size() > 0) {
        // 创建 Hash Range 选择器
        stickyKeyConsumerSelector = new HashRangeExclusiveStickyKeyConsumerSelector();
        stickyKeyConsumerSelector.addConsumer(consumer);
        isKeyHashRangeFiltered = true;
    } else {
        isKeyHashRangeFiltered = false;
    }
    // ========== Key Hash Range 过滤结束 ==========

    if (consumers.isEmpty()) {
        isFirstRead = true;
    }

    consumers.add(consumer);

    if (!pickAndScheduleActiveConsumer()) {
        Consumer currentActiveConsumer = getActiveConsumer();
        if (null == currentActiveConsumer) {
            log.debug("Current active consumer disappears while adding consumer {}", consumer);
        } else {
            consumer.notifyActiveConsumerChange(currentActiveConsumer);
        }
    }

    return CompletableFuture.completedFuture(null);
}
```

**HashRangeExclusiveStickyKeyConsumerSelector** (Exclusive 特有功能):

```java
/**
 * 基于 Hash Range 的消费者选择器
 * 用于 Exclusive 订阅，允许消费者只接收特定 Hash Range 的消息
 */
public class HashRangeExclusiveStickyKeyConsumerSelector implements StickyKeyConsumerSelector {
    private final int rangeSize;  // 默认 65536
    private final ConcurrentSkipListMap<Integer, Pair<Range, Consumer>> rangeMap;

    @Override
    public Consumer select(int hash) {
        if (rangeMap.isEmpty()) {
            return null;
        }

        // 根据 Hash 值查找对应的消费者
        Map.Entry<Integer, Pair<Range, Consumer>> floorEntry = rangeMap.floorEntry(hash);
        if (floorEntry == null) {
            return null;
        }
        Pair<Range, Consumer> pair = floorEntry.getValue();
        if (pair.getLeft().contains(hash)) {
            return pair.getRight();
        } else {
            return null;  // Hash 不在任何 Range 内
        }
    }

    @Override
    public synchronized CompletableFuture<Optional<ImpactedConsumersResult>> addConsumer(Consumer consumer) {
        // 验证 KeySharedMeta
        // 检查 Range 冲突
        // 添加到 rangeMap
    }
}
```

**消息过滤逻辑** (在 `readEntriesComplete` 中):

```java
// 如果启用了 Key Hash Range 过滤
if (isKeyHashRangeFiltered) {
    Iterator<Entry> iterator = entries.iterator();
    while (iterator.hasNext()) {
        Entry entry = iterator.next();
        byte[] key = peekStickyKey(entry);
        Consumer consumer = stickyKeyConsumerSelector.select(key);
        // 跳过不属于当前消费者的消息
        if (consumer == null || currentConsumer != consumer) {
            entry.release();
            iterator.remove();
        }
    }
}
```

**分析**:
- ✅ 连接活性检查 (防止"幽灵"消费者)
- ✅ 重试机制 (处理竞态条件)
- ✅ 详细的 ConsumerBusyException
- ✅ **Key Hash Range 过滤** (Exclusive 特有)
- ✅ 消费者变更通知

---

## 2. 差异矩阵

| 特性 | Pulsar Lite | 原生 Pulsar | 影响等级 |
|------|-------------|-------------|----------|
| **基本独占检查** | ✅ 简单检查 | ✅ 完整检查 | 低 |
| **消息分发** | ✅ 单消费者 | ✅ 单消费者 | 低 |
| **Flow 控制** | ✅ Permits | ✅ Permits | 低 |
| **批量限制** | ✅ 20 条 | ✅ 可配置 | 低 |
| **连接活性检查** | ❌ 无 | ✅ checkConnectionLiveness | **高** |
| **竞态条件处理** | ❌ 无 | ✅ 重试 5 次 | **高** |
| **ConsumerBusyException** | ⚠️ 简单消息 | ✅ 详细错误 | 中 |
| **Key Hash Range 过滤** | ❌ 无 | ✅ 完整支持 | **高** |
| **Sticky Key Selector** | ❌ 无 | ✅ HashRangeExclusive | **高** |
| **消费者通知** | ❌ 无 | ✅ notifyActiveConsumerChange | 低 |
| **Unsubscribe 检查** | ❌ 无检查 | ✅ canUnsubscribe | 低 |
| **Cursor Rewind** | ❌ 无 | ✅ 消费者切换时 | **高** |

---

## 3. 缺失功能

### 3.1 高优先级 (影响生产可用性)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **连接活性检查** | 高 | 僵尸消费者问题 | 需要检查现有消费者的连接是否存活 |
| **竞态条件处理** | 高 | 连接断开时的状态不一致 | 消费者连接断开但未完全移除时的处理 |
| **Key Hash Range 过滤** | 高 | 功能缺失 | Exclusive 订阅支持 Hash Range 过滤 |
| **Cursor Rewind** | 高 | 消费者重连后消息丢失 | 消费者断开后重连需要从正确位置读取 |

### 3.2 中优先级 (影响功能完整性)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **详细错误信息** | 中 | 调试困难 | ConsumerBusyException 需要更详细的信息 |
| **Unsubscribe 检查** | 中 | 安全问题 | 只有真正的消费者才能 unsubscribe |

### 3.3 低优先级 (可选优化)

| 功能 | 优先级 | 影响 | 描述 |
|------|--------|------|------|
| **消费者通知** | 低 | 状态感知 | 通知消费者它是唯一的消费者 |
| **Dispatch Rate Limiter** | 低 | 流量控制 | 消息分发速率限制 |

---

## 4. 关键差异详解

### 4.1 连接活性检查 (最重要)

**问题场景**:
1. 消费者 A 连接到 Exclusive 订阅
2. 消费者 A 的网络连接断开，但 TCP 连接尚未完全关闭
3. 消费者 B 尝试连接

**Pulsar Lite 行为**:
```
消费者 B 连接 → 检查 consumer.is_some() → true → 返回错误
```
此时消费者 A 已经实际断开，但 Pulsar Lite 不知道，消费者 B 无法连接。

**原生 Pulsar 行为**:
```
消费者 B 连接 → 检查 consumers 不为空 →
检查消费者 A 的连接活性 (checkConnectionLiveness) →
连接已断开 → 等待 100ms 重试 →
消费者 A 被移除 → 消费者 B 成功连接
```

### 4.2 Key Hash Range 过滤 (Exclusive 特有)

**用途**: 允许 Exclusive 订阅只接收特定 Hash Range 的消息，实现手动分片。

**原生 Pulsar 用法**:
```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("my-topic")
    .subscriptionName("my-sub")
    .subscriptionType(SubscriptionType.Exclusive)
    .keySharedMeta(KeySharedMode.STICKY, Range.of(0, 16383))  // 只处理 hash 0-16383
    .subscribe();
```

**实现逻辑**:
1. 消费者订阅时指定 Hash Range
2. Broker 创建 `HashRangeExclusiveStickyKeyConsumerSelector`
3. 分发消息时，根据消息 Key 的 Hash 值过滤
4. 只分发 Hash 值在消费者 Range 内的消息

**Pulsar Lite**: 完全不支持此功能。

---

## 5. 改进建议

### 5.1 必须实现 (高优先级)

#### 建议 1: 添加连接活性检查

**原因**: 防止"僵尸"消费者阻止新消费者连接

**实现思路**:

```rust
pub struct ExclusiveDispatcher {
    consumer: Option<Arc<Consumer>>,
    total_available_permits: AtomicU32,
    max_retry_count: u32,  // 默认 5
}

impl ExclusiveDispatcher {
    pub async fn add_consumer_with_retry(
        &mut self,
        new_consumer: Arc<Consumer>,
        retry_count: u32,
    ) -> Result<(), String> {
        // 如果没有现有消费者，直接添加
        if self.consumer.is_none() {
            self.consumer = Some(new_consumer);
            return Ok(());
        }

        let existing = self.consumer.as_ref().unwrap();

        // 检查现有消费者的连接活性
        let is_alive = existing.check_connection_liveness().await;

        if is_alive {
            // 连接存活，拒绝新消费者
            return Err(format!(
                "Exclusive consumer is already connected (consumer_id={})",
                existing.consumer_id
            ));
        }

        // 连接不活跃
        if retry_count >= self.max_retry_count {
            return Err(format!(
                "Exclusive consumer's connection is still inactive after {} attempts",
                self.max_retry_count
            ));
        }

        // 等待 100ms 后重试 (给现有消费者时间完成断开)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // 递归重试
        self.add_consumer_with_retry(new_consumer, retry_count + 1).await
    }
}

// 在 Consumer 中添加
impl Consumer {
    pub async fn check_connection_liveness(&self) -> bool {
        // 检查连接是否仍然活跃
        // 可以通过发送心跳或检查 TCP 状态实现
        self.connection.is_active()
    }
}
```

**参考代码**: `AbstractDispatcherSingleActiveConsumer.java:177-206`

#### 建议 2: 添加 Cursor Rewind 支持

**原因**: 消费者断开后重连需要从正确位置读取

**实现思路**:

```rust
impl ExclusiveDispatcher {
    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>> {
        if let Some(ref consumer) = self.consumer {
            if consumer.consumer_id == consumer_id {
                let removed = self.consumer.take();

                // 重置 permits
                self.total_available_permits.store(0, Ordering::Relaxed);

                // 注意：Cursor rewind 应该在下次添加消费者时触发
                // 或者在外部调用中处理

                return removed;
            }
        }
        None
    }

    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if self.consumer.is_some() {
            return Err("Exclusive subscription already has a consumer".to_string());
        }

        self.consumer = Some(consumer);

        // 标记需要 rewind (实际 rewind 在 dispatch 时执行)
        self.need_rewind = true;

        Ok(())
    }
}
```

#### 建议 3: 添加 Key Hash Range 过滤

**原因**: 支持 Exclusive 订阅的 Hash Range 过滤功能

**实现思路**:

```rust
/// Hash Range 定义
pub struct HashRange {
    pub start: u32,
    pub end: u32,
}

impl HashRange {
    pub fn contains(&self, hash: u32) -> bool {
        hash >= self.start && hash <= self.end
    }
}

pub struct ExclusiveDispatcher {
    consumer: Option<Arc<Consumer>>,
    total_available_permits: AtomicU32,

    // 新增：Hash Range 过滤
    hash_ranges: Vec<HashRange>,
    is_hash_range_filtered: bool,
}

impl ExclusiveDispatcher {
    /// 检查消息是否应该被分发
    fn should_dispatch(&self, key: Option<&[u8]>) -> bool {
        if !self.is_hash_range_filtered {
            return true;  // 未启用过滤，分发所有消息
        }

        if key.is_none() {
            return false;  // 启用过滤但没有 Key，不分发
        }

        let hash = self.hash_key(key.unwrap());

        // 检查 Hash 是否在任何 Range 内
        self.hash_ranges.iter().any(|r| r.contains(hash))
    }

    fn hash_key(&self, key: &[u8]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() & 0xFFFF) as u32  // 取低 16 位
    }

    async fn dispatch_messages(&self, ...) {
        if let Some(consumer) = &self.consumer {
            // ... 获取消息

            if let Some((message_id, payload, key)) = message_opt {
                // 检查 Hash Range
                if !self.should_dispatch(key.as_deref()) {
                    // 消息不在当前消费者的 Hash Range 内
                    // 需要跳过这条消息
                    continue;
                }

                consumer.enqueue_message(message_id, payload.clone()).await;
            }
        }
    }
}
```

**参考代码**: `HashRangeExclusiveStickyKeyConsumerSelector.java`

### 5.2 建议实现 (中优先级)

#### 建议 4: 改进错误信息

```rust
impl ExclusiveDispatcher {
    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String> {
        if let Some(ref existing) = self.consumer {
            return Err(format!(
                "Exclusive subscription already has a consumer: \
                 consumer_id={}, consumer_name={}, connection_id={}",
                existing.consumer_id,
                existing.consumer_name,
                existing.connection_id
            ));
        }
        self.consumer = Some(consumer);
        Ok(())
    }
}
```

#### 建议 5: 添加 Unsubscribe 检查

```rust
impl ExclusiveDispatcher {
    pub fn can_unsubscribe(&self, consumer_id: u64) -> bool {
        match &self.consumer {
            Some(consumer) => consumer.consumer_id == consumer_id,
            None => false,
        }
    }
}
```

---

## 6. Exclusive vs Failover 对比

虽然两者都用单消费者模式，但有关键差异：

| 特性 | Exclusive | Failover |
|------|-----------|----------|
| **消费者数量** | 严格 1 个 | 1 主 + N 备 |
| **新消费者加入** | 拒绝 (除非旧消费者断开) | 成为备用消费者 |
| **消费者优先级** | 不适用 | 决定谁是主消费者 |
| **Hash Range 过滤** | ✅ 支持 | ❌ 不支持 |
| **Failover 延迟** | 不适用 | 1000ms |
| **Active Consumer 通知** | 不需要 | 需要通知主/备状态 |

---

## 7. Exclusive 检查清单

### 已实现功能
- [x] 独占消费者检查
- [x] 单消费者消息分发
- [x] Flow 控制 (permits)
- [x] 批量消息限制
- [x] 消费者移除

### 缺失功能 (高优先级)
- [ ] **连接活性检查**
- [ ] **竞态条件处理 (重试机制)**
- [ ] **Key Hash Range 过滤**
- [ ] **Cursor Rewind**

### 缺失功能 (中优先级)
- [ ] 详细错误信息
- [ ] Unsubscribe 权限检查

### 缺失功能 (低优先级)
- [ ] 消费者状态通知
- [ ] Dispatch Rate Limiter

---

## 8. 参考文件

### Pulsar Lite
- `rust/src/broker/dispatcher/exclusive.rs` - Exclusive Dispatcher 实现
- `rust/src/broker/dispatcher/mod.rs` - Dispatcher trait 定义
- `rust/src/broker/service/consumer.rs` - Consumer 定义

### 原生 Pulsar
- `pulsar-broker/.../AbstractDispatcherSingleActiveConsumer.java` - 核心逻辑 (Exclusive 和 Failover 共用)
- `pulsar-broker/.../HashRangeExclusiveStickyKeyConsumerSelector.java` - Hash Range 选择器
- `pulsar-broker/.../PersistentDispatcherSingleActiveConsumer.java` - 持久化实现

---

## 9. 总结

### 9.1 核心差异

Pulsar Lite 的 Exclusive 实现是**最简版本**，功能相对完整但缺少关键的健壮性机制：

| 最关键的缺失 | 影响 |
|-------------|------|
| **连接活性检查** | 僵尸消费者阻止新连接 |
| **竞态条件处理** | 断开/连接同时发生时状态混乱 |
| **Key Hash Range** | 缺少 Hash 分片功能 |

### 9.2 Pulsar Lite 定位

- ✅ 基本的 Exclusive 订阅可用
- ✅ 适合开发测试场景
- ⚠️ 生产环境需要添加连接活性检查
- ⚠️ 不支持 Hash Range 过滤 (这是高级功能)

### 9.3 建议实施顺序

1. **第一阶段**: 连接活性检查 + 竞态处理 (保证基本可用性)
2. **第二阶段**: 详细错误信息 (改善调试体验)
3. **第三阶段**: Cursor Rewind (可选，取决于存储实现)
4. **第四阶段**: Key Hash Range 过滤 (高级功能，按需实现)

### 9.4 与 Failover 的关系

Exclusive 和 Failover 在原生 Pulsar 中共用 `PersistentDispatcherSingleActiveConsumer`，但：
- **Exclusive**: 不需要优先级、不需要通知、支持 Hash Range
- **Failover**: 需要优先级、需要通知、需要 Failover 延迟

Pulsar Lite 采用分离的 Dispatcher 实现是合理的设计选择。
