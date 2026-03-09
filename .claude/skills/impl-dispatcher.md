---
name: impl-dispatcher
description: 实现新的订阅类型 Dispatcher
triggers:
  - /impl-dispatcher
  - "实现.*Dispatcher"
  - "添加.*订阅类型"
  - "Key_Shared"
---

# 实现新的 Dispatcher

## 目标
实现新的订阅类型分发器 (如 Key_Shared, 或自定义分发策略)。

## 输入参数
从用户输入中提取：
- `dispatcher_type`: 分发器类型
  - `key_shared` - Key_Shared 订阅
  - `exclusive` - Exclusive 订阅 (参考实现)
  - `custom` - 自定义分发器
- `base_on`: 参考实现 (可选: shared/failover)

## 订阅类型说明

| 类型 | 说明 | 分发策略 |
|------|------|----------|
| Exclusive | 独占订阅 | 单个消费者 |
| Shared | 共享订阅 | Round-Robin |
| Failover | 故障转移 | 主备切换 |
| Key_Shared | 键共享 | 按 Key 哈希 |

## 执行步骤

### Step 1: 分析现有实现

1. 读取 `rust/src/broker/dispatcher/mod.rs` 了解 Dispatcher trait
2. 读取 `rust/src/broker/dispatcher/shared.rs` 作为参考
3. 读取 `rust/src/broker/dispatcher/failover.rs` 作为参考
4. 提取核心接口和模式

### Step 2: 创建 Dispatcher 文件

创建文件: `rust/src/broker/dispatcher/${type}.rs`

核心结构:
```rust
pub struct ${type}Dispatcher {
    consumers: HashMap<u64, Arc<Consumer>>,
    total_available_permits: AtomicU32,
    // 特定字段...
}
```

### Step 3: 实现 Dispatcher Trait

必须实现的方法:
```rust
impl Dispatcher for ${type}Dispatcher {
    fn get_type(&self) -> SubscriptionType;
    fn is_consumer_connected(&self) -> bool;
    fn get_consumers(&self) -> Vec<Arc<Consumer>>;
    fn add_consumer(&mut self, consumer: Arc<Consumer>) -> Result<(), String>;
    fn remove_consumer(&mut self, consumer_id: u64) -> Option<Arc<Consumer>>;
    fn consumer_flow(&self, consumer_id: u64, additional_permits: u32);
    async fn dispatch_messages(...) -> Result<...>;
}
```

### Step 4: 注册 Dispatcher

1. 编辑 `rust/src/broker/dispatcher/mod.rs`:
```rust
mod ${type};
pub use ${type}::${type}Dispatcher;
```

2. 编辑 `rust/src/broker/dispatcher/enums.rs`:
```rust
pub enum SubscriptionType {
    Exclusive,
    Shared,
    Failover,
    KeyShared,  // 添加新类型
}
```

3. 编辑 `rust/src/broker/service/topic/subscription.rs`:
在 `new()` 方法中添加新类型的分发器创建逻辑

### Step 5: 更新协议处理

编辑 `rust/src/broker/handler/consumer_handler.rs`:
支持新的订阅类型参数

### Step 6: 生成测试

创建文件: `tests/test_${type}_dispatcher.py`

### Step 7: 更新文档

更新 `README.md` 和 `docs/PROJECT_OVERVIEW.md`

## Key_Shared 实现指南

### 核心算法

Key_Shared 订阅确保相同 Key 的消息总是发送到同一个消费者。

```rust
pub struct KeySharedDispatcher {
    consumers: HashMap<u64, Arc<Consumer>>,
    total_available_permits: AtomicU32,

    // Hash 环，用于一致性哈希
    hash_ring: HashRing<u64>,

    // 每个 Key 对应的消费者缓存
    key_to_consumer: HashMap<u32, u64>,

    // 待确认的 Key (阻塞中的 Key)
    pending_keys: HashMap<u32, u64>,  // key_hash -> consumer_id
}

impl KeySharedDispatcher {
    /// 计算消息 Key 的哈希值
    fn hash_key(&self, key: &[u8]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// 根据 Key 选择消费者
    fn get_consumer_by_key(&self, key: &[u8]) -> Option<Arc<Consumer>> {
        let hash = self.hash_key(key);

        // 检查缓存
        if let Some(consumer_id) = self.key_to_consumer.get(&hash) {
            if let Some(consumer) = self.consumers.get(consumer_id) {
                if consumer.get_available_permits().await > 0 {
                    return Some(consumer.clone());
                }
            }
        }

        // 使用一致性哈希选择
        let consumer_id = self.hash_ring.get(hash)?;
        self.consumers.get(&consumer_id).cloned()
    }

    /// 当消费者断开时，清理相关映射
    fn remove_consumer_mappings(&mut self, consumer_id: u64) {
        self.key_to_consumer.retain(|_, &mut cid| cid != consumer_id);
        self.pending_keys.retain(|_, &mut cid| cid != consumer_id);
        self.hash_ring.remove(consumer_id);
    }
}
```

### HashRing 实现

```rust
use std::collections::BTreeMap;

/// 一致性哈希环
pub struct HashRing<T> {
    ring: BTreeMap<u32, T>,
    virtual_nodes: usize,
}

impl<T: Clone + Eq> HashRing<T> {
    pub fn new(virtual_nodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes,
        }
    }

    /// 添加节点
    pub fn add(&mut self, id: T, hash_base: u32) {
        for i in 0..self.virtual_nodes {
            let hash = self.hash_virtual_node(hash_base, i);
            self.ring.insert(hash, id.clone());
        }
    }

    /// 移除节点
    pub fn remove(&mut self, hash_base: u32) {
        for i in 0..self.virtual_nodes {
            let hash = self.hash_virtual_node(hash_base, i);
            self.ring.remove(&hash);
        }
    }

    /// 获取 Key 对应的节点
    pub fn get(&self, key_hash: u32) -> Option<T> {
        if self.ring.is_empty() {
            return None;
        }

        // 找到第一个 >= key_hash 的节点
        if let Some((_, node)) = self.ring.range(key_hash..).next() {
            return Some(node.clone());
        }

        // 回绕到第一个节点
        self.ring.values().next().cloned()
    }

    fn hash_virtual_node(&self, base: u32, index: usize) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        (base, index).hash(&mut hasher);
        hasher.finish() as u32
    }
}
```

### 消息分发逻辑

```rust
impl Dispatcher for KeySharedDispatcher {
    async fn dispatch_messages(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.dispatch_in_progress.swap(true, Ordering::Relaxed) {
            return Ok(());
        }

        let result = self.dispatch_by_key(storage, topic, subscription).await;

        self.dispatch_in_progress.store(false, Ordering::Relaxed);
        result
    }
}

impl KeySharedDispatcher {
    async fn dispatch_by_key(
        &self,
        storage: SharedStorage,
        topic: String,
        subscription: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // 获取下一条消息
            let message = {
                let mut guard = storage.lock().await;
                guard.peek_next_message(&topic, &subscription)?
            };

            let Some((message_id, payload, key)) = message else {
                break;
            };

            // 根据 Key 选择消费者
            let Some(consumer) = self.get_consumer_by_key(&key) else {
                break;
            };

            // 检查 permits
            if consumer.get_available_permits().await == 0 {
                break;
            }

            // 检查 Key 是否被阻塞 (有未确认的消息)
            let key_hash = self.hash_key(&key);
            if self.pending_keys.contains_key(&key_hash) {
                // 跳过这个消息，等待 Key 解除阻塞
                break;
            }

            // 分发消息
            consumer.use_permit().await;
            self.total_available_permits.fetch_sub(1, Ordering::Relaxed);

            {
                let mut guard = storage.lock().await;
                guard.assign_message(&topic, &subscription, &message_id, consumer.consumer_id)?;
            }

            consumer.enqueue_message(message_id, payload).await;

            // 记录 Key 到消费者的映射
            self.key_to_consumer.insert(key_hash, consumer.consumer_id);
            self.pending_keys.insert(key_hash, consumer.consumer_id);
        }

        Ok(())
    }
}
```

### Ack 处理

当消息被确认时，需要解除 Key 的阻塞:

```rust
impl KeySharedDispatcher {
    pub fn ack_message(&mut self, key: &[u8]) {
        let key_hash = self.hash_key(key);
        self.pending_keys.remove(&key_hash);
    }
}
```

## 测试用例

```python
# tests/test_key_shared_dispatcher.py
import pytest
import pulsar


class TestKeySharedDispatcher:
    """Test Key_Shared subscription"""

    def test_key_affinity(self):
        """Test that same key goes to same consumer"""
        client = pulsar.Client("pulsar://localhost:6650")
        producer = client.create_producer("test-kstopic")

        # 创建多个消费者
        consumers = []
        for i in range(3):
            c = client.subscribe(
                "test-kstopic",
                "ks-sub",
                consumer_type=pulsar.ConsumerType.KeyShared,
                consumer_name=f"consumer-{i}"
            )
            consumers.append(c)

        # 发送相同 Key 的消息
        for i in range(10):
            producer.send(
                f"message-{i}".encode(),
                partition_key="key-A"
            )

        # 验证所有相同 Key 的消息都到了同一个消费者
        # ...

        producer.close()
        for c in consumers:
            c.close()
        client.close()

    def test_key_distribution(self):
        """Test that different keys are distributed"""
        pass

    def test_consumer_failure(self):
        """Test key rebalancing when consumer fails"""
        pass
```

## 检查清单

- [ ] Dispatcher 文件已创建
- [ ] Dispatcher trait 已实现
- [ ] mod.rs 已更新
- [ ] SubscriptionType 枚举已更新
- [ ] Subscription 创建逻辑已更新
- [ ] 协议处理已更新
- [ ] 测试已创建
- [ ] 文档已更新
- [ ] `cargo test` 通过
- [ ] `pytest tests/` 通过

## 示例调用

```
用户: /impl-dispatcher key_shared

执行:
1. 分析 SharedDispatcher 作为参考
2. 创建 KeySharedDispatcher
3. 实现 HashRing
4. 实现 Key 亲和性分发
5. 注册到模块系统
6. 生成测试
7. 更新文档
```
