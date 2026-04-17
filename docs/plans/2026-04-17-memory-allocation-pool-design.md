# Consumer Dispatch Memory Optimization Design

## 目标

将 consumer dispatch 路径的消息传递机制从 unbounded channel 改为 permit-based bounded channel + try_send + drop，对齐 Apache Pulsar Java 的 non-persistent dispatcher 设计，解决内存暴涨和 CPU 热点问题。

## 性能瓶颈排查过程

### 第一步：Stress 压测暴露问题

通过 `tests/perf/run_non_persistent_stress.py` 运行 9 个不限速场景，发现 consumer 侧存在严重问题：

- Shared 1 consumer：RSS 从 9MB 涨到 **175MB**
- 8 subscriptions：RSS 涨到 **599MB**
- 5min sustained：RSS 涨到 **720MB**，CPU 114%

而 producer 侧 RSS 稳定在 13MB 不增长。说明问题只在 consumer dispatch 路径。

### 第二步：Broker CPU/RSS 时序数据确认趋势

通过 broker_timeseries.csv 时序数据确认：

- Producer sustained：RSS 全程稳定在 12-13MB，无增长趋势
- Consumer sustained：RSS 从 500MB 持续线性增长到 822MB

线性增长 = 内存泄漏/积压，不是一次性分配。消费速度跟不上生产速度，消息在 broker 内部堆积。

### 第三步：Flamegraph 定位热点函数

使用 `perf record -F 99 -g -p <broker_pid>` 采集 CPU profile，生成 flamegraph。关键发现：

| 场景 | malloc/free 占比 | 热点函数 |
|---|---|---|
| Consumer sustained | **~90%** | `__munmap`, `sysmalloc_mmap`, `_int_free` |
| Consumer multi-sub | **~45%** | `__munmap`, `_int_free` |
| Producer sustained | **~80%** | `malloc`, `_int_malloc` |

Consumer sustained 中 `__munmap` 独占 383M 样本，说明大量内存块被分配后又释放回操作系统。这不是正常的小对象分配，而是 mmap 级别的大块内存反复分配/释放。

### 第四步：代码审查定位根因

通过审查 consumer dispatch 代码路径，定位到两个结构性问题：

**问题 1：unbounded channel**

`consumer.rs` 中使用 `mpsc::unbounded_channel()` 连接 dispatcher 和 consumer connection task。当消费速度 < 生产速度时，消息无限堆积在 channel 中，每条消息持有 `Bytes` 引用，导致 RSS 持续增长。

```rust
// 当前实现
let (tx, _rx) = mpsc::unbounded_channel();  // 无上限
```

**问题 2：无 permit 流控**

dispatcher 在 `send_messages()` 中不检查 consumer 的消费能力，只要有 consumer 就无条件发送。慢 consumer 的消息不会 drop，全部堆积在 channel 里。

### 第五步：参考 Apache Pulsar Java 实现

查阅 Pulsar Java 源码（`NonPersistentDispatcherMultipleConsumers.java`），确认其设计：

- **无 channel/queue** — dispatcher 直接调 `consumer.sendMessages()`
- **Permit-based flow control** — 用 `AtomicInteger` 计数器，`totalAvailablePermits > 0` 才发
- **Non-persistent = 直接 drop** — permits 耗尽时 `entry.release()` + 记录 `msgDrop` 计数
- **无 backpressure 阻塞** — dispatcher 永远不等待 consumer

Pulsar Java 的设计与 pulsar-lite 当前实现的对比：

| | Pulsar Java | Pulsar-Lite 当前 |
|---|---|---|
| 消息传递 | 直接调用，无 channel | unbounded channel |
| 流控 | permit + drop | 无 |
| 慢 consumer 处理 | 消息 drop | 消息堆积到 720MB |
| 内存上限 | 由 permits 控制 | 无限增长 |

### 结论

性能瓶颈的根因是：**consumer 使用 unbounded channel + 无 permit 流控**，导致消息在 broker 内部无限堆积。这不是某个函数的性能问题，而是消息传递架构的缺陷。

## 设计方案

### 核心变更

将 consumer 的消息传递从 unbounded channel 改为 bounded channel + permit-based flow control + try_send + drop。

### 数据流变更

**当前**：
```
dispatcher.send_messages()
  → for each consumer:
      consumer.send_message()
        → PendingMessage(Bytes, Bytes)
        → message_tx.send() → unbounded channel (无限堆积)
```

**改为**：
```
dispatcher.send_messages()
  → for each consumer:
      check available_permits > 0 && is_writable
      → consumer.send_message()
        → PendingMessage(Bytes, Bytes)
        → message_tx.try_send() → bounded channel (size=1024)
        → if Full: dropped_messages += 1, release entry, return false
```

### 具体改动点

#### 1. `rust/src/broker/service/consumer.rs`

- `new()` 中 `mpsc::unbounded_channel()` → `mpsc::channel(CHANNEL_CAPACITY)`
- `message_tx` 类型从 `UnboundedSender<(u64, PendingMessage)>` → `Sender<(u64, PendingMessage)>`
- `message_rx` 类型从 `UnboundedReceiver` → `Receiver`
- `send_message()` 中 `.send()` → `.try_send()`
- `try_send` 返回 `Err(TrySendError::Full)` 时：递增 `dropped_messages`，return false
- `try_send` 返回 `Err(TrySendError::Closed)` 时：return false（consumer 已关闭）
- 新增 `available_permits(&self) -> usize` 方法
- 新增 `is_writable(&self) -> bool` 方法

#### 2. `rust/src/broker/non_persistent/dispatcher/multiple_consumers.rs`

- `send_messages()` 中发送前检查 `consumer.available_permits() > 0`
- 如果 permits 不足，跳过该 consumer
- 如果 `try_send` 返回 false（Full 或 Closed），记录 drop 并 release entry

#### 3. `rust/src/broker/non_persistent/dispatcher/single_active_consumer.rs`

- 同理，发前检查 active consumer 的 permits
- permits = 0 时 drop + release

#### 4. 不改动的部分

- `PendingMessage` 结构不变（Bytes 已经是共享引用）
- `ServerCnx` 不变（它只从 channel 消费端 recv）
- 协议编码不变
- Producer 路径不变

### CHANNEL_CAPACITY 参数

建议初始值 `1024`，即每个 consumer 最多缓冲 1024 条消息。理由：

- Pulsar Java 的 `receiverQueueSize` 默认值是 1000
- 1024 是 2 的幂，对内存分配器友好
- 后续可通过 config 文件暴露为可配置参数

### Drop 语义

对齐 Pulsar Java 的 non-persistent 语义：

- 消息被 drop 是正常行为，不是错误
- `dropped_messages` 计数器已经在 `Subscription::get_stats()` 中暴露
- 客户端不会收到被 drop 的消息
- Drop 后 entry 被立即 release，不占用内存

## 成功标准

1. **RSS 不再暴涨** — consumer 场景 peak RSS < 50MB（当前 720MB）
2. **Drop 计数可观测** — `dropped_messages` 准确反映被丢弃的消息数
3. **Dispatcher 不阻塞** — 慢 consumer 不影响其他 consumer 的消息分发
4. **吞吐保持或提升** — consumer 吞吐不低于当前 151K msg/s
5. **现有测试全部通过** — Python 集成测试 + Rust 单元测试
