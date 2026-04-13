# Non-Persistent Copy Path 优化记录

## 文档目的

本文档用于记录 non-persistent metadata / payload 拷贝路径这一阶段的优化前基线数据与后续对比结果。

这一阶段的出发点是：前两个阶段已经分别验证了

- Shared dispatcher selection path 优化没有体现稳定明显收益；
- topic fanout 按 subscription 批量化后，Shared topic dispatch 明显下降；

因此当前 Shared 的主要热点已经进一步收敛到 send path 更下游的拷贝链路，尤其是 dispatcher 把 `NonPersistentEntry` 中的 `Bytes` 转成 `Vec<u8>`，再通过 `PendingMessage` 沿 channel 传给连接层这段路径。

## 测试环境

- 仓库：`pulsar-lite`
- 分支：`perf/non-persistent-copy-path`
- 测试命令：

```bash
cargo test perf_copy_path --manifest-path rust/Cargo.toml -- --ignored --nocapture --test-threads=1
```

说明：

- 这是一组 copy-path stress baseline，用于放大当前 metadata / payload materialize 的开销；
- 当前不作为 CI 阈值，只用于记录优化前后相对差异；
- 对比对象是当前分支优化前后，不是原生 Pulsar；
- 当前基线结果统一采用：
  - 预热 `5` 次
  - 正式运行 `10` 次
  - 去掉最高值和最低值后，对剩余 `8` 次求平均

## 基线场景

### Shared dispatcher copy path

- consumers：`32`
- entries：`10_000`
- metadata：固定 `256 B`
- payload：固定 `4 KiB`
- permits：每个 consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开
- 计时范围：仅统计 `NonPersistentDispatcherMultipleConsumers::send_messages()`

### KeyShared dispatcher copy path / AutoSplit

- consumers：`32`
- entries：`10_000`
- key 数量：`128` 个轮转 ordering key
- metadata：包含 ordering key，并额外注入 `256 B` 的 `producer_name` padding
- payload：固定 `4 KiB`
- permits：每个 consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开
- 计时范围：仅统计 `NonPersistentStickyKeyDispatcher::send_messages()`

## 优化前基线结果

### Shared dispatcher copy path

- 当前结果：consumers=`32`，entries=`10_000`，metadata=`256 B`，payload=`4 KiB`：`50.75 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`62, 49, 47, 48, 47, 46, 51, 57, 56, 51`

### KeyShared dispatcher copy path / AutoSplit

- 当前结果：consumers=`32`，entries=`10_000`，metadata=`ordering_key + 256 B padding`，payload=`4 KiB`：`61.25 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`69, 62, 58, 57, 58, 60, 58, 61, 71, 64`

## 当前优化结果

### Shared dispatcher copy path

- 优化前基线：consumers=`32`，entries=`10_000`，metadata=`256 B`，payload=`4 KiB`：`50.75 ms`
- 当前结果：consumers=`32`，entries=`10_000`，metadata=`256 B`，payload=`4 KiB`：`16.88 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`16, 17, 18, 17, 18, 17, 17, 16, 17, 16`

### KeyShared dispatcher copy path / AutoSplit

- 优化前基线：consumers=`32`，entries=`10_000`，metadata=`ordering_key + 256 B padding`，payload=`4 KiB`：`61.25 ms`
- 当前结果：consumers=`32`，entries=`10_000`，metadata=`ordering_key + 256 B padding`，payload=`4 KiB`：`24.13 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`24, 25, 24, 26, 24, 25, 24, 24, 23, 23`

## 当前实现分析

本轮优化前，Shared dispatcher 在构造待发送 batch 时，仍然会把 `NonPersistentEntry` 中的共享 `Bytes` 物化为新的 `Vec<u8>`：

```rust
batch_messages.push((
    MessageId {
        ledger: batch_entry.ledger_id(),
        entry: batch_entry.entry_id(),
        partition: batch_entry.partition(),
    },
    batch_entry.metadata().to_vec(),
    batch_entry.payload().to_vec(),
    0,
));
```

这意味着：

- metadata 每条消息都会重新分配并拷贝一次；
- payload 每条消息都会重新分配并拷贝一次；
- 即使 `NonPersistentEntry` 底层已经是共享 `Bytes`，dispatcher 这一层仍然会把它 materialize 成新的堆内存。

KeyShared dispatcher 优化前也沿用同样的拷贝方式：

```rust
batch_messages.push((
    MessageId {
        ledger: entry.ledger_id(),
        entry: entry.entry_id(),
        partition: entry.partition(),
    },
    entry.metadata().to_vec(),
    entry.payload().to_vec(),
    0,
));
```

也就是说，selection path 优化之后，KeyShared 当前更重的部分已经明显转向：

- metadata decode / sticky key 解析
- 以及后续这段统一的 metadata / payload 复制路径

更下游的 `Consumer` 优化前也仍然以 `Vec<u8>` 为消息传递载体：

```rust
pub struct PendingMessage {
    pub message_id: MessageId,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}

pub async fn send_message(
    &self,
    message_id: MessageId,
    metadata: Vec<u8>,
    payload: Vec<u8>,
    redelivery_count: u32,
) -> bool
```

优化前调用链是：

```text
NonPersistentEntry(Bytes)
-> dispatcher.to_vec()
-> PendingMessage(Vec<u8>)
-> consumer channel
-> ServerCnx encode
```

在前一轮优化里，copy-path 已经把 dispatcher -> consumer 这一段改成了 `Bytes` 传递，但 `ServerCnx` 和协议层边界仍然保留了 materialize：

```rust
let cmd = ServerCommand::Message {
    consumer_id,
    ledger_id: pending_msg.message_id.ledger,
    entry_id: pending_msg.message_id.entry,
    partition: pending_msg.message_id.partition,
    metadata: pending_msg.metadata.to_vec(),
    payload: pending_msg.payload.to_vec(),
};
```

同时，codec 在已有 metadata 的主路径上也仍然会再做一次物化：

```rust
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

也就是说，上一轮优化虽然已经把热点从 dispatcher 继续往下推，但真正的最终 materialize 仍然发生在：

- `ServerCnx -> ServerCommand::Message`
- `codec encode_message()` 的 metadata 主路径

## 结果分析

这轮 copy-path 优化继续把 `Bytes` 传递推进到了协议边界：

- `PendingMessage.metadata` / `payload` 保持为 `Bytes`
- `ServerCommand::Message.metadata` / `payload` 已从 `Vec<u8>` 改为 `Bytes`
- `ServerCnx` 不再把 metadata / payload `to_vec()` 后再交给协议层
- codec 在已有 metadata 的主路径上直接从借用切片写入输出 buffer
- `Consumer::send_message()` / `enqueue_message()` 增加了 `Into<Bytes>` 兼容层，因此旧 dispatcher 调用点无需扩大改动范围

这使得 copy-path 主链从：

```text
NonPersistentEntry(Bytes)
-> dispatcher.to_vec()
-> PendingMessage(Vec<u8>)
-> consumer channel
-> ServerCnx encode
```

收敛成：

```text
NonPersistentEntry(Bytes)
-> dispatcher.metadata_bytes()/payload_bytes()
-> PendingMessage(Bytes)
-> consumer channel
-> ServerCommand::Message(Bytes)
-> codec writes borrowed slices into output buffer
```

当前结果表明，这一步在上一轮基础上还有进一步收益：

- Shared copy path：`50.75 ms -> 16.88 ms`
- KeyShared AutoSplit copy path：`61.25 ms -> 24.13 ms`

也就是说，前一阶段的判断仍然成立，而且新的结果进一步说明：

- `NonPersistentEntry` 早已是共享缓冲，不是问题本身；
- 真正重的是 send path 下游仍然保留的 materialize；
- 不仅 dispatcher 提前 `to_vec()` 值得消掉，`ServerCnx` / `ServerCommand::Message` / codec 主路径里的这层物化也值得继续收缩；
- 把 `Bytes` 一直推到协议边界之后，Shared / KeyShared 的 copy-path 成本还能进一步下降。

## 后续优化方向

1. 继续检查 frame encode 阶段是否还有可进一步减少中间分配的空间；
2. 保持当前 wire shape 不变的前提下，确认 `Bytes` 化没有引入顺序、丢消息或 payload/metadata 保真回退；
3. 若后续继续深入 send path，可进一步观察 permit 扣减、channel handoff 与 metadata decode 的占比；
4. 后续性能记录继续沿用相同的“预热 `5` 次 + 正式 `10` 次 + 去极值平均”方法。
