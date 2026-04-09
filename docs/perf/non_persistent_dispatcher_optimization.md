# Non-Persistent Dispatcher 优化记录

## 文档目的

本文档用于记录 non-persistent dispatcher 热路径优化前后的基线数据与对比结果，当前聚焦：

- Shared dispatcher 的 consumer 选择路径
- KeyShared dispatcher 的 assignment / range 选择路径

这份文档只关注 dispatcher 内部选择开销，不覆盖 topic fanout、协议编解码和 Python 客户端端到端耗时。

## 测试环境

- 仓库：`pulsar-lite`
- 分支：`perf/non-persistent-dispatch-selection`
- 基线命令：

```bash
cargo test perf_baseline --manifest-path rust/Cargo.toml -- --ignored --nocapture --test-threads=1
```

说明：

- 这是一组 benchmark 风格的 ignored tests
- 当前不作为 CI 阈值，只用于记录优化前后相对差异
- 对比对象是 `pulsar-lite` 优化前 vs 优化后，不是原生 Pulsar
- Shared dispatcher 的当前结果额外使用了 `5` 次预热 + `10` 次正式运行的方式采样，并采用“去掉最高值和最低值后对剩余 `8` 次求平均”的统计方式

## 基线场景

### Shared dispatcher

- consumers：`32`
- entries：`10_000`
- permits：每个 consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开

### KeyShared dispatcher / AutoSplit

- consumers：`32`
- entries：`10_000`
- routing key：固定轮转的 ordering key
- permits：每个 consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开

### KeyShared dispatcher / Sticky

- consumers：`32`
- entries：`10_000`
- routing key：固定轮转的 ordering key
- hash range：固定且平均切分的 sticky ranges
- permits：每个 consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开

## 优化前基线结果

### Shared dispatcher

- consumers=`32`，entries=`10_000`：`18 ms`

### KeyShared AutoSplit

- consumers=`32`，entries=`10_000`：`88 ms`

### KeyShared Sticky

- consumers=`32`，entries=`10_000`：`140 ms`

## 当前优化结果

### Shared dispatcher

- 当前结果：consumers=`32`，entries=`10_000`：`19.00 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`19, 21, 19, 20, 18, 18, 18, 18, 22, 19`
- 说明：Shared 结果存在一定波动，本轮按去极值平均后仍未观察到稳定且明显的正向收益

### KeyShared AutoSplit

- 优化前基线：consumers=`32`，entries=`10_000`：`88 ms`
- 当前结果：consumers=`32`，entries=`10_000`：`40 ms`

### KeyShared Sticky

- 优化前基线：consumers=`32`，entries=`10_000`：`140 ms`
- 当前结果：consumers=`32`，entries=`10_000`：`30 ms`


## 结果分析

- Shared dispatcher

Shared 当前按去极值平均后的结果为 `19.00 ms`，仍未观察到明显改善。原因是本轮 Shared 优化主要消除了 consumer 选择阶段的重复 `collect + sort` 开销，但当前 perf baseline 计时范围覆盖的是整个 `send_messages()` 路径，而 Shared 每条消息仍然需要执行 permit 消耗、metadata/payload 拷贝、pending ack 跟踪以及逐条 channel send。这些 per-message 成本目前仍占主要比例，因此仅优化 selection path 后，整体耗时变化不明显。

- KeyShared AutoSplit

AutoSplit 从 `88 ms` 降至 `40 ms`。主要原因是当前实现将原本位于 `select_consumer()` 热路径中的 assignment 构建前移到了 `add_consumer/remove_consumer`，避免了每次选择都重复执行 consumer 排序和 hash range 切分。由于 AutoSplit 原本的主要开销正集中在 assignment 重建上，因此收益较为明显。

- KeyShared Sticky

Sticky 从 `140 ms` 降至 `30 ms`。主要原因是当前实现不再在每次 `select_consumer()` 时动态构造 `BTreeMap` 并重新整理 range，而是预先在 consumer 集合变更时生成 `sticky_assignments` 缓存。由于 Sticky 之前每次选择都存在较重的 range map 构建成本，因此本轮优化收益最大。

- 总结

本轮优化验证了一个结论：对于 non-persistent dispatcher，凡是“consumer 集合低频变化、消息选择高频执行”的结构，都适合把路由准备工作前移到 add/remove consumer，而不是放在 per-message 热路径中重复构建。Shared 当前未取得明显收益，说明其后续瓶颈更多集中在 send path 本身，而不是 selection path。

## 现有 Python 集成测试覆盖情况

当前 `tests/non_persist` 已经覆盖了部分 non-persistent 外部行为，但对于本轮 dispatcher 优化最关心的“消费顺序”和“消息一致性”验证还不够完整。

### 已有覆盖

- `test_non_persist_send_async_delivers_and_preserves_message_metadata`
  - 已验证 payload、`partition_key`、`ordering_key` 与 properties 能够被正确保留
- `test_non_persist_shared_distributes_messages_across_consumers`
  - 已验证 Shared 模式下消息会分摊到多个 consumer，且基础样本下无明显丢失
- `test_non_persist_key_shared_routes_same_key_to_same_consumer`
  - 已验证 KeyShared 模式下同一个 `ordering_key` 会稳定路由到同一个 consumer
- `test_non_persist_key_shared_sticky_ranges_route_to_expected_consumer`
  - 已验证 Sticky range 配置下消息能命中预期 consumer
- `test_non_persist_shared_disconnect_drops_unacked_message`
  - 已验证 non-persistent Shared 下未 ack 消息在 owner close 后不会被错误重投
- `test_non_persist_shared_acked_message_is_not_redelivered_after_owner_closes`
  - 已验证已 ack 消息在 owner close 后不会被错误重投

### 当前缺口

- Shared 模式尚未验证更明确的消费顺序语义，现有测试只验证“两个 consumer 都分到了消息”，没有验证分发顺序是否稳定
- KeyShared 模式尚未验证“同 key 消息的接收顺序”，现有测试只验证“同 key 落到同一个 consumer”
- 现有一致性验证样本量较小，尚未系统验证大样本下的“总数守恒、无重复、无丢失”

## 下一步的 Python 集成测试

### 1. Shared 顺序验证

- 构造两名 Shared consumer 和一组带编号的消息
- 验证消息在两个 consumer 之间的分配结果符合当前预期语义，或至少验证分配稳定且无重复
- 目标是确认 Shared selection path 优化没有改变既有分发顺序行为

### 2. KeyShared 同 key 顺序验证

- 使用同一个 `ordering_key` 连续发送多条带编号的消息
- 验证这些消息全部落到同一个 consumer
- 同时验证接收顺序与发送顺序一致
- 目标是确认 KeyShared selection path 优化没有破坏同 key 顺序语义

### 3. Shared / KeyShared 大样本一致性验证

- 发送 `100` 或 `1000` 条带唯一编号的消息
- 汇总所有 consumer 收到的 payload
- 验证：
  - 收到的消息总数与发送数一致
  - 无重复消息
  - 无丢失消息
- 目标是确认本轮 dispatcher 优化未引入重复投递或消息遗漏

### 补充说明

- 这些测试更适合继续放在 Python 集成测试层，而不是只放在 Rust 单测里
- 原因是它们需要覆盖 producer -> topic -> subscription -> dispatcher -> consumer 的完整外部链路
- Rust 侧更适合继续承担 perf baseline 与内部语义快速校验，Python 侧更适合验证顺序与一致性是否从客户端视角成立
