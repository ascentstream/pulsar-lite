# Non-Persistent Topic Fanout 优化记录

## 文档目的

本文档用于记录 non-persistent topic fanout 这一阶段的优化前基线数据与后续对比结果。

这一阶段的出发点不是继续优化 dispatcher selection path，而是基于前一阶段 Shared dispatcher 的 perf 结果，进一步确认 Shared 当前主要瓶颈更可能位于 send path。当前 topic 层在 non-persistent 模式下仍采用“每条 entry × 每个 subscription 单独发送一次”的碎片化 fanout 编排，这很可能是 Shared send path 成本被放大的重要来源。

## 测试环境

- 仓库：`pulsar-lite`
- 分支：`perf/non-persistent-topic-fanout`
- 测试命令：

```bash
cargo test perf_non_persistent_shared_topic_dispatch_1_subscription_2_consumers_10k_entries --manifest-path rust/Cargo.toml -- --ignored --nocapture --test-threads=1
```

说明：

- 这是 topic 层新增的 ignored perf baseline
- 当前不作为 CI 阈值，只用于记录优化前后相对差异
- 对比对象是当前分支优化前后，不是原生 Pulsar
- 当前优化结果额外使用了 `5` 次预热 + `10` 次正式运行的方式采样，并采用“去掉最高值和最低值后对剩余 `8` 次求平均”的统计方式

## 基线场景

### Shared topic dispatch

- subscriptions：`1`
- consumers：`2`
- entries：`10_000`
- topic runtime：`NonPersistent`
- permits：两个 Shared consumer 的本地 permits 与 dispatcher aggregate permits 均已充分放开
- 计时范围：仅统计 `Topic::dispatch_to_subscriptions()`，不把 `publish_message()` 包入计时

## 优化前基线结果

### Shared topic dispatch

- subscriptions=`1`，consumers=`2`，entries=`10_000`：`26 ms`

## 当前优化结果

### Shared topic dispatch

- 优化前基线：subscriptions=`1`，consumers=`2`，entries=`10_000`：`26 ms`
- 当前结果：subscriptions=`1`，consumers=`2`，entries=`10_000`：`16.63 ms`
- 统计方法：预热 `5` 次，正式运行 `10` 次，去掉最高值和最低值后，对剩余 `8` 次取平均
- 正式运行样本（ms）：`24, 15, 16, 16, 17, 18, 16, 16, 17, 17`

## 结果分析

这轮 topic fanout 优化将 non-persistent 分发从“每条 entry × 每个 subscription 单独发送一次”改成了“每个 subscription 一次性发送该轮全部 entries”。在当前基线场景下，Shared topic dispatch 从 `26 ms` 降至按去极值平均后的 `16.63 ms`，说明 Shared send path 的一部分主要成本确实来自 topic 层碎片化 fanout，而不是 dispatcher selection path 本身。

当前结果也支持了前一阶段的判断：

- Shared dispatcher selection path 优化没有体现稳定收益
- Shared 当前更值得继续优化的是 send path
- topic 层按 subscription 批量 fanout 能直接降低上游 dispatch 编排成本

## 优化前实现分析

优化前的 non-persistent topic fanout 是逐条驱动的：

```rust
for entry in entries {
    for (sub_name, subscription) in &self.subscriptions {
        let sub_guard = subscription.read().await;
        let duplicate = entry.retained_duplicate();
        if let Err(e) = sub_guard
            .send_non_persistent_entries(vec![duplicate])
            .await
        {
            log::error!(
                "Failed to dispatch non-persistent entry to subscription '{}' on topic '{}': {}",
                sub_name, self.name, e
            );
        }
    }
    entry.release();
}
```

这意味着：

- 每条 entry 都会对每个 subscription 单独调用一次 `send_non_persistent_entries()`
- topic 层 fanout 是碎片化的，而不是“每个 subscription 一次批量发送这一轮全部 entries”
- 即使 dispatcher 内部已经做过 selection path 优化，topic 层仍会把 send path 调用切得很碎，放大 Shared 的整体 dispatch 成本

## 当前假设

本阶段的核心假设是：

- Shared dispatcher selection path 优化没有体现稳定收益，说明 Shared 当前主要瓶颈不在 selection
- non-persistent topic 层的碎片化 fanout 很可能是 Shared send path 成本被放大的重要来源
- 如果把 `dispatch_to_subscriptions()` 改为“按 subscription 批量发送这一轮 entries”，那么 Shared 整体 dispatch 耗时有机会下降

## 后续优化方向

1. 保持 Shared / KeyShared / Exclusive / Failover 的外部消费语义不变，并持续验证这次 fanout 批量化没有引入顺序和一致性回退
2. 下一步继续观察 Shared send path 中 permit 扣减、payload 拷贝和 channel send 的占比，判断是否值得继续往更下游推进
3. 如果后续继续做下游优化，继续沿用相同的“预热 `5` 次 + 正式 `10` 次 + 去极值平均”方法记录性能结果
