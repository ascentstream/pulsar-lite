# Query / Stats Path 优化记录

## 文档目的

本文档用于记录 Query / Stats 这一阶段的优化前基线数据与后续对比结果。

在 non-persistent copy-path 阶段完成后，下一步需要确认控制面与统计聚合路径里，哪些点值得继续优化。当前关注两类路径：

- Query 路径
  - `Lookup`
  - `PartitionMetadata`
- Stats 路径
  - `BrokerService::get_all_stats()`
  - `BrokerService::get_all_partitioned_stats()`

这一阶段的目标不是马上重构实现，而是先补齐 baseline，确认：

- 当前仓库是否已经存在可复用的 perf 基线；
- Query / Stats 中哪一类路径更值得优先优化；
- 哪些路径虽然属于第二部分，但当前成本仍然很轻，不值得优先动。

## 测试环境

- 仓库：`pulsar-lite`
- 分支：`perf/query-stats-path`
- 基线命令：

```bash
cargo test --manifest-path rust/Cargo.toml perf_query_lookup_handler_10k_requests -- --ignored --nocapture
cargo test --manifest-path rust/Cargo.toml perf_query_partition_metadata_handler_10k_requests -- --ignored --nocapture
cargo test --manifest-path rust/Cargo.toml perf_stats_get_all_stats_100_topics_4_subscriptions_8_consumers -- --ignored --nocapture
cargo test --manifest-path rust/Cargo.toml perf_stats_get_all_partitioned_stats_20_topics_8_partitions -- --ignored --nocapture
```

说明：

- 当前仓库原本没有现成的 Query / Stats perf baseline；
- 因此本轮先新增 ignored perf tests，再记录优化前基线；
- 当前结果用于方向判断，不作为 CI 阈值；
- 当前 Query baseline 先采用单次大样本请求；
- 当前 Stats baseline 采用固定拓扑 + 多次重复调用，输出总耗时与平均每次耗时。

## 基线场景

### Query / Lookup

- 请求数：`10_000`
- topic：固定 `persistent://public/default/perf-topic`
- broker service url：固定 `pulsar://127.0.0.1:6650`
- 计时范围：仅统计 `handle_lookup()` 处理与 response 写出

### Query / PartitionMetadata

- 请求数：`10_000`
- topic：预先创建好的 `persistent://public/default/perf-topic`
- partition 数：`8`
- 计时范围：仅统计 `handle_partition_metadata()` 处理与 response 写出

### Stats / get_all_stats

- topics：`100`
- 每个 topic 的 subscriptions：`4`
- 每个 subscription 的 consumers：`8`
- 重复调用次数：`100`
- 计时范围：统计 `BrokerService::get_all_stats()` 总耗时，并计算平均每次耗时

### Stats / get_all_partitioned_stats

- partitioned topics：`20`
- 每个 topic 的 partitions：`8`
- 每个 partition 挂 `1` 个 producer
- 重复调用次数：`200`
- 计时范围：统计 `BrokerService::get_all_partitioned_stats()` 总耗时，并计算平均每次耗时

## 优化前基线结果

### Query / Lookup

- 当前结果：requests=`10_000`：`31 ms`

### Query / PartitionMetadata

- 当前结果：requests=`10_000`，partitions=`8`：`37 ms`

### Stats / get_all_stats

- 当前结果：topics=`100`，subscriptions_per_topic=`4`，consumers_per_subscription=`8`，iterations=`100`
- 总耗时：`235 ms`
- 平均每次：`2.36 ms`

### Stats / get_all_partitioned_stats

- 当前结果：topics=`20`，partitions_per_topic=`8`，iterations=`200`
- 总耗时：`14 ms`
- 平均每次：`0.07 ms`

## 当前优化结果

### Query / Lookup

- 优化前基线：requests=`10_000`：`31 ms`
- 当前结果：requests=`10_000`：`31 ms`

### Query / PartitionMetadata

- 优化前基线：requests=`10_000`，partitions=`8`：`37 ms`
- 当前结果：requests=`10_000`，partitions=`8`：`36 ms`

### Stats / get_all_stats

- 优化前基线：topics=`100`，subscriptions_per_topic=`4`，consumers_per_subscription=`8`，iterations=`100`
- 优化前平均每次：`2.36 ms`
- 当前结果：topics=`100`，subscriptions_per_topic=`4`，consumers_per_subscription=`8`，iterations=`100`
- 当前总耗时：`94 ms`
- 当前平均每次：`0.94 ms`

### Stats / get_all_partitioned_stats

- 优化前基线：topics=`20`，partitions_per_topic=`8`，iterations=`200`
- 优化前平均每次：`0.07 ms`
- 当前结果：topics=`20`，partitions_per_topic=`8`，iterations=`200`
- 当前总耗时：`12 ms`
- 当前平均每次：`0.06 ms`

## 当前实现分析

### Query 路径

当前 `Lookup` 与 `PartitionMetadata` 都在 `rust/src/broker/handler/lookup_handler.rs`：

- `handle_lookup()`
  - 读取请求中的 `topic` 与 `request_id`
  - 构造 `ServerCommand::LookupResponse`
  - 通过 `framed.send(response)` 返回结果
- `handle_partition_metadata()`
  - 先读取 `broker_service` 的读锁
  - 判断 topic 是否应被视为 partitioned topic
  - 查询 partition count
  - 再构造 `ServerCommand::PartitionMetadataResponse`
  - 通过 `framed.send(response)` 返回结果

从当前路径看：

- `Lookup` 更像纯 response 构造 + 协议写出；
- `PartitionMetadata` 比 `Lookup` 多了一次 broker service 读锁与 metadata 查询；
- 两条 Query 路径都还带有较重的 `info!` 日志。

### Stats 路径

当前 Stats 聚合主要在以下几层：

- `BrokerService::get_all_stats()`
  - 遍历所有 topic
  - 对每个 topic 拿读锁
  - 调用 `Topic::get_stats()`
- `Topic::get_stats()`
  - 遍历所有 subscription
  - 对每个 subscription 拿读锁
  - 调用 `Subscription::get_stats()`
- `Subscription::get_stats()`
  - 读取 consumer 数
  - 聚合 permits
  - 对 non-persistent runtime 读取 dropped_messages

也就是说，`get_all_stats()` 当前的成本主要来自：

- topic 级遍历
- subscription 级遍历
- 多层异步读锁
- 每次都重新构造 stats 聚合对象

当前这轮优化先做了两类收缩：

### Query 路径

- 把 `Lookup` / `PartitionMetadata` 热路径上的 `info!` 日志降为 `debug!`
- 在 `BrokerService` 中新增 `get_partition_metadata_response_count()` helper
- 让 `handle_partition_metadata()` 不再先 `should_be_partitioned()` 再 `get_partition_count()`，而是收敛成一次 helper 调用

### Stats 路径

- `Topic::get_stats()` 在构造 `subscription_stats` 的同时，直接累计 `consumer_count`
- 不再在最后额外调用一次 `get_total_consumer_count()` 做第二次 subscription 遍历
- `Subscription::get_stats()` 不再分别调用 `get_consumer_count()` 与 `get_total_permits()`
- 改为一次拿到 consumers 集合后，同时计算：
  - `consumer_count`
  - `total_permits`

相比之下，`get_all_partitioned_stats()` 当前样本很轻，说明在当前测试规模下，它还不是更值得优先优化的热点。

## 结果分析

当前结果显示，这一轮优化的收益分布并不均匀：

### Query / Lookup

- `31 ms -> 31 ms`
- 基本没有观察到可感知收益
- 说明当前 `Lookup` 路径的主要成本很可能不在日志级别或这层简单 response 构造上

### Query / PartitionMetadata

- `37 ms -> 36 ms`
- 仅有轻微改善
- 说明把双重判断收敛成 helper 虽然是正确方向，但不是决定性热点

### Stats / get_all_stats()

- `2.36 ms -> 0.94 ms`
- 收益明显
- 这说明当前 `get_all_stats()` 的热点里，重复遍历与重复 consumer 集合扫描占比很高
- 把聚合逻辑收敛到一次 traversal 里，能够直接减少这条路径的主要开销

### Stats / get_all_partitioned_stats()

- `0.07 ms -> 0.06 ms`
- 变化很小，而且本来就很轻
- 当前仍然不像值得优先继续优化的点

## 当前优先级判断

根据这轮结果，第二部分里更值得优先继续看的顺序调整为：

1. `Stats / get_all_stats()`
2. `Query / PartitionMetadata`
3. `Query / Lookup`
4. `Stats / get_all_partitioned_stats()`（当前优先级较低）

原因是：

- `get_all_stats()` 已经证明存在明确的结构性重复工作，且优化后收益显著；
- `PartitionMetadata` 虽然单次收益较小，但在 Query 路径里仍然比 `Lookup` 更值得继续看；
- `Lookup` 当前 baseline 明确，但这轮小改对它几乎没有帮助，说明下一步如果继续做，需要更靠近协议编码或 response 构造本身；
- `get_all_partitioned_stats()` 当前平均仅 `0.07 ms`，暂时不像值得优先动的路径。

## 后续优化方向

1. 继续沿 `get_all_stats()` 看是否还有可进一步减少的重复锁获取与重复 consumer 扫描；
2. 再回到 Query 路径，优先检查：
   - `PartitionMetadata` response 构造与发送路径；
   - `Lookup` 是否值得从协议编码边界继续下探，而不是只看 handler 层逻辑；
3. `get_all_partitioned_stats()` 暂时先保留 baseline，不作为当前优先优化目标。
