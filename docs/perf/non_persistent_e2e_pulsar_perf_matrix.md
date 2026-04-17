# Non-Persistent E2E Pulsar Perf 场景矩阵

## 文档目的

本文档记录基于本地 Apache Pulsar `pulsar-perf` test client 对 `pulsar-lite` non-persistent 路径执行的一轮端到端（E2E）性能场景矩阵。目标是先把生产/消费主链路的功能型 perf coverage 跑通，并沉淀一套可复用的基线结果。

本轮重点覆盖：

- producer：单/多 producer、单/多线程、rate control、batching、compression、多 topic、多 partition、producer latency histogram
- consumer：单/多 consumer、多 subscription、多 subscription type、receiver queue、ack 行为、consumer latency histogram

> 说明：这一轮是 **coverage-oriented E2E baseline**，不是 CPU/内存打满型极限压测，因此它能回答“不同场景下链路有没有明显回退/差异”，但**不能单独回答热点锁竞争和资源打满后的 ceiling**。那部分需要下一阶段补 `perf record` / flamegraph / 更激进的 stress profile。

## 执行方式

- 代码分支：`perf/non-persistent-e2e-baseline`
- Pulsar test client：本地 `/home/xtline/code/work/pulsar`
- JDK：`/usr/lib/jvm/java-17-openjdk-amd64`
- Pulsar perf 执行入口：直接运行 `org.apache.pulsar.testclient.PulsarPerfTestTool`
- Broker：`pulsar-lite` release binary，使用临时 config 目录分别起两套隔离 profile：
  - non-partitioned：`127.0.0.1:6651`，`default_partitions = 0`
  - partitioned：`127.0.0.1:6652`，`default_partitions = 4`
- 执行脚本：`scripts/perf/run_non_persistent_e2e_matrix.py`
- 原始结果 JSON：`docs/perf/data/non_persistent_e2e_matrix_results.json`
- 原始 stdout/hdr 日志目录：`docs/perf/data/non_persistent_e2e_matrix_logs/`

运行命令：

```bash
python3 scripts/perf/run_non_persistent_e2e_matrix.py
```

## 本轮矩阵与结果概览

## 边界澄清

这轮**没有测 persistent topic**。

- 所有实际压测 topic 都由脚本统一生成：`non-persistent://public/default/<run_id>-<scenario>`
- 包括两组“多分区”场景，本质上也是 **non-persistent partitioned topic**，不是 persistent partitioned topic
- 之前文档里把 broker profile 简写成了 `partitioned`，容易误解成“测了 persistent 多分区”；这里已改成 **non-persistent partitioned**


运行批次：`20260415-172241`

### Producer 场景

| 场景 | 说明 | Broker Profile | 记录数 | 吞吐 msg/s | 吞吐 Mbit/s | mean ms | p95 ms | p99 ms | max ms | Broker Avg CPU % | Broker Peak RSS MB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `producer_single_baseline` | 单 producer / 单线程 / 速率控制基线 | non-partitioned | 5004 | 500.185 | 0.977 | 0.961 | 1.276 | 1.437 | 3.289 | 1.236 | 4.887 |
| `producer_multi_producer` | 多 producer（4） | non-partitioned | 5007 | 500.492 | 0.978 | 1.925 | 2.195 | 2.314 | 2.704 | 0.976 | 4.938 |
| `producer_multi_thread` | 多线程 producer（4 线程） | non-partitioned | 5004 | 500.097 | 0.977 | 0.849 | 1.266 | 1.607 | 2.343 | 1.475 | 5.199 |
| `producer_disable_batching` | 关闭 batching | non-partitioned | 5002 | 499.952 | 0.976 | 1.083 | 1.356 | 1.943 | 7.011 | 1.423 | 5.215 |
| `producer_lz4_compression` | LZ4 compression | non-partitioned | 5001 | 499.837 | 3.905 | 0.886 | 1.281 | 1.423 | 1.850 | 1.393 | 5.219 |
| `producer_multi_topic` | 4 topics fanout | non-partitioned | 5009 | 500.657 | 0.978 | 1.988 | 2.278 | 2.401 | 3.898 | 1.281 | 5.227 |
| `producer_non_persistent_partitioned_topic` | non-persistent 4 partitions auto topic | non-persistent partitioned | 5002 | 500.126 | 0.977 | 2.170 | 2.695 | 3.462 | 4.640 | 0.951 | 4.898 |

### Consumer / E2E 场景

| 场景 | 说明 | Broker Profile | Consumer 记录数 | Consumer 吞吐 msg/s | AckRate msg/s | mean ms | p95 ms | p99 ms | max ms | Feed Producer 吞吐 msg/s | Feed Producer mean ms | Broker Avg CPU % | Broker Peak RSS MB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `consume_shared_baseline` | Shared 单 consumer 基线 | non-partitioned | 5000 | 1088.891 | 1088.673 | 11.958 | 22 | 34 | 44 | 500.492 | 2.133 | 1.268 | 5.516 |
| `consume_shared_multi_consumer` | Shared 4 consumers | non-partitioned | 5000 | 1421.097 | 1420.813 | 12.000 | 22 | 29 | 42 | 500.382 | 2.075 | 1.190 | 5.613 |
| `consume_multi_subscription` | 4 subscriptions | non-partitioned | 20000 | 1972.435 | 1972.336 | 11.417 | 20 | 24 | 37 | 500.315 | 2.004 | 1.208 | 5.812 |
| `consume_exclusive` | Exclusive 单 consumer | non-partitioned | 5000 | 1042.475 | 1042.266 | 11.403 | 20 | 29 | 44 | 500.226 | 1.970 | 1.196 | 5.812 |
| `consume_failover` | Failover 双 consumer | non-partitioned | 5000 | 1067.940 | 1067.727 | 11.513 | 20 | 30 | 44 | 500.069 | 1.800 | 1.180 | 5.812 |
| `consume_key_shared` | Key_Shared 双 consumer | non-partitioned | 5000 | 1081.515 | 1081.299 | 11.345 | 19 | 26 | 40 | 500.377 | 1.156 | 1.137 | 5.816 |
| `consume_small_receiver_queue` | receiver queue = 1 | non-partitioned | 4990 | 165.904 | 165.904 | 0.976 | 2 | 2 | 28 | 166.223 | 2.364 | 1.232 | 5.816 |
| `consume_ack_delay_zero` | ack delay = 0ms | non-partitioned | 5000 | 305.968 | 305.907 | 11.271 | 20 | 27 | 42 | 249.502 | 1.976 | 1.150 | 5.914 |
| `consume_non_persistent_partitioned_shared` | non-persistent partitioned topic + Shared 4 consumers | non-persistent partitioned | 5000 | 545.516 | 545.407 | 13.953 | 25 | 31 | 36 | 500.940 | 2.213 | 0.566 | 5.055 |

## 关键观察

### 1. Producer 侧：在当前 rate-controlled 基线下，链路稳定，latency 整体较低

- non-partitioned producer 场景下，mean latency 基本落在 **0.85 ~ 1.99 ms**。
- `producer_multi_thread` 是这轮 producer 里 mean 最低的一组：**0.849 ms**。
- `producer_multi_producer` 和 `producer_multi_topic` 的 mean latency 明显比 baseline 高，分别达到 **1.925 ms** 和 **1.988 ms**，说明在当前参数下，并发 producer / topic fanout 会带来额外调度成本。
- 关闭 batching 后，p99 / max 明显变差：
  - baseline p99 **1.437 ms**
  - disable batching p99 **1.943 ms**
  - disable batching max **7.011 ms**
- LZ4 compression 没有造成明显回退；在当前 1 KiB payload 场景下，mean latency 仍保持在 **0.886 ms**。
- non-persistent partitioned producer mean latency 提升到 **2.170 ms**，高于 non-partitioned baseline，说明 non-persistent auto-partition route / partition fanout 至少在当前实现下有可见成本。

### 2. Consumer 侧：Shared 多 consumer 有提升，但订阅模式之间总体差距不大

在 non-partitioned profile 下：

- Shared baseline：**1088.891 msg/s**，mean **11.958 ms**
- Shared 4 consumers：**1421.097 msg/s**，mean **12.000 ms**
- Exclusive：**1042.475 msg/s**，mean **11.403 ms**
- Failover：**1067.940 msg/s**，mean **11.513 ms**
- Key_Shared：**1081.515 msg/s**，mean **11.345 ms**

结论：

- 在当前 5k / 256B / rate-controlled feed 下，Shared 增加到 4 consumers 后吞吐有提升，说明 dispatcher fanout 并没有出现明显功能性回退。
- Exclusive / Failover / Key_Shared 的吞吐和 latency 大体接近，暂时没看到某一种模式出现异常塌陷。
- 这一轮更像是**链路健康度基线**，而不是极限拉满下的 subscription ranking。

### 3. 多 subscription 场景吞吐最高，但这是 fanout 放大，不是“单消费者更快”

`consume_multi_subscription` 收到 **20000** 条，是因为同一批发布消息被 **4 个 subscription** 各自完整消费了一遍。

- consumer aggregate throughput：**1972.435 msg/s**
- mean latency：**11.417 ms**

这说明：

- 当前 non-persistent topic fanout 在 4 subscription 下是可工作的；
- 这组数字反映的是 **总 fanout 消费吞吐**，不是单个 subscription 的独占吞吐。

### 4. `receiver_queue_size = 1` 对吞吐影响非常大

`consume_small_receiver_queue` 是本轮最明显的消费者回退场景：

- consumer throughput 下降到 **165.904 msg/s**
- feed producer 也只跑到 **166.223 msg/s**
- consumer records 最终为 **4990**，没有达到完整 5000

解释：

- non-persistent 下 queue 太小会非常快地把 backpressure 传回 producer/dispatch path；
- 在当前实现里，这种低 queue 场景还伴随可见的消息损失/未达成目标计数，这与 non-persistent “不保 backlog”的语义一致；
- 这组结果很适合作为后续 flow-control / permit / drop 行为的专项 perf+semantics 对照样本。

### 5. `ack delay = 0ms` 也显著拉低消费吞吐

`consume_ack_delay_zero`：

- consumer throughput：**305.968 msg/s**
- feed producer throughput：**249.502 msg/s**
- mean latency：**11.271 ms**

与 Shared baseline 相比：

- Shared baseline consumer throughput：**1088.891 msg/s**
- ack delay = 0 只剩 **305.968 msg/s**

说明：

- 当前链路里 ack batching 对整体吞吐有显著帮助；
- 一旦把 ack delay 压到 0，ack path 开销会更直接暴露出来。

### 6. Non-Persistent Partitioned Shared 场景当前明显慢于 non-partitioned Shared

`consume_non_persistent_partitioned_shared`：

- consumer throughput：**545.516 msg/s**
- mean latency：**13.953 ms**

相比 non-partitioned `consume_shared_multi_consumer`：

- non-partitioned Shared 4 consumers：**1421.097 msg/s** / **12.000 ms**

这说明：

- 当前 `default_partitions = 4` auto-partition profile 下，non-persistent partitioned E2E 成本比 non-partitioned 更高；
- 后续如果要继续看 non-persistent partitioned path，值得再拆：
  - partition route / lookup
  - per-partition producer fanout
  - consumer side partition merge / dispatch

### 7. 这一轮资源采样表明：还没有把 broker 打到资源上限

按脚本的 `/proc/<pid>` 采样结果：

- Broker Avg CPU 大多只有 **0.5% ~ 1.5%**
- Broker Peak CPU 大多在 **6% ~ 10%** 以内
- Broker Peak RSS 大多在 **4.8 MB ~ 5.9 MB**

结论：

- 这一轮矩阵已经完成了 producer/consumer feature coverage；
- 但它**没有**把 CPU / 内存压到极限，因此还不能据此分析“热点锁竞争”或“资源 ceiling”。

## 结论

本轮已经确认：

1. `pulsar-perf` 可以稳定作为 `pulsar-lite` non-persistent 的主 E2E perf harness。
2. producer / consumer / 多 subscription type / batching / compression / multi-topic / partitioned profile 都已经跑通。
3. 当前 non-persistent 链路在常规 rate-controlled 条件下整体稳定，producer mean latency 维持在低毫秒级，consumer mean latency 大约在 **11 ~ 14 ms**。
4. 最值得继续深入的场景有两个：
   - `receiver_queue_size = 1`
   - `ack delay = 0ms`

它们都展示出了明显吞吐下降，说明下一轮如果要做热点锁竞争 / 资源瓶颈分析，应该优先从这两类“压力形状更尖锐”的场景入手。

## 下一阶段建议

1. 以 `consume_small_receiver_queue` 和 `consume_ack_delay_zero` 为重点场景，补：
   - `perf stat -p <broker_pid>`
   - `perf record -g -p <broker_pid>`
   - flamegraph
2. 对 partitioned Shared 再做一轮更细粒度拆分，确认性能差异主要来自：
   - route / partition lookup
   - partition fanout
   - consumer merge / dispatch
3. 如果目标转为“资源 ceiling”，下一轮应改成：
   - 更高 publish rate
   - 更长 test duration
   - 更大 payload
   - 结合 CPU / RSS / context switch / syscall 采样

## 附录：复现

确保本地已准备：

```bash
# 1) pulsar-lite release binary
cd /home/xtline/code/work/pulsar-lite/rust
cargo build --release

# 2) 本地 Pulsar test client + classpath
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
cd /home/xtline/code/work/pulsar
mvn -pl pulsar-testclient -am clean install -DskipTests -Dspotbugs.skip=true
mvn -pl pulsar-testclient dependency:build-classpath \
  -DincludeScope=runtime \
  -Dmdep.outputFile=/tmp/pulsar-testclient.classpath

# 3) 运行矩阵
cd /home/xtline/code/work/pulsar-lite
python3 scripts/perf/run_non_persistent_e2e_matrix.py
```

原始结果文件：

- `docs/perf/data/non_persistent_e2e_matrix_results.json`
- `docs/perf/data/non_persistent_e2e_matrix_logs/`
