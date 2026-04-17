# E2E Perf Stress Phase 2 Design

## 目标

找 pulsar-lite non-persistent 路径的吞吐极限（ceiling），包括吞吐上限、延迟拐点和资源瓶颈。

## 范围

- Non-persistent topic only
- 四个维度：高 publish rate、大 payload、长时间持续、高 fanout
- 工具：沿用 pulsar-perf testclient
- 可观测性：CPU/RSS 采样 + perf record + flamegraph

## 方案选择

选择**方案 B：独立 stress 脚本**。

理由：现有 coverage 脚本运行稳定不需要改动；stress 场景参数与 coverage 差异大（不限速、长时长、大 payload），独立脚本更清晰；共享基础设施的抽取成本低。

## 目录结构

```
tests/perf/
├── lib/                                    # 共享模块
│   ├── __init__.py
│   ├── broker.py                           # BrokerProcess, BrokerSampler
│   ├── parsing.py                          # parse_producer_output, parse_consumer_output
│   ├── perf_cmd.py                         # perf_cmd() 构造函数
│   └── observability.py                    # PerfCollector (perf record + flamegraph)
│
├── run_non_persistent_e2e_matrix.py        # coverage 基线（从 scripts/perf/ 迁入）
└── run_non_persistent_stress.py            # 新增 stress 脚本
```

`scripts/perf/` 内容迁入后清空或移除。

## Stress 场景定义

### Producer 压力场景

| 场景 | 目标 | 关键参数 |
|---|---|---|
| `stress_producer_max_rate` | 单连接吞吐 ceiling | `-r 0 -m 500000 -s 1024`，约 60s |
| `stress_producer_max_rate_multi_producer` | 并发吞吐 ceiling | `-r 0 -n 4 -m 500000 -s 1024` |
| `stress_producer_large_payload` | 大 payload 带宽瓶颈 | `-r 0 -s 102400 -m 100000`（100KiB） |
| `stress_producer_sustained` | 长时间稳定性 | `-time 300 -r 0 -s 1024`（5min） |

### Consumer / E2E 压力场景

| 场景 | 目标 | 关键参数 |
|---|---|---|
| `stress_consume_shared_max_rate` | Consumer 单连接吞吐 ceiling | feed: `-r 0`，consumer: `-q 10000 -st Shared` |
| `stress_consume_shared_high_fanout` | 高 consumer 数吞吐 | feed: `-r 0`，consumer: `-n 16 -q 10000 -st Shared` |
| `stress_consume_multi_subscription_fanout` | 高 subscription 数吞吐 | feed: `-r 0`，consumer: `-ns 8 -q 10000` |
| `stress_consume_sustained` | 长时间消费稳定性 | feed: `-time 300`，consumer: `-time 300 -st Shared` |
| `stress_consume_partitioned_max_rate` | Partitioned 吞吐 ceiling | 4 partitions，Shared 4 consumers，feed `-r 0` |

所有场景使用 `-r 0`（不限速），让 broker 成为瓶颈。

## 可观测性

### perf record 采集

每个 stress 场景运行期间自动启动：

```
perf record -F 99 -g -p <broker_pid> -o <scenario>.perf.data -- sleep <duration>
```

- 99Hz 采样频率，callgraph (-g)
- 绑定 broker PID
- duration 由场景时长决定

### Flamegraph 生成

全部场景跑完后批量生成：

```
perf script -i <data> | inferno-flamegraph > <svg>
```

依赖：`perf`（内核工具）+ `inferno`（`cargo install inferno`）

### BrokerSampler

stress 场景下每 1 秒采样 CPU/RSS（而非 0.5 秒），输出 CSV 时间序列。

## 结果与产物

### 输出目录

```
docs/perf/data/
├── non_persistent_stress_results.json
├── non_persistent_stress_logs/
│   └── <run_id>/
│       ├── stress_producer_max_rate/
│       │   ├── producer.log
│       │   ├── producer.hdr
│       │   ├── perf.data
│       │   ├── flamegraph.svg
│       │   └── broker_timeseries.csv
│       └── ...（每个场景一个子目录）
```

### JSON 格式

与现有 `non_persistent_e2e_matrix_results.json` 一致，额外增加：

```json
{
  "name": "stress_producer_max_rate",
  "broker_timeseries_file": "...",
  "flamegraph_file": "...",
  "perf_data_file": "..."
}
```

### 对比文档

stress 结果跑完后生成 `docs/perf/non_persistent_stress_results.md`，包含场景表格、关键观察、flamegraph 热点分析。
