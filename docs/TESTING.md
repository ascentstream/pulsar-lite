# 测试架构

Pulsar Lite 采用三层测试策略，覆盖单元、集成和端到端场景。

## 测试层次

### 1. 单元测试（Rust）

**位置**: Rust 源文件内的 `#[test]` 标注函数

**运行**: `cargo test` 或 `make test-rust`

**覆盖模块**:

- **配置解析** (`src/config.rs`)
  - `test_default_config`: 默认配置值验证
  - `test_from_env`: 环境变量覆盖
  - `test_from_partial_env`: 部分环境变量
  - `test_path_config`: 数据路径配置
  - `test_invalid_port`: 端口范围校验

- **协议命令** (`src/protocol/command.rs`)
  - `test_message_id_serialization`: MessageId 序列化正确性
  - `test_message_id_with_batch`: 批处理消息 ID 处理

- **存储引擎** (`src/storage/mod.rs`)
  - `test_store_and_retrieve`: 基本存储读取
  - `test_subscribe_to_topic`: 订阅创建
  - `test_multiple_subscriptions`: 多订阅隔离
  - `test_subscription_cursor_management`: 游标管理
  - `test_message_acknowledgment`: 消息确认
  - `test_get_subscription_state`: 状态查询
  - `test_add_multiple_consumers`: 多消费者管理
  - `test_exclusive_subscription`: 独占订阅控制
  - `test_failover_subscription`: 主备切换
  - `test_key_shared_subscription`: Key 路由
  - `test_consumer_flow_control`: 流控 permits
  - `test_unacknowledged_tracking`: 未确认消息追踪
  - `test_redelivery_after_consumer_close`: 重投机制

### 2. 集成测试（Rust + Python）

**位置**: `tests/test_*.py`（Python 测试启动真实 broker）

**运行**: `pytest tests/` 或 `make test-python`

**关键场景**:

- **二进制协议** (`test_binary_protocol.py`)
  - Protobuf 帧编解码正确性
  - Connect/Lookup/Producer/Send/CloseProducer 命令流
  - 消息元数据保留

- **Shared 订阅** (`test_shared_*.py`)
  - `test_shared_simple`: 基础消息分发
  - `test_shared_dispatcher`: 多消费者负载均衡
  - `test_shared_integration`: 端到端收发
  - `test_shared_flow_control`: permits 流控机制

- **连接管理** (`test_connection_management.py`)
  - 并发客户端连接
  - 心跳 Ping/Pong
  - 优雅关闭

- **客户端行为** (`test_client_behaviors.py`)
  - 双模式切换（嵌入式 vs 远程）
  - 进程管理引用计数
  - 自动端口分配

- **元数据持久化** (`test_metadata_persistence.py`)
  - 订阅状态重启后恢复
  - 消费者重连游标保持

### 3. 端到端测试（Python E2E）

**位置**: `tests/non_persist/test_*.py`

**运行**: `pytest tests/non_persist/`

**测试矩阵**: 38 个测试覆盖 non-persistent topic 全场景

**功能维度**:

- **实时语义** (`test_non_persist_basic.py`)
  - 晚到订阅者看不到 backlog
  - `send_async` 回调与投递一致性
  - 消息属性保留（properties, partition_key, ordering_key）

- **订阅模式** (`test_non_persist_subscription_modes.py`)
  - Exclusive: 拒绝第二个消费者
  - Failover: standby 待命，active 关闭后接管
  - Shared: 消息分发到在线消费者
  - KeyShared: 同 key 路由到同一消费者

- **KeyShared 策略** (`test_non_persist_key_shared.py`)
  - Sticky: range 路由正确性
  - AutoSplit: 拒绝不兼容 policy 消费者

- **动态消费者** (`test_non_persist_dynamic_consumers.py`)
  - Shared: 新消费者只收实时消息并分摊负载
  - Shared: 消费者关闭后幸存者继续
  - KeyShared Sticky: 新消费者加入后已有 key 归属稳定
  - KeyShared AutoSplit: 动态切分实时 key

- **顺序性** (`test_non_persist_ordering.py`)
  - Exclusive: 单消费者 FIFO + handoff 后仍 FIFO
  - Failover: 每个 active epoch 内 FIFO
  - Shared: 单消费者场景下 FIFO
  - KeyShared: 同 key FIFO（Sticky/AutoSplit + 动态加入场景）

- **流控** (`test_non_persist_flow_control.py`)
  - Shared: 满消费者停止接收，drain 后恢复，优先发给有容量的消费者
  - Exclusive: 队列满时丢弃，drain 后恢复
  - Failover: active 满时不转发给 standby
  - KeyShared: 目标消费者满时不转发同 key 给其他消费者

- **断连重连** (`test_non_persist_disconnect_reconnect.py`)
  - 进程异常退出后新消费者可接管
  - Exclusive 重连后无 backlog，只收实时消息

- **Ack 语义** (`test_non_persist_ack_semantics.py`)
  - Shared: 已 ack 消息 owner 关闭后不重投
  - Shared: 未 ack 消息断连后不自动 redelivery（non-persistent 语义）

- **隔离性** (`test_non_persist_isolation.py`)
  - 单 topic 多 producer 并发发送全部可见
  - 同 topic 多 subscription 相互独立
  - 不同 topic 不串消息
  - 同 topic 不同订阅模式互不影响

- **未支持语义** (`test_non_persist_unsupported_semantics.py`)
  - `negative_acknowledge()` 不触发 redelivery
  - `unacked_messages_timeout_ms` 不触发 redelivery
  - `redeliver_unacknowledged_messages()` 不触发 redelivery

**文档**: 详细测试覆盖见 [tests/non_persist 覆盖说明](tests/non_persistent_test_coverage.md)

## 运行测试

```bash
# 全部测试
make test

# 仅 Rust 单元测试
make test-rust

# 仅 Python 集成 + E2E 测试
make test-python

# 单独 non-persistent 测试套件
pytest tests/non_persist/ -v
```

## 测试设计原则

- **单元测试**: 快速反馈，验证单个模块逻辑正确性
- **集成测试**: 启动真实 broker，验证协议实现和组件协作
- **E2E 测试**: 使用官方客户端，验证端到端语义和边界行为

每层测试独立运行，支持快速定位问题层次。

