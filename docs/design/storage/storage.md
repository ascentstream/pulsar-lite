# storage层总体规划

本文档用于说明 `pulsar-lite` 当前 `rust/src/storage` 的总体职责、与原生 Pulsar 的对齐关系，以及后续从“大而全 `Storage`”收敛为分层架构的路线图。

## 架构概览

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Pulsar Native Broker                            │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         broker/resources                              │  │
│  │  BaseResources / TenantResources / NamespaceResources /               │  │
│  │  TopicResources / PulsarResources                                     │  │
│  │                         │                                              │  │
│  │                         ▼                                              │  │
│  │                     MetadataStore                                      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                          broker/storage                               │  │
│  │                    ManagedLedgerStorage                               │  │
│  │                         │                                              │  │
│  │                         ▼                                              │  │
│  │       ManagedLedger / ManagedCursor / ManagedLedgerFactory            │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────┐   ┌───────────────────────────────────┐  │
│  │  service/persistent          │   │  service/nonpersistent           │  │
│  │  PersistentTopic /           │   │  NonPersistentTopic              │  │
│  │  PersistentSubscription      │   │                                   │  │
│  └──────────────────────────────┘   └───────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────┐   ┌───────────────────────────────────┐  │
│  │  service/schema              │   │  transaction                     │  │
│  │  SchemaRegistryService       │   │  TransactionBuffer /             │  │
│  │                              │   │  PendingAckStore                 │  │
│  └──────────────────────────────┘   └───────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘


┌────────────────────────────────────────────────────────────────────────────┐
│                        pulsar-lite Target Architecture                     │
│                                                                            │
│  ┌──────────────────────┐  ┌────────────────────────────────────────────┐  │
│  │ metadata             │  │ resources                                  │  │
│  │ backend / document / │  │ tenant / namespace / topic /               │  │
│  │ state                │  │ subscription resources                     │  │
│  └──────────────────────┘  └────────────────────────────────────────────┘  │
│                                                                            │
│  ┌──────────────────────┐  ┌────────────────────────────────────────────┐  │
│  │ managedLedger        │  │ schema                                     │  │
│  │ durable message /    │  │ schema storage / registry /                │  │
│  │ cursor / ledger      │  │ compatibility                              │  │
│  └──────────────────────┘  └────────────────────────────────────────────┘  │
│                                                                            │
│  ┌──────────────────────┐                                                  │
│  │ transaction          │                                                  │
│  │ transaction buffer / │                                                  │
│  │ pending ack          │                                                  │
│  └──────────────────────┘                                                  │
│                                                                            │
│  目标：将 storage 相关能力重组为 metadata、resources、managedLedger            │
│  schema、transaction 五条主线，                                              │
└────────────────────────────────────────────────────────────────────────────┘
```

当前 `pulsar-lite` 的问题不只是目录还没拆完，而是原生 Pulsar 分散在多层的职责，仍然被压缩在单一 storage 概念里。

---

## 1. 文档目标

本文档讨论的是 `rust/src/storage` 的总体规划，而不是 broker 的全量架构设计。

当前 `pulsar-lite` 的 `Storage` 仍然是一个“大而全”的真实实现入口。它既承担了消息、cursor、assignment 等运行时状态，又承担了 metadata facade 和若干资源语义入口。这种结构在 MVP 阶段可以工作，但随着 Shared 能力、metadata 持久化和 storage 分层持续推进，已经开始暴露出边界不清的问题。

本文档的目标是：

- 对齐原生 Pulsar 的存储相关分层方式
- 识别 `pulsar-lite` 当前已经压进 `storage` 的职责
- 说明哪些职责应继续留在 `storage`，哪些职责应迁往 broker/resources 等更高层
- 为后续拆分 `storage/mod.rs` 提供总路线图

本文默认读者是后续实现 `storage` 重构的开发者，而不是最终用户。

---

## 2. 原生 Pulsar 的存储层全景

原生 Pulsar 的“存储相关能力”并不是都集中在一个统一的 `storage/mod.rs` 中，而是分散在多层。

### 2.1 metadata backend：`pulsar-metadata/api`

原生 Pulsar 最底层先有通用 metadata backend：

- `pulsar-metadata/src/main/java/org/apache/pulsar/metadata/api/MetadataStore.java`

这一层只负责：

- `get / put / delete / exists / getChildren`
- listener
- cache
- 路径型 metadata 访问

它不承担 tenant / namespace / topic / subscription 的业务语义。

结论：

**原生 `MetadataStore` 是 metadata backend 接口，不是 broker 资源语义入口。**

### 2.2 resources：`pulsar-broker-common/.../resources`

在通用 metadata backend 之上，原生 Pulsar 还有一层 broker 资源语义：

- `BaseResources.java`
- `TenantResources.java`
- `NamespaceResources.java`
- `TopicResources.java`
- `PulsarResources.java`

这一层承担的职责是：

- tenant 资源语义
- namespace 资源语义
- topic / partitioned topic 资源语义
- 基于 metadata store 的 typed resource 封装

这里的关键点是：

- `BaseResources<T>` 负责公共资源访问能力
- `TenantResources`、`NamespaceResources`、`TopicResources` 各自承载单一资源语义
- `PulsarResources` 只负责组合这些资源访问器

结论：

**tenant / namespace / topic / partitioned-topic 等资源语义属于 `broker/resources`，不属于 `MetadataStore`。**

### 2.3 broker storage 接入层：`pulsar-broker/storage`

原生 Pulsar broker 侧还有专门的 ledger storage 接入层：

- `pulsar-broker/src/main/java/org/apache/pulsar/broker/storage/ManagedLedgerStorage.java`

它的角色是：

- broker 接入底层 managed ledger storage 的总入口
- 选择 storage class
- 对 broker 暴露持久化存储能力

这层并不直接承载 topic runtime 行为，而是位于 broker 和 managed-ledger 之间。

### 2.4 持久化底座：`managed-ledger`

真正的持久化消息、cursor、factory、config 主线在：

- `ManagedLedger`
- `ManagedCursor`
- `ManagedLedgerFactory`

这些接口位于：

- `managed-ledger/src/main/java/org/apache/bookkeeper/mledger`

这层是原生 Pulsar 持久化消息语义的底座。

### 2.5 persistent / non-persistent runtime：broker service

原生 Pulsar 的 persistent / non-persistent 运行时实现不是放在统一的 storage 目录里，而是在 broker runtime 主线中：

- `pulsar-broker/service/persistent`
  - `PersistentTopic`
  - `PersistentSubscription`
- `pulsar-broker/service/nonpersistent`
  - `NonPersistentTopic`

这说明原生 Pulsar 中：

- persistent / non-persistent 是 topic runtime 行为的两条主线
- 而不是简单的“storage 内部两个子目录”

这一点对 `pulsar-lite` 的设计约束是：

- persistent / non-persistent 的分流选择属于 broker 层
- 它们不应被当作 `storage` 总体规划中的并列子系统
- `storage` 只需要承接其中与持久化消息、cursor、schema、transaction、metadata 相关的能力

### 2.6 其他存储相关子系统

原生 Pulsar 还有几块明确属于“存储相关”，但层次与前面不同：

- `pulsar-broker/service/schema`
  - `SchemaRegistryService`
- `pulsar-broker/transaction/buffer`
  - `TransactionBuffer`
- `pulsar-broker/transaction/pendingack`
  - `PendingAckStore`

这些子系统说明，原生 Pulsar 的“存储”并不是只有 topic payload 和 metadata 两件事，而是由多条不同职责链组成。

本章结论可以归纳为：

- 原生 Pulsar 没有统一的“大 storage/mod.rs”
- 原生 `MetadataStore` 不负责 tenant / namespace / topic / subscription 业务语义
- persistent / non-persistent 在原生是 broker runtime 主线，不是单纯 storage 内部模块
- `managed-ledger / schema / transaction` 都属于存储相关，但层次不同

---

## 3. `pulsar-lite` 应如何组织存储相关能力

如果参照原生 Pulsar 的组织方式，`pulsar-lite` 后续不应继续让“storage”承担过宽的职责。更合理的方式是把“存储相关能力”拆成并列子系统，而不是全部压在一个 `Storage` 对象或单个 `rust/src/storage` 目录概念下。

### 3.1 metadata：底层 metadata backend

这一层负责：

- metadata backend 抽象
- metadata document / state
- load / save / build / apply

如果对齐原生 Pulsar，对应的是：

- `pulsar-metadata/api`
- `pulsar-metadata/impl`

这一层不应直接承载 tenant / namespace / topic / subscription 资源语义。

### 3.2 resources：broker 资源语义层

这一层负责：

- tenant 资源
- namespace 资源
- topic / partitioned topic 资源
- subscription 资源语义入口

如果对齐原生 Pulsar，对应的是：

- `BaseResources`
- `TenantResources`
- `NamespaceResources`
- `TopicResources`
- `PulsarResources`

这意味着：

- metadata backend 与 resources 应拆开
- resources 应组合 metadata backend，而不是和 metadata state 混在一起

### 3.3 managedLedger：消息持久化核心

这一层负责：

- ledger
- cursor
- position
- factory
- storage class / config
- durable message persistence

如果对齐原生 Pulsar，对应的是两层：

- `pulsar-broker/storage/ManagedLedgerStorage`
- `managed-ledger`

这部分才是严格意义上最接近“storage core”的地方。

### 3.4 schema：消息结构定义存储与版本管理

这一层负责：

- schema storage
- schema registry
- schema compatibility check
- schema version 管理

如果对齐原生 Pulsar，对应的是：

- `pulsar-broker/service/schema`
- `SchemaRegistryService`
- `BookkeeperSchemaStorage`

这部分与 metadata 有交集，但不等于普通 metadata，也不应塞进消息持久化 storage core。

### 3.5 transaction：事务消息与 pending ack

这一层负责：

- transaction buffer
- pending ack store
- transactional visibility / commit / abort 相关状态

如果对齐原生 Pulsar，对应的是：

- `pulsar-broker/transaction/buffer`
- `pulsar-broker/transaction/pendingack`

这部分建立在更完整的 durable message persistence 之上，不属于 metadata，也不属于普通资源层。

本章结论固定为：

**对齐原生 Pulsar 后，`pulsar-lite` 更合理的组织方式不是让 storage 一层承担 metadata、resources、schema、transaction 和 broker runtime 语义，而是把它们拆成并列子系统；其中 storage core 应收敛到消息持久化与 cursor 主线。**

---

## 4. `storage` 与消费模式的边界

`storage/mod.rs` 里现在混入了部分 Shared 相关实现。这个现象本身不自动等于“设计错误”，关键要区分哪些是存储原语，哪些是消费模式策略。

### 4.1 应该属于 storage 的 Shared 相关内容

下面这些内容，本质上是 Shared 消费语义依赖的状态原语或运行时持久化状态，应继续留在 storage 域，只是后续要从 `mod.rs` 迁到 `managedLedger` 主线对应的实现中：

- `SubscriptionCursor`
- `mark_delete`
- `acked_holes`
- `message_assignments`
- `get_message_by_id`
- `is_acknowledged_shared`
- `get_assignment_owner`
- `ack_message_shared`
- `release_assignment`

原因是：

- 这些不是“怎么选 consumer”的策略
- 它们表达的是消息确认状态、cursor 状态、assignment 状态
- 它们是 Shared 语义能成立所依赖的状态原语

如果对照原生 Pulsar，这类能力更接近：

- `ManagedCursor`
- `PersistentSubscription`
- pending ack / redelivery 相关 runtime state

因此，虽然它们服务于 Shared 模式，但它们本质上仍是存储/状态层能力。

### 4.2 不应该属于 storage 的 Shared 相关内容

下面这些则属于 broker/service/dispatcher 的消费模式策略，不应写进 `storage` 的职责范围：

- consumer 选择策略
- priority dispatch
- round-robin
- permit / flow control
- dispatcher recovery policy
- consumer lifecycle / connection state

原因是：

- 这些属于“如何消费”和“如何调度”
- 它们依赖 consumer、dispatcher、connection 等更高层上下文
- 它们不是存储原语，而是消费模式策略

因此，这些能力应继续留在：

- `broker/service`
- `broker/dispatcher`
- `consumer`
- `server_cnx`

而不应迁入 `storage`

本章结论固定为：

**`storage` 可以承载 Shared 所需的状态原语，但不应承载 Shared 的分发策略与消费者行为策略。**

---

## 5. `pulsar-lite` 与原生 Pulsar 的关键差异

如果以原生 Pulsar 为目标，`pulsar-lite` 当前最核心的问题不是“代码还没拆干净”，而是“很多本应分层存在的子系统仍被压成一个 storage 概念”。

### 5.1 原生 `storage` 更窄，`pulsar-lite` 的 storage 概念过宽

- 原生 Pulsar 的 `broker/storage` 主要围绕 `ManagedLedgerStorage`
- `pulsar-lite` 当前的 `storage` 概念却同时覆盖了：
  - metadata
  - resources
  - message queue / cursor / assignment

这说明 `pulsar-lite` 现在的问题不是 storage 缺功能，而是 storage 这个层次承担了过多不应属于它的职责。

### 5.2 metadata / resources / schema / transaction 在原生都是独立链路

原生 Pulsar 中：

- metadata backend 是一条链
- resources 是一条链
- schema 是一条链
- transaction 是一条链

它们都与“存储相关”，但不是同一个 storage 模块的不同文件而已。

### 5.3 persistent / non-persistent 在原生更接近 runtime，而不是 backend

原生 Pulsar 中：

- persistent / non-persistent 属于 topic runtime 语义
- 其底层才分别依赖 managed-ledger 或内存链路

因此，`pulsar-lite` 的最终总体规划不应把 `persistent / non-persistent` 作为 `storage` 的并列部分，而应把它们视为 broker runtime 侧的分流主线；`storage` 只承接其中真正属于持久化与状态原语的部分。

本章最终判断是：

**`pulsar-lite` 当前最需要解决的不是给 storage 增加更多子目录，而是把原本压在 storage 里的并列子系统重新拆开。**

---

## 6. 当前尚未覆盖的原生存储子系统

除了 metadata、resources、message state 这几块，原生 Pulsar 还有三类明确的存储相关子系统。当前 `pulsar-lite` 还没有能力继续实现它们。

### 6.1 `managed_ledger/`

这部分对应原生 Pulsar 的：

- factory
- ledger
- cursor
- config

当前不能继续实现的原因是：

- `pulsar-lite` 还没有 durable message persistence
- 还没有真正的 ledger 抽象和 cursor factory 抽象

### 6.2 `schema/`

这部分对应：

- schema storage
- schema registry

当前不能继续实现的原因是：

- topic schema 的持久化与版本管理依赖更稳定的底层存储主线
- 当前 `pulsar-lite` 还没有独立 schema store 的承载基础

### 6.3 `transaction/`

这部分对应：

- transaction buffer
- pending ack

当前不能继续实现的原因是：

- 事务消息和 pending ack 需要建立在更完整的 durable message persistence 之上
- 当前 Shared ack / assignment 仍然只是 MVP 级别的内存状态模型

这一章的结论要固定为：

- `managed_ledger / schema / transaction` 已经被识别为后续存储相关子系统
- 但在 `pulsar-lite` 还没有完成 durable message persistence 之前，它们只能停留在设计上，不能继续真实实现

---

## 7. 目标架构与职责边界

如果按原生 Pulsar 的分层方式来组织，`pulsar-lite` 后续更合理的目标不是“继续扩 `rust/src/storage`”，而是把存储相关能力拆成几条并列主线：

```text
metadata
resources
managedLedger
schema
transaction
```

它们之间的职责边界应固定如下。

### 7.1 metadata

负责：

- metadata backend
- metadata document / state
- load / save / apply

这一层只回答“metadata 如何存”，不回答“tenant / topic 资源如何组织”。

### 7.2 resources

负责：

- tenant / namespace / topic / subscription 资源语义
- 组合 metadata backend / state

这里必须明确一句：

**resources 组合 metadata，而不是复制一份 metadata 状态。**

### 7.3 managedLedger

负责：

- durable message persistence
- ledger
- cursor
- position
- factory / config

这部分才是严格意义上的 storage core。

### 7.4 schema

负责：

- schema storage
- schema registry
- schema version / compatibility

### 7.5 transaction

负责：

- transaction buffer
- pending ack
- transactional visibility / commit / abort 相关状态

---

## 8. 渐进式拆分路线

后续路线应按“先拆边界，再收缩 storage core”的顺序执行：

1. 先把 metadata 与 resources 彻底分层，消除 metadata/backend 与资源语义混层
2. 再把消息、cursor、assignment、ack frontier 等状态原语从“大而全 `Storage`”中抽成持久化主线
3. 在 durable message persistence 尚未完成前，先把这条主线视为 `managedLedger` 的过渡实现
4. 后续再逐步引入 schema 与 transaction 的独立子系统
5. 最终让 storage core 只收敛到消息持久化与 cursor 主线，不再承担 resources、schema、transaction 的职责

这个顺序的核心原则是：

- 先解决“谁负责什么”
- 再解决“代码放在哪”
- 最后才补更深层的持久化能力

---

## 需要重点关注的核心类型

后续职责归位时，需要重点围绕这些类型和接口展开：

- `MetadataStore`
- `MetadataBackend`
- `JsonFileMetadataStore`
- `BaseResources`
- `TenantResources`
- `NamespaceResources`
- `TopicResources`
- `PulsarResources`
- `MessageId`
- `SubscriptionCursor`

这些类型不要求在本文中给出实现细节，但必须作为后续 storage 重构的核心对象来理解。

---

## 评审检查项

本文完成后，应能够满足以下检查项：

- 能独立回答“storage 到底负责什么，不负责什么”
- 能解释 `storage/mod.rs` 里的 Shared 相关实现哪些是存储原语、哪些不属于存储
- 能单独作为 storage 层总体规划文档成立，不依赖额外上下文才能理解
- 能让实现者据此继续推进 `resources` 与 `managedLedger` 主线的职责迁移，而不需要重新决定总体边界
- 不引入新的代码设计决策；结论都基于当前 repo 现状和原生 Pulsar 源码对照得出
