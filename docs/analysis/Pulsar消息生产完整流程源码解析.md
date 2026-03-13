# Pulsar 消息生产完整流程 - 源码级别解析

> 基于 Apache Pulsar 源码，详细解析从客户端生产消息到数据持久化的完整流程。

---

## 目录

1. [整体架构](#1-整体架构)
2. [核心组件职责](#2-核心组件职责)
3. [阶段1：客户端初始化](#3-阶段1客户端初始化)
4. [阶段2：服务发现 (Lookup)](#4-阶段2服务发现-lookup)
5. [阶段3：连接建立](#5-阶段3连接建立)
6. [阶段4：消息发送 (Client端)](#6-阶段4消息发送-client端)
7. [阶段5：Broker处理消息](#7-阶段5broker处理消息)
8. [阶段6：持久化到BookKeeper](#8-阶段6持久化到bookkeeper)
9. [阶段7：返回确认给客户端](#9-阶段7返回确认给客户端)
10. [核心概念详解](#10-核心概念详解)
11. [关键源码位置索引](#11-关键源码位置索引)

---

## 1. 整体架构

### 1.1 架构图

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Producer   │────►│    Broker    │────►│ ManagedLedger│────►│  BookKeeper  │
│   (Client)   │     │              │     │              │     │   (Bookie)   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
       │                    │                    │                    │
       ▼                    ▼                    ▼                    ▼
   用户代码            处理消息              Ledger管理           持久化存储
   链式配置            路由分发              写入协调             多副本写入

                                                    ┌──────────────┐
                                                    │  ZooKeeper   │
                                                    │  • 元数据    │
                                                    │  • 服务发现  │
                                                    │  • 分布式锁  │
                                                    └──────────────┘
```

### 1.2 数据流转路径

```
用户消息: "message-1"
    │
    │  ① Producer 处理 (压缩、批处理)
    ▼
网络传输 (CommandSend)
    │
    │  ② Broker 接收
    ▼
PersistentTopic 处理 (去重、分发)
    │
    │  ③ ManagedLedger 写入
    ▼
BookKeeper (多副本写入)
    │
    │  ④ Bookie 持久化
    ▼
磁盘存储 (Journal + Ledger)
    │
    │  ⑤ 返回 MessageId
    ▼
MessageId: 12345:67890:0 (ledgerId:entryId:partitionIndex)
```

---

## 2. 核心组件职责

| 组件 | 职责 | 关键操作 |
|------|------|---------|
| **ProducerImpl** | 消息发送入口 | 压缩、批处理、流量控制 |
| **ClientCnx** | 网络连接管理 | 发送命令、接收响应 |
| **ServerCnx** | Broker连接处理 | 解析命令、路由消息 |
| **PersistentTopic** | Topic消息管理 | 去重、分发、回调 |
| **ManagedLedger** | Ledger管理 | Ledger创建、Entry写入 |
| **BookKeeper** | 分布式日志存储 | 多副本写入、Quorum确认 |
| **Bookie** | 实际存储节点 | Journal + Ledger持久化 |
| **ZooKeeper** | 元数据&协调 | 服务发现、分布式锁、配置存储 |

---

## 3. 阶段1：客户端初始化

### 3.1 用户代码示例

```java
// 创建 PulsarClient
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .authentication(AuthenticationFactory.token("xxx"))
    .build();

// 创建 Producer
Producer<String> producer = client.newProducer(Schema.STRING)
    .topic("persistent://public/default/test")
    .compressionType(CompressionType.LZ4)
    .batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
    .maxPendingMessages(1000)
    .sendTimeout(30000, TimeUnit.MILLISECONDS)
    .create();
```

### 3.2 PulsarClient 构建流程

**源码位置**: `ClientBuilderImpl.java:60-80`

```
ClientBuilderImpl.build()
    │
    ├─► 校验 serviceUrl
    ├─► 处理 Authentication (Token解析)
    └─► PulsarClientImpl.builder().build()
```

**PulsarClientImpl 构造函数** (`PulsarClientImpl.java:203-309`):

```
PulsarClientImpl 构造函数
    │
    ├─► 创建 EventLoopGroup (Netty)
    ├─► 启动 Authentication
    ├─► 创建 ConnectionPool (TCP连接池)
    ├─► 创建 Timer (超时/重试)
    ├─► 创建 LookupService (服务发现)
    └─► 创建 MemoryLimitController (内存控制)
```

### 3.3 Producer 创建流程

**源码位置**: `ProducerBuilderImpl.java:85-123`

```
ProducerBuilderImpl.create()
    │
    ├─► 校验配置 (批处理和分块不能同时启用)
    ├─► 校验 Topic 名称
    └─► PulsarClientImpl.createProducerAsync()
            │
            ├─► 校验 Schema
            ├─► 查询 Topic 分区元数据
            └─► 创建 ProducerImpl
                    │
                    ├─► 初始化压缩器 (LZ4)
                    ├─► 初始化批处理容器
                    ├─► 初始化信号量 (maxPendingMessages)
                    └─► ★ grabCnx() 触发连接建立
```

### 3.4 接口与实现的关系

```
PulsarClient (接口) ──implements──► PulsarClientImpl (实现)
     │
     └─► static ClientBuilder builder()
              │
              └─► DefaultImplementation.getDefaultImplementation()
                        │
                        └─► new ClientBuilderImpl()
```

---

## 4. 阶段2：服务发现 (Lookup)

### 4.1 Lookup 触发流程

```
ProducerImpl.grabCnx()
    │
    └─► ConnectionHandler.grabCnx()
            │
            └─► PulsarClientImpl.getConnection(topic)
                    │
                    └─► LookupService.getBroker(TopicName.get(topic))
```

### 4.2 Topic 解析

**源码位置**: `TopicName.java`

```
Topic: "persistent://public/default/test"
        │          │      │      │
        │          │      │      └── localName (Topic名称)
        │          │      └── namespace
        │          └── tenant (租户)
        └── domain (持久化/非持久化)

解析结果:
├─ domain: "persistent"
├─ tenant: "public"
├─ namespace: "default"
└─ localName: "test"
```

### 4.3 Topic → Bundle 映射

```
Step 1: 获取 Namespace 的 Bundle 配置
    Namespace: public/default (4 Bundles)
    Bundle 0: [0x00000000, 0x40000000)
    Bundle 1: [0x40000000, 0x80000000)
    Bundle 2: [0x80000000, 0xc0000000)
    Bundle 3: [0xc0000000, 0xffffffff]

Step 2: 计算 Topic Hash
    hash = MurmurHash3("persistent://public/default/test")
    结果: 0x7a3b2c1d

Step 3: 找到对应的 Bundle
    0x40000000 ≤ 0x7a3b2c1d < 0x80000000
    → Bundle 1: "public/default/0x40000000_0x80000000"
```

### 4.4 LoadManager 初始化与负载收集

**源码位置**: `BrokerService.java`, `LoadManagerImpl.java`

#### 4.4.1 LoadManager 在 Broker 启动时初始化

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Broker 启动时 LoadManager 初始化                      │
└─────────────────────────────────────────────────────────────────────────────┘

Broker 启动流程:
$ bin/pulsar broker

Step 1: BrokerService 初始化
    BrokerService.start()
        │
        ├─► 创建 LoadManager
        │       loadManager = LoadManager.create(brokerService)
        │
        ├─► 根据 loadManagerClassName 选择实现类
        │       配置项: loadManagerClassName
        │       ├─► org.apache.pulsar.broker.loadbalance.ModularLoadManagerImpl (默认)
        │       └─► org.apache.pulsar.broker.loadbalance.SimpleLoadManagerImpl
        │
        └─► 调用 LoadManager.start()
```

#### 4.4.2 LoadManager 接口与实现

```
LoadManager 接口定义 (LoadManager.java):
┌─────────────────────────────────────────────────────────────────┐
│  interface LoadManager                                          │
│  ├── start()                    // 启动负载管理器               │
│  ├── stop()                     // 停止负载管理器               │
│  ├── assignBroker(topics)       // 为 Topic 分配 Broker         │
│  ├── getAvailableBrokers()      // 获取可用 Broker 列表         │
│  ├── updateBrokerLoad()         // 更新 Broker 负载信息         │
│  └── writeBrokerLoadInfo()      // 写入负载信息到 ZK            │
└─────────────────────────────────────────────────────────────────┘

实现类:
┌─────────────────────────────────────────────────────────────────┐
│  ModularLoadManagerImpl (默认推荐)                              │
│  ├── 更细粒度的负载度量                                         │
│  ├── 支持多种负载策略                                           │
│  └── 支持Bundle拆分和卸载                                       │
├─────────────────────────────────────────────────────────────────┤
│  SimpleLoadManagerImpl                                          │
│  ├── 简单的负载均衡                                             │
│  └── 适合小规模集群                                             │
└─────────────────────────────────────────────────────────────────┘
```

#### 4.4.3 负载信息收集与上报

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Broker 负载信息收集与上报流程                            │
└─────────────────────────────────────────────────────────────────────────────┘

Broker 启动后，定期收集本地负载信息:

LoadManager.start()
    │
    └─► 启动定时任务 (每 loadBalancerReportIntervalSeconds 秒执行)
            │
            └─► collectBrokerLoadInfo()
                    │
                    ├─► 收集系统资源
                    │       ├─► CPU 使用率
                    │       ├─► 内存使用率
                    │       ├─► 网络带宽 (入站/出站)
                    │       └─► 磁盘 I/O
                    │
                    ├─► 收集 Pulsar 指标
                    │       ├─► 连接数 (Producers + Consumers)
                    │       ├─► Bundle 数量
                    │       ├─► Topic 数量
                    │       ├─► 消息速率 (in/out)
                    │       └─► 消息吞吐量 (in/out bytes)
                    │
                    ├─► 计算 LoadReport
                    │       LoadReport {
                    │         brokerId: "broker1:8080",
                    │         brokerUrl: "broker1:6650",
                    │         cpu: { usage: 45.2, limit: 100 },
                    │         memory: { usage: 60.5, limit: 8192 },
                    │         bandwidth: { in: 1024, out: 2048 },
                    │         bundles: ["public/default/0x00000000_0x40000000", ...],
                    │         topics: 150,
                    │         producers: 200,
                    │         consumers: 300,
                    │         msgRateIn: 5000,
                    │         msgRateOut: 4500,
                    │         ...
                    │       }
                    │
                    └─► 写入 ZooKeeper
                            路径: /loadbalance/brokers/broker1:8080
                            节点类型: 临时节点 (Ephemeral)
                            数据: LoadReport JSON
```

#### 4.4.4 ZooKeeper 中的负载信息存储

```
ZooKeeper 目录结构:

/loadbalance/
├── brokers/                              # 所有 Broker 的负载信息
│   ├── broker1:8080                      # Broker 1 (临时节点)
│   │   └── data: {
│   │         "brokerId": "broker1:8080",
│   │         "cpu": {"usage": 45.2},
│   │         "memory": {"usage": 60.5},
│   │         "bundles": ["bundle-1", "bundle-2"],
│   │         "timestamp": 1234567890
│   │       }
│   │
│   ├── broker2:8080                      # Broker 2 (临时节点)
│   │   └── data: {...}
│   │
│   └── broker3:8080                      # Broker 3 (临时节点)
│       └── data: {...}
│
├── bundles/                              # Bundle → Broker 映射
│   └── public/default/
│       ├── 0x00000000_0x40000000         # Bundle 0 → broker1
│       ├── 0x40000000_0x80000000         # Bundle 1 → broker2
│       └── ...
│
└── leaders/                              # Leader 选举 (可选)
    └── loadbalance                       # 负载均衡 Leader
```

#### 4.4.5 选择目标 Broker 的策略

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     LoadManager 选择 Broker 策略                            │
└─────────────────────────────────────────────────────────────────────────────┘

当需要为 Bundle 分配 Broker 时:

LoadManager.assignBroker(bundle)
    │
    ├─► Step 1: 获取所有可用 Broker
    │       从 ZooKeeper 读取 /loadbalance/brokers/ 下所有节点
    │       过滤掉不健康的 Broker
    │
    ├─► Step 2: 应用负载均衡策略 (可配置)
    │       │
    │       ├─► LeastLongTermMessageRate (默认)
    │       │       选择消息速率最低的 Broker
    │       │       score = msgRateIn + msgRateOut
    │       │
    │       ├─► LeastCPUBased
    │       │       选择 CPU 使用率最低的 Broker
    │       │       score = cpuUsage
    │       │
    │       ├─► LeastMemoryBased
    │       │       选择内存使用率最低的 Broker
    │       │       score = memoryUsage
    │       │
    │       ├─► LeastBundleBased
    │       │       选择 Bundle 数量最少的 Broker
    │       │       score = bundleCount
    │       │
    │       └─► weightedRandomSelection
    │               基于权重的随机选择
    │               避免所有新 Topic 都集中到同一个 Broker
    │
    ├─► Step 3: 应用过滤规则
    │       ├─► 资源阈值检查 (CPU < 80%, Memory < 85%)
    │       ├─► Broker 隔离策略 (isolationPolicies)
    │       └─► 自动排除过载的 Broker
    │
    └─► Step 4: 返回选中的 Broker
            selectedBroker = "broker2:8080"
```

#### 4.4.6 LoadManager 完整流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  LoadManager 初始化到 Bundle 分配完整流程                    │
└─────────────────────────────────────────────────────────────────────────────┘

时间线:
─────────────────────────────────────────────────────────────────────────────►

T1: Broker 启动阶段
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  Broker-1 启动                                                          │
    │      │                                                                  │
    │      ├─► BrokerService.start()                                         │
    │      │       │                                                          │
    │      │       └─► LoadManager.create()                                  │
    │      │               └─► new ModularLoadManagerImpl()                  │
    │      │                                                                  │
    │      ├─► LoadManager.start()                                           │
    │      │       └─► 启动定时任务: 每 30s 收集负载并上报到 ZK              │
    │      │                                                                  │
    │      └─► 在 ZK 注册临时节点: /loadbalance/brokers/broker1:8080        │
    └─────────────────────────────────────────────────────────────────────────┘

T2: 稳定运行阶段 (各 Broker 定期上报负载)
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  ZK: /loadbalance/brokers/                                              │
    │  ├── broker1:8080 → LoadReport { cpu: 45%, bundles: 10, ... }          │
    │  ├── broker2:8080 → LoadReport { cpu: 30%, bundles: 5, ... }           │
    │  └── broker3:8080 → LoadReport { cpu: 60%, bundles: 15, ... }          │
    └─────────────────────────────────────────────────────────────────────────┘

T3: 客户端首次访问 Topic (需要分配 Bundle)
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  Client ──LOOKUP──► Broker-1                                           │
    │                                                                          │
    │  Broker-1 处理:                                                         │
    │      │                                                                  │
    │      ├─► 查询 ZK: Bundle "0x40000000_0x80000000" 有 Owner 吗?         │
    │      │       结果: 无                                                   │
    │      │                                                                  │
    │      ├─► 调用 LoadManager.assignBroker()                               │
    │      │       │                                                          │
    │      │       ├─► 读取所有 Broker 负载: [B1:45%, B2:30%, B3:60%]        │
    │      │       ├─► 应用策略: LeastCPUBased                               │
    │      │       └─► 选中: Broker-2 (负载最低)                             │
    │      │                                                                  │
    │      ├─► 通知 Broker-2 获取 Ownership                                  │
    │      │       Broker-2 在 ZK 创建临时节点成功                           │
    │      │       /loadbalance/bundles/public/default/0x40000000_0x80000000 │
    │      │                                                                  │
    │      └─► 返回给 Client: Broker-2 的地址                                │
    └─────────────────────────────────────────────────────────────────────────┘
```

### 4.5 Bundle → Broker 查找

**源码位置**: `BinaryProtoLookupService.java:172-239`

```
findBroker(socketAddress, topicName)
    │
    ├─► 从连接池获取连接
    ├─► 发送 LOOKUP 命令
    │       CommandLookupTopic {
    │         topic: "persistent://public/default/test"
    │         requestId: 12345
    │         authoritative: false
    │       }
    │
    └─► 处理响应
            ├─► redirect=true: 递归调用 findBroker()
            └─► redirect=false: 返回 Broker 地址
```

#### 4.5.1 Lookup 完整流程（含首次访问场景）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Bundle → Broker Lookup 完整流程                       │
└─────────────────────────────────────────────────────────────────────────────┘

客户端发起 Lookup:
ProducerImpl.grabCnx()
    │
    └─► LookupService.getBroker(TopicName)
            │
            ├─► Step 1: 计算 Topic 所属 Bundle
            │       hash = MurmurHash3(topic)
            │       bundle = "public/default/0x40000000_0x80000000"
            │
            ├─► Step 2: 向任意 Broker 发送 LOOKUP 命令
            │       (通常先连接到配置的 bootstrap 地址)
            │
            └─► Step 3: Broker 处理 LOOKUP 请求
                    │
                    ├─► 检查 Bundle 的 Ownership
                    │       查询 ZooKeeper:
                    │       /loadbalance/bundles/public/default/0x40000000_0x80000000
                    │
                    ├─► 场景A: Bundle 已有 Owner
                    │       │
                    │       ├─► 当前 Broker 就是 Owner?
                    │       │       Yes → 返回自己的地址
                    │       │       No  → 返回 redirect=true + Owner地址
                    │       │
                    │       └─► 响应格式:
                    │               CommandLookupTopicResponse {
                    │                 brokerUrl: "broker2:6650",
                    │                 redirect: true/false,
                    │                 authoritative: true
                    │               }
                    │
                    └─► 场景B: Bundle 无 Owner (首次访问)
                            │
                            ├─► LoadManager 选择目标 Broker
                            │       考虑因素:
                            │       ├─► 负载均衡 (选择负载最低的)
                            │       ├─► 资源使用率 (CPU/Memory/_bandwidth)
                            │       └─► Broker 可用性
                            │
                            ├─► 目标 Broker 尝试获取 Ownership
                            │       │
                            │       ├─► 在 ZooKeeper 创建临时节点 (分布式锁)
                            │       │       路径: /loadbalance/bundles/public/default/0x40000000_0x80000000
                            │       │       数据: {"broker": "broker1:8080", "native_broker": "broker1:6650"}
                            │       │
                            │       ├─► 创建成功?
                            │       │       Yes → 成为 Bundle Owner
                            │       │       No  → 其他 Broker 抢先了，重试
                            │       │
                            │       └─► 获取 Ownership 后:
                            │               ├─► 加载 Topic (PersistentTopic)
                            │               ├─► 初始化 ManagedLedger
                            │               └─► 创建 Ledger (如果需要)
                            │
                            └─► 返回响应
                                    CommandLookupTopicResponse {
                                      brokerUrl: "broker1:6650",
                                      redirect: false,
                                      authoritative: true
                                    }
```

#### 4.5.2 Redirect 递归查找详解

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Redirect 递归查找流程                                 │
└─────────────────────────────────────────────────────────────────────────────┘

场景: 客户端连接到 Broker-A，但 Bundle 归 Broker-B 负责

Client                 Broker-A               Broker-B               ZooKeeper
  │                       │                      │                      │
  │ LOOKUP (topic)        │                      │                      │
  │──────────────────────►│                      │                      │
  │                       │                      │                      │
  │                       │ 查询 Bundle Owner    │                      │
  │                       │────────────────────────────────────────────►│
  │                       │◄────────────────────────────────────────────│
  │                       │ {"broker": "Broker-B"}                     │
  │                       │                      │                      │
  │                       │ 我不是 Owner        │                      │
  │                       │                      │                      │
  │ Response              │                      │                      │
  │ {redirect=true,       │                      │                      │
  │  brokerUrl="Broker-B"}│                      │                      │
  │◄──────────────────────│                      │                      │
  │                       │                      │                      │
  │ LOOKUP (topic,        │                      │                      │
  │  authoritative=true)  │                      │                      │
  │──────────────────────────────────────────────►│                      │
  │                       │                      │                      │
  │                       │                      │ 我是 Owner ✓         │
  │                       │                      │                      │
  │ Response              │                      │                      │
  │ {redirect=false,      │                      │                      │
  │  brokerUrl="Broker-B"}│                      │                      │
  │◄──────────────────────────────────────────────│                      │
  │                       │                      │                      │
  │ 连接 Broker-B 建立长连接                    │                      │
  │════════════════════════════════════════════►│                      │
```

#### 4.5.3 authoritative 参数说明

```
authoritative 参数含义:

authoritative = false (默认)
    │
    └─► 表示这不是"权威"查找
        ├─► Broker 可以返回 redirect=true
        └─► 用于首次查找，允许 Broker 重定向

authoritative = true
    │
    └─► 表示这是"权威"查找
        ├─► Broker 必须返回确定结果
        ├─► 如果不是 Owner，必须先尝试获取 Ownership
        └─► 用于 redirect 后的二次查找

使用场景:
1. 客户端首次 LOOKUP
   └─► authoritative=false
       └─► Broker-A 返回 redirect + Broker-B 地址

2. 客户端向 Broker-B LOOKUP
   └─► authoritative=true (或 Broker-B 直接处理)
       └─► Broker-B 确认自己是 Owner，返回自己的地址
```

### 4.6 ZooKeeper 中的 Bundle 映射

```
/loadbalance/bundles/public/default/0x40000000_0x80000000
    │
    └─► data: {
            "broker": "broker1:8080",
            "native_broker": "broker1:6650",
            "timestamp": 1234567890
        }
```

---

## 5. 阶段3：连接建立

### 5.1 从连接池获取连接

**源码位置**: `ConnectionPool.java:160-210`

```
ConnectionPool.getConnection(broker1:6650)
    │
    ├─► 检查连接池是否有复用连接
    │       有 → 直接返回
    │       无 → 创建新连接
    │
    └─► 创建新连接:
            ├─► DNS解析 broker1 → 192.168.1.1
            └─► Netty Bootstrap.connect(192.168.1.1, 6650)
```

### 5.2 TCP连接 + 认证握手

**源码位置**: `ClientCnx.java:298-330`

```
ClientCnx.channelActive()
    │
    └─► 发送 CONNECT 命令 (携带 Token)
            CommandConnect {
                clientVersion: "Pulsar-Java-v2.10.0"
                authMethodName: "token"
                authData: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
            }
    │
    └─► Broker 验证 Token，返回 CONNECTED
```

### 5.3 注册 Producer

**源码位置**: `ProducerImpl.java:1879-2000`

```
ProducerImpl.connectionOpened(cnx)
    │
    └─► 发送 PRODUCER 命令
            CommandProducer {
                topic: "persistent://public/default/test"
                producerId: 1
                requestId: 12345
                schema: {type: "STRING", ...}
            }
    │
    └─► Broker 返回 CommandProducerSuccess
            {producerName: "standalone-0-0", ...}
```

---

## 6. 阶段4：消息发送 (Client端)

### 6.1 用户代码

```java
MessageId messageId = producer.send("message-1");
```

### 6.2 ProducerImpl.sendAsync() 处理流程

**源码位置**: `ProducerImpl.java:545-696`

```
sendAsync(message, callback)
    │
    ├─► ① 检查 Producer 状态
    │       isValidProducerState()
    │
    ├─► ② 流量控制
    │       canEnqueueRequest()
    │       - 获取信号量 (maxPendingMessages=1000)
    │       - 内存限制检查
    │
    ├─► ③ 压缩处理 (LZ4)
    │       if (size > compressMinSize) {
    │           compressedPayload = compressor.encode(payload)
    │       }
    │
    ├─► ④ 更新消息元数据
    │       updateMessageMetadata()
    │
    └─► ⑤ 序列化并发送（synchronized 块内）
            synchronized (this) {
                sequenceId = updateMessageMetadataSequenceId(msgMetadata);
                serializeAndSendMessage(...)
            }
```

#### 6.2.1 为什么 sendAsync 是"异步"但内部需要"串行化"？

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     异步 vs 串行化 的区别                                    │
└─────────────────────────────────────────────────────────────────────────────┘

"异步" (Async) 的含义：
    ──► 对用户线程非阻塞
    ──► 用户调用 producer.sendAsync() 后立即返回 CompletableFuture
    ──► 用户线程不需要等待网络 I/O 完成

"串行化" (Serialized) 的含义：
    ──► 消息按顺序处理，保证消息顺序性
    ──► 使用 synchronized 块确保多线程并发调用时消息不会乱序
    ──► sequenceId 严格递增

代码体现：
    // sendAsync 方法本身不是 synchronized 的
    public void sendAsync(Message<?> message, SendCallback callback) {
        // ... 前置检查（可并发执行）
        checkArgument(message instanceof MessageImpl);

        // ... 压缩处理（可并发执行）
        compressedPayload = applyCompression(payload);

        // ★ 关键：synchronized 块保证顺序
        synchronized (this) {
            final long sequenceId = updateMessageMetadataSequenceId(msgMetadata);
            serializeAndSendMessage(...);  // 串行执行
        }
    }

为什么需要 synchronized？
    1. sequenceId 必须严格递增（msgIdGenerator++）
    2. 批处理容器状态需要一致性检查
    3. lastSequenceIdPushed 等状态需要原子更新
    4. 保证消息在 pendingMessages 队列中的顺序
```

### 6.3 serializeAndSendMessage() 详解

**源码位置**: `ProducerImpl.java:742-850`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    serializeAndSendMessage() 完整流程                        │
└─────────────────────────────────────────────────────────────────────────────┘

private void serializeAndSendMessage(MessageImpl<?> msg,
                                     ByteBuf payload,
                                     long sequenceId,
                                     String uuid,
                                     int chunkId,
                                     int totalChunks,
                                     ...) throws IOException {
    │
    ├─► ① 分块消息处理 (大消息场景)
    │       if (totalChunks > 1 && TopicName.get(topic).isPersistent()) {
    │           chunkPayload = compressedPayload.slice(readStartIndex, chunkMaxSizeInBytes);
    │           msgMetadata.setChunkId(chunkId)
    │                     .setNumChunksFromMsg(totalChunks)
    │                     .setTotalChunkMsgSize(compressedPayloadSize);
    │       }
    │
    ├─► ② 判断是否可以加入批处理
    │       if (canAddToBatch(msg) && totalChunks <= 1) {
    │           // 走批处理路径
    │           if (canAddToCurrentBatch(msg)) {
    │               if (isLastSequenceIdPotentialDuplicated) {
    │                   doBatchSendAndAdd(msg, callback, payload);  // 先发送当前批次
    │               } else {
    │                   boolean isBatchFull = batchMessageContainer.add(msg, callback);
    │                   triggerSendIfFullOrScheduleFlush(isBatchFull);
    │               }
    │           } else {
    │               doBatchSendAndAdd(msg, callback, payload);
    │           }
    │       }
    │
    └─► ③ 非批处理路径（单条消息或分块消息）
            else {
                // 再次压缩（如果需要）
                if (!compressed && chunkPayload.readableBytes() > conf.getCompressMinMsgBodySize()) {
                    chunkPayload = applyCompression(chunkPayload);
                }

                // 加密处理
                ByteBuf encryptedPayload = encryptMessage(msgMetadata, chunkPayload);

                // 创建 OpSendMsg 对象
                if (msg.getSchemaState() == MessageImpl.SchemaState.Ready) {
                    ByteBufPair cmd = sendMessage(producerId, sequenceId, numMessages, ...);
                    op = OpSendMsg.create(rpcLatencyHistogram, msg, cmd, sequenceId, callback);
                } else {
                    // Schema 未就绪，延迟创建 cmd
                    op = OpSendMsg.create(rpcLatencyHistogram, msg, null, sequenceId, callback);
                    op.rePopulate = () -> { ... };  // 回调函数，后续填充 cmd
                }

                // ★ 关键：调用 processOpSendMsg 进行网络发送
                processOpSendMsg(op);
            }
}
```

#### 6.3.1 批处理路径 vs 非批处理路径

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    两条发送路径对比                                          │
└─────────────────────────────────────────────────────────────────────────────┘

路径A: 批处理模式 (Batching Enabled)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
serializeAndSendMessage()
    │
    └─► batchMessageContainer.add(msg, callback)
            │
            ├─► 批次未满: 等待定时刷新
            │       maybeScheduleBatchFlushTask()
            │       └─► batchMessageAndSend() ─► processOpSendMsg()
            │
            └─► 批次已满: 立即发送
                    batchMessageAndSend(false)
                    └─► processOpSendMsg()


路径B: 非批处理模式 (Batching Disabled 或 大消息)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
serializeAndSendMessage()
    │
    ├─► 创建 OpSendMsg 对象
    │       op = OpSendMsg.create(...)
    │
    └─► processOpSendMsg(op)  // 直接发送
```

### 6.4 processOpSendMsg() 网络发送

**源码位置**: `ProducerImpl.java:2462-2504`

```
// processOpSendMsg() 方法 (行2462)
protected synchronized void processOpSendMsg(OpSendMsg op) {
    if (op == null) {
        return;
    }
    try {
        │
        ├─► ① 如果有新消息到来且启用了批处理，先发送当前批次
        │       if (op.msg != null && isBatchMessagingEnabled()) {
        │           batchMessageAndSend(false);
        │       }
        │
        ├─► ② 检查消息大小是否超限
        │       if (isMessageSizeExceeded(op)) {
        │           op.cmd.release();
        │           return;
        │       }
        │
        ├─► ③ 加入待确认队列（关键：保证顺序和重试）
        │       pendingMessages.add(op);
        │       updateLastSeqPushed(op);
        │
        ├─► ④ 检查 Schema 注册状态
        │       if (State.RegisteringSchema.equals(getState())) {
        │           return;  // 等待 Schema 注册完成后继续
        │       }
        │
        ├─► ⑤ 获取连接
        │       final ClientCnx cnx = getCnxIfReady();
        │       if (cnx != null) {
        │           │
        │           ├─► ⑤.1 检查 Schema 状态
        │           │       if (op.msg != null && op.msg.getSchemaState() == None) {
        │           │           tryRegisterSchema(cnx, op.msg, op.callback, ...);
        │           │           return;
        │           │       }
        │           │
        │           └─► ⑤.2 ★★★ 关键：通过 EventLoop 发送 ★★★
        │                   op.cmd.retain();  // 增加引用计数
        │                   cnx.ctx().channel().eventLoop().execute(
        │                       WriteInEventLoopCallback.create(this, cnx, op)
        │                   );
        │                   stats.updateNumMsgsSent(op.numMessagesInBatch, op.batchSizeByte);
        │       } else {
        │           // 连接未就绪，消息保留在 pendingMessages 中
        │           // 连接建立后会重试发送
        │       }
        │
    } catch (Throwable t) {
        releaseSemaphoreForSendOp(op);
        op.sendComplete(new PulsarClientException(t, op.sequenceId));
    }
}

★ 关键点：
    1. 方法声明为 synchronized，保证多线程调用时消息顺序
    2. pendingMessages 队列保存所有待确认的消息
    3. 通过 eventLoop().execute() 确保在 Netty EventLoop 线程中执行写入
    4. 这保证了同一连接的消息串行发送（Netty Channel 不是线程安全的）
```

### 6.5 WriteInEventLoopCallback 完整实现

**源码位置**: `ProducerImpl.java:1114-1165`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    WriteInEventLoopCallback 内部类                          │
└─────────────────────────────────────────────────────────────────────────────┘

// 这是一个静态内部类，实现了 Runnable 接口
private static final class WriteInEventLoopCallback implements Runnable {
    private ProducerImpl<?> producer;
    private ClientCnx cnx;
    private long sequenceId;
    private ByteBufPair cmd;
    private OpSendMsg op;

    // 工厂方法（使用对象池 Recycler 优化性能）
    static WriteInEventLoopCallback create(ProducerImpl<?> producer, ClientCnx cnx, OpSendMsg op) {
        WriteInEventLoopCallback c = RECYCLER.get();
        c.producer = producer;
        c.cnx = cnx;
        c.sequenceId = op.sequenceId;
        c.cmd = op.cmd;
        c.op = op;
        return c;
    }

    @Override
    public void run() {
        // ★ 在 Netty EventLoop 线程中执行 ★
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] Sending message cnx {}, sequenceId {}",
                producer.topic, producer.producerName, cnx, sequenceId);
        }

        try {
            // ★★★ 最终的网络发送操作 ★★★
            cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());
            op.updateSentTimestamp();
        } finally {
            recycle();  // 回收到对象池
        }
    }

    private void recycle() {
        producer = null;
        cnx = null;
        cmd = null;
        sequenceId = -1;
        op = null;
        recyclerHandle.recycle(this);
    }

    // ... Recycler 相关代码
}

★ 为什么需要 WriteInEventLoopCallback？

    1. 线程安全：Netty Channel 不是线程安全的，必须在 EventLoop 线程中操作
    2. 顺序保证：EventLoop 是单线程的，保证消息按顺序发送
    3. 性能优化：使用对象池 (Recycler) 减少 GC 压力
    4. 异步解耦：用户线程不直接执行网络 I/O

调用链：
    用户线程
        │
        └─► processOpSendMsg(op)
                │
                └─► eventLoop().execute(WriteInEventLoopCallback)
                        │
                        └─► [切换到 Netty EventLoop 线程]
                                │
                                └─► WriteInEventLoopCallback.run()
                                        │
                                        └─► cnx.ctx().writeAndFlush(cmd)
```

### 6.6 消息从 Client 到 Broker 的完整路径（含源码函数）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    消息发送完整路径（跨进程视角 + 源码函数）                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│ CLIENT 端进程 (pulsar-client 模块)                                          │
│                                                                              │
│  用户线程-1              用户线程-2              用户线程-3                 │
│      │                      │                      │                        │
│      │ send(msg-A1)         │ send(msg-B1)         │ send(msg-A2)          │
│      ▼                      ▼                      ▼                        │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ProducerImpl.java                                                 │     │
│  │                                                                     │     │
│  │  // ① 入口方法 (行545)                                             │     │
│  │  sendAsync(Message<T> message, SendCallback callback)              │     │
│  │      │                                                              │     │
│  │      ├─► isValidProducerState()      // 检查状态 (行551)           │     │
│  │      ├─► canEnqueueRequest()         // 信号量控制 (行558)         │     │
│  │      ├─► applyCompression()          // 压缩处理 (行573)           │     │
│  │      ├─► updateMessageMetadata()     // 更新元数据 (行620)         │     │
│  │      │                                                              │     │
│  │      └─► synchronized (this) {       // ★ 进入同步块               │     │
│  │              sequenceId = updateMessageMetadataSequenceId();        │     │
│  │              serializeAndSendMessage(...);  // (行683)              │     │
│  │          }                                                          │     │
│  │                                                                     │     │
│  │  // 多线程并发调用，synchronized 块保证顺序                         │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  ▼ [synchronized 块内]                      │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ProducerImpl.java                                                 │     │
│  │                                                                     │     │
│  │  // ② 序列化并发送 (行742-850)                                     │     │
│  │  serializeAndSendMessage(...)                                      │     │
│  │      │                                                              │     │
│  │      ├─► [路径A] 批处理模式:                                        │     │
│  │      │       batchMessageContainer.add(msg, callback)              │     │
│  │      │       └─► triggerSendIfFullOrScheduleFlush()                │     │
│  │      │               └─► batchMessageAndSend()                     │     │
│  │      │                       └─► processOpSendMsg(op)              │     │
│  │      │                                                              │     │
│  │      └─► [路径B] 非批处理模式:                                      │     │
│  │              ByteBufPair cmd = sendMessage(...)                    │     │
│  │              OpSendMsg op = OpSendMsg.create(...)                  │     │
│  │              processOpSendMsg(op)  // (行848)                       │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  ▼ [synchronized 方法]                      │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ProducerImpl.java                                                 │     │
│  │                                                                     │     │
│  │  // ③ 网络发送处理 (行2462-2504)                                   │     │
│  │  protected synchronized void processOpSendMsg(OpSendMsg op)        │     │
│  │      │                                                              │     │
│  │      ├─► pendingMessages.add(op)     // 加入待确认队列 (行2474)    │     │
│  │      ├─► updateLastSeqPushed(op)     // 更新 sequenceId (行2475)  │     │
│  │      │                                                              │     │
│  │      └─► if (cnx != null) {                                       │     │
│  │              op.cmd.retain();        // 增加引用计数 (行2490)       │     │
│  │              cnx.ctx().channel().eventLoop().execute(              │     │
│  │                  WriteInEventLoopCallback.create(this, cnx, op)    │     │
│  │              );  // (行2491)                                        │     │
│  │          }                                                          │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  │ eventLoop().execute()                    │
│                                  │ [切换到 Netty EventLoop 线程]            │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ProducerImpl.java - WriteInEventLoopCallback (行1114-1165)        │     │
│  │                                                                     │     │
│  │  // ④ 在 Netty EventLoop 线程中执行                                │     │
│  │  private static final class WriteInEventLoopCallback               │     │
│  │      implements Runnable {                                          │     │
│  │                                                                     │     │
│  │      @Override                                                      │     │
│  │      public void run() {                                           │     │
│  │          // ★★★ 最终的网络发送操作 (行1139) ★★★                   │     │
│  │          cnx.ctx().writeAndFlush(cmd, cnx.ctx().voidPromise());    │     │
│  │          op.updateSentTimestamp();                                 │     │
│  │          recycle();  // 回收到对象池                                │     │
│  │      }                                                              │     │
│  │  }                                                                  │     │
│  │                                                                     │     │
│  │  ★ 同一 TCP 连接的消息由同一 EventLoop 线程串行发送                 │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ TCP 网络传输
                                   │ cnx.ctx().writeAndFlush(cmd)
                                   │
┌──────────────────────────────────┼──────────────────────────────────────────┐
│ BROKER 端进程 (pulsar-broker 模块)           │                              │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ServerCnx.java (Netty Handler)                                    │     │
│  │                                                                     │     │
│  │  // Netty 接收消息，EventLoop 线程执行 (行850)                     │     │
│  │  protected void handleSend(CommandSend send, ByteBuf data)         │     │
│  │      │                                                              │     │
│  │      ├─► long producerId = send.getProducerId()                    │     │
│  │      ├─► Producer producer = producers.get(producerId)             │     │
│  │      └─► producer.publishMessage(data, sequenceId, ...)            │     │
│  │                                                                     │     │
│  │  ★ 同一 Producer 的消息由同一 EventLoop 线程串行处理                │     │
│  │  ★ 不同 Producer 可能由不同 EventLoop 并发处理                      │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  AbstractTopic.java / PersistentTopic.java                         │     │
│  │                                                                     │     │
│  │  // 发布消息 (行633)                                                │     │
│  │  public void publishMessage(ByteBuf msg, long sequenceId, ...)     │     │
│  │      │                                                              │     │
│  │      ├─► 检查 Topic 状态 (isFenced)                                │     │
│  │      ├─► 消息去重检查 (可选)                                       │     │
│  │      └─► ledger.asyncAddEntry(msg, callback, ctx)                  │     │
│  │                                                                     │     │
│  │  ★ 可能被多线程并发调用，无显式锁                                   │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  ManagedLedgerImpl.java                                            │     │
│  │                                                                     │     │
│  │  // 异步写入入口 (行828)                                           │     │
│  │  public void asyncAddEntry(ByteBuf data, AddEntryCallback cb, ...) │     │
│  │      │                                                              │     │
│  │      ├─► buffer.retain()              // 保留引用                   │     │
│  │      └─► executor.execute(() -> {     // 提交到固定线程执行         │     │
│  │              OpAddEntry op = OpAddEntry.create(...)                │     │
│  │              internalAsyncAddEntry(op)                             │     │
│  │          })                                                         │     │
│  │                                                                     │     │
│  │  // 内部处理 (行844) - synchronized 方法                           │     │
│  │  protected synchronized void internalAsyncAddEntry(OpAddEntry op)  │     │
│  │      │                                                              │     │
│  │      ├─► pendingAddEntries.add(op)     // ★ 入队                   │     │
│  │      │                                                              │     │
│  │      └─► 根据状态决定是否立即写入:                                 │     │
│  │              if (state == LedgerOpened) {                          │     │
│  │                  addOperation.initiate()  // 立即发起写入           │     │
│  │              }                                                      │     │
│  │              // 其他状态(CreatingLedger/ClosingLedger)时等待       │     │
│  │                                                                     │     │
│  │  // 写入完成回调 - 在 OpAddEntry.run() 中处理 (OpAddEntry.java)    │     │
│  │  public void run() {                                               │     │
│  │      OpAddEntry first = pendingAddEntries.poll()  // 出队          │     │
│  │      cb.addComplete(position, data, ctx)         // 回调           │     │
│  │  }                                                                  │     │
│  │                                                                     │     │
│  │  ★ 核心串行化：chooseThread(name) + synchronized + pendingAddEntries│     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
│                                  ▼                                          │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  LedgerHandle.java (BookKeeper 客户端，在 Broker 进程中)           │     │
│  │                                                                     │     │
│  │  // 异步写入 (行450)                                               │     │
│  │  public void asyncAddEntry(byte[] data, AddCallback cb, ...)       │     │
│  │      │                                                              │     │
│  │      lock.lock();                                                  │     │
│  │      try {                                                         │     │
│  │          long entryId = lastAddPushed.getAndIncrement();           │     │
│  │          PendingAddOp op = PendingAddOp.create(entryId, ...);      │     │
│  │          pendingAdds.put(entryId, op);                             │     │
│  │          sendAddRequest(op);  // 发送到 Bookie                     │     │
│  │      } finally {                                                   │     │
│  │          lock.unlock();                                            │     │
│  │      }                                                             │     │
│  │                                                                     │     │
│  │  ★ entryId 原子递增，决定消息在 Ledger 中的位置                    │     │
│  └───────────────────────────────┬───────────────────────────────────┘     │
│                                  │                                          │
└──────────────────────────────────┼──────────────────────────────────────────┘
                                   │
                                   │ TCP 网络传输
                                   │
                                   ▼
                           BookKeeper (Bookie 进程)
```

### 6.7 多线程顺序保证的核心机制（源码级别）

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    各层顺序保证机制总结（含源码位置）                          │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│层级│进程   │ 源码文件           │ 关键函数/变量                    │ 机制    │
├─────────────────────────────────────────────────────────────────────────────┤
│ L1 │Client │ ProducerImpl.java  │ sendAsync()                      │信号量   │
│    │       │ 行545              │ msgIdGenerator.getAndIncrement() │seq递增  │
├─────────────────────────────────────────────────────────────────────────────┤
│ L2 │Client │ ClientCnx.java     │ channel.writeAndFlush()          │Netty    │
│    │       │                    │                                  │单线程   │
├─────────────────────────────────────────────────────────────────────────────┤
│ L3 │Broker │ ServerCnx.java     │ handleSend()                     │Netty    │
│    │       │ 行1975             │                                  │单线程   │
├─────────────────────────────────────────────────────────────────────────────┤
│ L4 │Broker │ PersistentTopic.java│ publishMessage()                │无锁     │
│    │       │ 行633              │                                  │依赖下层 │
├─────────────────────────────────────────────────────────────────────────────┤
│ L5 │Broker │ ManagedLedgerImpl  │ pendingAddEntries (Queue)        │chooseThread│
│    │       │ 行828,844         │ executor (OrderedExecutor)       │+synchronized│
│    │       │                    │ asyncAddEntry()                  │+队列     │
│    │       │                    │ internalAsyncAddEntry()          │         │
├─────────────────────────────────────────────────────────────────────────────┤
│ L6 │Broker │ LedgerHandle.java  │ lock (ReentrantLock)             │Lock+    │
│    │       │(BK客户端) 行450    │ lastAddPushed (AtomicLong)       │entryId  │
│    │       │                    │ asyncAddEntry()                  │递增     │
├─────────────────────────────────────────────────────────────────────────────┤
│ L7 │BK     │ Bookie.java        │ Journal.write()                  │顺序写   │
│    │       │                    │                                  │Journal  │
└─────────────────────────────────────────────────────────────────────────────┘

★ 核心串行化点：ManagedLedgerImpl.pendingAddEntries 队列
```

### 6.8 Broker 端 Netty EventLoop Hash 分配机制

**源码位置**: `BrokerService.java:240-241, 370-372, 581-592`

#### 6.8.1 EventLoopGroup 的创建

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    BrokerService 中的 EventLoopGroup 创建                   │
└─────────────────────────────────────────────────────────────────────────────┘

// BrokerService.java:240-241
private final EventLoopGroup acceptorGroup;  // 接收新连接
private final EventLoopGroup workerGroup;    // 处理 I/O 操作

// BrokerService.java:347 构造函数
public BrokerService(PulsarService pulsar, EventLoopGroup eventLoopGroup) throws Exception {
    // ...
    // 创建 acceptor 线程组（接收新连接）
    final DefaultThreadFactory acceptorThreadFactory =
            new ExecutorProvider.ExtendedThreadFactory("pulsar-acceptor");
    this.acceptorGroup = EventLoopUtil.newEventLoopGroup(
            pulsar.getConfiguration().getNumAcceptorThreads(), false, acceptorThreadFactory);

    // worker 线程组（处理 I/O，由外部传入）
    this.workerGroup = eventLoopGroup;
}

// BrokerService.java:581-592 ServerBootstrap 配置
private ServerBootstrap defaultServerBootstrap() {
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.option(ChannelOption.SO_REUSEADDR, true);
    bootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
    // ★ 关键：bossGroup 接收连接，workerGroup 处理 I/O
    bootstrap.group(acceptorGroup, workerGroup);
    bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
    // ...
    return bootstrap;
}
```

#### 6.8.2 Channel → EventLoop 的 Hash 分配机制

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Netty Channel 到 EventLoop 的 Hash 分配                   │
└─────────────────────────────────────────────────────────────────────────────┘

这是 Netty 框架的内部机制（位于 io.netty.channel.SingleThreadEventLoop）：

当新连接建立时，Channel 需要绑定到一个 EventLoop：
    1. ServerBootstrap 接收新连接
    2. 调用 EventLoopGroup.register(channel)
    3. EventLoopGroup 选择一个 EventLoop：

// Netty 源码（AbstractEventExecutorGroup.java 或 PowerOfTwoEventExecutorChooser）
public EventExecutor next() {
    // 方式1：取模运算（Netty 默认使用 power-of-two 优化）
    return executors[idx.getAndIncrement() & executors.length - 1];

    // 方式2：取模运算（通用情况）
    // int index = channel.hashCode() % eventLoops.length;
    // return eventLoops[index];
}

★ 核心机制：
    - Channel 的 hashCode() 决定它被分配到哪个 EventLoop
    - 一旦绑定，该 Channel 的所有 I/O 操作都由这个 EventLoop 处理
    - EventLoop 是单线程的，保证同一 Channel 的事件串行处理
```

#### 6.8.3 多 Producer 到 EventLoop 的分配示例

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Producer 连接到 EventLoop 的分配示例                      │
└─────────────────────────────────────────────────────────────────────────────┘

假设 workerGroup 有 8 个 EventLoop（对应 8 个线程）：

Producer 连接建立时：
    Producer-A 的 Channel (hashCode = 1001)
        1001 % 8 = 1 → EventLoop-1 处理

    Producer-B 的 Channel (hashCode = 2002)
        2002 % 8 = 2 → EventLoop-2 处理

    Producer-C 的 Channel (hashCode = 3003)
        3003 % 8 = 3 → EventLoop-3 处理

    Producer-D 的 Channel (hashCode = 1009)
        1009 % 8 = 1 → EventLoop-1 处理  ← 和 Producer-A 同一个线程！

┌─────────────────────────────────────────────────────────────────────────────┐
│  EventLoop-1 (Thread-1)                                                     │
│  ├── Channel-A (Producer-A)  ──► handleSend(A1), handleSend(A2), ...       │
│  └── Channel-D (Producer-D)  ──► handleSend(D1), handleSend(D2), ...       │
│                                                                              │
│  ★ 两个 Producer 的消息由同一个线程处理，但是各自保持顺序                    │
│  ★ EventLoop 轮询处理两个 Channel 的事件，互不干扰                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 6.8.4 同一 Producer 消息顺序保证的完整链路

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    同一 Producer 消息顺序保证的完整链路                      │
└─────────────────────────────────────────────────────────────────────────────┘

Producer-A 发送消息：A1, A2, A3

Client 端：
    1. synchronized (this) { sequenceId++ }  → A1(seq=1), A2(seq=2), A3(seq=3)
    2. EventLoop.execute(writeAndFlush)       → 按顺序发送

网络传输：
    TCP 保证字节流顺序 → A1, A2, A3 按顺序到达

Broker 端：
    1. Channel-A 的 hashCode 决定绑定到 EventLoop-1
    2. EventLoop-1 单线程处理 Channel-A 的所有事件：
       ┌──────────────────────────────────────┐
       │  handleSend(A1) → publishMessage(A1) │
       │  handleSend(A2) → publishMessage(A2) │
       │  handleSend(A3) → publishMessage(A3) │
       └──────────────────────────────────────┘
       严格按照到达顺序执行！

★ 关键点：
    1. Channel → EventLoop 的绑定在连接建立时确定，之后不变
    2. EventLoop 是单线程的，事件按 FIFO 顺序处理
    3. 同一 Channel 的所有事件（包括 handleSend）串行执行
```

#### 6.8.5 为什么还需要 ManagedLedger 的队列？

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    为什么 ManagedLedger 还需要队列？                         │
└─────────────────────────────────────────────────────────────────────────────┘

问题：既然 EventLoop 保证顺序，为什么 ManagedLedger 还要队列？

答案：因为同一 Topic 可能有多个 Producer（多个 Channel/EventLoop）

场景：
    EventLoop-1 处理 Producer-A 的消息 → Topic-X
    EventLoop-2 处理 Producer-B 的消息 → Topic-X（同一个 Topic！）
    EventLoop-3 处理 Producer-C 的消息 → Topic-X

这三个线程可能并发调用同一个 Topic 的 publishMessage()！

ManagedLedgerImpl 的队列机制：
    ┌─────────────────────────────────────────────────────────────────────┐
    │  asyncAddEntry(A1) [from EventLoop-1] ─┐                          │
    │  asyncAddEntry(B1) [from EventLoop-2] ─┼─► executor.execute()     │
    │  asyncAddEntry(C1) [from EventLoop-3] ─┘   (chooseThread(name))   │
    │                                                                     │
    │  所有请求被路由到同一个线程 (基于 name hash)：                       │
    │  executor = bookKeeper.getMainWorkerPool().chooseThread(name)     │
    │                                                                     │
    │  synchronized internalAsyncAddEntry() 串行处理：                   │
    │  - 消息入 pendingAddEntries 队列                                   │
    │  - 根据状态机决定是否立即写入                                       │
    │  - 写入完成后在 OpAddEntry.run() 中从队列取出下一个                │
    └─────────────────────────────────────────────────────────────────────┘

★ 总结：
    - EventLoop 保证：同一个 Producer 的消息顺序
    - ManagedLedger 队列保证：同一个 Topic 的写入串行化（跨 Producer）
```

---

## 7. 阶段5：Broker处理消息

### 7.1 ServerCnx.handleSend()

**源码位置**: `ServerCnx.java`

```
handleSend(CommandSend)
    │
    ├─► 解析消息元数据和 Payload
    ├─► 找到对应的 Producer 对象
    └─► producer.publishMessage()
```

### 7.2 PersistentTopic.publishMessage()

**源码位置**: `PersistentTopic.java:633-672`

```
publishMessage(headersAndPayload, ctx)
    │
    ├─► 检查 Topic 状态 (isFenced)
    ├─► 检查消息大小
    ├─► 消息去重检查
    │       messageDeduplication.isDuplicate()
    │
    └─► 执行持久化
            asyncAddEntry(headersAndPayload, ctx)
```

### 7.3 asyncAddEntry()

**源码位置**: `PersistentTopic.java:692-695`

```java
private void asyncAddEntry(ByteBuf data, PublishContext ctx) {
    ledger.asyncAddEntry(data, numMessages, callback, ctx);
}
```

---

## 8. 阶段6：持久化到BookKeeper

### 8.1 Broker 如何发现 Bookie（关键前置流程）

#### 8.1.1 Bookie 启动并注册到 ZooKeeper

```
Bookie 启动流程:
$ bin/bookkeeper bookie

Step 1: 读取配置文件 (bookkeeper.conf)
    ├─► zkServers=localhost:2181        # ZooKeeper 地址
    ├─► bookiePort=3181                 # Bookie 监听端口
    ├─► journalDirectory=/data/journal  # Journal 目录
    └─► ledgerDirectories=/data/ledgers # Ledger 目录

Step 2: 连接 ZooKeeper
    ZooKeeper zk = new ZooKeeperClient("localhost:2181", ...)

Step 3: 注册到 ZooKeeper (创建临时节点)
    路径: /ledgers/available/192.168.1.1:3181
    数据: {"host": "192.168.1.1", "port": 3181, "rack": "/default-rack"}

    ★ 这是临时节点 (Ephemeral)
    ★ Bookie 宕机或会话断开，节点自动删除

Step 4: 启动网络服务
    监听端口 3181，等待 Broker 连接
```

**ZooKeeper 中的 Bookie 注册信息:**

```
/ledgers/
├── available/                              # 可用 Bookie 列表
│   ├── 192.168.1.1:3181                   # Bookie 1 (临时节点)
│   ├── 192.168.1.2:3181                   # Bookie 2 (临时节点)
│   └── 192.168.1.3:3181                   # Bookie 3 (临时节点)
│
├── segments/                               # Ledger 元数据
│   ├── L12345/
│   │   └── metadata: {...}
│   └── ...
│
└── cookies/                                # Bookie Cookie (唯一标识)
    └── ...
```

#### 8.1.2 Broker 启动时创建 BookKeeper 客户端

**源码位置**: `BookKeeperClientFactoryImpl.java:66-89`

```java
// Broker 启动时
BookKeeperClientFactory.create(conf, metadataStore, eventLoopGroup, ...)
    │
    ├─► 创建 BookKeeper 客户端配置
    │       ClientConfiguration bkConf = new ClientConfiguration();
    │       bkConf.setMetadataServiceUri("zk:localhost:2181/ledgers");
    │
    ├─► 设置 EnsemblePlacementPolicy (Bookie选择策略)
    │       - RackawareEnsemblePlacementPolicy (机架感知)
    │       - RegionAwareEnsemblePlacementPolicy (区域感知)
    │
    └─► 创建 BookKeeper 客户端
            BookKeeper.forConfig(bkConf).build()
            │
            └─► 连接 ZooKeeper，获取 Bookie 列表
                    读取 /ledgers/available/ 下的所有节点
```

**BookKeeper 客户端初始化流程:**

```
BookKeeper 客户端
    │
    ├─► 连接 ZooKeeper
    │
    ├─► 读取 /ledgers/available/ 获取所有可用 Bookie
    │       结果: [bookie1:3181, bookie2:3181, bookie3:3181]
    │
    ├─► 初始化 EnsemblePlacementPolicy
    │       - 维护 Bookie 列表
    │       - 监听 Bookie 状态变化 (Watch ZooKeeper)
    │       - 实现负载均衡策略
    │
    └─► 定期更新 Bookie 列表
            Watch /ledgers/available/ 目录
            Bookie 上线/下线时自动更新
```

#### 8.1.3 创建 Ledger 时选择 Bookie

**源码位置**: `ManagedLedgerImpl.java`

```
创建 Ledger 流程:

ManagedLedger.createLedger()
    │
    └─► BookKeeper.createLedger(ensemble=3, writeQuorum=2, ackQuorum=2)
            │
            ├─► ① 从 EnsemblePlacementPolicy 获取可用 Bookie 列表
            │       List<BookieId> availableBookies =
            │           placementPolicy.onClusterChange(bookieList)
            │
            ├─► ② 选择 Bookie (考虑以下因素)
            │       ├─► 机架感知: 同一机架不能有太多副本
            │       ├─► 负载均衡: 选择负载较低的 Bookie
            │       ├─► 隔离策略: 某些 Topic 只能用特定 Bookie
            │       └─► 故障域: 避免单点故障
            │
            ├─► ③ 选出 ensemble 个 Bookie (例如 3 个)
            │       selectedBookies = [bookie1:3181, bookie2:3181, bookie3:3181]
            │
            ├─► ④ 在 ZooKeeper 创建 Ledger 元数据
            │       路径: /ledgers/segments/L12345/metadata
            │       数据: {
            │           "ensemble": ["bookie1:3181", "bookie2:3181", "bookie3:3181"],
            │           "writeQuorum": 2,
            │           "ackQuorum": 2,
            │           "length": 0,
            │           "lastEntryId": -1,
            │           "state": "OPEN"
            │       }
            │
            └─► ⑤ 返回 LedgerHandle
                    包含选定的 Bookie 列表和 Ledger 元数据
```

#### 8.1.4 EnsemblePlacementPolicy 选择策略

```
RackawareEnsemblePlacementPolicy (机架感知策略):

选择 Bookie 时考虑:
    1. 机架分布
       ┌─────────────────────────────────────────────────────────┐
       │  Rack 1                Rack 2                Rack 3    │
       │  ┌─────────┐          ┌─────────┐          ┌─────────┐│
       │  │ Bookie 1│          │ Bookie 2│          │ Bookie 3││
       │  │ Bookie 4│          │ Bookie 5│          │ Bookie 6││
       │  └─────────┘          └─────────┘          └─────────┘│
       │                                                        │
       │  选择策略: 优先选择不同机架的 Bookie                   │
       │  例如: [Bookie1, Bookie2, Bookie3] (每个机架一个)     │
       └─────────────────────────────────────────────────────────┘

    2. 负载均衡
       - 维护每个 Bookie 的负载信息
       - 优先选择负载较低的 Bookie

    3. 故障隔离
       - 检测 Bookie 健康状态
       - 自动排除不健康的 Bookie
```

#### 8.1.5 完整流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Broker 发现 Bookie 并写入的完整流程                       │
└─────────────────────────────────────────────────────────────────────────────┘

                      ┌──────────────────────────────────────┐
                      │            ZooKeeper                 │
                      │                                      │
                      │  /ledgers/available/                 │
                      │  ├── bookie1:3181 (临时节点)        │
                      │  ├── bookie2:3181 (临时节点)        │
                      │  └── bookie3:3181 (临时节点)        │
                      └──────────────┬───────────────────────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         │                           │                           │
         ▼                           ▼                           ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│    Bookie 1     │         │    Bookie 2     │         │    Bookie 3     │
│  启动时注册     │         │  启动时注册     │         │  启动时注册     │
└─────────────────┘         └─────────────────┘         └─────────────────┘


Broker 启动:
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  BookKeeperClientFactory.create()                                           │
│      │                                                                       │
│      ├─► 连接 ZooKeeper                                                     │
│      │                                                                       │
│      ├─► 读取 /ledgers/available/ 获取 Bookie 列表                          │
│      │       结果: [bookie1, bookie2, bookie3]                              │
│      │                                                                       │
│      └─► 初始化 EnsemblePlacementPolicy (Bookie选择策略)                     │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘


创建 Ledger (写入消息时):
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  BookKeeper.createLedger(ensemble=3, writeQuorum=2, ackQuorum=2)            │
│      │                                                                       │
│      ├─► EnsemblePlacementPolicy.selectBookies()                            │
│      │       │                                                               │
│      │       ├─► 考虑机架分布                                               │
│      │       ├─► 考虑负载均衡                                               │
│      │       └─► 选出: [bookie1, bookie2, bookie3]                          │
│      │                                                                       │
│      └─► 在 ZooKeeper 创建 Ledger 元数据                                     │
│              /ledgers/segments/L12345/metadata                              │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘


写入消息:
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  LedgerHandle.asyncAddEntry(data)                                           │
│      │                                                                       │
│      └─► 并行发送到选定的 Bookie                                            │
│              ├─► bookie1:3181  ──► Ack ✓                                   │
│              ├─► bookie2:3181  ──► Ack ✓                                   │
│              └─► bookie3:3181  ──► (稍后)                                   │
│                                                                              │
│          收到 ackQuorum(2) 个 Ack → 写入成功                                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 8.2 ManagedLedger 写入

```
ManagedLedgerImpl.asyncAddEntry()
    │
    ├─► 获取当前写入的 Ledger
    │
    ├─► 如果 Ledger 已满，创建新 Ledger
    │       ├─► 从 EnsemblePlacementPolicy 获取 Bookie 列表
    │       ├─► 选择 ensemble 个 Bookie
    │       └─► 在 ZooKeeper 创建 Ledger 元数据
    │
    └─► 调用 BookKeeper 客户端写入
```

### 8.3 BookKeeper Quorum 写入

```
配置: ensemble=3, writeQuorum=3, ackQuorum=2

写入流程:
    Broker
      │
      │ asyncAddEntry(entry)
      ▼
    ┌─────────────────────────────────────────────┐
    │                                             │
    │   Bookie 1      Bookie 2      Bookie 3     │
    │      │             │             │         │
    │   写入Entry    写入Entry    写入Entry      │
    │   写入Journal  写入Journal  写入Journal   │
    │      │             │             │         │
    │    Ack ✓        Ack ✓      (延迟中)       │
    │      │             │                       │
    │      └──────┬──────┘                       │
    │             │                              │
    │        收到 2 个 Ack                        │
    │        (ackQuorum=2)                       │
    │             │                              │
    │             ▼                              │
    │        ★ 写入成功 ★                        │
    └─────────────────────────────────────────────┘
```

#### 8.3.1 E/W/A 参数详解

**参数定义:**

| 参数 | 名称 | 含义 | 约束 |
|------|------|------|------|
| **E** | Ensemble | 一个 Ledger 关联的 Bookie 总数 | E ≥ W |
| **W** | Write Quorum | 每条 Entry 写入的副本数 | W ≥ A |
| **A** | Ack Quorum | 成功确认需要的 Ack 数 | A ≤ W ≤ E |

**E/W/A 与 Ledger/Bookie 的关系:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    E/W/A 参数与 Ledger/Bookie 关系                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  配置示例: E=5, W=3, A=2                                                    │
│                                                                              │
│  含义解读:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  E=5 (Ensemble)                                                     │   │
│  │  ├── 一个 Ledger 的数据分布在 5 个 Bookie 上                        │   │
│  │  ├── Ledger 元数据中记录 5 个 Bookie 地址                          │   │
│  │  └── 当 Bookie 故障时，有备用节点可用                               │   │
│  │                                                                      │   │
│  │  W=3 (Write Quorum)                                                 │   │
│  │  ├── 每条 Entry 写入 3 个副本                                       │   │
│  │  ├── 数据冗余度为 3                                                 │   │
│  │  └── 最多可容忍 2 个节点故障不丢数据                                │   │
│  │                                                                      │   │
│  │  A=2 (Ack Quorum)                                                   │   │
│  │  ├── 收到 2 个 Ack 即返回成功                                       │   │
│  │  ├── 写入延迟取决于第 2 快的 Bookie                                 │   │
│  │  └── 平衡了性能与可靠性                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ★ 关键理解: W=3 意味着需要 3 个 Bookie 存储副本，                          │
│     但 E 可以大于 W，提供故障时的备用节点                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**常见配置场景:**

| 场景 | E | W | A | 说明 |
|------|---|---|---|------|
| 高性能 | 3 | 2 | 1 | 快速确认，适合容忍少量数据丢失 |
| 平衡型 | 3 | 2 | 2 | 平衡性能和可靠性 |
| 高可靠 | 5 | 3 | 2 | 多副本+备用节点，生产环境推荐 |
| 极高可靠 | 6 | 4 | 3 | 金融级别，容忍更多故障 |

#### 8.3.2 Ledger 条带化与数据散列

**条带化写入 (Striping):**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Ledger 条带化写入机制                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  配置: E=5, W=3, A=2                                                        │
│  Ledger 的 Bookie 列表: [B1, B2, B3, B4, B5]                                │
│                                                                              │
│  Entry 条带化分布:                                                          │
│                                                                              │
│         Bookie 1   Bookie 2   Bookie 3   Bookie 4   Bookie 5               │
│         ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐                  │
│  Entry 0│   ✓   │ │   ✓   │ │   ✓   │ │       │ │       │ ← 写入 [B1,B2,B3]│
│  Entry 1│       │ │   ✓   │ │   ✓   │ │   ✓   │ │       │ ← 写入 [B2,B3,B4]│
│  Entry 2│       │ │       │ │   ✓   │ │   ✓   │ │   ✓   │ ← 写入 [B3,B4,B5]│
│  Entry 3│   ✓   │ │       │ │       │ │   ✓   │ │   ✓   │ ← 写入 [B4,B5,B1]│
│  Entry 4│   ✓   │ │   ✓   │ │       │ │       │ │   ✓   │ ← 写入 [B5,B1,B2]│
│         └───────┘ └───────┘ └───────┘ └───────┘ └───────┘                  │
│                                                                              │
│  ★ 条带化效果:                                                               │
│  • 每个 Entry 只写入 W=3 个 Bookie                                          │
│  • 连续 Entry 轮转使用不同的 Bookie 组合                                    │
│  • 负载均匀分布到 E=5 个 Bookie 上                                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**多个 Ledger 实现数据散列:**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    多 Ledger 数据散列 (Sharding)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Topic A 的消息流 (随时间滚动创建多个 Ledger):                              │
│                                                                              │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐            │
│  │ Ledger 1  │ → │ Ledger 2  │ → │ Ledger 3  │ → │ Ledger 4  │ → ...      │
│  │ (Hour 1)  │   │ (Hour 2)  │   │ (Hour 3)  │   │ (Hour 4)  │            │
│  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘            │
│        │               │               │               │                   │
│        ▼               ▼               ▼               ▼                   │
│   ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐             │
│   │B1,B2,B3 │     │B2,B4,B5 │     │B1,B3,B5 │     │B2,B3,B4 │             │
│   └─────────┘     └─────────┘     └─────────┘     └─────────┘             │
│                                                                              │
│  ★ 每个 Ledger 创建时重新选择 Bookie (通过 EnsemblePlacementPolicy)        │
│  ★ 随着时间滚动，数据自动散列到整个 Bookie 集群                             │
│  ★ 避免热点问题，实现负载均衡                                               │
│                                                                              │
│  Bookie 集群视图 (数据分布):                                                │
│  ┌───────────────────────────────────────────────────────────────────┐     │
│  │  Bookie 1       Bookie 2       Bookie 3       Bookie 4       Bookie 5 │ │
│  │  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐│ │
│  │  │Ledger 1 │   │Ledger 1 │   │Ledger 1 │   │Ledger 2 │   │Ledger 2 ││ │
│  │  │Ledger 3 │   │Ledger 2 │   │Ledger 3 │   │Ledger 3 │   │Ledger 3 ││ │
│  │  │  ...    │   │Ledger 4 │   │Ledger 4 │   │Ledger 4 │   │  ...    ││ │
│  │  └─────────┘   └─────────┘   └─────────┘   └─────────┘   └─────────┘│ │
│  └───────────────────────────────────────────────────────────────────┘     │
│                                                                              │
│  ★ Ledger ID 的重要性:                                                      │
│  • 全局唯一标识一个日志段                                                    │
│  • 用于定位消息的物理存储位置                                                │
│  • 通过 Ledger → Bookie 映射实现数据散列                                    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Ledger 滚动触发条件:**

| 触发条件 | 配置参数 | 说明 |
|---------|---------|------|
| 时间滚动 | `managedLedgerMaxLedgerRolloverTimeMinutes` | 默认 4 小时 |
| 大小滚动 | `managedLedgerMaxEntriesPerLedger` | 默认 50000 条 |
| 空间滚动 | `managedLedgerMaxSizePerLedgerMbytes` | 默认 100MB |

### 8.4 持久化完成回调

**源码位置**: `PersistentTopic.java:735-748`

```java
addComplete(Position pos, ByteBuf data, Object ctx) {
    Position position = pos;  // (ledgerId=12345, entryId=67890)

    // 返回 MessageId 给客户端
    ctx.completed(null,
        position.getLedgerId(),   // 12345
        position.getEntryId());   // 67890
}
```

---

## 9. 阶段7：返回确认给客户端

### 9.1 Broker 发送响应

```
CommandSendReceipt {
    producerId: 1
    sequenceId: 100
    ledgerId: 12345
    entryId: 67890
}
```

### 9.2 Client 接收响应

**源码位置**: `ClientCnx.java`

```
handleSendReceipt(receipt)
    │
    ├─► 解析: ledgerId, entryId
    │
    ├─► 构造 MessageId
    │       MessageIdImpl(ledgerId=12345, entryId=67890, partitionIndex=0)
    │
    └─► producer.ackReceived(sequenceId, messageId)
            ├─► 从 pendingMessages 队列移除
            ├─► 释放信号量
            └─► 触发用户回调
```

### 9.3 用户收到结果

```java
MessageId messageId = producer.send("message-1");
System.out.println("发送成功: " + messageId);
// 输出: 发送成功: 12345:67890:0
```

---

## 10. 核心概念详解

### 10.1 Topic vs Bundle vs 分区

| 概念 | 作用 | 配置级别 |
|------|------|---------|
| **Topic** | 消息目的地 | 用户定义 |
| **Bundle** | 负载均衡单位，决定由哪个Broker服务 | Namespace级别 |
| **分区(Partition)** | 并行处理，提高吞吐量 | Topic级别 |

### 10.2 Namespace Bundle 分配机制

```
Bundle → Broker 映射存储在 ZooKeeper:

/loadbalance/bundles/{tenant}/{namespace}/{bundleRange}
    │
    └─► data: {
            "broker": "broker1:8080",
            "native_broker": "broker1:6650"
        }

分配流程:
1. 检查 ZooKeeper 是否有 Broker 负责
2. 无 → LoadManager 选择最空闲的 Broker
3. 在 ZooKeeper 创建临时节点 (分布式锁)
4. 成功的 Broker 负责 Bundle
```

### 10.3 BookKeeper 存储架构

```
Bookie 节点目录结构:

/data/bookkeeper/
├── journal/                    # Journal (WAL)
│   └── txn.log                 # 顺序写入
│
├── ledgers/                    # Ledger 数据
│   └── current/
│       └── {ledgerId}/
│           └── {entryId}.log
│
└── index/                      # 索引

写入流程:
1. 收到 Entry → 先写入 Journal (顺序写)
2. Journal 写入成功 → 返回 Ack
3. 后台异步写入 Ledger 文件
```

### 10.4 元数据 vs 实际数据

| 类型 | 存储位置 | 内容 | 大小 |
|------|---------|------|------|
| **元数据** | ZooKeeper | 配置、映射、状态 | KB-MB |
| **实际数据** | BookKeeper | 消息内容 | GB-TB |

### 10.5 持久化 vs 非持久化 Topic

| 特性 | persistent:// | non-persistent:// |
|------|--------------|-------------------|
| 存储 | BookKeeper | Broker内存 |
| 可靠性 | 高 | Broker重启丢失 |
| 延迟 | 较高 | 低 |
| 吞吐量 | 适中 | 高 |

---

## 11. 关键源码位置索引

### Client 端

| 功能 | 文件路径 | 关键方法 |
|------|---------|---------|
| Client构建 | `ClientBuilderImpl.java` | `build()` |
| Client实现 | `PulsarClientImpl.java` | 构造函数 |
| Producer构建 | `ProducerBuilderImpl.java` | `create()` |
| Producer实现 | `ProducerImpl.java` | `sendAsync()` |
| 网络连接 | `ClientCnx.java` | `channelActive()`, `handleSendReceipt()` |
| 连接池 | `ConnectionPool.java` | `getConnection()` |
| Lookup服务 | `BinaryProtoLookupService.java` | `findBroker()` |

### Broker 端

| 功能 | 文件路径 | 关键方法 |
|------|---------|---------|
| 连接处理 | `ServerCnx.java` | `handleSend()`, `handleLookup()` |
| Topic管理 | `PersistentTopic.java` | `publishMessage()`, `asyncAddEntry()` |
| Namespace服务 | `NamespaceService.java` | `getBrokerServiceUrlAsync()` |
| Bundle分配 | `OwnershipCache.java` | `tryAcquiringOwnership()` |

### BookKeeper

| 功能 | 说明 |
|------|------|
| Ledger | 有序日志条目序列 |
| Entry | 单条消息记录 |
| Bookie | 存储节点 |
| Journal | WAL日志 |
| Quorum | 多副本确认机制 |

---

## 附录：完整时序图

```
┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐  ┌────────┐
│Producer│  │ClientCnx│  │ServerCnx│  │Topic   │  │Ledger  │  │Bookie  │  │ ZooKeeper│
│  Impl  │  │        │  │        │  │        │  │        │  │        │  │        │
└───┬────┘  └───┬────┘  └───┬────┘  └───┬────┘  └───┬────┘  └───┬────┘  └───┬────┘
    │           │           │           │           │           │           │
    │ ════════════════ 初始化 ════════════════│           │           │
    │           │           │           │           │           │           │
    │ create()  │           │           │           │           │           │
    │──────────►│           │           │           │           │           │
    │           │           │           │           │           │           │
    │           │ ═══════════ Lookup & 连接 ═══════════════════│           │
    │           │           │           │           │           │           │
    │           │ grabCnx() │           │           │           │           │
    │           │───────────────────────────────────────────────────────────►│
    │           │◄─────────────────────────────────────────────────────────  │
    │           │ Broker地址│           │           │           │           │
    │           │           │           │           │           │           │
    │           │ TCP+认证  │           │           │           │           │
    │           │──────────────────────►│           │           │           │
    │           │           │           │           │           │           │
    │           │ PRODUCER命令         │           │           │           │
    │           │──────────────────────►│           │           │           │
    │           │◄──────────────────────│           │           │           │
    │           │           │           │           │           │           │
    │ ════════════════ 消息发送 ══════════════════│           │           │
    │           │           │           │           │           │           │
    │ send(msg) │           │           │           │           │           │
    │──────────►│           │           │           │           │           │
    │           │           │           │           │           │           │
    │           │ CommandSend│           │           │           │           │
    │           │──────────────────────►│           │           │           │
    │           │           │           │           │           │           │
    │           │           │ publishMessage()      │           │           │
    │           │           │──────────────────────►│           │           │
    │           │           │           │           │           │           │
    │           │           │           │ asyncAddEntry()        │           │
    │           │           │           │──────────────────────►│           │
    │           │           │           │           │           │           │
    │           │           │           │           │   写入    │           │
    │           │           │           │           │──────────────────────►│
    │           │           │           │           │◄──────────────────────│
    │           │           │           │           │  2/3 Ack │           │
    │           │           │           │           │           │           │
    │           │           │           │ addComplete()         │           │
    │           │           │           │◄──────────────────────│           │
    │           │           │           │           │           │           │
    │           │           │ SendReceipt│           │           │           │
    │           │◄──────────────────────│           │           │           │
    │           │           │           │           │           │           │
    │ MessageId │           │           │           │           │           │
    │◄──────────│           │           │           │           │           │
```

---

## 总结

**Pulsar 消息生产的完整流程**:

1. **客户端初始化**: 创建 PulsarClient 和 Producer，配置压缩、批处理等参数
2. **服务发现**: 通过 Lookup 根据 Topic 找到负责的 Broker (Bundle → Broker 映射)
3. **连接建立**: TCP 连接 + Token 认证 + Producer 注册
4. **消息发送**: 压缩 → 批处理 → 网络传输
5. **Broker处理**: 接收消息 → 去重检查 → 分发到 PersistentTopic
6. **持久化**: 通过 ManagedLedger 写入 BookKeeper，多副本存储
7. **确认返回**: Quorum 确认后返回 MessageId 给客户端

---

*文档基于 Apache Pulsar 源码分析整理*
