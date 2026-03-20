# storage metadata 对比

## 1. 总体说明

原生 Pulsar 的 metadata 不是一份“消息内容清单”，而是一套**资源层真相来源**。  
broker 依赖这层 metadata 知道：

- 系统里有哪些 tenant
- tenant 下有哪些 namespace
- namespace 下有哪些 topic
- 哪些 topic 是 partitioned topic，分区数是多少
- topic 下有哪些 subscription
- 每一层资源还挂了哪些 policy、schema、配置项

这些 metadata 和消息 payload、cursor、pending ack、dispatcher 运行时状态不是同一层东西。

当前 `pulsar-lite` 这次实现的是：

- 轻量资源层 metadata
- 本地 JSON 持久化
- 重启后恢复 topic 的分区结构

没有实现的是：

- 完整 policy 体系
- schema
- subscription 状态恢复
- 独立 admin 管理面

下面按资源层级从上到下描述。

---

## 2. tenant 层

### 2.1 原生 Pulsar 中 tenant metadata 的作用

tenant 是资源树的最上层。原生 Pulsar 中，tenant metadata 不只是“有这个名字”，通常还承担：

- 标识 tenant 是否存在
- 记录 tenant 的 admin roles
- 记录 tenant 允许落在哪些 clusters
- 作为 namespace 的父资源
- 作为 admin API 和权限控制的基础单位

也就是说，tenant metadata 至少要回答两个问题：

1. 这个 tenant 是否存在
2. 这个 tenant 的管理边界和可用范围是什么

### 2.2 tenant 层 metadata 至少应覆盖的功能

从资源结构角度看，tenant 层 metadata 至少应支持：

- tenant 资源存在性判断
- tenant 名称持久化
- 作为 namespace 的父资源进行引用
- 为后续 tenant policy、权限控制、集群范围控制预留挂载点
- broker 重启后仍能恢复 tenant 资源树根节点
### 2.3 当前 pulsar-lite 已实现的部分

当前 `pulsar-lite` 已实现：

- `TenantMetadata { name }`
- 标准 topic 名解析后自动 ensure tenant
- tenant metadata 落盘到本地 JSON

这意味着现在已经能保证：

- 只要 broker 见过 `persistent://tenant/namespace/topic`
- 对应 tenant 名就会进入 metadata 快照
- broker 重启后 tenant 资源名仍存在

### 2.4 当前仍与原生 Pulsar 的差异

当前 tenant 层仍然缺少：

- admin roles
- allowed clusters
- tenant policy
- tenant create/update/delete 管理链
- admin 查询与修改入口

所以现在 `pulsar-lite` 的 tenant metadata 只是：

- **存在性登记**

还不是原生 Pulsar 那种：

- **存在性 + 权限/集群边界 + 管理能力**

---

## 3. namespace 层

### 3.1 原生 Pulsar 中 namespace metadata 的作用

namespace 是 tenant 下的资源容器。原生 Pulsar 中，namespace metadata 承担的职责比 tenant 更重，通常包括：

- 标识 namespace 是否存在
- 作为 topic 的父资源
- 承载 namespace 级 policy
- 为 topic 默认配置提供继承来源

这些 policy 可能包括：

- retention
- replication
- backlog quota
- dispatch / publish 限制
- auth / schema 兼容策略

换句话说，namespace metadata 的作用不只是“目录结构”，而是：

- **topic 行为规则的上一层默认配置来源**

### 3.2 namespace 层 metadata 至少应覆盖的功能

从资源结构角度看，namespace 层 metadata 至少应支持：

- namespace 资源存在性判断
- 明确归属到某个 tenant
- 作为 topic 的直接父资源
- 为 namespace policy 提供挂载点
- broker 重启后仍能恢复 `tenant/namespace` 结构关系
### 3.3 当前 pulsar-lite 已实现的部分

当前 `pulsar-lite` 已实现：

- `NamespaceMetadata { tenant, name }`
- 标准 topic 名解析后自动 ensure namespace
- namespace metadata 落盘到本地 JSON

这意味着现在已经能保证：

- topic 不是孤立字符串
- 它会被归属到 `tenant / namespace / local_name` 结构下
- 重启后 namespace 名称能恢复

### 3.4 当前仍与原生 Pulsar 的差异

当前 namespace 层仍然缺少：

- namespace policies
- create/update/delete 管理链
- namespace 级配置继承逻辑
- admin 查询与修改入口

所以目前 namespace metadata 的能力仍然只是：

- **资源层级定位**

而不是原生 Pulsar 那种：

- **资源定位 + 默认策略承载**

---

## 4. topic 层

### 4.1 原生 Pulsar 中 topic metadata 的作用

topic 是资源树里最关键的一层。原生 Pulsar 中，topic metadata 至少承担：

- 标识 topic 是否存在
- 将 topic 绑定到 tenant / namespace
- 区分普通 topic 和 partitioned topic
- 对 partitioned topic 记录逻辑 topic 的 `partition_count`
- 作为 topic policy、schema、subscription、运行时 topic 实例的父资源

其中最关键的部分是：

- **partitioned topic metadata**

例如：

- `persistent://public/default/my-topic`
- `partitions = 3`

这条 metadata 的作用是：

1. broker 知道它是一个逻辑 partitioned topic
2. lookup / partition metadata 查询时能返回 `3`
3. client 会继续访问：
   - `my-topic-partition-0`
   - `my-topic-partition-1`
   - `my-topic-partition-2`
4. broker 重启后仍然知道这个逻辑 topic 应该展开成 3 个 partition

也就是说，原生 Pulsar 的核心不是“把每个 `-partition-N` 持久化成独立资源”，而是：

- **持久化逻辑 topic 的分区结构**

### 4.2 topic 层 metadata 至少应覆盖的功能

从资源结构角度看，topic 层 metadata 至少应支持：

- topic 名称和标准路径解析
- topic 归属到 tenant / namespace
- 区分普通 topic 和 partitioned topic
- 对 partitioned topic 持久化 `partition_count`
- 重启后恢复逻辑 topic 的分区结构
- 为 schema、topic policy、subscription 等下层资源预留挂载点
### 4.3 当前 pulsar-lite 已实现的部分

当前 `pulsar-lite` 在 topic 层已经补上了本轮最核心的能力：

- `parse_topic_name(topic)`，严格解析：
  - `domain`
  - `tenant`
  - `namespace`
  - `local_name`
- `TopicMetadata`
  - `full_name`
  - `domain`
  - `tenant`
  - `namespace`
  - `local_name`
  - `partitioned`
  - `partition_count`
- topic metadata 自动 ensure
- partitioned topic 的 `partition_count` 本地 JSON 落盘
- broker 启动时恢复 `partition_metadata`
- `get_or_create_topic_auto()` 会按恢复出的 `partition_count` 重新创建 partitioned topic

这一层已经具备的实际语义是：

- 如果某个逻辑 topic 上次运行时是 3 分区
- broker 重启后仍会把它当成 3 分区 topic

### 4.4 当前 pulsar-lite 在 topic 层额外修正的关键点

为了更贴近原生 Pulsar，这次还补了两个关键行为：

#### 1. 资源树和 partitioned topic 结构 metadata 分离

现在对：

- `persistent://public/default/my-topic-partition-0`

这类分区实例访问，资源树中允许保留具体 topic 节点；同时：

- 逻辑 topic `persistent://public/default/my-topic`
- 分区结构 `partitions = N`

会被单独保存到顶层 `partitioned_topics`。

这样一来：

- 资源树更接近 `managed-ledgers/{tenant}/{namespace}/persistent/{topic}`
- 逻辑 topic 的分区结构更接近原生 Pulsar 的 `partitioned-topics/...` 语义

本轮继续向这个方向又收了一步：

- namespace 节点下不再保留额外的 `domains` 包装层
- JSON 直接写成 `tenant -> namespace -> persistent -> topic -> subscriptions`
- 对 partitioned topic，不再在资源树里额外写一个空的逻辑 topic 节点
- 逻辑 topic 的分区信息只保留在顶层 `partitioned_topics`

例如现在 partitioned topic 的 JSON 语义更接近：

```json
{
  "./pulsar-lite.metadata.json": {
    "public": {
      "default": {
        "persistent": {
          "my-topic-partition-0": {
            "subscriptions": {
              "sub-a": {}
            }
          }
        }
      }
    }
  },
  "partitioned_topics": {
    "persistent://public/default/my-topic": {
      "partitions": 3
    }
  }
}
```

这里有两个边界需要明确：

- `my-topic` 这个逻辑 partitioned topic 不再作为空节点重复挂在资源树里
- 资源树只表达“具体资源存在和订阅挂载”，逻辑分区结构单独放在 `partitioned_topics`

#### 2. producer / consumer 入口改为走自动 topic 路由

之前 producer / consumer 入口直接 `get_or_create_topic()`，会把分区实例误当成普通 topic 创建。  
现在入口已经改成走 `get_or_create_topic_auto()`，保证：

- 分区 topic 的运行时实例
- 和 metadata 中记录的资源树 / partitioned topic 结构

是对齐的。

### 4.5 当前仍与原生 Pulsar 的差异

当前 topic 层仍然缺少：

- topic policy
- schema
- topic create/update/delete 管理链
- partition count 变更管理
- topic 级 admin 查询
- 更完整的 topic 附属资源

所以目前 topic metadata 的能力是：

- **标准命名解析**
- **逻辑 topic 结构建模**
- **partitioned topic 分区结构恢复**

还不是原生 Pulsar 那种完整的 topic 资源体系。

---

## 5. subscription 层

### 5.1 原生 Pulsar 中 subscription metadata 的作用

subscription 在原生 Pulsar 里不只是一个名字。它通常至少关联：

- subscription name
- topic
- cursor / mark-delete
- durable state
- backlog
- 运行时 dispatcher / consumer 状态

也就是说，subscription 在原生 Pulsar 中其实横跨两层：

1. 资源层：
   - 有这个 subscription 名
2. 状态层：
   - 它消费到哪了
   - 当前 backlog 是多少
   - 是否有未确认状态

### 5.2 subscription 层 metadata 至少应覆盖的功能

从资源结构角度看，subscription 层 metadata 至少应支持：

- subscription 名称存在性判断
- subscription 明确归属到某个具体 topic
- 为 cursor、mark-delete、backlog、pending ack 等运行时状态预留绑定点
- broker 重启后至少能恢复“这个 topic 下有这个 subscription 名”
### 5.3 当前 pulsar-lite 已实现的部分

当前 `pulsar-lite` 已实现：

- `SubscriptionMetadata { topic, name }`
- subscription 自动 ensure
- subscription name 落盘到本地 JSON
- 对 partitioned topic 的 subscription metadata 保留在具体 partition topic 下

这意味着现在可以保证：

- 某个 topic 下曾经创建过某个 subscription name
- broker 重启后这条资源记录仍存在

### 5.4 当前仍与原生 Pulsar 的差异

当前 subscription 层还没有实现：

- cursor 持久化恢复
- mark-delete / 消费进度恢复
- backlog 恢复
- pending ack 恢复
- assignment 恢复
- subscription 管理接口

所以当前 subscription metadata 的真实语义只是：

- **subscription 名字登记**

而不是原生 Pulsar 那种：

- **subscription 资源 + 持久化消费状态**

---

## 6. 当前实现边界

这次实现明确只覆盖**资源结构层 metadata**，不覆盖运行时状态。

当前已覆盖：

- tenant 名
- namespace 名
- 具体 topic 名
- subscription 名
- partitioned topic 的逻辑 `partition_count`
- 本地分层 JSON
- broker 启动恢复 partitioned topic 结构

当前未覆盖：

- message payload
- cursor / 消费进度
- pending ack
- assignment
- topic policy
- schema
- tenant / namespace policy
- admin API
- 删除 / 更新资源的完整管理链

因此当前 broker 重启后的语义是：

- 可以恢复“有哪些资源”
- 可以恢复“某个 topic 的分区结构”
- 不能恢复“某个 subscription 消费到哪里了”
- 不能恢复运行时 in-flight 状态

---

## 7. 当前分层 JSON 方案与原生 Pulsar 的结构差异

### 7.1 当前 `pulsar-lite` 的实现思路

当前 `pulsar-lite` 采用的是：

- 内存中使用 `HashMap`
- 落盘时构造成一份分层 `MetadataDocument`
- 再整体写入单个 JSON 文件

对应结构是：

- 顶层 `version`
- 顶层真实 metadata 文件路径 key
- 路径 key 下按 `tenant -> namespace -> persistent -> topic -> subscriptions` 分层嵌套
- 顶层独立 `partitioned_topics`

其中当前这版相比前一轮又多了两个收敛：

- namespace 下直接用 `persistent` / `non-persistent` 作为 key，不再额外包一层 `domains`
- partitioned topic 在资源树里只保留具体 `...-partition-N` 资源，不再重复写逻辑 topic 空节点

这个方案的优点是：

- 实现简单
- 单机本地调试方便
- JSON 结果直观
- 测试验证容易

但它本质上是一种：

- **单文件分层快照式资源持久化**

### 7.2 为什么这个方案和原生 Pulsar 不一样

原生 Pulsar 并不是把 tenant、namespace、topic、subscription 全部收进一份总快照里统一落盘。  
从源码看，原生 Pulsar 更接近：

- tenant 独立存储
- namespace 独立存储
- partitioned topic metadata 独立存储
- persistent topic existence 走类似 `/managed-ledgers/{tenant}/{namespace}/persistent/{topic}` 的资源路径
- subscription 的持久化状态主要绑定在 managed ledger / cursor 体系

也就是说，原生 Pulsar 是：

- **按资源路径拆分存储**

而不是：

- **按整份 JSON 快照统一存储**

### 7.3 当前方案和原生 Pulsar 更接近的地方

当前这版比旧的扁平 `MetadataSnapshot` 更接近原生 Pulsar 的地方有两点：

- 资源树按 `tenant -> namespace -> persistent -> topic` 组织，语义上更接近 ZooKeeper / metadata store 路径
- `partitioned_topics` 被单独拆出来，和原生 Pulsar 中“逻辑 topic 分区结构 metadata 独立存储”的思路更接近
- 具体 partition topic 资源和逻辑 partitioned topic 结构不再混写，避免 `my-topic` 和 `my-topic-partition-N` 同时作为同层真相来源

也就是说，现在已经不再是单纯的扁平数组快照，而是：

- **资源树**
- **逻辑 partitioned topic 元数据**

两块分开表达。

### 7.4 当前方案仍然和原生 Pulsar 不一样的地方

当前分层 JSON 仍然和原生 Pulsar 有明显差异：

- 仍然是**单文件 JSON**
- 原生 Pulsar 是**每类资源独立挂到 metadata store 路径**
- 顶层保留了“真实 metadata 文件路径”这个 key，这是当前实现特有的本地文件语义，原生 Pulsar 不会这样组织
- subscription 这里只是名字挂在 topic 下，不是原生 Pulsar 那种和 cursor / backlog / managed ledger 绑定的完整状态模型
- 旧版本已经落盘的冗余节点，需要通过重新生成 metadata 文件或后续迁移逻辑进一步收敛；当前实现已经保证新写入结构不再继续引入这类重复节点

这意味着当前实现虽然有了路径层级感，但仍然不是完整 metadata store。

### 7.5 当前方案的取舍

这在当前轻量实现里是可接受的，因为它换来了：

- 启动恢复简单
- JSON 易读
- 单文件便于调试
- 测试验证集中

### 7.6 这部分后续更接近原生 Pulsar 的改进方向

后续如果要继续向原生 Pulsar 靠拢，这部分可以按下面的方向演进：

1. 将当前单文件 `MetadataDocument` 继续拆分成按资源层级管理的 metadata store 抽象
2. tenant / namespace / partitioned topic metadata 改成分资源单独读写，而不是整份 JSON 统一重写
3. topic existence 与 partitioned topic metadata 继续解耦，并按原生路径组织
4. subscription 从“名字登记”逐步过渡到和 cursor / mark-delete / backlog 绑定的状态模型
5. 在此基础上再补 create / update / delete 的资源管理链和 admin 查询能力

也就是说，当前方案的定位应该明确为：

- **轻量、可恢复、单机友好的第一版资源层 metadata**

而不是已经完全等价于原生 Pulsar 的资源存储模型。

---

## 8. 当前结论

如果按资源层级看，这次 `pulsar-lite` 已经从“只有运行时内存结构”推进到了：

- tenant 资源名可持久化
- namespace 资源名可持久化
- topic 资源骨架可持久化
- partitioned topic 的分区结构可持久化并在重启后恢复
- subscription name 可持久化

也就是说，`pulsar-lite` 现在已经具备了：

- **轻量资源层 metadata**

但和原生 Pulsar 相比，仍然只是一个最小骨架版本。  
原生 Pulsar 的完整能力还包括：

- 各层 policy
- schema
- topic / subscription 更完整管理面
- 持久化运行时状态

这些仍是后续需要继续补的部分。
