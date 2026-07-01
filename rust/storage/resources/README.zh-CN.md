# pulsar-lite-storage-resources

[English](README.md) | 简体中文

`pulsar-lite-storage-resources` 是 Pulsar metadata 之上的 broker 资源层。
它负责把 Pulsar 的领域操作，例如确保 tenant、namespace、topic、subscription 存在，
转换成对 metadata store 的状态修改。

## 和 Metadata 的关系

`metadata` crate 负责底层数据模型和持久化边界：

- `MetadataState` 在内存中保存 tenant、namespace、topic、subscription。
- `MetadataStore` 暴露状态访问能力，以及 `load` / `persist` 这类持久化操作。
- `FileMetadataStore` 和 `InMemoryMetadataStore` 提供具体后端实现。

`resources` crate 位于 metadata 之上。它不定义新的存储格式，也不直接拥有持久化逻辑。
它的职责是把 Pulsar 的资源语义应用到任意实现了 `MetadataStore` 的后端上。

简单来说：

```text
metadata = 数据模型 + 内存状态 + 持久化后端
resources = 基于 metadata 的领域操作
```

## Resource 分工

- `TenantResources` 负责确保和查询 tenant metadata。
- `NamespaceResources` 负责确保和查询 namespace metadata，同时补齐它的 tenant 父资源。
- `TopicResources` 负责确保 topic、partitioned topic metadata 和 subscription。
- `PulsarResources` 把各类 resource accessor 和一个 metadata store 实例组合起来。

每个 public resource 操作都会先修改内存中的 metadata 状态。如果状态确实发生变化，
最后最多只调用一次持久化。

## 示例

```rust
use pulsar_lite_storage_metadata::InMemoryMetadataStore;
use pulsar_lite_storage_resources::PulsarResources;

let store = InMemoryMetadataStore::new();
let mut resources = PulsarResources::from_metadata_store(store);

resources
    .ensure_topic("persistent://public/default/topic", false, 0, 2)
    .unwrap();
```
