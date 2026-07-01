# pulsar-lite-storage-resources

English | [简体中文](README.zh-CN.md)

`pulsar-lite-storage-resources` is the broker-side resource layer on top of
Pulsar metadata. It translates Pulsar domain operations, such as ensuring
tenants, namespaces, topics, and subscriptions, into changes on a metadata
store.

## Relationship with Metadata

The `metadata` crate owns the low-level data model and persistence boundary:

- `MetadataState` stores tenants, namespaces, topics, and subscriptions in memory.
- `MetadataStore` exposes access to that state plus `load` / `persist` operations.
- `FileMetadataStore` and `InMemoryMetadataStore` provide concrete backends.

The `resources` crate sits above that layer. It does not define a new storage
format and does not own persistence directly. Instead, it applies Pulsar
resource semantics to any backend that implements `MetadataStore`.

In short:

```text
metadata = data model + in-memory state + persistence backend
resources = domain operations over metadata
```

## Resource Responsibilities

- `TenantResources` ensures and queries tenant metadata.
- `NamespaceResources` ensures and queries namespace metadata, including its tenant parent.
- `TopicResources` ensures topics, partitioned topic metadata, and subscriptions.
- `PulsarResources` groups the resource accessors with one metadata store instance.

Each public resource operation mutates in-memory metadata first. If anything
changed, it persists the metadata at most once at the end of the operation.

## Example

```rust
use pulsar_lite_storage_metadata::InMemoryMetadataStore;
use pulsar_lite_storage_resources::PulsarResources;

let store = InMemoryMetadataStore::new();
let mut resources = PulsarResources::from_metadata_store(store);

resources
    .ensure_topic("persistent://public/default/topic", false, 0, 2)
    .unwrap();
```
