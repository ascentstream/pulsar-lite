//! Low-level metadata store: key, document, model, memory/file backend.

mod model;
mod key;
mod store;
mod memory;
mod file;

pub use file::FileMetadataStore;
pub use key::{namespace_key, subscription_key, MetadataKey};
pub use memory::InMemoryMetadataStore;
pub use model::{
    DomainNode, MetadataDocument, MetadataFileNode, NamespaceMetadata, NamespaceNode,
    PartitionedTopicNode, ParsedTopicName, SubscriptionMetadata, SubscriptionNode, TenantMetadata,
    TenantNode, TopicMetadata, TopicNode,
};
pub use model::{logical_topic_name, parse_topic_name};