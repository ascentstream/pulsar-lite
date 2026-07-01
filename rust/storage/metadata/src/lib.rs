//! Low-level metadata store: key, document, model, memory/file backend.

mod file;
mod key;
mod memory;
mod model;
mod store;

pub use file::FileMetadataStore;
pub use key::{namespace_key, subscription_key, MetadataKey};
pub use memory::InMemoryMetadataStore;
pub use store::{MetadataState, MetadataStore};
pub use model::{logical_topic_name, parse_topic_name};
pub use model::{
    DomainNode, MetadataDocument, MetadataFileNode, NamespaceMetadata, NamespaceNode,
    ParsedTopicName, PartitionedTopicNode, SubscriptionMetadata, SubscriptionNode, TenantMetadata,
    TenantNode, TopicMetadata, TopicNode,
};