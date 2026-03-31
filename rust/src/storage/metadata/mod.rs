mod traits;
mod json;
mod store;
mod types;

pub use traits::MetadataBackend;
pub use json::JsonFileMetadataStore;
pub use store::MetadataStore;
pub(crate) use types::parse_topic_name;
pub use types::{
    DomainNode, MetadataDocument, MetadataFileNode, NamespaceMetadata, NamespaceNode,
    ParsedTopicName, PartitionedTopicNode, SubscriptionMetadata, SubscriptionNode, TenantMetadata,
    TenantNode, TopicMetadata, TopicNode,
};

impl super::Storage {
    pub fn parse_topic_name(topic: &str) -> anyhow::Result<ParsedTopicName> {
        crate::storage::metadata::types::parse_topic_name(topic)
    }

    #[cfg(test)]
    pub(crate) fn metadata_path(&self) -> &std::path::Path {
        self.resources.metadata_path()
    }

    #[cfg(test)]
    pub(crate) fn build_metadata_document(&self) -> MetadataDocument {
        self.resources.build_metadata_document(Self::METADATA_VERSION)
    }
}
